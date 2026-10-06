"""Per-table windowed fetch + native-type verification for keboola.ex-retain-cloud.

Implements Retain's real `paging/paged` contract (verified live, 2026-10):

1. `create_page_result` POSTs an explicit `fields` list and gets back a `key` (a handle to a
   server-side, cached result set) plus the table's total `rowCount`.
2. `fetch_table` reads that result set in fixed-size windows via concurrent
   `GET ...?id={key}&from={offset}&count={n}` calls, and streams every window straight into a
   `/tmp` scratch file — never directly into `/data/out/tables/`, so a mid-fetch failure never
   leaves a partial file for Storage to upload.
3. Memory is bounded by the pipeline depth, not the table size. The component runs under a 256 MB
   container limit (Developer Portal `requiredMemory` unset → platform default); job 1334279502 was
   OOM-killed on a 3.48M-row table because the primary-key uniqueness check kept every GUID in a
   Python set (~430 MB for that table) and a batch of 12 fetched windows stayed referenced until the
   batch ended. Now at most `_WINDOW_PIPELINE_DEPTH` windows exist at once, each is released as
   soon as it is written, and PK uniqueness is tracked in an on-disk SQLite store (`PkTracker`).

This replaces the earlier single-call model (one `paging/paged` POST sized to the whole table),
which could not page the large report tables at all: a request for a whole big table deterministically
`504`s at the gateway's ~20s limit. Windowing removes that ceiling — the big report tables are now
fully extractable.

V1 only ever implements `full_fetch` — there is no `fetch_mode` field or constant anywhere in this
component, unlike `load_type`, which is a real row-level field (see `configuration.py`).
"""

import csv
import hashlib
import itertools
import json
import logging
import re
import sqlite3
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException

from client import RetainCloudClient

logger = logging.getLogger(__name__)

_DATETIME_TYPE = "DateTime"
_VERIFY_TYPES = {"Bool", "Int", "Float"}  # verified against real streamed rows before going native
_NATIVE_TYPE_FOR: dict[str, SupportedDataTypes] = {
    "Bool": SupportedDataTypes.BOOLEAN,
    "Int": SupportedDataTypes.INTEGER,
    "Float": SupportedDataTypes.FLOAT,
}
# Windowed-paging tuning. 3000 rows/window and 6 concurrent windows are the values proven against
# the live API on the three large report tables (1.2M–3.5M rows): small enough that a single window
# GET returns well inside the ~20s gateway limit, parallel enough to finish a multi-million-row
# table in minutes rather than hours. A short window (fewer rows than asked for) is retried a few
# times before the table is failed.
_WINDOW_CHUNK = 3000
_WINDOW_WORKERS = 6
_WINDOW_RETRIES = 6
# Windows alive at once — running in the pool, or fetched and waiting for the main thread to write
# them. The workers' own, plus a small margin so the pool never idles while a window is being
# written. Each consumed window is released immediately, so this (times the window size and the
# table's width) is what bounds the extractor's memory — the table's row count does not.
_WINDOW_PIPELINE_DEPTH = _WINDOW_WORKERS + 2
# Progress is logged at INFO after every 10% of the table and at least every 60s, so a
# multi-million-row table that runs for many minutes visibly advances in the job log.
_PROGRESS_STEP_PCT = 10
_PROGRESS_MIN_INTERVAL_SECONDS = 60.0

# Storage rejects (fails the WHOLE table) any column name over 64 characters. Retain's flat
# `<table>_<field>` naming — foreign keys are `<table>_<ref>_guid` — routinely exceeds that, e.g.
# `rolerequestresourcerejectreason_rolerequestpredefinedrejectreason_guid` (70 chars). This only
# ever renames the OUTPUT (manifest/Storage) column name — see `safe_column_name`'s docstring.
_MAX_STORAGE_COLUMN_NAME_LENGTH = 64
_SHORTENED_NAME_HEAD_LENGTH = 55
_SHORTENED_NAME_HASH_LENGTH = 8


def safe_column_name(name: str) -> str:
    """Shorten `name` to Storage's 64-char column-name cap, deterministically and collision-safely.

    A name at or under the cap is returned UNCHANGED — source-faithful naming is preserved for the
    overwhelming majority of columns. Only a name OVER the cap is shortened, to a fixed 55-char
    head of the original plus an underscore plus an 8-hex-char SHA-256 prefix of the FULL original
    name: `orig[:55] + "_" + sha256(orig).hexdigest()[:8]`, which is always exactly 64 characters.

    The hash (over the full name, not just the truncated head) is what makes this collision-safe:
    two long names that happen to share the same first 55 characters (a real risk with this API's
    `<table>_<field>` naming) still diverge after that shared head, so their hashes — and therefore
    their shortened names — differ. A plain truncation alone would silently merge two distinct
    columns into one in Storage.

    Deterministic and stable across runs (same input always yields the same output), so a
    shortened name never drifts between runs of the same table/column — required for both the
    manifest and an incremental load's primary-key reference to stay consistent run over run.

    This is applied ONLY to the manifest's column names (and the primary-key reference) — never to
    the CSV data itself, which is headerless and positional (columns are matched to the manifest by
    ORDER, not by name), so renaming a column here never touches a single data byte.
    """
    if len(name) <= _MAX_STORAGE_COLUMN_NAME_LENGTH:
        return name
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:_SHORTENED_NAME_HASH_LENGTH]
    shortened = f"{name[:_SHORTENED_NAME_HEAD_LENGTH]}_{digest}"
    logger.debug(
        "Column name %r (%d chars) exceeds Storage's %d-char limit; shortened to %r (original name "
        "preserved in the column's Storage metadata/description).",
        name,
        len(name),
        _MAX_STORAGE_COLUMN_NAME_LENGTH,
        shortened,
    )
    return shortened


@dataclass
class ColumnSchema:
    name: str
    declared_type: str
    base_type: SupportedDataTypes
    # True for a Retain `CalculatedField`. Such a column must NOT be listed in the `paging/paged`
    # create body (the API rejects it with a 400), so it is excluded from the fetch — but it stays
    # in the output manifest (emitted empty), matching the released component, whose single-call
    # projection never returned calculated fields either.
    is_calculated: bool = False


@dataclass
class TableSchema_:
    """Retain Cloud table schema, distinct from `keboola.component.table_schema.TableSchema`
    (component.py converts one into the other when building the output manifest)."""

    table: str
    columns: list[ColumnSchema]
    pk_column: str | None


@dataclass
class FetchResult:
    scratch_path: Path
    row_count: int
    pk_unique: bool
    verified_columns: dict[str, bool] = field(default_factory=dict)


def build_table_schema(table: str, rich_fields: list[dict]) -> TableSchema_:
    """Turn a `richfieldstructure` response into column order + manifest typing + PK detection.

    Only `DateTime` is trusted as native without per-row verification (native-data-types.md's own
    safe-default table). `Bool`/`Int`/`Float` start as native *candidates* — `fetch_table` may
    downgrade them to STRING after checking real streamed values. `ID`/`String`/`Unknown` are
    always STRING (an ID is definitionally a string, never at risk of the "numeric but really a
    status code" failure mode).
    """
    # The table's own identity column is `<table>_guid`. We target that exact name (not just any
    # `*_guid` column) because the flat model prefixes every column with the table name and foreign
    # keys are ALSO guids — e.g. `booking` carries `booking_guid` (its PK) plus `booking_resource_guid`
    # / `booking_job_guid` (FKs); matching any `_guid` column would pick an FK. The API is inconsistent
    # about casing (`SkillType_guid`, `SkillLevel_Guid`), so compare case-insensitively and keep the
    # column's ACTUAL name as the PK — row dicts are keyed by the real casing, so a lowercased guess
    # would break `row.get(pk_column)` at fetch time.
    pk_candidate_lower = f"{table}_guid".lower()
    columns: list[ColumnSchema] = []
    pk_column: str | None = None
    for field_def in rich_fields:
        # `name` is a response-schema assumption, not guaranteed. Tenant-configurable tables
        # (e.g. the `*CustomLookup` tables) can transiently return a field with no `name` while a
        # custom field is being created/renamed. Without this guard a `KeyError` escapes `run()`'s
        # `RequestException`/`ijson.JSONError` handlers to `__main__`'s bare `except` → opaque exit 2
        # ("Internal Server Error"); surface it as a clear, actionable UserException instead.
        try:
            name = field_def["name"]
        except KeyError as e:
            raise UserException(
                f"Retain Cloud returned a field with no 'name' in the schema for table '{table}'. "
                "This can happen transiently while a custom field is being created or renamed — "
                "re-run the extraction; if it persists, check the table's custom-field configuration "
                "in Retain Cloud."
            ) from e
        declared = field_def.get("dataType", "Unknown")
        is_calculated = field_def.get("uiFieldCategory") == "CalculatedField"
        if name.lower() == pk_candidate_lower:
            pk_column = name
        if declared == _DATETIME_TYPE:
            base_type = SupportedDataTypes.TIMESTAMP
        elif declared in _VERIFY_TYPES:
            base_type = _NATIVE_TYPE_FOR[declared]
        else:
            base_type = SupportedDataTypes.STRING
        columns.append(
            ColumnSchema(name=name, declared_type=declared, base_type=base_type, is_calculated=is_calculated)
        )
    calculated = sum(1 for c in columns if c.is_calculated)
    logger.debug(
        "Table %s: built schema with %d columns (%d calculated, excluded from fetch); detected primary key column %r.",
        table,
        len(columns),
        calculated,
        pk_column,
    )
    return TableSchema_(table=table, columns=columns, pk_column=pk_column)


_BOOL_TOKENS = {True, False, "true", "false", "True", "False", "1", "0"}
# Matched against `_stringify(value)` — i.e. the exact text that will land in the CSV cell — not
# against `value` itself, so verification guarantees the *emitted representation* is a valid
# literal for the native type Storage will declare, not merely that Python can cast it.
_INT_LITERAL_RE = re.compile(r"^-?\d+$")
_FLOAT_LITERAL_RE = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")


def _coerces(declared_type: str, value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        # `int(True) == 1` and `float(True) == 1.0` both succeed, but `_stringify` writes the
        # literal text "True"/"False" for a bool — not a valid INTEGER/FLOAT CSV literal. A bool
        # value only ever legitimately coerces into a Bool column.
        return declared_type == "Bool" and value in _BOOL_TOKENS
    if declared_type == "Int":
        return bool(_INT_LITERAL_RE.match(_stringify(value)))
    if declared_type == "Float":
        return bool(_FLOAT_LITERAL_RE.match(_stringify(value)))
    if declared_type == "Bool":
        return value in _BOOL_TOKENS
    return True


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


class PkTracker:
    """Tracks primary-key uniqueness in an on-disk SQLite table instead of an in-process set.

    A Python set of the PK values grows with the row count: ~110 MB per million GUIDs (85 B per
    36-char str plus the hash table), so the 3.48M-row report tables need ~430 MB for the set alone
    and the container is OOM-killed at its 256 MB limit — that is job 1334279502. SQLite keeps a
    fixed page cache; the values live in a scratch file (deleted on `close()`) and the final
    `COUNT(DISTINCT ...)` is an external sort that spills to disk, so process memory stays flat
    whatever the table size.

    Values are compared as their CSV text (`_stringify`), because the CSV cell is what Storage
    dedupes on. Main-thread only, like `_RowConsumer` that owns it.
    """

    _FLUSH_EVERY = 5000

    def __init__(self, path: Path):
        self._path = path
        path.unlink(missing_ok=True)
        self._conn = sqlite3.connect(path)
        # Durability is irrelevant for a scratch file that is deleted at the end of the run, and
        # FILE temp storage keeps the final DISTINCT sort out of process memory.
        self._conn.execute("PRAGMA journal_mode=OFF")
        self._conn.execute("PRAGMA synchronous=OFF")
        self._conn.execute("PRAGMA temp_store=FILE")
        self._conn.execute("CREATE TABLE pk (value TEXT NOT NULL)")
        self._pending: list[tuple[str]] = []
        self.has_null = False
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()

    def add(self, value: Any) -> None:
        if value is None:
            self.has_null = True
            return
        self._pending.append((_stringify(value),))
        if len(self._pending) >= self._FLUSH_EVERY:
            self.flush()

    def flush(self) -> None:
        if self._pending:
            self._conn.executemany("INSERT INTO pk (value) VALUES (?)", self._pending)
            self._pending.clear()

    def is_unique(self, row_count: int) -> bool:
        """True when every one of `row_count` rows carried a distinct, non-null key.

        A null/missing PK value must never be treated as "the one unique value" — Storage would
        then declare a nullable column as the primary key, which upserts can't handle safely.
        """
        if self.has_null:
            return False
        self.flush()
        (distinct,) = self._conn.execute("SELECT COUNT(DISTINCT value) FROM pk").fetchone()
        return distinct == row_count

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._path.unlink(missing_ok=True)
            self._closed = True


class _Progress:
    """INFO-level progress for a long fetch: a line after every `_PROGRESS_STEP_PCT` of the table,
    and at least one every `_PROGRESS_MIN_INTERVAL_SECONDS` so a slow table still shows it is
    moving. The final count is reported by `fetch_table`'s finish line, not here."""

    def __init__(self, table: str, total: int):
        self._table = table
        self._total = total
        self._step = max(1, total * _PROGRESS_STEP_PCT // 100)
        self._next = self._step
        self._last_logged = time.monotonic()

    def update(self, done: int) -> None:
        if done >= self._total:
            return
        now = time.monotonic()
        if done < self._next and now - self._last_logged < _PROGRESS_MIN_INTERVAL_SECONDS:
            return
        logger.info("Table %s: %d / %d rows fetched (%d%%).", self._table, done, self._total, done * 100 // self._total)
        while self._next <= done:
            self._next += self._step
        self._last_logged = now


def _format_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m {secs:02d}s" if minutes else f"{secs}s"


class _RowConsumer:
    """Writes fetched rows into the scratch CSV while tracking native-type and PK state.

    Fed from the main thread only (window GETs run in worker threads, but their results are consumed
    in offset order on the main thread), so the CSV writer, the counters and the `PkTracker` need no
    locking. Carries the exact per-row logic the old streaming path had: native-type downgrade on a
    non-coercing value, primary-key null detection and uniqueness, and a one-shot schema-drift
    warning. Retains nothing per row: the PK values go to the on-disk `PkTracker`.
    """

    def __init__(self, schema: TableSchema_, csv_path: Path, pk_store_path: Path):
        self._schema = schema
        self._csv_path = csv_path
        self._fieldnames = [c.name for c in schema.columns]
        self._declared_by_name = {c.name: c.declared_type for c in schema.columns}
        self.verified = {c.name: True for c in schema.columns if c.declared_type in _VERIFY_TYPES}
        self.row_count = 0
        self._schema_drift_logged = False
        self._pk = PkTracker(pk_store_path) if schema.pk_column else None
        # Held open across many windows; use `_RowConsumer` as a context manager (or call close())
        # so the handle and the PK store are released even if a window fetch raises before finish().
        self._file = open(csv_path, "w", encoding="utf-8", newline="")  # noqa: SIM115
        self._writer = csv.writer(self._file)
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._file.close()
            if self._pk is not None:
                self._pk.close()
            self._closed = True

    def consume(self, rows: list[dict]) -> None:
        for row in rows:
            self.row_count += 1
            extra_keys = set(row) - set(self._fieldnames)
            if extra_keys and not self._schema_drift_logged:
                logger.warning(
                    "Table %s: row carries fields outside the discovered schema: %s",
                    self._schema.table,
                    sorted(extra_keys),
                )
                self._schema_drift_logged = True
            for name, still_verified in self.verified.items():
                if still_verified and not _coerces(self._declared_by_name[name], row.get(name)):
                    self.verified[name] = False
                    logger.warning(
                        "Table %s column %s: value did not match declared type %s, downgrading to STRING",
                        self._schema.table,
                        name,
                        self._declared_by_name[name],
                    )
            self._writer.writerow([_stringify(row.get(name)) for name in self._fieldnames])
            if self._pk is not None:
                self._pk.add(row.get(self._schema.pk_column))

    def finish(self) -> FetchResult:
        # The verdict must be read BEFORE close(): closing deletes the PK store's file.
        pk_unique = self._pk is not None and self._pk.is_unique(self.row_count)
        self.close()
        return FetchResult(
            scratch_path=self._csv_path,
            row_count=self.row_count,
            pk_unique=pk_unique,
            verified_columns=self.verified,
        )


def _fetch_window(client: RetainCloudClient, table: str, key: str, start: int, want: int) -> list[dict]:
    """Fetch exactly `want` rows starting at `start`, retrying a short read a few times.

    A window GET occasionally returns fewer rows than asked for (transient server behaviour). We
    retry with a slightly larger `count` and a short backoff; if it is still short after
    `_WINDOW_RETRIES`, the table fails loud rather than ship a gap.
    """
    count = want
    last = None
    for attempt in range(_WINDOW_RETRIES):
        rows = client.fetch_page_window(table, key, start, count)
        if len(rows) >= want:
            return rows[:want]
        last = f"{len(rows)}/{want}"
        count = want + 1 + attempt
        time.sleep(1 + attempt)
    raise UserException(
        f"Failed to fetch table '{table}': window from={start} returned too few rows ({last}) "
        f"after {_WINDOW_RETRIES} attempts."
    )


def _fetch_windows(client: RetainCloudClient, table: str, key: str, total: int, consumer: _RowConsumer) -> None:
    """Pull every window of the paged result through a bounded pipeline, in offset order.

    At most `_WINDOW_PIPELINE_DEPTH` windows exist at once — running in the pool, or fetched and
    waiting for the main thread — and each is dropped as soon as its rows are written. The previous
    batch-of-12 pattern kept every window of a batch referenced (via its `Future`) until the whole
    batch had been consumed, AND drained the pool between batches; this keeps the workers busy
    continuously and makes peak memory a function of the pipeline depth, not of the table size.
    """
    progress = _Progress(table, total)
    offsets = iter(range(0, total, _WINDOW_CHUNK))
    with ThreadPoolExecutor(max_workers=_WINDOW_WORKERS) as pool:

        def submit(start: int) -> Future:
            return pool.submit(_fetch_window, client, table, key, start, min(_WINDOW_CHUNK, total - start))

        pending = deque(submit(start) for start in itertools.islice(offsets, _WINDOW_PIPELINE_DEPTH))
        while pending:
            rows = pending.popleft().result()
            consumer.consume(rows)
            del rows  # release this window before blocking on the next one
            progress.update(consumer.row_count)
            start = next(offsets, None)
            if start is not None:
                pending.append(submit(start))


def fetch_table(client: RetainCloudClient, table: str, schema: TableSchema_, scratch_dir: Path) -> FetchResult:
    """Fetch a whole table into a `/tmp` scratch file using Retain's windowed paging contract.

    Creates a server-side paged result set (`create_page_result`), then pulls it in fixed-size
    windows (`_WINDOW_CHUNK`) with `_WINDOW_WORKERS` concurrent GETs through a bounded pipeline
    (`_fetch_windows`), consumed in offset order so the output row order stays deterministic.

    Fails loud (`UserException`) if the number of rows extracted does not match the `rowCount` the
    create call reported — a mismatch means a window was lost or the result set changed under us, and
    shipping a silent partial is exactly what this component must not do. HTTP failures from any call
    propagate as `requests.exceptions.RequestException` for `component.py` to describe.
    """
    scratch_dir.mkdir(parents=True, exist_ok=True)
    csv_path = scratch_dir / f"{table}.csv"
    pk_store_path = scratch_dir / f"{table}.pk.sqlite"

    # Calculated fields are excluded: the create call returns 400 if one is listed. They stay in the
    # output manifest (emitted empty by _RowConsumer, which writes every schema column) — matching the
    # released component, whose projection never returned calculated fields either.
    field_names = [c.name for c in schema.columns if not c.is_calculated]
    if not field_names:
        raise UserException(f"Failed to fetch table '{table}': it has no non-calculated columns to request.")
    key, row_count_total = client.create_page_result(table, field_names)
    window_count = -(-row_count_total // _WINDOW_CHUNK)
    logger.info(
        "Table %s: fetching %d rows in %d windows of up to %d rows with %d workers (%d of %d columns requested).",
        table,
        row_count_total,
        window_count,
        _WINDOW_CHUNK,
        _WINDOW_WORKERS,
        len(field_names),
        len(schema.columns),
    )
    started = time.monotonic()

    # The context manager guarantees the scratch file and the PK store are closed even if a window
    # fetch raises before finish() (e.g. a persistently short window) — the job then fails without
    # leaking them, and the partial file is never moved to out/tables/ (component.py only moves on
    # success).
    with _RowConsumer(schema, csv_path, pk_store_path) as consumer:
        if row_count_total > 0:
            _fetch_windows(client, table, key, row_count_total, consumer)
        result = consumer.finish()

    if result.row_count != row_count_total:
        raise UserException(
            f"Failed to fetch table '{table}': extracted {result.row_count} rows but the API reported "
            f"{row_count_total}. A window was lost or the paged result changed mid-fetch — re-run the extraction."
        )
    logger.info(
        "Table %s: fetched %d rows in %s (primary key verified unique: %s).",
        table,
        result.row_count,
        _format_duration(time.monotonic() - started),
        result.pk_unique,
    )
    return result
