"""Per-table streaming fetch + native-type verification for keboola.ex-retain-cloud.

Implements the resolved `paging/paged` contract (spec §6): `pageSize` is a single-call row cap, not
a page window. Every response streams into a `/tmp` scratch file — never directly into
`/data/out/tables/` — so a failed second call never leaves a partial/truncated file for Storage to
upload (see the spec's "Corrected staging rule").

V1 only ever implements `full_fetch` (spec §2's sanctioned Fetch-Mode omission) — there is no
`fetch_mode` field or constant anywhere in this component, unlike `load_type`, which is a real
row-level field (see `configuration.py`).
"""

import csv
import json
import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import ijson
import requests
from keboola.component.dao import SupportedDataTypes

from client import RetainCloudClient

logger = logging.getLogger(__name__)

_DATETIME_TYPE = "DateTime"
_VERIFY_TYPES = {"Bool", "Int", "Float"}  # verified against real streamed rows before going native
_NATIVE_TYPE_FOR: dict[str, SupportedDataTypes] = {
    "Bool": SupportedDataTypes.BOOLEAN,
    "Int": SupportedDataTypes.INTEGER,
    "Float": SupportedDataTypes.FLOAT,
}
_MIN_SECOND_CALL_MARGIN = 1000
_SECOND_CALL_MARGIN_RATIO = 0.02


@dataclass
class ColumnSchema:
    name: str
    declared_type: str
    base_type: SupportedDataTypes


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
    pk_candidate = f"{table}_guid"
    columns: list[ColumnSchema] = []
    has_pk_column = False
    for field_def in rich_fields:
        name = field_def["name"]
        declared = field_def.get("dataType", "Unknown")
        if name == pk_candidate:
            has_pk_column = True
        if declared == _DATETIME_TYPE:
            base_type = SupportedDataTypes.TIMESTAMP
        elif declared in _VERIFY_TYPES:
            base_type = _NATIVE_TYPE_FOR[declared]
        else:
            base_type = SupportedDataTypes.STRING
        columns.append(ColumnSchema(name=name, declared_type=declared, base_type=base_type))
    return TableSchema_(table=table, columns=columns, pk_column=pk_candidate if has_pk_column else None)


_BOOL_TOKENS = {True, False, "true", "false", "True", "False", "1", "0"}


def _coerces(declared_type: str, value: Any) -> bool:
    if value is None:
        return True
    if declared_type == "Int":
        try:
            int(value)
            return True
        except (TypeError, ValueError):  # fmt: skip
            return False
    if declared_type == "Float":
        try:
            float(value)
            return True
        except (TypeError, ValueError):  # fmt: skip
            return False
    if declared_type == "Bool":
        return value in _BOOL_TOKENS
    return True


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _iter_envelope(response: requests.Response):
    """Yield ("meta", row_count:int) once, then ("row", dict) for every element of `data`.

    Uses `ijson.parse` + `ijson.ObjectBuilder` rather than `ijson.items`, because we need BOTH the
    scalar `rowCount` sibling key and the streamed `data` array elements from the same single-pass
    response body — `ijson.items(f, "data.item")` alone only yields the matched array elements and
    silently discards sibling scalars.
    """
    builder: ijson.ObjectBuilder | None = None
    depth = 0
    for prefix, event, value in ijson.parse(response.raw):
        if prefix == "rowCount" and event == "number":
            yield "meta", int(value)
            continue
        if prefix == "data.item" and event == "start_map":
            builder = ijson.ObjectBuilder()
            depth = 0
        if builder is not None:
            builder.event(event, value)
            if event == "start_map":
                depth += 1
            elif event == "end_map":
                depth -= 1
                if depth == 0 and prefix == "data.item":
                    yield "row", builder.value
                    builder = None


def _stream_to_csv(response: requests.Response, schema: TableSchema_, csv_path: Path) -> tuple[FetchResult, int]:
    fieldnames = [c.name for c in schema.columns]
    declared_by_name = {c.name: c.declared_type for c in schema.columns}
    verified = {c.name: True for c in schema.columns if c.declared_type in _VERIFY_TYPES}
    pk_values: set = set()
    row_count = 0
    row_count_total = 0
    schema_drift_logged = False

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for kind, payload in _iter_envelope(response):
            if kind == "meta":
                row_count_total = payload
                continue
            row = payload
            row_count += 1
            extra_keys = set(row) - set(fieldnames)
            if extra_keys and not schema_drift_logged:
                logger.warning(
                    "Table %s: row carries fields outside the discovered schema: %s",
                    schema.table,
                    sorted(extra_keys),
                )
                schema_drift_logged = True
            for name, still_verified in verified.items():
                if still_verified and not _coerces(declared_by_name[name], row.get(name)):
                    verified[name] = False
                    logger.warning(
                        "Table %s column %s: value did not match declared type %s, downgrading to STRING",
                        schema.table,
                        name,
                        declared_by_name[name],
                    )
            writer.writerow([_stringify(row.get(name)) for name in fieldnames])
            if schema.pk_column:
                pk_values.add(row.get(schema.pk_column))

    pk_unique = schema.pk_column is not None and len(pk_values) == row_count
    return (
        FetchResult(scratch_path=csv_path, row_count=row_count, pk_unique=pk_unique, verified_columns=verified),
        row_count_total,
    )


def fetch_table(
    client: RetainCloudClient, table: str, schema: TableSchema_, page_size: int, scratch_dir: Path
) -> FetchResult:
    """Fetch a table to completion into a `/tmp` scratch file (spec §6 algorithm).

    Raises `requests.HTTPError` on any HTTP failure and `ijson.JSONError` on a malformed response —
    both propagate to the caller (`component.py`), which turns this into a `UserException` for this
    row's job rather than a per-table "log and continue" (there is no other table in this row).
    """
    scratch_dir.mkdir(parents=True, exist_ok=True)
    csv_path = scratch_dir / f"{table}.csv"

    response = client.fetch_table_page(table, page_size)
    result, row_count_total = _stream_to_csv(response, schema, csv_path)

    if result.row_count >= row_count_total:
        return result

    margin = max(_MIN_SECOND_CALL_MARGIN, math.ceil(row_count_total * _SECOND_CALL_MARGIN_RATIO))
    second_page_size = row_count_total + margin
    response = client.fetch_table_page(table, second_page_size)
    result, row_count_total_2 = _stream_to_csv(response, schema, csv_path)

    if result.row_count < row_count_total_2:
        logger.warning(
            "Table %s: second paging/paged call still short of its own rowCount (%s < %s) — "
            "treating this run as best-effort complete; the next run will pick up any tail rows.",
            table,
            result.row_count,
            row_count_total_2,
        )
    return result
