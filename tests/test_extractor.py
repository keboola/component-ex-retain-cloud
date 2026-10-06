import csv
import io
import tracemalloc
import unittest
from pathlib import Path
from unittest import mock

from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException

import extractor
from client import RetainCloudClient
from extractor import PkTracker, build_table_schema, fetch_table, safe_column_name

RICH_FIELDS_BOOKING = [
    {"name": "booking_guid", "dataType": "ID"},
    {"name": "booking_hours", "dataType": "Int"},
    {"name": "booking_rate", "dataType": "Float"},
    {"name": "booking_active", "dataType": "Bool"},
    {"name": "booking_createdon", "dataType": "DateTime"},
    {"name": "booking_notes", "dataType": "String"},
    {"name": "booking_meta", "dataType": "Unknown"},
]

RICH_FIELDS_WITH_CALC = [
    {"name": "widget_guid", "dataType": "ID", "uiFieldCategory": "DatabaseField"},
    {"name": "widget_name", "dataType": "String", "uiFieldCategory": "DatabaseField"},
    {"name": "widget_total", "dataType": "Float", "uiFieldCategory": "CalculatedField"},
]


def _booking_row(guid, hours=8, rate=1.5, active=True, created="2026-01-01T00:00:00Z", notes="x", meta=None):
    return {
        "booking_guid": guid,
        "booking_hours": hours,
        "booking_rate": rate,
        "booking_active": active,
        "booking_createdon": created,
        "booking_notes": notes,
        "booking_meta": meta,
    }


class _FakeClient(RetainCloudClient):
    """Serves windows from an in-memory row list, mimicking the two paging methods the real client
    exposes: `create_page_result` (returns a key + total row count) and `fetch_page_window`
    (returns rows[start:start+count]). Records how it was called so tests can assert the request
    shape (which columns were requested, which windows were read).

    Subclasses `RetainCloudClient` only to satisfy `fetch_table`'s type signature — it deliberately
    does NOT call `super().__init__` (no real HttpClient is needed), because `fetch_table` only ever
    touches the two methods overridden below.
    """

    def __init__(self, rows, *, reported_row_count=None, key="PAGEKEY"):
        self._rows = rows
        self._key = key
        self.reported_row_count = len(rows) if reported_row_count is None else reported_row_count
        self.create_args: tuple[str, list[str]] | None = None
        self.window_calls: list[tuple[int, int]] = []

    def create_page_result(self, table: str, fields: list[str]) -> tuple[str, int]:
        self.create_args = (table, list(fields))
        return self._key, self.reported_row_count

    def fetch_page_window(self, table: str, key: str, start: int, count: int) -> list[dict]:
        assert key == self._key, "window read used a key the create call did not return"
        self.window_calls.append((start, count))
        return self._rows[start : start + count]


class _LazyRowsClient(_FakeClient):
    """Builds each window's rows on demand, so every row object is allocated INSIDE the fetch —
    exactly as `response.json()` does in production. A pre-materialized row list (as `_FakeClient`
    holds) would let the extractor keep references to strings the test had already paid for, hiding
    any per-row memory the extractor itself retains."""

    def __init__(self, total: int):
        super().__init__(rows=[], reported_row_count=total)
        self._total = total

    def fetch_page_window(self, table: str, key: str, start: int, count: int) -> list[dict]:
        self.window_calls.append((start, count))
        return [_booking_row(f"guid-{i}") for i in range(start, min(start + count, self._total))]


class TestBuildTableSchema(unittest.TestCase):
    def test_maps_datetime_to_timestamp(self):
        schema = build_table_schema("booking", RICH_FIELDS_BOOKING)
        col = next(c for c in schema.columns if c.name == "booking_createdon")
        self.assertEqual(col.base_type, SupportedDataTypes.TIMESTAMP)

    def test_detects_pk_column(self):
        schema = build_table_schema("booking", RICH_FIELDS_BOOKING)
        self.assertEqual(schema.pk_column, "booking_guid")

    def test_no_pk_column_when_guid_field_absent(self):
        fields = [f for f in RICH_FIELDS_BOOKING if f["name"] != "booking_guid"]
        schema = build_table_schema("booking", fields)
        self.assertIsNone(schema.pk_column)

    def test_pk_column_matched_case_insensitively_keeps_api_casing(self):
        # The API returns the guid column with inconsistent casing (e.g. `SkillType_guid`,
        # `SkillLevel_Guid`) that does not equal the lowercase `<table>_guid`. The PK must still be
        # detected, and stored with the API's ACTUAL casing so `row.get(pk_column)` works at fetch.
        fields = [
            {"name": "SkillLevel_Guid", "dataType": "ID"},
            {"name": "SkillLevel_name", "dataType": "String"},
        ]
        schema = build_table_schema("SkillLevel", fields)
        self.assertEqual(schema.pk_column, "SkillLevel_Guid")

    def test_pk_does_not_match_a_foreign_key_guid_column(self):
        # `<table>_<ref>_guid` foreign keys are also guids; the `<table>_guid` targeting (even
        # case-insensitively) must not pick one of them as the primary key.
        fields = [
            {"name": "booking_resource_guid", "dataType": "ID"},
            {"name": "booking_job_guid", "dataType": "ID"},
        ]
        schema = build_table_schema("booking", fields)
        self.assertIsNone(schema.pk_column)

    def test_field_without_name_raises_user_exception_naming_the_table(self):
        # A tenant-configurable table (e.g. jobCustomLookup) can transiently return a field entry
        # with no "name"; that must surface as a clear UserException (exit 1) naming the table, not
        # a raw KeyError escaping to the exit-2 "internal error" path.
        fields = [
            {"name": "jobCustomLookup_guid", "dataType": "ID"},
            {"dataType": "ID"},  # malformed: no "name"
        ]
        with self.assertRaises(UserException) as ctx:
            build_table_schema("jobCustomLookup", fields)
        self.assertIn("jobCustomLookup", str(ctx.exception))

    def test_bool_int_float_start_as_native_candidates(self):
        schema = build_table_schema("booking", RICH_FIELDS_BOOKING)
        by_name = {c.name: c for c in schema.columns}
        self.assertEqual(by_name["booking_hours"].base_type, SupportedDataTypes.INTEGER)
        self.assertEqual(by_name["booking_rate"].base_type, SupportedDataTypes.FLOAT)
        self.assertEqual(by_name["booking_active"].base_type, SupportedDataTypes.BOOLEAN)

    def test_id_string_unknown_map_to_string(self):
        schema = build_table_schema("booking", RICH_FIELDS_BOOKING)
        by_name = {c.name: c for c in schema.columns}
        self.assertEqual(by_name["booking_guid"].base_type, SupportedDataTypes.STRING)
        self.assertEqual(by_name["booking_notes"].base_type, SupportedDataTypes.STRING)
        self.assertEqual(by_name["booking_meta"].base_type, SupportedDataTypes.STRING)

    def test_calculated_field_flagged(self):
        schema = build_table_schema("widget", RICH_FIELDS_WITH_CALC)
        by_name = {c.name: c for c in schema.columns}
        self.assertTrue(by_name["widget_total"].is_calculated)
        self.assertFalse(by_name["widget_guid"].is_calculated)
        self.assertFalse(by_name["widget_name"].is_calculated)


class TestSafeColumnName(unittest.TestCase):
    def test_name_under_cap_returned_unchanged(self):
        name = "booking_guid"
        self.assertEqual(safe_column_name(name), name)

    def test_name_at_exactly_64_chars_returned_unchanged(self):
        # Boundary case: the cap itself (`<=`, not `<`) must not be shortened.
        name = "a" * 64
        result = safe_column_name(name)
        self.assertEqual(result, name)
        self.assertEqual(len(result), 64)

    def test_name_over_cap_shortened_to_exactly_64_chars(self):
        name = "a" * 70
        shortened = safe_column_name(name)
        self.assertEqual(len(shortened), 64)
        self.assertNotEqual(shortened, name)

    def test_shortening_is_deterministic_across_calls(self):
        name = "rolerequestresourcerejectreason_rolerequestpredefinedrejectreason_guid"
        self.assertEqual(safe_column_name(name), safe_column_name(name))

    def test_names_sharing_first_55_chars_do_not_collide(self):
        # Regression: a plain truncation-only scheme would silently merge these two distinct
        # columns into one Storage column. The hash is computed over the FULL name, so two names
        # that agree on their first 55 characters must still diverge after shortening.
        shared_head = "x" * 55
        name_a = shared_head + "_first_variant_tail"
        name_b = shared_head + "_second_variant_tail"
        self.assertGreater(len(name_a), 64)
        self.assertGreater(len(name_b), 64)
        self.assertEqual(name_a[:55], name_b[:55])
        self.assertNotEqual(safe_column_name(name_a), safe_column_name(name_b))


class TestPkTracker(unittest.TestCase):
    """Primary-key uniqueness is tracked on disk, not in a Python set: a 3.48M-row table's GUIDs
    alone would need ~430 MB in-process, past the 256 MB container limit the job runs under."""

    def setUp(self):
        self.scratch_dir = Path("/tmp/ex-retain-cloud-test")
        self.scratch_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.scratch_dir / "pk-test.sqlite"
        self.path.unlink(missing_ok=True)

    def test_distinct_values_verify_unique(self):
        with PkTracker(self.path) as tracker:
            for v in ("a", "b", "c"):
                tracker.add(v)
            self.assertTrue(tracker.is_unique(row_count=3))

    def test_duplicate_value_is_not_unique(self):
        with PkTracker(self.path) as tracker:
            for v in ("a", "a", "b"):
                tracker.add(v)
            self.assertFalse(tracker.is_unique(row_count=3))

    def test_null_value_is_not_unique(self):
        # A null/missing PK must never count as "the one unique value" — same rule as before.
        with PkTracker(self.path) as tracker:
            tracker.add("a")
            tracker.add(None)
            self.assertFalse(tracker.is_unique(row_count=2))

    def test_zero_rows_verify_unique(self):
        with PkTracker(self.path) as tracker:
            self.assertTrue(tracker.is_unique(row_count=0))

    def test_values_compared_as_their_csv_text(self):
        # The CSV cell is what Storage dedupes on, so 1 and "1" are the same key.
        with PkTracker(self.path) as tracker:
            tracker.add(1)
            tracker.add("1")
            self.assertFalse(tracker.is_unique(row_count=2))

    def test_close_removes_the_on_disk_store(self):
        tracker = PkTracker(self.path)
        tracker.add("a")
        tracker.close()
        self.assertFalse(self.path.exists())


class TestFetchTableWindowed(unittest.TestCase):
    def setUp(self):
        self.schema = build_table_schema("booking", RICH_FIELDS_BOOKING)
        self.scratch_dir = Path("/tmp/ex-retain-cloud-test")
        self.scratch_dir.mkdir(parents=True, exist_ok=True)

    def test_single_window_writes_typed_headerless_csv(self):
        rows = [
            _booking_row("a", hours=8, rate=1.5, active=True, created="2026-01-01T00:00:00Z", notes="x", meta=None),
            _booking_row(
                "b", hours=4, rate=2.0, active=False, created="2026-01-02T00:00:00Z", notes="y", meta={"k": 1}
            ),
        ]
        client = _FakeClient(rows)

        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)

        # create requested every (non-calculated) column, in schema order
        self.assertEqual(client.create_args, ("booking", [c.name for c in self.schema.columns]))
        # one window, capped to the row count (chunk is far larger than 2 rows)
        self.assertEqual(client.window_calls, [(0, 2)])
        self.assertEqual(result.row_count, 2)
        self.assertTrue(result.pk_unique)

        content = result.scratch_path.read_text()
        self.assertIn("a,8,1.5,True,2026-01-01T00:00:00Z,x,", content)
        self.assertNotIn("booking_guid", content)  # headerless — no header row
        # nested object serialized as compact JSON; parse via csv so RFC4180 quoting is handled
        written_rows = list(csv.reader(io.StringIO(content)))
        self.assertEqual(written_rows[1][-1], '{"k":1}')

    def test_fetches_across_multiple_windows_in_order(self):
        rows = [_booking_row(g) for g in ("a", "b", "c", "d", "e")]
        client = _FakeClient(rows)

        with mock.patch.object(extractor, "_WINDOW_CHUNK", 2):
            result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)

        self.assertEqual(result.row_count, 5)
        # windows tile [0, 5) in chunks of 2 (order across worker threads is not guaranteed)
        self.assertEqual(sorted(client.window_calls), [(0, 2), (2, 2), (4, 1)])
        # rows are consumed in offset order, so output row order is deterministic
        written_rows = list(csv.reader(io.StringIO(result.scratch_path.read_text())))
        self.assertEqual([r[0] for r in written_rows], ["a", "b", "c", "d", "e"])

    def test_excludes_calculated_fields_from_request_but_keeps_them_in_output(self):
        schema = build_table_schema("widget", RICH_FIELDS_WITH_CALC)
        # The API never returns a calculated column, because we never request it.
        rows = [{"widget_guid": "g1", "widget_name": "Alpha"}, {"widget_guid": "g2", "widget_name": "Beta"}]
        client = _FakeClient(rows)

        result = fetch_table(client, "widget", schema, scratch_dir=self.scratch_dir)

        # create omits the calculated column (listing it returns 400 from the real API)
        self.assertEqual(client.create_args, ("widget", ["widget_guid", "widget_name"]))
        self.assertEqual(result.row_count, 2)
        # the calculated column still appears in the output, emitted empty (parity with the old model)
        written_rows = list(csv.reader(io.StringIO(result.scratch_path.read_text())))
        self.assertEqual(written_rows[0], ["g1", "Alpha", ""])

    def test_pk_not_unique_on_duplicate(self):
        rows = [_booking_row("dup"), _booking_row("dup")]
        client = _FakeClient(rows)
        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        self.assertFalse(result.pk_unique)

    def test_pk_null_value_is_not_unique(self):
        # A None/missing PK must never count as "the one unique value" — that would let Storage
        # declare a nullable column as the primary key. `pk_unique` requires every PK value non-null.
        rows = [_booking_row(None)]
        client = _FakeClient(rows)
        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        self.assertFalse(result.pk_unique)

    def test_int_column_downgraded_to_string_on_non_numeric_value(self):
        rows = [_booking_row("a", hours="DELIVERED")]
        client = _FakeClient(rows)
        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_hours", downgraded)

    def test_int_column_downgraded_to_string_on_float_value(self):
        # `int(1.0)` succeeds, but `_stringify` writes "1.0" — not a valid INTEGER CSV literal.
        rows = [_booking_row("a", hours=1.0)]
        client = _FakeClient(rows)
        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_hours", downgraded)

    def test_int_column_downgraded_to_string_on_bool_value(self):
        # `int(True) == 1` succeeds, but `_stringify` writes "True". A bool only coerces into a Bool.
        rows = [_booking_row("a", hours=True)]
        client = _FakeClient(rows)
        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_hours", downgraded)

    def test_float_column_downgraded_to_string_on_bool_value(self):
        rows = [_booking_row("a", rate=True)]
        client = _FakeClient(rows)
        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_rate", downgraded)

    def test_empty_table_creates_file_without_any_window_calls(self):
        client = _FakeClient([], reported_row_count=0)
        result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        self.assertEqual(result.row_count, 0)
        self.assertTrue(result.scratch_path.exists())
        self.assertEqual(client.window_calls, [])  # no windows fetched for a zero-row table

    def test_persistently_short_window_raises_user_exception(self):
        # A window that never returns the rows it should (after retries) must fail the table loud,
        # not ship a gap. Here create claims 5 rows but only 3 exist, so the single window stays short.
        client = _FakeClient([_booking_row(g) for g in ("a", "b", "c")], reported_row_count=5)
        # patch sleep so the retry backoff does not actually wait during the test
        with mock.patch.object(extractor.time, "sleep"), self.assertRaises(UserException) as ctx:
            fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        message = str(ctx.exception)
        self.assertIn("booking", message)
        self.assertIn("too few rows", message)

    def test_row_count_mismatch_fails_loud(self):
        # Defensive completeness guard: if the consumed row total does not match the reported
        # rowCount (a lost window), fetch_table fails loud rather than ship a silent partial.
        client = _FakeClient([_booking_row("a")], reported_row_count=3)
        with (
            mock.patch.object(extractor, "_fetch_window", return_value=[_booking_row("a")]),
            self.assertRaises(UserException) as ctx,
        ):
            fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
        message = str(ctx.exception)
        self.assertIn("1", message)  # extracted
        self.assertIn("3", message)  # reported

    def test_table_with_only_calculated_columns_raises(self):
        fields = [{"name": "calc_only_total", "dataType": "Float", "uiFieldCategory": "CalculatedField"}]
        schema = build_table_schema("calc_only", fields)
        client = _FakeClient([])
        with self.assertRaises(UserException) as ctx:
            fetch_table(client, "calc_only", schema, scratch_dir=self.scratch_dir)
        self.assertIn("no non-calculated columns", str(ctx.exception))

    def test_many_windows_beyond_pipeline_depth_stay_in_order(self):
        # 30 single-row windows is well past however many windows the fetch keeps in flight at once,
        # so this exercises the refill path (submit the next window as one is consumed) and proves
        # output order still follows offset order, not completion order.
        rows = [_booking_row(f"g{i:02d}") for i in range(30)]
        client = _FakeClient(rows)

        with mock.patch.object(extractor, "_WINDOW_CHUNK", 1):
            result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)

        self.assertEqual(result.row_count, 30)
        self.assertEqual(sorted(client.window_calls), [(i, 1) for i in range(30)])
        written_rows = list(csv.reader(io.StringIO(result.scratch_path.read_text())))
        self.assertEqual([r[0] for r in written_rows], [f"g{i:02d}" for i in range(30)])

    def test_python_heap_does_not_grow_with_row_count(self):
        # Regression for the production OOM (job killed at the 256 MB container limit on a 3.48M-row
        # table): the extractor must not retain anything per row — neither a primary-key value set
        # nor whole fetched windows past their consumption. 150k rows at 100 rows/window: a per-row
        # PK set alone would retain ~17 MB here; a bounded pipeline of tiny windows retains ~1 MB.
        total = 150_000
        client = _LazyRowsClient(total)

        tracemalloc.start()
        try:
            baseline, _ = tracemalloc.get_traced_memory()
            with mock.patch.object(extractor, "_WINDOW_CHUNK", 100):
                result = fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        self.assertEqual(result.row_count, total)
        self.assertTrue(result.pk_unique)
        growth_mb = (peak - baseline) / (1024 * 1024)
        self.assertLess(growth_mb, 8, f"extractor retained {growth_mb:.1f} MB of Python heap for {total} rows")

    def test_logs_start_and_finish_at_info(self):
        rows = [_booking_row(g) for g in ("a", "b", "c", "d", "e")]
        client = _FakeClient(rows)

        with (
            mock.patch.object(extractor, "_WINDOW_CHUNK", 2),
            self.assertLogs("extractor", level="INFO") as logs,
        ):
            fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)

        messages = [r.getMessage() for r in logs.records if r.levelno == 20]  # INFO only
        self.assertTrue(
            any("booking" in m and "5 rows" in m and "window" in m for m in messages),
            f"no INFO start line naming the table, row total and windowing; got {messages}",
        )
        self.assertTrue(
            any("booking" in m and "fetched 5 rows" in m for m in messages),
            f"no INFO finish line with the fetched row count; got {messages}",
        )

    def test_logs_progress_milestones_at_info(self):
        # A multi-million-row table runs for many minutes; the job log must show it advancing.
        # 20 single-row windows → progress is reported at roughly every 10% of the total.
        rows = [_booking_row(f"g{i:02d}") for i in range(20)]
        client = _FakeClient(rows)

        with (
            mock.patch.object(extractor, "_WINDOW_CHUNK", 1),
            self.assertLogs("extractor", level="INFO") as logs,
        ):
            fetch_table(client, "booking", self.schema, scratch_dir=self.scratch_dir)

        progress = [r.getMessage() for r in logs.records if r.levelno == 20 and "rows fetched (" in r.getMessage()]
        self.assertGreaterEqual(len(progress), 5, f"expected periodic progress lines, got {progress}")
        self.assertTrue(all("/ 20 rows fetched" in m for m in progress), progress)


if __name__ == "__main__":
    unittest.main()
