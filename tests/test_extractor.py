import csv
import io
import json
import unittest
from pathlib import Path
from unittest import mock

import ijson
import requests
from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException

from extractor import build_table_schema, fetch_table, safe_column_name

RICH_FIELDS_BOOKING = [
    {"name": "booking_guid", "dataType": "ID"},
    {"name": "booking_hours", "dataType": "Int"},
    {"name": "booking_rate", "dataType": "Float"},
    {"name": "booking_active", "dataType": "Bool"},
    {"name": "booking_createdon", "dataType": "DateTime"},
    {"name": "booking_notes", "dataType": "String"},
    {"name": "booking_meta", "dataType": "Unknown"},
]


def _envelope_response(row_count: int, rows: list[dict]) -> requests.Response:
    body = json.dumps({"key": "guid", "rowCount": row_count, "rowsProcessed": len(rows), "data": rows}).encode()
    resp = mock.Mock(spec=requests.Response)
    resp.raw = io.BytesIO(body)
    return resp


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


class TestFetchTableSingleCall(unittest.TestCase):
    def setUp(self):
        self.schema = build_table_schema("booking", RICH_FIELDS_BOOKING)
        self.scratch_dir = Path("/tmp/ex-retain-cloud-test")
        self.scratch_dir.mkdir(parents=True, exist_ok=True)

    def test_single_call_when_first_call_covers_the_whole_table(self):
        rows = [
            {
                "booking_guid": "a",
                "booking_hours": 8,
                "booking_rate": 1.5,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            },
            {
                "booking_guid": "b",
                "booking_hours": 4,
                "booking_rate": 2.0,
                "booking_active": False,
                "booking_createdon": "2026-01-02T00:00:00Z",
                "booking_notes": "y",
                "booking_meta": {"k": 1},
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=2, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)

        client.fetch_table_page.assert_called_once_with("booking", 20000)
        self.assertEqual(result.row_count, 2)
        self.assertTrue(result.pk_unique)
        content = result.scratch_path.read_text()
        self.assertIn("a,8,1.5,True,2026-01-01T00:00:00Z,x,", content)
        self.assertNotIn("booking_guid", content)  # headerless — no header row
        # nested object serialized as compact JSON — parsed via csv.reader rather than a raw
        # substring match, since a well-formed CSV writer correctly RFC4180-quotes a field that
        # itself contains double quotes (doubling them), so the raw bytes are `"{""k"":1}"`.
        written_rows = list(csv.reader(io.StringIO(content)))
        self.assertEqual(written_rows[1][-1], '{"k":1}')

    def test_second_call_issued_when_first_call_is_short(self):
        first_rows = [
            {
                "booking_guid": "a",
                "booking_hours": 1,
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            }
        ]
        second_rows = first_rows + [
            {
                "booking_guid": "b",
                "booking_hours": 2,
                "booking_rate": 2.0,
                "booking_active": False,
                "booking_createdon": "2026-01-02T00:00:00Z",
                "booking_notes": "y",
                "booking_meta": None,
            },
            {
                "booking_guid": "c",
                "booking_hours": 3,
                "booking_rate": 3.0,
                "booking_active": True,
                "booking_createdon": "2026-01-03T00:00:00Z",
                "booking_notes": "z",
                "booking_meta": None,
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.side_effect = [
            _envelope_response(row_count=3, rows=first_rows),
            _envelope_response(row_count=3, rows=second_rows),
        ]

        result = fetch_table(client, "booking", self.schema, page_size=1, scratch_dir=self.scratch_dir)

        self.assertEqual(client.fetch_table_page.call_count, 2)
        second_call_args = client.fetch_table_page.call_args_list[1]
        self.assertEqual(second_call_args.args[0], "booking")
        self.assertGreaterEqual(second_call_args.args[1], 3)  # rowCount + margin, not the raw rowCount
        self.assertEqual(result.row_count, 3)  # final result reflects the SECOND call only
        content = result.scratch_path.read_text()
        self.assertEqual(content.count("\n"), 3)  # not 1 (first) + 3 (second) — first call discarded

    def test_second_call_http_error_raises_user_exception_with_status_table_and_row_count(self):
        # Bug 3 regression: the SECOND (rowCount-sized) call is issued with
        # `fail_fast_on_http_error=True`, so a report table's deterministic 504 must surface here as
        # a `requests.HTTPError` (not retried away into a `RetryError`) and `fetch_table` must turn
        # it into a specific `UserException` naming the status, the table, and the row count that
        # was being requested — rather than letting a generic `RequestException` propagate for
        # `component.py` to describe more vaguely. Also confirms `fetch_table` itself does not loop
        # around the second call (exactly 2 total calls: first + second).
        first_rows = [
            {
                "booking_guid": "a",
                "booking_hours": 1,
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            }
        ]
        client = mock.Mock()
        client.fetch_table_page.side_effect = [
            _envelope_response(row_count=7, rows=first_rows),  # 1 row, short of 7 -> triggers 2nd call
            requests.HTTPError(response=mock.Mock(status_code=504, reason="Gateway Timeout")),
        ]

        with self.assertRaises(UserException) as ctx:
            fetch_table(client, "booking", self.schema, page_size=1, scratch_dir=self.scratch_dir)

        message = str(ctx.exception)
        self.assertIn("504", message)
        self.assertIn("booking", message)
        self.assertIn("7", message)  # row_count_total from the first call's rowCount
        self.assertEqual(client.fetch_table_page.call_count, 2)  # no retry loop around the 2nd call

    def test_pk_not_unique_falls_back_to_no_pk(self):
        rows = [
            {
                "booking_guid": "dup",
                "booking_hours": 1,
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            },
            {
                "booking_guid": "dup",
                "booking_hours": 2,
                "booking_rate": 2.0,
                "booking_active": False,
                "booking_createdon": "2026-01-02T00:00:00Z",
                "booking_notes": "y",
                "booking_meta": None,
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=2, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        self.assertFalse(result.pk_unique)

    def test_pk_null_value_is_not_unique(self):
        # Regression: a None/missing PK value used to count as "the one unique value" — a single
        # row whose PK column is null made `pk_unique` True, which would let Storage declare a
        # nullable column as the primary key. `pk_unique` must now require every observed PK value
        # to be non-null, not just distinct.
        rows = [
            {
                "booking_guid": None,
                "booking_hours": 1,
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=1, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        self.assertFalse(result.pk_unique)

    def test_int_column_downgraded_to_string_on_non_numeric_value(self):
        rows = [
            {
                "booking_guid": "a",
                "booking_hours": "DELIVERED",
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=1, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_hours", downgraded)

    def test_int_column_downgraded_to_string_on_float_value(self):
        # Regression: `int(1.0)` succeeds, but `_stringify` writes the literal text "1.0" for a
        # float value — not a valid native INTEGER CSV literal — so a JSON float landing in an
        # Int-declared column must still be downgraded to STRING.
        rows = [
            {
                "booking_guid": "a",
                "booking_hours": 1.0,
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=1, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_hours", downgraded)

    def test_int_column_downgraded_to_string_on_bool_value(self):
        # Regression: `int(True) == 1` succeeds, but `_stringify` writes the literal text "True" —
        # not a valid native INTEGER CSV literal. A bool value only ever legitimately coerces into
        # a Bool-declared column.
        rows = [
            {
                "booking_guid": "a",
                "booking_hours": True,
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=1, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_hours", downgraded)

    def test_float_column_downgraded_to_string_on_bool_value(self):
        # Same bool-rejection rule as above, but for a Float-declared column: `float(True) == 1.0`
        # succeeds, but "True" is not a valid native FLOAT CSV literal.
        rows = [
            {
                "booking_guid": "a",
                "booking_hours": 1,
                "booking_rate": True,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            },
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=1, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_rate", downgraded)

    def test_empty_table_still_produces_a_file(self):
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=0, rows=[])
        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        self.assertEqual(result.row_count, 0)
        self.assertTrue(result.scratch_path.exists())

    def test_malformed_response_raises_ijson_json_error(self):
        # Confirms the premise `component.py`'s `except ijson.JSONError` clause relies on: a
        # truncated `paging/paged` body genuinely raises `ijson.JSONError` here (not, say, a plain
        # `ValueError` or something `requests.exceptions.RequestException` would already catch).
        client = mock.Mock()
        resp = mock.Mock(spec=requests.Response)
        resp.raw = io.BytesIO(b'{"key": "guid", "rowCount": 2, "rowsProcessed": 1, "data": [{"booking_guid": "a"')
        client.fetch_table_page.return_value = resp

        with self.assertRaises(ijson.JSONError):
            fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)

    def test_response_without_rowcount_key_raises_ijson_json_error(self):
        # Regression: an envelope whose JSON body never carries a `rowCount` key AT ALL (distinct
        # from the legitimate "empty table" case, `rowCount: 0`, covered by
        # `test_empty_table_still_produces_a_file`) used to silently default `row_count_total` to
        # 0, making a truncated first page look complete. It must now raise `ijson.JSONError`,
        # same as a malformed body.
        client = mock.Mock()
        resp = mock.Mock(spec=requests.Response)
        body = json.dumps({"key": "guid", "rowsProcessed": 1, "data": [{"booking_guid": "a"}]}).encode()
        resp.raw = io.BytesIO(body)
        client.fetch_table_page.return_value = resp

        with self.assertRaises(ijson.JSONError):
            fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)

    def test_second_call_still_short_returns_best_effort_without_a_third_call(self):
        # Spec §6: when the SECOND `paging/paged` call is ALSO short of its own reported
        # `rowCount` (the table grew faster than the `rowCount + margin` safety margin covered),
        # `fetch_table` does not issue a third call at all — it logs a warning and returns the
        # second call's result as best-effort-complete, not a failure. This exact branch
        # (`result.row_count < row_count_total_2`) previously had zero coverage.
        first_rows = [
            {
                "booking_guid": "a",
                "booking_hours": 1,
                "booking_rate": 1.0,
                "booking_active": True,
                "booking_createdon": "2026-01-01T00:00:00Z",
                "booking_notes": "x",
                "booking_meta": None,
            }
        ]
        second_rows = first_rows + [
            {
                "booking_guid": "b",
                "booking_hours": 2,
                "booking_rate": 2.0,
                "booking_active": False,
                "booking_createdon": "2026-01-02T00:00:00Z",
                "booking_notes": "y",
                "booking_meta": None,
            }
        ]
        client = mock.Mock()
        client.fetch_table_page.side_effect = [
            _envelope_response(row_count=3, rows=first_rows),  # 1st call: 1 row, short of 3
            _envelope_response(row_count=5, rows=second_rows),  # 2nd call: 2 rows, STILL short of 5
        ]

        with self.assertLogs("extractor", level="WARNING") as cm:
            result = fetch_table(client, "booking", self.schema, page_size=1, scratch_dir=self.scratch_dir)

        self.assertEqual(client.fetch_table_page.call_count, 2)  # no third call issued
        self.assertTrue(any("still short of its own rowCount" in message for message in cm.output))
        # Result reflects what the second call actually delivered (2), not its own rowCount claim (5).
        self.assertEqual(result.row_count, 2)


if __name__ == "__main__":
    unittest.main()
