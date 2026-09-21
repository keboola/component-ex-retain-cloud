import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests
from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException

from component import Component
from extractor import ColumnSchema, FetchResult, TableSchema_

ROW_PARAMS = {
    "environment": "us",
    "tenant": "acme",
    "username": "svc@example.com",
    "#password": "pw",
    "table": "booking",
}


def _schema(table: str) -> TableSchema_:
    return TableSchema_(
        table=table,
        columns=[ColumnSchema(name=f"{table}_guid", declared_type="ID", base_type=SupportedDataTypes.STRING)],
        pk_column=f"{table}_guid",
    )


def _fetch_result(scratch_dir: Path, table: str, row_count=1, pk_unique=True) -> FetchResult:
    # `fetch_table` is mocked out in every test below, so its own `scratch_dir.mkdir(...)` (the
    # real function's first action, per extractor.py) never runs — recreate that side effect here
    # since `scratch_dir` is `component.py`'s real `_SCRATCH_DIR` constant, not a test-local path.
    scratch_dir.mkdir(parents=True, exist_ok=True)
    path = scratch_dir / f"{table}.csv"
    path.write_text(f"{table}-row-1\n" * row_count)
    return FetchResult(scratch_path=path, row_count=row_count, pk_unique=pk_unique, verified_columns={})


class TestRunOrchestration(unittest.TestCase):
    def setUp(self):
        self.data_dir = Path(tempfile.mkdtemp())
        (self.data_dir / "out" / "tables").mkdir(parents=True)
        (self.data_dir / "in" / "tables").mkdir(parents=True)

    def tearDown(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _component(self, params=None):
        # `keboola.component`'s `configuration` property re-reads `config.json` from disk on every
        # access (`Configuration(self.data_folder_path)`) rather than caching an instance, so a
        # test cannot inject parameters via `comp.configuration.parameters = ...` after
        # construction — a real fixture file is required. Writing it straight into `self.data_dir`
        # also means it's already the exact directory `tables_out_path`/`tables_in_path` (properties
        # reading `data_folder_path`) resolve against, so `/data/out/tables/` lands in the sandbox.
        # `ComponentBase`'s own data-dir default (no `KBC_DATADIR` set) is `cwd.parent/data`
        # unconditionally (see `ComponentBase._get_data_folder_override_path`) — it doesn't fall
        # back to `cwd/data` the way `CommonInterface._get_data_folder_from_context` does, so
        # `KBC_DATADIR` must be set explicitly for construction to resolve here at all.
        config = {
            "storage": {"input": {"files": [], "tables": []}, "output": {"files": [], "tables": []}},
            "action": "run",
            "parameters": dict(params or ROW_PARAMS),
        }
        (self.data_dir / "config.json").write_text(json.dumps(config))
        with mock.patch.dict(os.environ, {"KBC_DATADIR": str(self.data_dir)}):
            return Component()

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_table_succeeds(self, mock_client_cls, mock_build_schema, mock_fetch_table):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking", "resource"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = lambda _client, table, _schema, _page_size, scratch_dir: _fetch_result(
            scratch_dir, table
        )

        comp = self._component()
        comp.run()  # must not raise

        self.assertTrue((self.data_dir / "out" / "tables" / "booking.csv").exists())

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_table_fetch_failure_raises_user_exception(self, mock_client_cls, mock_build_schema, mock_fetch_table):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = requests.HTTPError(response=mock.Mock(status_code=403))

        comp = self._component()
        with self.assertRaises(UserException):
            comp.run()

        self.assertFalse((self.data_dir / "out" / "tables" / "booking.csv").exists())

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_table_missing_from_structure_raises_user_exception(
        self, mock_client_cls, mock_build_schema, mock_fetch_table
    ):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["resource"]  # "booking" was selected but no longer exists

        comp = self._component()
        with self.assertRaises(UserException):
            comp.run()

        mock_build_schema.assert_not_called()
        mock_fetch_table.assert_not_called()

    def _run_and_capture(self, comp):
        """Run `comp` and capture both the `incremental` kwarg and the `primary_keys` actually
        passed to `create_out_table_definition_from_schema` — both must be checked together,
        since the data-loss bug this test guards against (spec §7 case 13) is specifically about
        `primary_keys` being set independently of `incremental`."""
        captured = {}
        original = comp.create_out_table_definition_from_schema

        def capture(table_schema, **kwargs):
            captured["incremental"] = kwargs.get("incremental")
            captured["primary_keys"] = table_schema.primary_keys
            return original(table_schema, **kwargs)

        with mock.patch.object(comp, "create_out_table_definition_from_schema", side_effect=capture):
            comp.run()
        return captured

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_full_load_with_nonunique_pk_declares_no_primary_key(
        self, mock_client_cls, mock_build_schema, mock_fetch_table
    ):
        # This is the direct regression test for spec §7 case 13 / the gate-fix data-loss bug: a
        # DEFAULT full_load row (load_type not set at all) whose <table>_guid has a duplicate value
        # this run must NOT declare a primary key — declaring one would make Storage deduplicate on
        # import and silently drop a row every single run, on the component's default path.
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = lambda _client, table, _schema, _page_size, scratch_dir: _fetch_result(
            scratch_dir, table, pk_unique=False
        )

        comp = self._component(ROW_PARAMS)  # load_type defaults to full_load
        captured = self._run_and_capture(comp)

        self.assertFalse(captured["incremental"])
        self.assertIsNone(captured["primary_keys"])

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_full_load_with_unique_pk_still_declares_primary_key(
        self, mock_client_cls, mock_build_schema, mock_fetch_table
    ):
        # Symmetric case: a unique PK is declared even on full_load (Storage can still dedupe
        # within a single load), confirming the fix doesn't over-correct into never declaring a PK.
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = lambda _client, table, _schema, _page_size, scratch_dir: _fetch_result(
            scratch_dir, table, pk_unique=True
        )

        comp = self._component(ROW_PARAMS)
        captured = self._run_and_capture(comp)

        self.assertFalse(captured["incremental"])
        self.assertEqual(captured["primary_keys"], ["booking_guid"])

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_incremental_load_falls_back_when_pk_not_verified(
        self, mock_client_cls, mock_build_schema, mock_fetch_table
    ):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = lambda _client, table, _schema, _page_size, scratch_dir: _fetch_result(
            scratch_dir, table, pk_unique=False
        )

        comp = self._component({**ROW_PARAMS, "load_type": "incremental_load"})
        captured = self._run_and_capture(comp)  # must NOT raise — this is a fallback, not a failure

        self.assertFalse(captured["incremental"])  # fell back to full load for this run
        self.assertIsNone(captured["primary_keys"])  # AND no PK declared — same bug, same fix

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_incremental_load_applied_when_pk_verifies(self, mock_client_cls, mock_build_schema, mock_fetch_table):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = lambda _client, table, _schema, _page_size, scratch_dir: _fetch_result(
            scratch_dir, table, pk_unique=True
        )

        comp = self._component({**ROW_PARAMS, "load_type": "incremental_load"})
        captured = self._run_and_capture(comp)

        self.assertTrue(captured["incremental"])
        self.assertEqual(captured["primary_keys"], ["booking_guid"])


if __name__ == "__main__":
    unittest.main()
