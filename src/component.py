"""Thin Component orchestrator for keboola.ex-retain-cloud.

Each row selects exactly one table (spec §2/§5's config-rows convention) — so `run()` is a single
straight-line sequence with no per-table loop and no partial-failure bookkeeping: a table failure
fails this row's job outright, which the platform already isolates from every other row.
"""

import logging
import shutil
import sys
from pathlib import Path

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.table_schema import FieldSchema, TableSchema

from client import RetainCloudClient
from configuration import Configuration, RootConfig
from extractor import TableSchema_, build_table_schema, fetch_table

logger = logging.getLogger(__name__)

_SCRATCH_DIR = Path("/tmp/ex-retain-cloud")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self) -> None:
        cfg = Configuration(**self.configuration.parameters)
        client = self._build_authenticated_client(cfg)

        if cfg.table not in set(client.list_tables()):
            raise UserException(f"Table '{cfg.table}' is no longer present in this tenant's structure.")

        try:
            self._process_table(client, cfg)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            raise UserException(f"Failed to fetch table '{cfg.table}' (HTTP {status}).") from e

    def _build_authenticated_client(self, cfg: RootConfig) -> RetainCloudClient:
        client = RetainCloudClient(
            environment=cfg.environment.value,
            tenant=cfg.tenant,
            username=cfg.username,
            password=cfg.password.get_secret_value(),
        )
        client.authenticate()
        return client

    def _process_table(self, client: RetainCloudClient, cfg: Configuration) -> None:
        rich_fields = client.get_table_schema(cfg.table)
        schema = build_table_schema(cfg.table, rich_fields)
        result = fetch_table(client, cfg.table, schema, cfg.page_size, _SCRATCH_DIR)

        incremental_for_table = cfg.incremental and result.pk_unique
        if cfg.incremental and not result.pk_unique:
            logger.warning(
                "Table %s: Incremental Load was requested but the primary key did not verify "
                "unique this run — falling back to full load for this run.",
                cfg.table,
            )

        # `create_out_table_definition_from_schema`'s `incremental` kwarg is VERIFIED (not
        # inferred) against the installed keboola-component library:
        # `uv run python -c "import inspect; from keboola.component.base import ComponentBase;
        # print(inspect.signature(ComponentBase.create_out_table_definition_from_schema))"` reports
        # `(self, table_schema, is_sliced=False, destination='', incremental: bool = None,
        # enclosure='"', delimiter=',', delete_where=None)` — `incremental` is real.
        table_def = self.create_out_table_definition_from_schema(
            self._to_output_schema(schema, result.pk_unique),
            incremental=incremental_for_table,
        )
        Path(table_def.full_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(result.scratch_path), table_def.full_path)
        self.write_manifest(table_def)

    @staticmethod
    def _to_output_schema(schema: TableSchema_, pk_unique: bool) -> TableSchema:
        # The PK is declared ONLY when this run's uniqueness check passed (spec §6/§7 case 13) —
        # for BOTH full and incremental load. Declaring a non-unique PK would make Storage
        # deduplicate on the declared key at import time, silently dropping a row every run, on
        # the component's DEFAULT (full_load) path — not just the incremental one. `pk_unique` is
        # already False whenever `schema.pk_column` is None (extractor.py's `_stream_to_csv`
        # computes `pk_unique = schema.pk_column is not None and len(pk_values) == row_count`), so
        # the explicit `schema.pk_column` check below is redundant at runtime — it's here purely to
        # narrow `str | None` to `str` for the type checker.
        primary_keys = [schema.pk_column] if pk_unique and schema.pk_column else None
        fields = [FieldSchema(name=c.name, base_type=c.base_type, nullable=True) for c in schema.columns]
        return TableSchema(name=schema.table, fields=fields, primary_keys=primary_keys)

    @sync_action("testConnection")
    def test_connection(self) -> None:
        cfg = RootConfig(**self.configuration.parameters)
        self._build_authenticated_client(cfg)

    @sync_action("list_tables")
    def list_tables(self) -> list[dict]:
        # Deliberately RootConfig, not Configuration — a fresh row may not have `table` set yet
        # (spec §6's "partial instantiation" fix). Any row-level keys present in the merged
        # parameters (table/load_type/page_size) are simply ignored by RootConfig's extra="ignore".
        cfg = RootConfig(**self.configuration.parameters)
        client = self._build_authenticated_client(cfg)
        table_names = client.list_tables()
        labels_by_name = {row["name"]: row.get("alias") for row in client.list_table_labels()}
        return [{"value": name, "label": labels_by_name.get(name) or name} for name in table_names]


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException:
        logger.exception("Component failed with a user error")
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
