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
from keboola.component.dao import SupportedDataTypes
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

        try:
            table_exists = cfg.table in set(client.list_tables())
        except requests.exceptions.RequestException as e:
            raise UserException(
                self._describe_request_failure(f"Failed to verify table '{cfg.table}' exists", e)
            ) from e
        if not table_exists:
            raise UserException(f"Table '{cfg.table}' is no longer present in this tenant's structure.")

        try:
            self._process_table(client, cfg)
        except requests.exceptions.RequestException as e:
            raise UserException(self._describe_request_failure(f"Failed to fetch table '{cfg.table}'", e)) from e

    @staticmethod
    def _describe_request_failure(prefix: str, error: requests.exceptions.RequestException) -> str:
        """Build a `UserException` message covering both an HTTP-status failure (`HTTPError`,
        whose `.response.status_code` is reported) and a retries-exhausted/connection failure
        (`RetryError`/`ConnectionError`/`Timeout` — none of which carry a usable `.response`, since
        `HttpClient`'s retry adapter raises them from within the request call itself, before any
        response exists)."""
        status = getattr(getattr(error, "response", None), "status_code", None)
        if status is not None:
            return f"{prefix} (HTTP {status})."
        return f"{prefix}: Retain Cloud API unavailable after retries."

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
            self._to_output_schema(schema, result.pk_unique, result.verified_columns),
            incremental=incremental_for_table,
        )
        Path(table_def.full_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(result.scratch_path), table_def.full_path)
        self.write_manifest(table_def)

    @staticmethod
    def _to_output_schema(schema: TableSchema_, pk_unique: bool, verified_columns: dict[str, bool]) -> TableSchema:
        # The PK is declared ONLY when this run's uniqueness check passed (spec §6/§7 case 13) —
        # for BOTH full and incremental load. Declaring a non-unique PK would make Storage
        # deduplicate on the declared key at import time, silently dropping a row every run, on
        # the component's DEFAULT (full_load) path — not just the incremental one. `pk_unique` is
        # already False whenever `schema.pk_column` is None (extractor.py's `_stream_to_csv`
        # computes `pk_unique = schema.pk_column is not None and len(pk_values) == row_count`), so
        # the explicit `schema.pk_column` check below is redundant at runtime — it's here purely to
        # narrow `str | None` to `str` for the type checker.
        primary_keys = [schema.pk_column] if pk_unique and schema.pk_column else None
        fields = []
        for c in schema.columns:
            # `verified_columns` only has entries for Bool/Int/Float-declared columns (extractor.py's
            # `_stream_to_csv` seeds it from `_VERIFY_TYPES`); a column absent from it (DateTime/ID/
            # String/Unknown) was never a native-type candidate, so it keeps its already-STRING or
            # trusted-DateTime `base_type` unconditionally (`.get(c.name, True)` below). A column
            # PRESENT but False failed this run's per-row coercion check and must ship as STRING —
            # forwarding the pre-verification `base_type` here would silently re-introduce the
            # exact "declared numeric but really not" failure mode the streaming verification pass
            # exists to catch (spec §6).
            base_type = c.base_type if verified_columns.get(c.name, True) else SupportedDataTypes.STRING
            fields.append(FieldSchema(name=c.name, base_type=base_type, nullable=True))
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
