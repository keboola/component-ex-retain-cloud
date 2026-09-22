"""Thin Component orchestrator for keboola.ex-retain-cloud.

Each row selects exactly one table (spec §2/§5's config-rows convention) — so `run()` is a single
straight-line sequence with no per-table loop and no partial-failure bookkeeping: a table failure
fails this row's job outright, which the platform already isolates from every other row.
"""

import base64
import json
import logging
import shutil
import sys
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any

import ijson
import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.table_schema import FieldSchema, TableSchema

# `keboola.vcr` is a *runtime* dependency here, not a dev-only one: `keboola-component>=1.10.0`
# (a `[project] dependencies` entry) declares `keboola-vcr` as its own dependency, so it is present
# in the `--no-dev` production image too. Verified against `uv.lock` (`keboola-component 1.11.0`
# → `dependencies = [deprecated, keboola-vcr, pygelf]`), not assumed.
from keboola.vcr import BaseSanitizer, DefaultSanitizer, UrlPatternSanitizer

from client import RetainCloudClient, describe_request_error
from configuration import Configuration, RootConfig
from extractor import TableSchema_, build_table_schema, fetch_table, safe_column_name

logger = logging.getLogger(__name__)

_SCRATCH_DIR = Path("/tmp/ex-retain-cloud")

# --------------------------------------------------------------------------------------------
# VCR sanitizers — cassettes for this component are committed to a PUBLIC repo while the tenant
# behind them is a real customer. The four sanitizers below are what makes leaking the tenant
# name, the username, or the password structurally impossible rather than a thing to remember —
# including by arithmetic, which is what entry 4 closes off.
# The scaffolder and the test runner both pick `VCR_SANITIZERS` up automatically.
# --------------------------------------------------------------------------------------------

# Far-future `exp` (year 2286) so that `RetainCloudClient._ensure_token()`'s 300s refresh margin
# never fires mid-replay and demands a token call the cassette does not contain.
_VCR_SYNTHETIC_TOKEN_EXP = 9999999999
_VCR_SYNTHETIC_JWT_HEADER = "REDACTEDHEADER"
_VCR_SYNTHETIC_JWT_SIGNATURE = "REDACTEDSIGNATURE"


def _synthetic_bearer_token() -> str:
    """Build a `Bearer <jwt>` string that is fake but still parseable by `decode_jwt_exp()`."""
    payload = base64.urlsafe_b64encode(json.dumps({"exp": _VCR_SYNTHETIC_TOKEN_EXP}).encode()).decode().rstrip("=")
    return f"Bearer {_VCR_SYNTHETIC_JWT_HEADER}.{payload}.{_VCR_SYNTHETIC_JWT_SIGNATURE}"


def _sync_content_length(headers: Any, new_length: int) -> None:
    """Rewrite an already-present `Content-Length` so it matches a body a sanitizer just rewrote.

    Shared by both directions, since both rewrite a body whose declared length survives into the
    cassette: `DefaultSanitizer`'s header whitelist keeps `content-length`, so a length left at its
    pre-sanitization value is written out next to a body that no longer has it.

    The two sides hand in different container types, hence the `MutableMapping` check rather than
    `isinstance(..., dict)`: a recorded response's headers are a plain dict, but a live vcrpy
    `Request.headers` is a `HeadersDict`, which subclasses `requests`' `CaseInsensitiveDict` and
    is therefore NOT a `dict` — a `dict` check would silently no-op on every request. Values are
    plain strings on the request side and lists once serialized into a cassette, so both shapes
    are handled, and the items are snapshotted before the rewrite rather than mutated mid-iteration.

    The header is only ever UPDATED, never added: a message that never declared a length has
    nothing that can go stale, and inventing one would change what the cassette claims.
    """
    if not isinstance(headers, MutableMapping):
        return
    for key, value in list(headers.items()):
        if key.lower() == "content-length":
            headers[key] = [str(new_length)] if isinstance(value, list) else str(new_length)


def _body_byte_length(body: Any) -> int | None:
    """Byte length of a request/response body, or None when it cannot be known without cost.

    `None` covers both "no body at all" (every GET here) and a stream/file-like body, which we
    must not consume just to measure it.
    """
    if isinstance(body, str):
        return len(body.encode("utf-8"))
    if isinstance(body, bytes | bytearray):
        return len(body)
    return None


class BearerTokenBodySanitizer(BaseSanitizer):
    """Replace the `IntegrationApi/token` response body with a *structurally valid* synthetic JWT.

    `POST /IntegrationApi/token` answers with the bare string `Bearer eyJhbGci...` — plain text, no
    JSON wrapper — and `client.decode_jwt_exp()` base64-decodes its middle segment on EVERY
    `authenticate()` call, including during cassette replay in CI (the component re-parses whatever
    body the cassette serves back). A generic `"REDACTED"` here would therefore crash replay inside
    `json.loads(base64.urlsafe_b64decode(...))`, so the replacement has to keep the three-segment
    shape and a decodable `exp` claim.

    The check is on the *body*, not the request URL, because `BaseSanitizer.before_record_response`
    is handed only the response — the originating request is not available here.

    `scrub_before_read` stays at its default `False` (cassette-only): during the live recording run
    the component must keep using the REAL token to make its subsequent calls. Only the bytes
    written to the cassette file are synthetic.
    """

    _PREFIX = "Bearer "

    def before_record_response(self, response: dict) -> dict:
        body = response.get("body")
        if not isinstance(body, dict):
            return response

        raw = body.get("string")
        if isinstance(raw, bytes):
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                return response
            encode_back = True
        elif isinstance(raw, str):
            text = raw
            encode_back = False
        else:
            return response

        # `client.authenticate()` does `response.text.strip()`, so tolerate surrounding whitespace.
        if not text.lstrip().startswith(self._PREFIX):
            return response

        replacement = _synthetic_bearer_token()
        body["string"] = replacement.encode("utf-8") if encode_back else replacement
        # The synthetic token is a different length from the real one. vcrpy's replay stub reads
        # the body from a buffer rather than honouring this header, so a stale value is most
        # likely harmless — but a cassette whose declared length contradicts its body is a trap
        # for any future reader or tool, so fix it.
        _sync_content_length(response.get("headers"), len(replacement.encode("utf-8")))
        return response


class RequestContentLengthSanitizer(BaseSanitizer):
    """Resync a recorded REQUEST's `Content-Length` with the body the sanitizers ahead of it left.

    The response side of this problem is handled inside `BearerTokenBodySanitizer`; this is the
    request side, and here a stale length is not merely untidy. `DefaultSanitizer` rewrites the
    token POST body to `{"useremail": "REDACTED", "userpassword": "REDACTED", "environment": "us",
    "tenant": "REDACTED"}` but keeps `content-length` (it is on its safe-header whitelist) at the
    PRE-redaction value, so the difference between the declared length and the redacted body's
    real length is exactly the combined length of the three original values. The
    deliberately-wrong-credential recordings (`02`/`04`/`07`) pin the username and password to
    publicly-known dummies, which makes the real tenant's character count recoverable by
    subtraction against the success recordings. A character count is not identifying by itself,
    but "leaking the tenant name is structurally impossible" is the guarantee the block above
    exists to make, and an arithmetic side channel is not that.

    Ordered LAST in `VCR_SANITIZERS`: `CompositeSanitizer` applies sanitizers in list order, so
    running last is what guarantees this one measures the FINAL body rather than an intermediate
    one — true for today's chain and for any body rewrite added to it later.

    `scrub_before_read` stays at its default `False`: this only ever touches cassette bytes, and
    the live request the component actually sends is never reshaped by it.
    """

    def before_record_request(self, request: Any) -> Any:
        length = _body_byte_length(getattr(request, "body", None))
        if length is not None:
            _sync_content_length(getattr(request, "headers", None), length)
        return request


VCR_SANITIZERS = [
    # 1. Field-name redaction for the token REQUEST body, which `client.authenticate()` posts as
    #    `{"useremail", "userpassword", "environment", "tenant"}`. `DefaultSanitizer` matches keys
    #    EXACTLY and its built-in defaults cover `password`/`token` but NOT `userpassword` /
    #    `useremail` — this API's actual field names. The password would usually also be caught by
    #    the recorder's automatic exact-value pass over `#`-prefixed secrets, but `username` and
    #    `tenant` are not `#`-prefixed in `secrets.json`, so without these entries they would get
    #    no protection at all. Redacting by NAME also means it still works for the deliberately
    #    -wrong-credentials recordings, where the values are not the real ones.
    #
    #    Note the header whitelist is deliberately left at its default (`content-type`,
    #    `content-length`, `accept`): that is what strips the `Authorization: Bearer <jwt>` request
    #    header from every non-token call. Do not add it to `additional_safe_headers`.
    DefaultSanitizer(additional_sensitive_fields=["userpassword", "useremail", "tenant"]),
    # 2. The tenant is a real customer identifier and it sits in the URL PATH of every
    #    DataAccessAPI call (`client.py`: `https://{host}/DataAccessAPI/{tenant}/api/`), not just in
    #    the token body. The default `match_on` includes `path`, so the rewrite has to be identical
    #    on the recorded and the replayed side — hence a GENERIC pattern with a fixed replacement
    #    (hardcoding the real tenant here would itself leak it into this public repo) plus a
    #    `"tenant"` placeholder in every test config so replay produces the same path.
    UrlPatternSanitizer(patterns=[(r"/DataAccessAPI/[^/]+/", "/DataAccessAPI/tenant/")]),
    # 3. Structurally valid synthetic JWT for the plain-text token response — see the class docstring.
    BearerTokenBodySanitizer(),
    # 4. Content-Length fix-up for the request bodies entry 1 shortened. MUST STAY LAST — see the
    #    class docstring; it measures whatever body the preceding sanitizers ended up with.
    RequestContentLengthSanitizer(),
]


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
        except ijson.JSONError as e:
            # `ijson.JSONError` is NOT a `requests.exceptions.RequestException` subclass (it's a
            # plain `Exception`), so it needs its own clause: a malformed/truncated `paging/paged`
            # body must still fail this row's job as a `UserException` (exit 1), not fall through
            # to `__main__`'s bare `except Exception` (exit 2, "unexpected internal bug") — a
            # malformed response from the source API is a user-visible, not a code, problem.
            raise UserException(f"Failed to fetch table '{cfg.table}': received a malformed response.") from e

    @staticmethod
    def _describe_request_failure(prefix: str, error: requests.exceptions.RequestException) -> str:
        """Delegates to `client.describe_request_error` — see its docstring for exactly which
        failure shapes (timeout vs HTTP status vs connection vs retries-exhausted) map to which
        wording."""
        return describe_request_error(prefix, error)

    def _build_authenticated_client(self, cfg: RootConfig) -> RetainCloudClient:
        """Construct and authenticate a fresh client for the calling entrypoint.

        Deliberately NOT hoisted into `__init__` and cached: `test_connection` reports a bad
        credential as a clean `UserException` result for that one sync action, which requires the
        construct-then-authenticate call to happen inside the entrypoint that owns it — building
        (or authenticating) the client any earlier, e.g. in `__init__`, would tie every sync
        action's failure reporting to whichever entrypoint happened to run first, instead of each
        one owning its own client and its own outcome. `run()` and `list_tables()` reuse this same
        helper for consistency, at the cost of one extra client object per invocation — cheap, and
        correctness here matters more than saving that one allocation.
        """
        client = RetainCloudClient(
            environment=cfg.environment.value,
            tenant=cfg.tenant,
            username=cfg.username,
            password=cfg.password.get_secret_value(),
        )
        client.authenticate()
        return client

    def _process_table(self, client: RetainCloudClient, cfg: Configuration) -> None:
        logger.debug(
            "Table %s: starting extraction (load_type=%s, page_size=%d).", cfg.table, cfg.load_type, cfg.page_size
        )
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
        #
        # `safe_column_name` shortens a name ONLY if it is over Storage's 64-char column-name cap
        # (Retain's `<table>_<field>` naming — FKs are `<table>_<ref>_guid` — routinely exceeds it,
        # e.g. `rolerequestresourcerejectreason_rolerequestpredefinedrejectreason_guid` = 70 chars),
        # which used to fail the WHOLE table at Storage import. This is applied here — the manifest
        # (Storage-facing) name — not in extractor.py, which keeps working with the API's real names
        # throughout (row lookups, PK verification): the CSV is headerless and positional, so
        # renaming a column at this stage never touches a single data byte, only its declared name.
        # The PK reference is shortened the same way so it always names whatever the matching
        # column actually ended up called in `fields` below.
        primary_keys = [safe_column_name(schema.pk_column)] if pk_unique and schema.pk_column else None
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
            output_name = safe_column_name(c.name)
            # The ORIGINAL name is preserved in the column's Storage metadata/description whenever it
            # was shortened, so it stays traceable back to the source field — never silently lost.
            description = f"Original Retain Cloud column name: {c.name}" if output_name != c.name else None
            fields.append(FieldSchema(name=output_name, base_type=base_type, nullable=True, description=description))
        logger.debug(
            "Table %s: built output schema with %d columns (primary_keys=%s); %d column name(s) shortened for "
            "Storage's 64-char limit.",
            schema.table,
            len(fields),
            primary_keys,
            sum(1 for c in schema.columns if safe_column_name(c.name) != c.name),
        )
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
        try:
            table_names = client.list_tables()
            labels_by_name = {row["name"]: row.get("alias") for row in client.list_table_labels()}
        except requests.exceptions.RequestException as e:
            # Without this, a non-401 HTTP failure here (or a retries-exhausted/connection outage)
            # would leak `HttpClient`'s raw exception message — including the internal API URL — to
            # the config UI, rather than the same clean `UserException` `run()` already gives for
            # the equivalent failure.
            raise UserException(self._describe_request_failure("Failed to load the table list", e)) from e
        except KeyError as e:
            # `row["name"]` above is a response-schema assumption, not an HTTP failure — a label
            # entry missing `name` must still fail as a clean `UserException` (exit 1), not escape
            # this `try` (KeyError isn't a `RequestException`) to `__main__`'s bare `except
            # Exception` (exit 2, "unexpected internal bug").
            raise UserException("Retain Cloud returned a table label entry without a 'name' field.") from e
        return [{"value": name, "label": labels_by_name.get(name) or name} for name in table_names]


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as e:
        # No `exc_info` here, deliberately: exit-1 messages are shown directly to the user, and a
        # `UserException` is by definition a clean, already-understood failure — a full stack trace
        # adds noise, not information. `logger.exception(...)` (full traceback) stays reserved for
        # the generic `except Exception` branch below, where a trace is the only diagnostic we have.
        logger.error("Component failed with a user error: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
