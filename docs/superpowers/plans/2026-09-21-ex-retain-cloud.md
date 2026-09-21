# keboola.ex-retain-cloud Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Revision note (same day):** this plan was rewritten after the spec's config shape changed from
> a single config with a `tables[]` multi-select to **config rows, one row per table**. If you are
> resuming a partially-executed run of the *previous* version of this plan, treat everything below
> as authoritative and re-check any already-completed task against it — the previous version's
> `Configuration` model (with `tables: list[str]`) and `run()` orchestration (a per-table loop) are
> both superseded.

**Goal:** Build the `keboola.ex-retain-cloud` extractor end-to-end on the `initial-implementation`
branch: a root config model + a row config model, a Retain Cloud DataAccessAPI client, a streaming
single-table fetch/typing algorithm, two sync actions (one root-level, one row-level), and a thin
`run()` orchestrator that handles exactly the one table its row selected — matching the committed
design spec.

**Architecture:** A `RetainCloudClient(keboola.http_client.HttpClient)` owns auth (JWT lifecycle,
environment→host map) and every HTTP call. A separate `extractor` module owns the per-table
streaming fetch (`ijson` over `POST .../paging/paged`), the `/tmp`-staged CSV write, per-column
native-type verification, and PK-uniqueness detection — unchanged by the config-shape revision,
since it always operated on one table at a time. `component.py` stays a thin orchestrator: parse
the merged config → authenticate → validate the row's one table → fetch it (schema → fetch → move →
manifest) → done. There is no per-table loop and no partial-failure bookkeeping — a table failure
now fails this row's job outright, which the platform already isolates from every other row.

**Tech Stack:** Python 3.14, `keboola-component`, `keboola-http-client` (already a dependency),
Pydantic v2, `ijson` (new dependency), standard library `csv`/`base64`/`json`. Tests: `pytest`,
`unittest.mock`, `freezegun` (all already dev dependencies) for unit tests; `keboola.datadirtest`
VCR functional tests are a separate delegated task (Task 8).

**Spec:** `docs/superpowers/specs/2026-09-21-ex-retain-cloud-design.md`

## Global Constraints

- Python `~=3.14.0` (per `pyproject.toml`); `uv run ruff check`, `uv run ruff format --check`,
  `uv run ty check`, and `uv run pytest` must all pass after every task (matches
  `.pre-commit-config.yaml`'s four local hooks — run all four, not just pytest).
- Never log or format the token/password into any log message or exception string (spec §3).
- `#password` config key maps to the Pydantic field `password` via `Field(alias="#password")`,
  typed `pydantic.SecretStr` (never plain `str`) so an accidental `str(config)`/`repr(config)`
  cannot leak it.
- `environment` → host is an explicit `dict[str, str]` map (`us`/`eu`/`aus` → `<env>.retaincloud.com`,
  `uk` → `app.retaincloud.com`) — never `f"https://{environment}.retaincloud.com"` (spec §3).
- **Two Pydantic models, matching the root/row schema split exactly (spec §6):** `RootConfig`
  (`environment`, `tenant`, `username`, `password`) with `extra="ignore"` — used for partial
  instantiation by sync actions that don't have (or don't need) the row's fields — and
  `Configuration(RootConfig)` (adds `table`, `load_type`, `page_size`) with `extra="forbid"` — used
  only by `run()`, where the platform guarantees a fully merged, fully valid row config. `load_type`
  is a REAL field on `Configuration` (row-level, default `full_load`) — it is **not** internal or
  omitted; only `fetch_mode` (`full_fetch`) is a pure code constant, never a model field at all.
- Every `paging/paged` response streams into a `/tmp` scratch file; a file only lands under
  `/data/out/tables/` after the row's fetch fully succeeds (spec §6 staging rule — this is a
  correctness requirement, not a style preference: `output-mapping.md` uploads everything under
  `/data/out/tables/` regardless of the output-mapping config).
- A table-level failure (missing from `structure`, `403`/`404`/exhausted-retry `5xx` on
  `richfieldstructure` or `paging/paged`) raises `UserException` in `run()` — it does **not** log a
  warning and continue. There is nothing else in this row's job to continue to; other tables' rows
  are separate container executions, already isolated by the platform.
- Manifests are built via `ComponentBase.create_out_table_definition_from_schema(TableSchema(...))`
  (verified in the installed `keboola-component` library, not assumed) — CSVs are written
  **headerless** (no `csv.DictWriter.writeheader()` call), matching that method's own default
  `has_header` behaviour when a schema is supplied (spec §6).
- No new dependency for JWT decoding — the `exp` claim is read via manual base64url decoding
  (spec §3/§6).

---

### Task 1: Configuration models — `RootConfig` and `Configuration`

**Files:**
- Modify: `src/configuration.py` (replace the cookiecutter placeholder entirely)
- Test: `tests/test_configuration.py` (new)

**Interfaces:**
- Produces: `class Environment(str, Enum)` (`us`, `eu`, `uk`, `aus`); `class LoadType(str, Enum)`
  (`full_load`, `incremental_load`); `class RootConfig(BaseModel)` with fields `environment:
  Environment`, `tenant: str`, `username: str`, `password: SecretStr` (alias `#password`), `extra=
  "ignore"`; `class Configuration(RootConfig)` adding `table: str`, `load_type: LoadType =
  LoadType.full_load`, `page_size: int = 20000`, `extra="forbid"`; property `Configuration.incremental:
  bool` (`load_type == LoadType.incremental_load`). Both raise `keboola.component.exceptions.
  UserException` on a Pydantic `ValidationError`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_configuration.py
import unittest

from keboola.component.exceptions import UserException
from pydantic import SecretStr

from configuration import Configuration, Environment, LoadType, RootConfig

ROOT_PARAMS = {
    "environment": "us",
    "tenant": "acme",
    "username": "svc@example.com",
    "#password": "secret-value",
}
ROW_PARAMS = {**ROOT_PARAMS, "table": "booking"}


class TestRootConfig(unittest.TestCase):
    def test_valid_root_config_parses(self):
        cfg = RootConfig(**ROOT_PARAMS)
        self.assertEqual(cfg.environment, Environment.us)
        self.assertIsInstance(cfg.password, SecretStr)
        self.assertEqual(cfg.password.get_secret_value(), "secret-value")

    def test_tolerates_extra_row_fields_present_in_merged_config(self):
        # Simulates the platform handing a row-level sync action the merged root+row-draft
        # parameters, where row fields may be present even though RootConfig doesn't need them.
        cfg = RootConfig(**{**ROOT_PARAMS, "table": "booking", "load_type": "full_load", "page_size": 20000})
        self.assertEqual(cfg.tenant, "acme")

    def test_tolerates_missing_table_field_entirely(self):
        # The exact scenario list_tables hits on a brand-new row: `table` isn't set yet at all.
        cfg = RootConfig(**ROOT_PARAMS)  # no KeyError / ValidationError despite no `table` key
        self.assertEqual(cfg.username, "svc@example.com")

    def test_missing_required_root_field_raises_user_exception(self):
        params = dict(ROOT_PARAMS)
        del params["tenant"]
        with self.assertRaises(UserException):
            RootConfig(**params)

    def test_invalid_environment_raises_user_exception(self):
        with self.assertRaises(UserException):
            RootConfig(**{**ROOT_PARAMS, "environment": "ca"})


class TestConfiguration(unittest.TestCase):
    def test_valid_row_config_parses(self):
        cfg = Configuration(**ROW_PARAMS)
        self.assertEqual(cfg.table, "booking")
        self.assertEqual(cfg.page_size, 20000)
        self.assertEqual(cfg.load_type, LoadType.full_load)
        self.assertFalse(cfg.incremental)

    def test_incremental_load_type(self):
        cfg = Configuration(**{**ROW_PARAMS, "load_type": "incremental_load"})
        self.assertTrue(cfg.incremental)

    def test_missing_table_raises_user_exception(self):
        with self.assertRaises(UserException):
            Configuration(**ROOT_PARAMS)  # no `table` — this is the strict, run()-time model

    def test_extra_unknown_field_rejected(self):
        with self.assertRaises(UserException):
            Configuration(**{**ROW_PARAMS, "unexpected_field": "x"})

    def test_password_never_appears_in_string_representation(self):
        cfg = Configuration(**ROW_PARAMS)
        self.assertNotIn("secret-value", str(cfg))
        self.assertNotIn("secret-value", repr(cfg))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_configuration.py -v`
Expected: `ImportError`/`ModuleNotFoundError` — `RootConfig`/`Configuration`/`Environment`/`LoadType`
don't exist yet (`configuration.py` still has the cookiecutter placeholder fields).

- [ ] **Step 3: Replace `src/configuration.py`**

```python
"""Pydantic configuration models for keboola.ex-retain-cloud.

Two models, matching the root/row `configSchema.json` / `configRowSchema.json` split exactly
(spec §5/§6):

- `RootConfig` — the four shared connection fields. Deliberately `extra="ignore"`: this model is
  also used for *partial* instantiation by sync actions that only need the connection fields
  (`test_connection`, and `list_tables` before a row's `table` is chosen) — it must tolerate
  whatever row-level keys happen to be present, absent, or blank in the merged parameters handed to
  a sync action, per the "partial instantiation only required when a sync action needs fewer fields
  than run()" pattern.
- `Configuration(RootConfig)` — adds the three row fields. `extra="forbid"`: used only by `run()`,
  where the platform guarantees the merged config is complete and valid, so unexpected keys should
  be treated as a real problem, not silently ignored.

`fetch_mode` is deliberately NOT a field on either model — V1 only implements `full_fetch` (spec
§2), and that fact lives as the `FETCH_MODE_FULL_FETCH` constant in `extractor.py`, not as a
user-configurable or even internally-modeled value here.
"""

import logging
from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

logger = logging.getLogger(__name__)


class Environment(str, Enum):
    us = "us"
    eu = "eu"
    uk = "uk"
    aus = "aus"


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


def _raise_user_exception(e: ValidationError) -> None:
    error_messages = [f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()]
    raise UserException(f"Configuration error: {', '.join(error_messages)}") from e


class RootConfig(BaseModel):
    """The shared connection fields — root config, per spec §5."""

    model_config = ConfigDict(extra="ignore")

    environment: Environment
    tenant: str
    username: str
    password: SecretStr = Field(alias="#password")

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            _raise_user_exception(e)


class Configuration(RootConfig):
    """The fully merged root+row config used by `run()` — per spec §5."""

    model_config = ConfigDict(extra="forbid")

    table: str
    load_type: LoadType = LoadType.full_load
    page_size: int = 20000

    @property
    def incremental(self) -> bool:
        """True when this row selected Incremental Load — the per-run PK-safety fallback (spec
        §2/§6) is applied later in `component.py`, not here."""
        return self.load_type == LoadType.incremental_load
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_configuration.py -v`
Expected: PASS (11 tests).

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/ && uv run ty check`

- [ ] **Step 6: Commit**

```bash
git add src/configuration.py tests/test_configuration.py
git commit -m "feat: add root/row configuration models for Retain Cloud"
```

---

### Task 2: `RetainCloudClient` — host map, JWT lifecycle, discovery, and the raw paging call

Unaffected by the config-shape revision — this task is identical to the prior plan version.

**Files:**
- Create: `src/client.py`
- Test: `tests/test_client.py`

**Interfaces:**
- Consumes: nothing from Task 1 directly (constructed from plain strings, not a `Configuration`
  instance, so it stays testable without Pydantic in the loop).
- Produces: `ENVIRONMENT_HOSTS: dict[str, str]`; `decode_jwt_exp(token_body: str) -> int`; `class
  RetainCloudClient(HttpClient)` with `__init__(self, environment: str, tenant: str, username: str,
  password: str)`, `authenticate() -> None`, `list_tables() -> list[str]`, `list_table_labels() ->
  list[dict]`, `get_table_schema(table: str) -> list[dict]`, `fetch_table_page(table: str,
  page_size: int) -> requests.Response` (raw, streamed, `response.raw.decode_content = True` already
  set). Task 3 consumes `fetch_table_page`'s returned response directly.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_client.py
import base64
import json
import time
import unittest
from unittest import mock

import requests

from client import ENVIRONMENT_HOSTS, RetainCloudClient, decode_jwt_exp


def _fake_jwt(exp: int) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).rstrip(b"=").decode()
    return f"Bearer {header}.{payload}.sig"


def _response(status_code=200, text="", json_body=None, raw=None):
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = json_body
    resp.raw = raw
    if status_code >= 400:
        http_error = requests.HTTPError(response=resp)
        resp.raise_for_status.side_effect = http_error
    else:
        resp.raise_for_status.side_effect = None
    return resp


class TestEnvironmentHosts(unittest.TestCase):
    def test_uk_does_not_use_naive_interpolation(self):
        self.assertEqual(ENVIRONMENT_HOSTS["uk"], "app.retaincloud.com")

    def test_all_four_environments_present(self):
        self.assertEqual(set(ENVIRONMENT_HOSTS), {"us", "eu", "uk", "aus"})


class TestDecodeJwtExp(unittest.TestCase):
    def test_reads_exp_claim(self):
        token = _fake_jwt(exp=1234567890)
        self.assertEqual(decode_jwt_exp(token), 1234567890)


class TestRetainCloudClientAuth(unittest.TestCase):
    def setUp(self):
        self.client = RetainCloudClient("us", "acme", "user@example.com", "pw")

    def test_base_url_uses_host_map_not_naive_interpolation(self):
        uk_client = RetainCloudClient("uk", "acme", "user@example.com", "pw")
        self.assertTrue(uk_client.base_url.startswith("https://app.retaincloud.com/"))

    @mock.patch.object(RetainCloudClient, "post_raw")
    def test_authenticate_stores_token_and_exp(self, mock_post_raw):
        exp = int(time.time()) + 3600
        mock_post_raw.return_value = _response(status_code=200, text=_fake_jwt(exp))
        self.client.authenticate()
        self.assertEqual(self.client._token_exp, exp)
        self.assertEqual(self.client._auth_header["Authorization"], _fake_jwt(exp))

    @mock.patch.object(RetainCloudClient, "post_raw")
    def test_authenticate_sends_credentials_not_environment_host(self, mock_post_raw):
        mock_post_raw.return_value = _response(status_code=200, text=_fake_jwt(int(time.time()) + 3600))
        self.client.authenticate()
        _, kwargs = mock_post_raw.call_args
        self.assertEqual(
            kwargs["json"],
            {"useremail": "user@example.com", "userpassword": "pw", "environment": "us", "tenant": "acme"},
        )

    @mock.patch.object(RetainCloudClient, "post_raw")
    def test_authenticate_raises_user_exception_on_401(self, mock_post_raw):
        from keboola.component.exceptions import UserException

        mock_post_raw.return_value = _response(status_code=401)
        with self.assertRaises(UserException):
            self.client.authenticate()

    @mock.patch.object(RetainCloudClient, "authenticate")
    def test_ensure_token_reauthenticates_when_near_expiry(self, mock_authenticate):
        self.client._token_exp = int(time.time()) + 60  # under the 5-minute margin
        self.client._ensure_token()
        mock_authenticate.assert_called_once()

    @mock.patch.object(RetainCloudClient, "authenticate")
    def test_ensure_token_skips_reauthentication_when_fresh(self, mock_authenticate):
        self.client._token_exp = int(time.time()) + 3600
        self.client._ensure_token()
        mock_authenticate.assert_not_called()


class TestRetainCloudClientDiscovery(unittest.TestCase):
    def setUp(self):
        self.client = RetainCloudClient("us", "acme", "user@example.com", "pw")
        self.client._token_exp = int(time.time()) + 3600  # skip auth in these tests

    @mock.patch.object(RetainCloudClient, "get")
    def test_list_tables(self, mock_get):
        mock_get.return_value = ["booking", "resource"]
        self.assertEqual(self.client.list_tables(), ["booking", "resource"])
        mock_get.assert_called_once_with("structure")

    @mock.patch.object(RetainCloudClient, "get")
    def test_get_table_schema(self, mock_get):
        mock_get.return_value = [{"name": "booking_guid", "dataType": "ID"}]
        result = self.client.get_table_schema("booking")
        self.assertEqual(result, [{"name": "booking_guid", "dataType": "ID"}])
        mock_get.assert_called_once_with("structure/richfieldstructure", params={"table": "booking"})

    @mock.patch.object(RetainCloudClient, "authenticate")
    @mock.patch.object(RetainCloudClient, "get")
    def test_discovery_reauthenticates_once_on_401_then_retries(self, mock_get, mock_authenticate):
        unauthorized = requests.HTTPError(response=_response(status_code=401))
        mock_get.side_effect = [unauthorized, ["booking"]]
        result = self.client.list_tables()
        self.assertEqual(result, ["booking"])
        mock_authenticate.assert_called_once()
        self.assertEqual(mock_get.call_count, 2)


class TestFetchTablePage(unittest.TestCase):
    def setUp(self):
        self.client = RetainCloudClient("us", "acme", "user@example.com", "pw")
        self.client._token_exp = int(time.time()) + 3600

    @mock.patch.object(RetainCloudClient, "post_raw")
    def test_fetch_table_page_streams_and_sets_decode_content(self, mock_post_raw):
        raw = mock.Mock()
        raw.decode_content = False
        mock_post_raw.return_value = _response(status_code=200, raw=raw)
        response = self.client.fetch_table_page("booking", 20000)
        _, kwargs = mock_post_raw.call_args
        self.assertEqual(kwargs["params"], {"pageSize": 20000, "sequential": "true"})
        self.assertTrue(kwargs["stream"])
        self.assertTrue(response.raw.decode_content)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_client.py -v`
Expected: `ModuleNotFoundError: No module named 'client'`.

- [ ] **Step 3: Write `src/client.py`**

```python
"""HTTP client for the Retain Cloud DataAccessAPI."""

import base64
import json
import logging
import time

import requests
from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient

logger = logging.getLogger(__name__)

ENVIRONMENT_HOSTS: dict[str, str] = {
    "us": "us.retaincloud.com",
    "eu": "eu.retaincloud.com",
    "uk": "app.retaincloud.com",  # NOT uk.retaincloud.com — that hostname does not resolve
    "aus": "aus.retaincloud.com",
}

_TOKEN_REFRESH_MARGIN_SECONDS = 300


def decode_jwt_exp(token_body: str) -> int:
    """Read the `exp` claim out of a `Bearer <jwt>` string, without verifying its signature.

    We trust our own freshly-issued token — this is a local read of a claim we already own, not
    validation of a token from an untrusted third party — so no JWT library is needed for this.
    """
    jwt = token_body.removeprefix("Bearer ").strip()
    payload_segment = jwt.split(".")[1]
    padding = "=" * (-len(payload_segment) % 4)
    payload = json.loads(base64.urlsafe_b64decode(payload_segment + padding))
    return int(payload["exp"])


class RetainCloudClient(HttpClient):
    def __init__(self, environment: str, tenant: str, username: str, password: str):
        host = ENVIRONMENT_HOSTS[environment]
        base_url = f"https://{host}/DataAccessAPI/{tenant}/api/"
        super().__init__(
            base_url=base_url,
            max_retries=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
        )
        self._host = host
        self._environment = environment
        self._tenant = tenant
        self._username = username
        self._password = password
        self._token_exp = 0

    def authenticate(self) -> None:
        """POST the credentials to `IntegrationApi/token` and store the resulting bearer token.

        Never logs `self._password` or the response body — only the HTTP status on failure.
        """
        token_url = f"https://{self._host}/IntegrationApi/token"
        response = self.post_raw(
            token_url,
            is_absolute_path=True,
            ignore_auth=True,
            json={
                "useremail": self._username,
                "userpassword": self._password,
                "environment": self._environment,
                "tenant": self._tenant,
            },
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            raise UserException(f"Retain Cloud authentication failed (HTTP {status}).") from e

        token_body = response.text.strip()
        self._token_exp = decode_jwt_exp(token_body)
        self.update_auth_header({"Authorization": token_body}, overwrite=True)

    def _ensure_token(self) -> None:
        if self._token_exp - time.time() < _TOKEN_REFRESH_MARGIN_SECONDS:
            self.authenticate()

    def _get_with_reauth(self, path: str, **kwargs):
        try:
            return self.get(path, **kwargs)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                self.authenticate()
                return self.get(path, **kwargs)
            raise

    def list_tables(self) -> list[str]:
        self._ensure_token()
        return self._get_with_reauth("structure")

    def list_table_labels(self) -> list[dict]:
        self._ensure_token()
        return self._get_with_reauth("structure/tablestructure")

    def get_table_schema(self, table: str) -> list[dict]:
        self._ensure_token()
        return self._get_with_reauth("structure/richfieldstructure", params={"table": table})

    def fetch_table_page(self, table: str, page_size: int) -> requests.Response:
        """Issue one `paging/paged` call and return the raw, streamable response.

        The caller (`extractor.py`) is responsible for consuming `response.raw` with `ijson` — this
        method never reads the body itself, so the "one/two calls per table" contract (spec §6)
        stays entirely in the caller's hands.
        """
        self._ensure_token()
        params = {"pageSize": page_size, "sequential": "true"}
        path = f"tableaccess/{table}/paging/paged"
        response = self.post_raw(path, params=params, stream=True)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                self.authenticate()
                response = self.post_raw(path, params=params, stream=True)
                response.raise_for_status()
            else:
                raise
        # requests does not auto-decompress `response.raw` the way it does `.content`/`.json()` —
        # without this, a gzip-compressed body would be handed to ijson as garbled raw bytes.
        response.raw.decode_content = True
        return response
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_client.py -v`
Expected: PASS (all cases above).

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/ && uv run ty check`

- [ ] **Step 6: Commit**

```bash
git add src/client.py tests/test_client.py
git commit -m "feat: add RetainCloudClient with JWT lifecycle and discovery calls"
```

---

### Task 3: Table schema construction + streaming fetch (`extractor.py`)

Unaffected by the config-shape revision — this task is identical to the prior plan version. It is
the core algorithm from spec §6: the two-call `paging/paged` contract, `/tmp` staging, PK
uniqueness, and per-column native-type verification. It consumes `RetainCloudClient.fetch_table_page`
(Task 2) and `RetainCloudClient.get_table_schema` (Task 2), and is consumed by `component.py`
(Task 5).

**Files:**
- Create: `src/extractor.py`
- Modify: `pyproject.toml` (add `ijson` dependency)
- Test: `tests/test_extractor.py`

**Interfaces:**
- Consumes: `RetainCloudClient.get_table_schema(table) -> list[dict]`,
  `RetainCloudClient.fetch_table_page(table, page_size) -> requests.Response` (Task 2).
- Produces: `@dataclass ColumnSchema(name: str, declared_type: str, base_type: SupportedDataTypes)`;
  `@dataclass TableSchema_(table: str, columns: list[ColumnSchema], pk_column: str | None)` (named
  `TableSchema_` to avoid colliding with `keboola.component.table_schema.TableSchema`, which
  `component.py` also imports in Task 5 — component.py converts one into the other); `@dataclass
  FetchResult(scratch_path: Path, row_count: int, pk_unique: bool)`;
  `build_table_schema(table: str, rich_fields: list[dict]) -> TableSchema_`; `fetch_table(client:
  RetainCloudClient, table: str, schema: TableSchema_, page_size: int, scratch_dir: Path) ->
  FetchResult` (raises `requests.HTTPError` / `ijson.JSONError` on failure — callers must catch).

- [ ] **Step 1: Add the `ijson` dependency**

Edit `pyproject.toml`'s `dependencies` list:

```toml
dependencies = [
    "keboola-component>=1.10.0",
    "keboola-http-client>=1.0.1",
    "keboola-utils>=1.1.0",
    "pydantic>=2.11.7",
    "ijson>=3.2.3",
]
```

Run: `uv sync` (updates `uv.lock`).

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_extractor.py
import io
import json
import unittest
from pathlib import Path
from unittest import mock

import requests
from keboola.component.dao import SupportedDataTypes

from extractor import build_table_schema, fetch_table


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
        self.assertIn('{"k":1}', content)  # nested object serialized as compact JSON
        self.assertNotIn("booking_guid", content)  # headerless — no header row

    def test_second_call_issued_when_first_call_is_short(self):
        first_rows = [{"booking_guid": "a", "booking_hours": 1, "booking_rate": 1.0,
                        "booking_active": True, "booking_createdon": "2026-01-01T00:00:00Z",
                        "booking_notes": "x", "booking_meta": None}]
        second_rows = first_rows + [
            {"booking_guid": "b", "booking_hours": 2, "booking_rate": 2.0, "booking_active": False,
             "booking_createdon": "2026-01-02T00:00:00Z", "booking_notes": "y", "booking_meta": None},
            {"booking_guid": "c", "booking_hours": 3, "booking_rate": 3.0, "booking_active": True,
             "booking_createdon": "2026-01-03T00:00:00Z", "booking_notes": "z", "booking_meta": None},
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

    def test_pk_not_unique_falls_back_to_no_pk(self):
        rows = [
            {"booking_guid": "dup", "booking_hours": 1, "booking_rate": 1.0, "booking_active": True,
             "booking_createdon": "2026-01-01T00:00:00Z", "booking_notes": "x", "booking_meta": None},
            {"booking_guid": "dup", "booking_hours": 2, "booking_rate": 2.0, "booking_active": False,
             "booking_createdon": "2026-01-02T00:00:00Z", "booking_notes": "y", "booking_meta": None},
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=2, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        self.assertFalse(result.pk_unique)

    def test_int_column_downgraded_to_string_on_non_numeric_value(self):
        rows = [
            {"booking_guid": "a", "booking_hours": "DELIVERED", "booking_rate": 1.0, "booking_active": True,
             "booking_createdon": "2026-01-01T00:00:00Z", "booking_notes": "x", "booking_meta": None},
        ]
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=1, rows=rows)

        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        downgraded = {name for name, ok in result.verified_columns.items() if not ok}
        self.assertIn("booking_hours", downgraded)

    def test_empty_table_still_produces_a_file(self):
        client = mock.Mock()
        client.fetch_table_page.return_value = _envelope_response(row_count=0, rows=[])
        result = fetch_table(client, "booking", self.schema, page_size=20000, scratch_dir=self.scratch_dir)
        self.assertEqual(result.row_count, 0)
        self.assertTrue(result.scratch_path.exists())


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_extractor.py -v`
Expected: `ModuleNotFoundError: No module named 'extractor'`.

- [ ] **Step 4: Write `src/extractor.py`**

```python
"""Per-table streaming fetch + native-type verification for keboola.ex-retain-cloud.

Implements the resolved `paging/paged` contract (spec §6): `pageSize` is a single-call row cap, not
a page window. Every response streams into a `/tmp` scratch file — never directly into
`/data/out/tables/` — so a failed second call never leaves a partial/truncated file for Storage to
upload (see the spec's "Corrected staging rule").

`FETCH_MODE_FULL_FETCH` is the one internal-only constant referenced by `component.py` — spec §2's
sanctioned Fetch-Mode omission. It is not a Pydantic field on any model (unlike `load_type`, which
is a real row-level field — see `configuration.py`).
"""

import csv
import json
import logging
import math
from dataclasses import dataclass, field
from pathlib import Path

import ijson
import requests
from keboola.component.dao import SupportedDataTypes

logger = logging.getLogger(__name__)

FETCH_MODE_FULL_FETCH = "full_fetch"

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


_BOOL_TOKENS = {True, False, "true", "false", "True", "False", "1", "0", 1, 0}


def _coerces(declared_type: str, value) -> bool:
    if value is None:
        return True
    if declared_type == "Int":
        try:
            int(value)
            return True
        except (TypeError, ValueError):
            return False
    if declared_type == "Float":
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            return False
    if declared_type == "Bool":
        return value in _BOOL_TOKENS
    return True


def _stringify(value) -> str:
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
                logger.warning("Table %s: row carries fields outside the discovered schema: %s", schema.table, sorted(extra_keys))
                schema_drift_logged = True
            for name in verified:
                if verified[name] and not _coerces(declared_by_name[name], row.get(name)):
                    verified[name] = False
                    logger.warning("Table %s column %s: value did not match declared type %s, downgrading to STRING", schema.table, name, declared_by_name[name])
            writer.writerow([_stringify(row.get(name)) for name in fieldnames])
            if schema.pk_column:
                pk_values.add(row.get(schema.pk_column))

    pk_unique = schema.pk_column is not None and len(pk_values) == row_count
    return FetchResult(scratch_path=csv_path, row_count=row_count, pk_unique=pk_unique, verified_columns=verified), row_count_total


def fetch_table(client, table: str, schema: TableSchema_, page_size: int, scratch_dir: Path) -> FetchResult:
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
            table, result.row_count, row_count_total_2,
        )
    return result
```

**Note for the implementer:** `test_second_call_issued_when_first_call_is_short` asserts
`content.count("\n") == 3`, i.e. exactly the second call's 3 rows survive in the scratch file — this
is the regression test for the staging-rule bug the grounding-reconciliation pass caught (an earlier
draft could have left both calls' rows concatenated). `_stream_to_csv` opens the file with mode
`"w"` (not `"a"`) on every call, so the second call's write always starts from an empty file — this
is what makes the "discard and reopen" step of the algorithm correct without any explicit `truncate()` call.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_extractor.py -v`
Expected: PASS. If `ijson.ObjectBuilder` fails to import on the installed `ijson` version, check
`import ijson; ijson.ObjectBuilder` interactively first — this was verified against `ijson` 3.x
during planning; if a materially different major version resolves via `uv sync`, adjust the import
(`ijson.common.ObjectBuilder` is the fallback location) but do not change the parsing algorithm
itself.

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/ && uv run ty check`

- [ ] **Step 7: Commit**

```bash
git add src/extractor.py tests/test_extractor.py pyproject.toml uv.lock
git commit -m "feat: add streaming per-table fetch with native-type verification"
```

---

### Task 4: Sync actions — root-level `test_connection` and row-level `list_tables`

**Files:**
- Modify: `src/component.py` (add sync-action methods; full `run()` rewrite is Task 5)
- Test: `tests/test_sync_actions.py`

**Interfaces:**
- Consumes: `RootConfig` (Task 1), `RetainCloudClient` (Task 2).
- Produces: `Component.test_connection(self) -> None` (raises `UserException` on failure, per the
  `keboola.component` sync-action convention — the framework serializes success/exception into the
  sync-action response), `Component.list_tables(self) -> list[dict]` returning `[{"value": <raw
  table name>, "label": <alias or raw name>}, ...]`. Both parse `RootConfig`, **not**
  `Configuration` — this is the load-bearing fix from spec §6: `list_tables` must work even when the
  row's `table` field isn't set yet.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_sync_actions.py
import unittest
from unittest import mock

from keboola.component.exceptions import UserException

from component import Component

ROOT_PARAMS = {
    "environment": "us",
    "tenant": "acme",
    "username": "svc@example.com",
    "#password": "pw",
}


def _component(params):
    comp = Component()
    comp.configuration.parameters = params
    return comp


class TestTestConnection(unittest.TestCase):
    @mock.patch("component.RetainCloudClient")
    def test_success_does_not_raise(self, mock_client_cls):
        mock_client_cls.return_value.authenticate.return_value = None
        comp = _component(ROOT_PARAMS)
        comp.test_connection()  # must not raise

    @mock.patch("component.RetainCloudClient")
    def test_auth_failure_raises_user_exception(self, mock_client_cls):
        mock_client_cls.return_value.authenticate.side_effect = UserException("bad creds")
        comp = _component(ROOT_PARAMS)
        with self.assertRaises(UserException):
            comp.test_connection()


class TestListTablesSyncAction(unittest.TestCase):
    @mock.patch("component.RetainCloudClient")
    def test_works_before_table_field_is_set(self, mock_client_cls):
        # The exact scenario a brand-new row hits: `table` isn't in the merged params at all yet.
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking", "resource"]
        client.list_table_labels.return_value = [{"name": "booking", "alias": "Bookings"}]
        comp = _component(ROOT_PARAMS)  # no "table" key present

        options = comp.list_tables()  # must not raise a validation error

        self.assertIn({"value": "booking", "label": "Bookings"}, options)
        self.assertIn({"value": "resource", "label": "resource"}, options)  # no alias -> raw name

    @mock.patch("component.RetainCloudClient")
    def test_tolerates_partially_filled_row_fields(self, mock_client_cls):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        client.list_table_labels.return_value = []
        comp = _component({**ROOT_PARAMS, "table": "", "load_type": "full_load"})

        options = comp.list_tables()

        self.assertEqual(options, [{"value": "booking", "label": "booking"}])

    @mock.patch("component.RetainCloudClient")
    def test_auth_failure_raises_user_exception(self, mock_client_cls):
        mock_client_cls.return_value.authenticate.side_effect = UserException("bad creds")
        comp = _component(ROOT_PARAMS)
        with self.assertRaises(UserException):
            comp.list_tables()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_sync_actions.py -v`
Expected: `AttributeError: 'Component' object has no attribute 'test_connection'` (or similar — the
methods don't exist yet).

- [ ] **Step 3: Add the sync actions to `src/component.py`**

Add these imports and methods to the `Component` class (the full class, including `run()`, is
finalized in Task 5 — for this task, only add what's needed for the two sync actions to work in
isolation):

```python
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException

from client import RetainCloudClient
from configuration import RootConfig


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def _build_authenticated_client(self, cfg: RootConfig) -> RetainCloudClient:
        client = RetainCloudClient(
            environment=cfg.environment.value,
            tenant=cfg.tenant,
            username=cfg.username,
            password=cfg.password.get_secret_value(),
        )
        client.authenticate()
        return client

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
```

(`sync_action` decorator name/import path: confirm against the installed `keboola-component`
version — `from keboola.component.base import sync_action` is correct for `keboola-component>=1.10.0`
per this repo's `pyproject.toml`; if `ty`/`ruff` flags the import, check
`.venv/lib/python3.14/site-packages/keboola/component/base.py` for the exact export before changing
the import path.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_sync_actions.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/ && uv run ty check`

- [ ] **Step 6: Commit**

```bash
git add src/component.py tests/test_sync_actions.py
git commit -m "feat: add test_connection and list_tables sync actions"
```

---

### Task 5: `run()` orchestration — one row, one table, no loop

Significantly simpler than the prior single-config plan version: there is exactly one table to
handle per execution, so there is no per-table loop, no `failed_tables` bookkeeping, and no
"some/all tables failed" branching — a table failure now raises `UserException` directly, and the
platform's per-row isolation does the rest.

**Files:**
- Modify: `src/component.py` (replace the placeholder `run()` entirely; keep the sync actions from
  Task 4)
- Modify: `data/config.json` (local dev fixture — replace the cookiecutter placeholder parameters
  with a single merged root+row config, since that's the shape the component actually receives)
- Test: `tests/test_run.py`

**Interfaces:**
- Consumes: `Configuration` (Task 1), `RetainCloudClient` (Task 2), `build_table_schema` /
  `fetch_table` / `TableSchema_` / `FetchResult` (Task 3).
- Produces: `Component.run(self) -> None` — the only entry point the platform calls; no other module
  calls `run()` directly.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_run.py
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
        comp = Component()
        comp.configuration.parameters = dict(params or ROW_PARAMS)
        return comp

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_table_succeeds(self, mock_client_cls, mock_build_schema, mock_fetch_table):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking", "resource"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = lambda _client, table, _schema, _page_size, scratch_dir: _fetch_result(scratch_dir, table)

        comp = self._component()
        with mock.patch.object(type(comp), "_data_dir", new_callable=mock.PropertyMock, return_value=str(self.data_dir)):
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
        with mock.patch.object(type(comp), "_data_dir", new_callable=mock.PropertyMock, return_value=str(self.data_dir)):
            with self.assertRaises(UserException):
                comp.run()

        self.assertFalse((self.data_dir / "out" / "tables" / "booking.csv").exists())

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_table_missing_from_structure_raises_user_exception(self, mock_client_cls, mock_build_schema, mock_fetch_table):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["resource"]  # "booking" was selected but no longer exists

        comp = self._component()
        with mock.patch.object(type(comp), "_data_dir", new_callable=mock.PropertyMock, return_value=str(self.data_dir)):
            with self.assertRaises(UserException):
                comp.run()

        mock_build_schema.assert_not_called()
        mock_fetch_table.assert_not_called()

    @mock.patch("component.fetch_table")
    @mock.patch("component.build_table_schema")
    @mock.patch("component.RetainCloudClient")
    def test_incremental_load_falls_back_when_pk_not_verified(self, mock_client_cls, mock_build_schema, mock_fetch_table):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        mock_build_schema.return_value = _schema("booking")
        mock_fetch_table.side_effect = lambda _client, table, _schema, _page_size, scratch_dir: _fetch_result(
            scratch_dir, table, pk_unique=False
        )

        comp = self._component({**ROW_PARAMS, "load_type": "incremental_load"})
        captured = {}
        original = comp.create_out_table_definition_from_schema

        def capture(table_schema, **kwargs):
            captured["incremental"] = kwargs.get("incremental")
            return original(table_schema, **kwargs)

        with (
            mock.patch.object(type(comp), "_data_dir", new_callable=mock.PropertyMock, return_value=str(self.data_dir)),
            mock.patch.object(comp, "create_out_table_definition_from_schema", side_effect=capture),
        ):
            comp.run()  # must NOT raise — this is a fallback, not a failure

        self.assertFalse(captured["incremental"])  # fell back to full load for this run

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
        captured = {}
        original = comp.create_out_table_definition_from_schema

        def capture(table_schema, **kwargs):
            captured["incremental"] = kwargs.get("incremental")
            return original(table_schema, **kwargs)

        with (
            mock.patch.object(type(comp), "_data_dir", new_callable=mock.PropertyMock, return_value=str(self.data_dir)),
            mock.patch.object(comp, "create_out_table_definition_from_schema", side_effect=capture),
        ):
            comp.run()

        self.assertTrue(captured["incremental"])


if __name__ == "__main__":
    unittest.main()
```

**Note for the implementer:** `_data_dir` is patched as a stand-in for however this `keboola-component`
version resolves `KBC_DATADIR` / the constructor's `data_path` argument — check
`ComponentBase.__init__`/`CommonInterface` in the installed library for the actual attribute/property
name before writing these tests for real (it may be `self.data_folder_path` or similar rather than
`_data_dir`); keep the test's *intent* (control where `/data/out/tables/` resolves to in the test
sandbox) and adjust the patched attribute name to match what's actually there.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_run.py -v`
Expected: failures — `run()` still has the cookiecutter placeholder body, not this component's logic.

- [ ] **Step 3: Replace `run()` in `src/component.py`**

```python
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
                "unique this run — falling back to full load for this run.", cfg.table,
            )

        table_def = self.create_out_table_definition_from_schema(
            self._to_output_schema(schema),
            incremental=incremental_for_table,
        )
        Path(table_def.full_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(result.scratch_path), table_def.full_path)
        self.write_manifest(table_def)

    @staticmethod
    def _to_output_schema(schema: TableSchema_) -> TableSchema:
        # The PK is declared whenever the column exists, regardless of incremental/full load —
        # Storage can still dedupe on it within a single load either way (output-mapping.md).
        primary_keys = [schema.pk_column] if schema.pk_column else None
        fields = [FieldSchema(name=c.name, base_type=c.base_type, nullable=True) for c in schema.columns]
        return TableSchema(name=schema.table, fields=fields, primary_keys=primary_keys)

    @sync_action("testConnection")
    def test_connection(self) -> None:
        cfg = RootConfig(**self.configuration.parameters)
        self._build_authenticated_client(cfg)

    @sync_action("list_tables")
    def list_tables(self) -> list[dict]:
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
```

- [ ] **Step 4: Update `data/config.json`** (local dev fixture — a single, fully merged root+row
  config, since that's the exact shape the component receives per `config-rows.md`; the
  cookiecutter placeholder still has `print_hello`/`#api_token`):

```json
{
  "storage": {
    "input": { "files": [], "tables": [] },
    "output": { "files": [], "tables": [] }
  },
  "parameters": {
    "environment": "us",
    "tenant": "acme",
    "username": "svc@example.com",
    "#password": "replace-with-a-real-test-password-locally-never-commit-one",
    "table": "booking",
    "page_size": 20000,
    "load_type": "full_load"
  }
}
```

(This file is a local dev convenience only — the actual VCR test fixtures live under
`tests/functional/` and are Task 8's responsibility, not this one's.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_run.py tests/test_sync_actions.py tests/test_configuration.py tests/test_client.py tests/test_extractor.py -v`
Expected: PASS across the whole new suite.

- [ ] **Step 6: Run the full existing suite too**

Run: `uv run pytest tests/ -v`
Expected: PASS, including the pre-existing `tests/test_component.py::test_run_no_cfg_fails` (it
should be unaffected — it never reaches config parsing).

- [ ] **Step 7: Lint and type-check**

Run: `uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/ && uv run ty check`

- [ ] **Step 8: Commit**

```bash
git add src/component.py data/config.json
git commit -m "feat: wire single-table run() orchestration for config-row execution"
```

---

### Task 6: Never-log-secrets regression test

A dedicated, explicit test for spec §3's "never log the token or password" requirement — the named,
confirmed incident risk. This deserves its own small task rather than being folded into Task 2/5,
since it's a cross-cutting property review will specifically look for, not a feature. Unaffected by
the config-shape revision.

**Files:**
- Test: `tests/test_no_secret_leakage.py`

**Interfaces:**
- Consumes: `RetainCloudClient` (Task 2), `Configuration` (Task 1). Produces nothing new — this task
  only adds tests.

- [ ] **Step 1: Write the test**

```python
# tests/test_no_secret_leakage.py
import logging
import unittest
from unittest import mock

from configuration import Configuration

SECRET_PASSWORD = "super-secret-value-should-never-appear-in-logs"


class TestNoSecretLeakage(unittest.TestCase):
    def setUp(self):
        self.log_records: list[str] = []
        handler = logging.Handler()
        handler.emit = lambda record: self.log_records.append(record.getMessage())
        logging.getLogger().addHandler(handler)
        self.addCleanup(logging.getLogger().removeHandler, handler)

    def test_authenticate_failure_does_not_log_password(self):
        import requests

        from client import RetainCloudClient

        client = RetainCloudClient("us", "acme", "user@example.com", SECRET_PASSWORD)
        with mock.patch.object(RetainCloudClient, "post_raw") as mock_post_raw:
            resp = mock.Mock(spec=requests.Response, status_code=401)
            resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
            mock_post_raw.return_value = resp
            try:
                client.authenticate()
            except Exception:
                pass

        for message in self.log_records:
            self.assertNotIn(SECRET_PASSWORD, message)

    def test_config_str_and_repr_never_contain_password(self):
        cfg = Configuration(
            environment="us", tenant="acme", username="user@example.com",
            **{"#password": SECRET_PASSWORD}, table="booking",
        )
        self.assertNotIn(SECRET_PASSWORD, str(cfg))
        self.assertNotIn(SECRET_PASSWORD, repr(cfg))
        self.assertNotIn(SECRET_PASSWORD, str(vars(cfg)))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/test_no_secret_leakage.py -v`
Expected: PASS (if it fails on `test_config_str_and_repr_never_contain_password`, the `password`
field's type is wrong — it must be `pydantic.SecretStr`, not `str`, per Task 1's Global Constraint).

- [ ] **Step 3: Lint and type-check**

Run: `uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/ && uv run ty check`

- [ ] **Step 4: Commit**

```bash
git add tests/test_no_secret_leakage.py
git commit -m "test: add regression coverage for secret-leakage into logs"
```

---

### Task 7: Config schema / UI (delegated to `component-developer:ui-developer`)

**Owner: `component-developer:ui-developer`** (or the `component-build-ui` skill it wraps) — this
plan does not author `configSchema.json`/`configRowSchema.json` itself, per the hard boundary
between component code and schema/UI work. This task's "step" is the brief to hand that skill, not
JSON to write directly. **Both files are now populated** — unlike the prior plan version,
`configRowSchema.json` is no longer `{}`, since this component uses config rows.

**Files:**
- Modify: `component_config/configSchema.json` (root config — replace the `print_hello`/`debug`
  placeholder entirely)
- Modify: `component_config/configRowSchema.json` (row config — replace the empty `{}` placeholder
  entirely)

- [ ] **Step 1: Dispatch to `component-developer:ui-developer`** with this exact brief (copied from
  spec §5's "Root config fields" / "Row config fields" / "UI scope & config shape" / "UI
  presentation" tables — the field-by-field decisions are already made; this is implementation, not
  re-derivation):

  **Root config (`configSchema.json`):**
  - `environment`: required, `enum` (`us`/`eu`/`uk`/`aus`) + `enum_titles` (`US`/`EU`/`UK`/`Australia`), no default.
  - `tenant`: required, plain text, no default.
  - `username`: required, plain text, no default.
  - `#password`: required, password widget, no default.
  - Add a `format: "test-connection"` widget on this group (auto-invokes the `testConnection` sync
    action).

  **Row config (`configRowSchema.json`):**
  - `table`: required, **single**-select (not multi-select), async `select` backed by the
    `list_tables` sync action, `autoload: ["environment", "tenant", "username", "#password"]` (the
    root fields — `table` autoloads once the shared connection is present, even though the sync
    action is itself row-level).
  - `load_type`: optional, `enum` (`full_load`/`incremental_load`) + `enum_titles` (`Full Load`/
    `Incremental Load`), default `full_load`, **no `options.dependencies` gate** — always visible.
    Description notes the per-run PK-safety fallback (spec §2/§6) in `options.tooltip`.
  - `page_size`: optional, number, default `20000`, nested under an "Advanced options" `type:
    object` section; description + tooltip explain it as a fetch-sizing cap, not a page count (spec
    §6 algorithm).
  - `fetch_mode` has **no schema field at all**, on either schema — it is internal-only (spec §5).

- [ ] **Step 2: Verify** — once `ui-developer` completes this, confirm neither
  `component_config/configSchema.json` nor `component_config/configRowSchema.json` has leftover
  cookiecutter placeholder content, every `enum` has a matching `enum_titles`, `table`'s `autoload`
  list matches the four root connection field keys exactly (a typo here silently breaks autoload,
  per `component-checklist-review/checklists/ui-schema.md`), and `table` is genuinely a
  single-select (a multi-select here would silently reintroduce the superseded single-config
  design at the UI layer even though the Python model expects one string).

- [ ] **Step 3: Commit** (commit message reflects the actual schemas authored, e.g.):

```bash
git add component_config/configSchema.json component_config/configRowSchema.json
git commit -m "feat: add root configSchema and row configRowSchema for Retain Cloud"
```

---

### Task 8: VCR + datadir functional tests (delegated to `component-developer:tester`)

**Owner: `component-developer:tester`** (or `generate-vcr-tests`) — per the hard boundary, this plan
does not author `tests/functional/` fixtures or cassettes itself.

**Files:**
- Create: `tests/functional/*/configs.json`, `tests/functional/*/expected/**`, VCR cassettes (owned
  entirely by the tester skill's tooling).
- `secrets.json` already exists at the repo root with `username`, `#password`, `tenant` keys
  matching this component's root config shape — the tester skill should add `environment` (root)
  and `table`/`page_size`/`load_type` (row) to the corresponding test `config.json` fixtures as a
  single flat merged parameters object (per `config-rows.md` — the component never sees a root/row
  split, so a test fixture is just one flat `parameters` dict with every field present, same as any
  other config's fixture).

- [ ] **Step 1: Dispatch to `component-developer:tester`** with the full case list from spec §7 (16
  cases: `01_testConnection_success` through `16_run_secondCallFails_userException_noPartialOutput`)
  as the required coverage. Flag these explicitly, since they differ from a naive port of the prior
  (single-config) test list:
  - Cases `09_run_tableFails_userException`, `14_run_selectedTableGoneFromStructure`, and
    `16_run_secondCallFails_userException_noPartialOutput` must assert `UserException`/exit 1 — an
    earlier version of this plan (pre-revision) had these as "warn and continue, exit 0" cases; that
    is no longer correct now that one row = one table.
  - `06_run_twoCall_fullLoad`: the fixture table's declared `rowCount` in the cassette must be
    deliberately larger than a small test `page_size` (e.g. `rowCount: 5`, `page_size: 2`) — do not
    use a real 100k-row cassette; the goal is to exercise the two-call branch cheaply, not to
    load-test.
  - **There is deliberately no "two tables in one run" test case** (spec §7) — the component cannot
    tell it's part of a multi-row config; that proof belongs to the Phase 7 cf-dev smoke test (§8),
    which creates two real rows and confirms two independent job outputs.
  - `03_listTables_success`: must specifically exercise `list_tables` **before** `table` is set on
    the row (the `RootConfig` partial-instantiation fix, Task 4) — a fixture with `table` already
    filled in would not catch a regression here.
- [ ] **Step 2: Verify** the VCR sanitizer wiring redacts `Authorization` and any literal
  username/password values in every recorded cassette interaction (spec §7) — hand off to
  `component-developer:vcr-cassette-validator` per that skill's normal usage before committing
  cassettes.
- [ ] **Step 3: Commit** (owned by the tester skill's own commit, once its suite is green).

---

## Self-Review

**Spec coverage:** §2 (Keboola mapping, config-rows convention, extraction modes) → Tasks 1, 5, 7.
§3 (auth) → Task 2. §4 (capability scope) → reflected in which client methods exist (Task 2) and
which don't (no `filter`/`filter/minimised`/write endpoints/plain `tableaccess` GET anywhere in this
plan). §5 (root/row config split) → Tasks 1, 7. §6 (architecture: two Pydantic models, single-table
`run()`, algorithm, typing, error handling, the corrected staging rule) → Tasks 2, 3, 5, 6. §7
(testing, including "no multi-row test case" and the changed failure semantics) → Task 8 (delegated)
plus Tasks 1–6's own unit tests. §8 (deployment, two-row smoke test) → out of this plan's scope,
owned by the tracker's Phase 7. §9 (risks) → risk #7 (new concurrent-load consideration from
`parallelism`) is a Developer Portal/Phase 6+ decision, noted here so it isn't lost; risk #6
(`dataTypeSupport` Dev Portal flip) likewise.

**Placeholder scan:** no `TBD`/`TODO`/"add appropriate error handling" in any task body. The two
"Note for the implementer" callouts (Task 3's `ijson.ObjectBuilder` import path, Task 5's `_data_dir`
attribute name) are explicit, bounded uncertainty about third-party library internals verified
against a specific installed version, not vague placeholders — both name the exact fallback if the
verified detail doesn't hold.

**Type consistency:** `RootConfig` → `Configuration(RootConfig)` inheritance matches spec §6 exactly
(every field each schema emits is present on the matching model — the gate-fix this revision
applies). `TableSchema_` (Task 3, Retain Cloud's own schema) vs.
`keboola.component.table_schema.TableSchema` (Task 5's manifest schema) are deliberately named
differently and the conversion function (`Component._to_output_schema`) is the single place they
meet. `FetchResult`, `ColumnSchema`, `build_table_schema`, `fetch_table` signatures match between
their Task 3 definition and every consumer in Task 5's tests/code — unchanged by the revision, since
`extractor.py` always operated on one table. `test_connection`/`list_tables` in Task 4 use
`RootConfig`; `run()` in Task 5 uses `Configuration` — checked that no task accidentally uses the
strict model where the permissive one is required (or vice versa).
