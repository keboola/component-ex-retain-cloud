# keboola.ex-retain-cloud — Design Spec

> Type: extractor
> Component ID: keboola.ex-retain-cloud
> Status: draft
> Date: 2026-09-21 (revised same day: config shape changed from a single config to config rows per
> explicit user direction — see §2 and §5. A prior draft's grounding-reconciliation findings are
> still folded in throughout; only the config-shape decision itself changed.)

## 1. Overview & source system

`keboola.ex-retain-cloud` extracts data from Retain Cloud (retaininternational.com), a
resource-planning SaaS, via its **DataAccessAPI** — a generic, tenant-scoped, table-access REST
API (JSON over HTTPS). One call lists every table a service account may read; one call reads a
table's rows; there is no history kept server-side, so a full snapshot of the selected table *is*
that row's output on every run.

Primary use case: a customer that runs Retain Cloud for resource/booking/timesheet planning wants
its operational tables mirrored into Keboola Storage so they can be joined with other systems
(finance, HR, project data) in transformations, without hand-rolling API calls per table.

There is no public top-level API reference URL to link (the vendor ships its API reference as a
ReDoc-style portal export behind a customer login, plus a short help-center PDF); source material
for this spec is a vendor "Retain API Docs" export and a live-credentialed probe against a test
tenant, both recorded in the (gitignored, local-only) Phase 2 research doc.

## 2. Keboola mapping

- **Source objects → output tables.** Each Retain Cloud table selected by a config row (`api/structure`
  entry) maps 1:1 to one Keboola Storage output table, named after the source table (e.g. `booking`
  → `booking.csv` / table `booking`). No fan-out, no joins, no child tables — the source is already
  flat, prefixed rows (`<table>_<field>` columns).
- **Config rows, one row per table — the standard Tier A convention, applied directly.** Root
  config holds the shared connection (`environment`, `tenant`, `username`, `#password`); each row
  holds exactly one `table` plus that table's own `load_type` and `page_size`. This is a straight
  application of "multiple objects → config rows" (`architecture-conventions.md`), not an override:
  each table is an independent object, fetched independently, with its own load-type choice — the
  textbook case the convention exists for. (An earlier draft of this spec used a single config with
  a `tables[]` multi-select instead, reasoning that every table's mechanics were identical enough to
  not need per-row granularity; the user explicitly overrode that back to config rows, and on
  reflection the row-based design is also the better fit for `incremental-state.md`'s own
  non-negotiable that "`load_type` and `fetch_mode` are row-level config, both explicit pickers" —
  see §5 and §9 for what changes as a result, all of it in the direction of *simpler*, not more
  complex.)
- **Extraction modes (required).** `fetch_mode = full_fetch` is hardcoded/omitted from the UI (the
  sanctioned override — see below); this did not change with the config-shape revision. `load_type`
  **is** a real, always-visible, ungated field **on each row** (`Full Load` / `Incremental Load`,
  default `Full Load`) — now genuinely row-scoped, exactly as `incremental-state.md` recommends,
  rather than one global picker applied across every selected table. `state.json` is not used
  regardless of which `load_type` a row picks: Full Fetch is stateless by definition (no cursor
  exists to persist), and Incremental *Load* is a Storage-side upsert-on-primary-key mechanic
  (`incremental=true` in the manifest), not a `state.json` cursor — the two axes stay independent
  per the standard.

  **Why Full Fetch is omitted (sanctioned, unchanged by the config-shape revision):** no verified
  "changed since X" filter exists. The only endpoint that could carry that semantic
  (`tableaccess/{table}/filter`) has an undocumented request-body DSL (never reverse-engineered —
  see §4, Excluded/deferred) and is capped at 1,000 rows even if it did — useless as a bulk
  incremental mechanism for tables running into the hundreds of thousands of rows. `Incremental
  Fetch` and `Date Window` are both infeasible today; `Full Fetch` is the only implementable mode,
  so the field is omitted per the explicit "when only one mode is possible, omit the field
  entirely" clause.

  **Why Load Type is now simpler, not just "still a real field":** a prior single-config draft had
  to reconcile `load_type` applying *globally* across up to ~130 heterogeneous tables of varying PK
  reliability, which was flagged as a genuine, unresolved coarseness risk. With one row per table,
  that problem doesn't arise in the first place — each row's `load_type` only ever governs *that
  row's one table*. What's still true, and still needed, is the **per-run PK-safety fallback**: a
  row set to `Incremental Load` only actually loads as `incremental=true` (upsert) if this run's
  streamed data confirms the table's `<table>_guid` column is unique; if the check fails (or the
  column doesn't exist), that row's run falls back to `incremental=false` for this run, with a
  warning logged (`incremental_load` requested but PK not verified this run, falling back to full
  load) — the same verify-then-fall-back idiom the original component request already uses for PK
  detection itself. `Full Load` remains the strongly recommended default in the field's UI
  description, since Retain Cloud is a live, mutable, current-state system (bookings get cancelled,
  records get deleted) with **no server-kept history** — an upsert-only load never removes rows the
  source has since deleted — but `Incremental Load` is a legitimate, available per-row choice.
  - The default pairing, **Full Load + Full Fetch = "Mirror"** in the extraction-modes taxonomy, is
    the one combination where upstream deletes propagate — the desired default for a source with no
    native history. **Incremental Load + Full Fetch = "Refresh-by-upsert"** is the available
    alternative for a row that wants accumulation instead (accepting that deletes are never
    removed), gated by the PK-safety fallback above.
- **Secrets → `#`-prefixed config keys.** `#password` (see §5) is the only secret; it maps to the
  API's `userpassword` field, and lives on the **root** config (shared across every row — it's the
  same service account for every table). `username`, `tenant`, `environment` are not secrets (they
  identify the account/tenant, not authenticate it) and also live on the root config, matching the
  already-provisioned `secrets.json` fixture's key names (`username`, `#password`, `tenant`).
- **Test-connection / dynamic dropdowns.** Two sync actions, at two different levels: `test_connection`
  (root-level, Tier A default — validates the four connection fields by authenticating) and
  `list_tables` (row-level — populates each row's single-select `table` field from `api/structure`,
  using the root connection fields already entered).
- **Output bucket.** Default-bucket behaviour (`in.c-{componentId}-{configId}`) — no hardcoded
  destination in the manifest; the Developer Portal's `default_bucket: true` setting (Phase 6)
  governs this per `keboola-context/references/default-bucket.md`. Every row's table lands in the
  same default bucket (the bucket is keyed by config, not by row).

## 3. Authentication & connection

Unchanged by the config-shape revision — auth is a root-config concern regardless of how tables are
selected.

- **Auth method:** a custom bearer/API-key scheme dressed as a JWT, not OAuth 2.0 —
  `POST https://{host}/IntegrationApi/token` with a JSON body
  `{"useremail": <username>, "userpassword": <password>, "environment": <environment>, "tenant": <tenant>}`.
  The response is **`200` with a `text/plain` body**, not JSON: `Bearer eyJ...`. The body is used
  **verbatim** as the `Authorization` header on every subsequent call — it is not re-parsed,
  re-prefixed, or treated as a JSON envelope.
  - The vendor's own `IdentityServerAPI/.../connect/token` (the JWT's issuer) is **closed to direct
    callers (403)** — not a usable alternative, documented only for completeness (§4).
- **Host is environment-dependent, not a bare string interpolation.** Verified (unauthenticated
  DNS/HTTP probe against the vendor's own login form and a 2-page help PDF, both citing the same
  four hosts):

  | `environment` value | Host |
  |---|---|
  | `us` | `us.retaincloud.com` |
  | `eu` | `eu.retaincloud.com` |
  | `uk` | `app.retaincloud.com` (**not** `uk.retaincloud.com` — that hostname does not resolve) |
  | `aus` | `aus.retaincloud.com` |

  The client uses an explicit `dict[str, str]` map, never `f"https://{environment}.retaincloud.com"`.
  **Open item flagged to the requester:** `uk → app.retaincloud.com` is corroborated by two
  independent public sources (the login form's region selector and the help-center PDF's
  environment list) but was **not** confirmed against a live `uk` tenant — the test tenant used for
  this build is on `us`. Confirm with the requester before a `uk` customer is provisioned (§9).
- **Token lifecycle:** the token is a JWT with a **1-hour TTL** (`exp` claim). The client decodes
  the JWT payload locally (base64url-decode the middle segment, no signature verification needed —
  we trust our own freshly-issued token, we're not validating a token from an untrusted party) to
  read `exp`, avoiding a dependency on a full JWT library for one claim. Refresh proactively when
  fewer than 5 minutes remain, and reactively on any `401` (refresh once, retry the failed call
  once, then give up and surface the error).
- **Provisioning: fully headless.** No OAuth consent screen, no admin-portal app registration, no
  manual token minting — `useremail`/`userpassword`/`environment`/`tenant` are sufficient. Exactly
  the shape a Keboola worker can execute unattended.
- **Blockers / access:** none open. A test tenant with an API-only service account exists and was
  used for the live probe in Phase 2 (details in the gitignored research doc, not reproduced here).
  Every table the probe checked (`structure`, `tableaccess`, `paging/paged` on a representative
  spread of table sizes) returned `200` — **no tables were denied** for this account.
- **Transient failure mode on the token endpoint:** an unauthenticated probe with an empty JSON
  body returned `HTTP 502 {"error":{"code":"NoResponse", ...}}` rather than a clean `400` — the
  token gateway proxies to an upstream identity service and can surface `502`/`503` as a
  transient/malformed-request failure, not just `401`/`403` for bad credentials. Treat `502`/`503`
  from the token endpoint as retryable-with-backoff (see §6); raise `UserException` only on a real
  `4xx` returned **with credentials supplied** (i.e. after the retryable codes are exhausted or a
  genuine `400`/`401`/`403` is returned). **Row-based config note (§9):** since every row
  authenticates independently, each row's container makes its own token call — see §9 for the new
  concurrent-load consideration this introduces if row `parallelism` is ever enabled.
- **Never log the token or password**, at any log level, in any exception message. This is a
  named, confirmed incident risk (a debug run of a different extractor previously leaked a full
  JWT into a Keboola job log) — see §6 for the concrete mechanism (the code never builds a log
  string containing the auth header or the config's password field; the http client's
  `_auth_header` is never dumped).

## 4. Capability inventory & scope

Every operation in the vendor's exported API reference, plus the two endpoints known only from the
live probe (`IntegrationApi/token`, `paging/paged`) and the one explicitly closed one
(`IdentityServerAPI`). This is the full DataAccessAPI menu (Phase 2's capability inventory,
verbatim) — nothing is silently narrowed. The config-shape revision does not change which
capabilities are in/out of scope, only how a table is selected (§5) — the verdicts below are
unchanged from the prior draft.

| Capability | Verdict | Rationale |
|---|---|---|
| `POST IntegrationApi/token` | **In scope** | The only supported machine auth path (§3). |
| `POST IdentityServerAPI/.../connect/token` | Excluded | Closed to direct callers — returns `403`. Not a usable capability at all, not a scope choice. |
| `GET api/structure` | **In scope** | Table list → row-level `list_tables` sync action; also re-checked at the start of every row's run to confirm its selected table still exists. |
| `GET api/structure/tablestructure` | **In scope** | Human-readable table alias/description, used to enrich the `list_tables` sync-action labels (raw table name remains the stored value). |
| `GET api/structure/fieldnames?table=` | Excluded (redundant) | Strict subset of `richfieldstructure`'s field list — no unique data, one more round trip for nothing once `richfieldstructure` is already being called per row's table. |
| `GET api/structure/fieldstructure?table=` | Excluded (superseded) | `richfieldstructure` is a live-verified strict superset (same 61 fields on the sampled table, plus 10 extra keys) at the same call cost — no reason to keep both. |
| `GET api/structure/richfieldstructure?table=` | **In scope** | Core discovery call: column names/order for the CSV, `dataType` for manifest typing (§6), and detection of the `<table>_guid` primary-key candidate. |
| `GET api/tableaccess/{table}` | Excluded (superseded by design) | The design constraints allow using this for small (<1,000-row) reference tables, but it is deliberately **not** used — see the justified deviation below (unaffected by the config-shape revision — the deviation's reasoning is about response-shape uniformity, not about how many tables one config manages). |
| `POST api/tableaccess/{table}/paging/paged` | **In scope** | The core, and only, bulk-read path used — for every table, regardless of size (§6 algorithm). |
| `POST api/tableaccess/{table}/filter` | Excluded (deferred) | Request-body query DSL was never reverse-engineered (not a blocker for V1, since full-load doesn't need it); capped at 1,000 rows regardless, so not viable for bulk extraction even if solved. Recorded as the sole candidate for a **future incremental fetch mode** (§9). |
| `POST api/tableaccess/{table}/filter/minimised` | Excluded (deferred) | Same undocumented DSL as `filter`, same 1,000-row cap; only difference is a tabular `{columns, rows}` response shape. Same future-incremental candidate, no separate rationale. |
| `GET api/tableaccess/{table}/{id}` | Excluded | Single-row point lookup by GUID. No bulk-extraction use case, and there is no change-feed to pair it with (no capability here reports "which IDs changed"). |
| `PUT/POST/PATCH/DELETE api/tableaccess/{table}...` | Excluded | Write endpoints. An extractor has no use for row-level create/update/delete; building against them would be building a writer, which is a separate, unrequested component. |
| "Legacy APIs" (mentioned, undocumented in the export we have) | Excluded | Vendor-flagged as being deprecated in favour of the v2-style API this spec targets ("Legacy APIs are still supported in parallel. They will be deprecated in a future release."). Not detailed in any doc we could reach; not worth building against a surface the vendor itself is retiring. |

**Justified deviation — uniform use of `paging/paged`, never the plain `GET tableaccess/{table}`:**
the design constraints explicitly *permit* (not require) using the plain GET for tables known to
be under 1,000 rows. This spec does not use it, for three reasons: (1) knowing a table is "small"
ahead of time requires either hardcoding today's row counts (which will drift as the tenant's data
grows) or making a sizing call anyway — and the `paging/paged` algorithm already in §6 makes that
sizing call as an integral, unavoidable part of fetching the table, so the plain GET buys no extra
efficiency; (2) the plain GET's hard 1,000-row cap is a silent-truncation risk if a table that is
small today grows past 1,000 rows before a future run — it would fail closed (truncate silently)
rather than open; (3) one code path for every table (any row could point at any of the tenant's
tables) is simpler to reason about, test, and review than two response shapes (bare array vs.
envelope) gated on a size threshold. No capability is lost — every table remains fully readable via
`paging/paged`.

Pagination, rate limits, and the response envelope are fully covered in §6 (the mechanics are
inseparable from the algorithm, not a separate note).

## 5. Configuration & schema

The config is **row-based**: one root config (shared connection) and one row per table. This is a
direct application of the Tier A "multiple objects → config rows" convention (§2) — not an
override, so unlike the prior draft there is no escape-hatch justification needed here.

### Root config fields (`configSchema.json`)

| Field | Required | Purpose |
|---|---|---|
| `environment` | yes | Selects the API host (`us`/`eu`/`uk`/`aus`) via the explicit map in §3 — never a bare string interpolation. |
| `tenant` | yes | Case-sensitive tenant identifier, sent in the token request body. |
| `username` | yes | Maps to the API's `useremail` field. |
| `#password` | yes (secret) | Maps to the API's `userpassword` field. Platform-encrypted (`KBC::ProjectSecure::`), decrypted to plaintext at runtime, never logged. |

### Row config fields (`configRowSchema.json`)

| Field | Required | Purpose |
|---|---|---|
| `table` | yes | **Single**-select of one source table name, populated by the row-level `list_tables` sync action from live `api/structure` output. Stores the raw table name (the value), not the human label. |
| `load_type` | no (default `full_load`) | `full_load` / `incremental_load`, always visible per the extraction-modes convention (§2) — now genuinely row-scoped (one table per row); the per-run PK-safety fallback (§6) is handled in code, not in the schema. |
| `page_size` | no (default `20000`) | The row cap used on this table's *first* `paging/paged` call (§6) — not a true "page size," since there is no continuation mechanism; see the algorithm for exactly how it's used. |

`fetch_mode` is **not** a schema field anywhere (§2) — it is a fixed, internal constant
(`full_fetch`) referenced only in code (`extractor.py`), never modeled on either the root or row
Pydantic class (§6). There is also no user-facing primary-key picker: the PK is auto-detected for
the row's one table from `richfieldstructure` plus a per-run uniqueness check (§6) — the UI-schema
convention "a stable source record id is used as the PK automatically; the picker appears only when
the source has no reliable id" applies directly, and Retain Cloud's own `<table>_guid` naming
convention is exactly that reliable id.

### Sync actions

- **`test_connection`** (root-level, Tier A default — always add one). Authenticates with the four
  connection fields (`environment`, `tenant`, `username`, `#password`) and reports success/failure;
  does not need to call `structure` as well, since a successful token response already proves every
  connection field is correct (`environment`/`tenant` are part of the token request body itself, so
  a bad tenant or wrong environment host both surface as a token-endpoint failure).
- **`list_tables`** (row-level). Calls `api/structure` (the list of readable table names) and
  `api/structure/tablestructure` (aliases/descriptions) and returns one selectable option per
  table: value = raw table name, label = alias/description when available, else the raw name.
  Invoked with the merged root connection fields **plus whatever the current row's draft
  parameters are** (per the platform's normal sync-action behaviour) — critically, `table` itself
  may not be set yet the first time a user opens this dropdown on a new row, so the code path that
  builds a client for this sync action must validate against a **connection-only** partial model
  (root fields only), never against the full row model that requires `table` (§6 — this is the
  "Partial instantiation only required when a sync action needs fewer fields than `run()`" pattern
  from the configuration checklist, applied concretely here).

### UI scope & config shape

| Field | Level | Required | User-facing or internal | Default | In a NEW config/row's saved params? |
|---|---|---|---|---|---|
| `environment` | root | yes | user-facing (enum select) | none — user must choose | yes |
| `tenant` | root | yes | user-facing (text) | none | yes |
| `username` | root | yes | user-facing (text) | none | yes |
| `#password` | root | yes | user-facing (password widget) | none | yes |
| `table` | row | yes | user-facing (single-select, sync-action backed) | none | yes — the selection is the entire point of the row |
| `load_type` | row | no | user-facing, always visible (never gated) | `full_load` | yes — a single visible, self-explanatory enum field |
| `page_size` | row | no | user-facing, under an "Advanced options" section | `20000` | yes — a single visible, self-explanatory number field; no `dependencies` gate to hide it behind |
| `fetch_mode` (internal marker) | — | n/a | internal — not in either schema at all | `full_fetch` (code constant) | no |

**What changed from the prior single-config draft, and why it's simpler now, not more complex:**
the previous draft spent several paragraphs justifying a deliberate deviation from the config-rows
convention (needing to argue it wasn't a natural fit for `architecture-conventions.md`'s own escape
hatch, and separately flagging that a single global `load_type` picker was coarser than the
row-level norm `incremental-state.md` describes). None of that justification is needed anymore:
this *is* the convention, applied directly. Concretely, this revision:

1. **Gains real per-table process isolation.** Each row is a separate job/container execution — one
   table's failure, OOM, or hang cannot affect another table's row at all. This directly resolves
   what the prior draft's risk about "the config-shape override trades away container-level
   isolation" was warning about (§9 no longer carries that risk).
2. **Gains free per-row retry-from-the-UI and the platform's `parallelism` feature.** A user can
   re-run just the one row/table that failed, and can opt into concurrent row execution
   (`config-rows.md`) without this component writing any concurrency code itself. **New
   consideration this introduces:** if `parallelism` is ever enabled, multiple containers hit the
   vendor's token endpoint and `paging/paged` concurrently — see §9's new risk about this; the safe
   default (platform default, no action needed for V1) is sequential row execution.
3. **`load_type` becomes genuinely row-scoped for free.** No more "one global toggle across ~130
   heterogeneous tables" coarseness — each row's `load_type` only ever governs that row's one table,
   which is exactly what `incremental-state.md` already recommends as the norm.
4. **UX at scale is now the platform's native row-management UI**, not a custom multi-select —
   consistent with how every other multi-object CF extractor works, so it needs no bespoke
   explanation to a user already familiar with Keboola.
5. **Slightly higher token-endpoint traffic** (one auth call per row's execution instead of one per
   whole-config run) is the only real cost, and it's a small one: the token endpoint's own
   `502`/`503` transient-failure mode (§3) is already handled per-call with retries regardless of
   how many rows exist.

### UI presentation

| Field | Level | Widget | Notes |
|---|---|---|---|
| `environment` | root | `enum` + `enum_titles` (`US`, `EU`, `UK`, `Australia`) | Fixed 4-value list; no free text — the host map in §3 only covers these four. |
| `tenant` | root | text | Case-sensitive, no enumeration possible (per-customer value). |
| `username` | root | text | Not a secret — the email/account name, not the credential. |
| `#password` | root | password | Encrypted; matches the `#password` config key exactly. |
| `table` | row | single-select, async `select` + `autoload`, backed by `list_tables` | First thing a user must fill on a new row, so it autoloads once the root connection fields are present (`autoload: ["environment","tenant","username","#password"]`). Stores the raw table name. |
| `load_type` | row | `enum` + `enum_titles` (`Full Load`, `Incremental Load`) | Always visible, no `options.dependencies` gate (per `extraction-modes.md`). Default `Full Load`; description notes `Incremental Load` upserts this row's table and silently keeps deleted-upstream rows, with the per-run PK-safety fallback (§6) explained in `options.tooltip`. |
| `page_size` | row | number, inside an "Advanced options" (`type: object`, collapsible) section | Default `20000`; a one-line description explaining it as a fetch-sizing cap, not a page count, with the full mechanics in `options.tooltip`. |
- Add a `format: "test-connection"` widget on the root config's connection-fields group (auto-invokes `testConnection`).

## 6. Code architecture

### Files

- `src/configuration.py` — **two Pydantic models**, matching the root/row schema split exactly (the
  gate-fix corrected an earlier draft that omitted `load_type` from any model entirely, calling it
  "internal," which contradicted §2/§5 where it is a real user field — every field each schema
  emits must be present on the matching model, and now is):
  - `class RootConfig(BaseModel)`: `environment: Environment`, `tenant: str`, `username: str`,
    `password: SecretStr = Field(alias="#password")`. `model_config = ConfigDict(extra="ignore")` —
    deliberately permissive, not `"forbid"`: this model is also used for **partial instantiation**
    by sync actions that only need the connection fields (`test_connection`, and `list_tables` before
    `table` is chosen) — per the configuration checklist's "partial instantiation only required when
    a sync action needs fewer fields than `run()`" — so it must tolerate whatever row-level keys
    happen to be present (or absent, or blank) in the merged parameters it's handed without raising.
  - `class Configuration(RootConfig)`: adds `table: str`, `load_type: LoadType = LoadType.full_load`,
    `page_size: int = 20000`. `model_config = ConfigDict(extra="forbid")` — used only for `run()`,
    where the platform guarantees a fully merged, fully valid row config (a row cannot execute
    without its required `table` value), so strict validation here catches genuine problems instead
    of masking them.
  - `fetch_mode` remains a pure code constant (`FETCH_MODE = "full_fetch"` in `extractor.py`), not a
    field on either model — the one thing that is genuinely internal-only, unlike `load_type`.
- `src/client.py` — `RetainCloudClient(keboola.http_client.HttpClient)`. Unaffected by the
  config-shape revision (auth/discovery mechanics don't care how a table was selected). The repo
  already declares `keboola-http-client>=1.0.1` as a dependency (confirmed by reading the installed
  `keboola/http_client/http.py`); this spec builds on it rather than hand-rolling a second
  `requests` + `urllib3.Retry` wrapper:
  - `HttpClient.__init__` already accepts `max_retries`, `backoff_factor`, and `status_forcelist`,
    and mounts a `urllib3.Retry`-backed `HTTPAdapter` for every call made through it. The client is
    constructed with `status_forcelist=(429, 500, 502, 503, 504)` and a generous `backoff_factor` —
    this alone satisfies "retry 429/5xx with backoff" for every data endpoint, and also covers the
    token endpoint's own `502`/`503` transient mode (§3), since `4xx` codes are never in the
    forcelist and so raise immediately via `raise_for_status()`.
  - `base_url` is `https://{host}/DataAccessAPI/{tenant}/api/` (host from the §3 map). The token
    endpoint lives at a different path root (`https://{host}/IntegrationApi/token`, no tenant
    segment) — called via `post_raw(url, is_absolute_path=True, ignore_auth=True, json={...})`.
  - `authenticate()`: calls the token endpoint, stores the raw response body verbatim, and installs
    it via `self.update_auth_header({"Authorization": token_body})` so every subsequent call
    through the client automatically carries it. Decodes the JWT's middle segment locally to cache
    the `exp` timestamp (no external JWT library — see §3).
  - `_ensure_token()`: called before every data call; re-authenticates when `exp` is less than 5
    minutes away. A `401` on any data call triggers one reactive re-authenticate-and-retry.
  - `list_tables()` → `GET structure` (`.get()`, JSON-decoded automatically).
  - `list_table_labels()` → `GET structure/tablestructure` (`.get()`).
  - `get_table_schema(table)` → `GET structure/richfieldstructure?table=` (`.get()`).
  - `fetch_table_page(table, page_size)` → `POST tableaccess/{table}/paging/paged?pageSize=&sequential=true`
    via **`post_raw(..., stream=True)`**, so the caller gets the raw streamable `requests.Response`
    (needed for the `ijson` streaming parse below) rather than the JSON-decoding `post()` wrapper.
  - Nothing in this module ever formats the password or the auth header into a log message or
    exception string — errors re-raise the underlying `requests.HTTPError` (which does not include
    the request headers or body in its default string form) or a `UserException` built from the
    HTTP status code and table/endpoint name only.
- `src/extractor.py` — the single-table fetch-and-write logic, kept separate from `run()`'s
  orchestration and from the HTTP client. **Unaffected by the config-shape revision** — this module
  always operated on one table at a time; that never changed:
  - `build_table_schema(rich_fields)` — turns a `richfieldstructure` response into an ordered
    column list, the manifest schema (see typing rules below), and the PK candidate column name
    (`<table>_guid`, if present in the field list).
  - `fetch_table(client, table, page_size, scratch_dir)` — implements the two-call algorithm below,
    streaming rows into a **`/tmp` scratch file** (never directly into `/data/out/tables/` — see
    the corrected staging rule below) and returning the scratch file's path, the final row count,
    the set of encountered "declared-type still holds" flags per column, and the observed-unique
    flag for the PK column. Raises on any HTTP/parse failure; never leaves a partially-written file
    outside `/tmp`.
  - Nothing here is a config concern — it operates purely on a `RetainCloudClient`, a table name,
    a page-size cap, and a scratch directory.
- `src/component.py` — thin `Component(ComponentBase)`, now genuinely simpler than the prior draft
  because there is exactly one table to handle per execution — no loop, no partial-failure
  bookkeeping across tables:
  1. `test_connection` sync action: parse `RootConfig(**self.configuration.parameters)`, construct
     `RetainCloudClient`, call `authenticate()`. Raising is enough — the framework surfaces the
     `UserException`/success as the sync-action result.
  2. `list_tables` sync action: parse `RootConfig(**self.configuration.parameters)` (the partial
     model — tolerates `table` being absent/blank on a fresh row), authenticate, call
     `client.list_tables()` + `client.list_table_labels()`, return the merged options.
  3. `run()`:
     a. Parse `Configuration(**self.configuration.parameters)` (the full merged root+row config).
     b. Construct `RetainCloudClient`, `authenticate()`.
     c. Call `client.list_tables()`; if `cfg.table` is not in the live list, raise `UserException`
        naming the table — this row's entire job is that one table, so a missing table means the
        job genuinely has nothing to do (this replaces the prior draft's "log a warning and
        continue to the next table" — there is no next table in this row's job; other tables' rows
        are separate container executions entirely, unaffected either way).
     d. `get_table_schema` → `build_table_schema` → `fetch_table` (writes to `/tmp` only). Any
        `requests.HTTPError` (e.g. `403`/`404`/exhausted-retry `5xx`) is caught here and re-raised
        as `UserException` naming the table and HTTP status — again, this row's whole job failed,
        which is the correct, and now free, "fail the table" behaviour: the platform already
        isolates this failure from every other row.
     e. Determine `incremental_for_table = cfg.incremental and result.pk_unique`; log a warning if
        `cfg.incremental` is true but `result.pk_unique` is false (the per-run PK-safety fallback,
        §2).
     f. Build the output `TableSchema`, call `create_out_table_definition_from_schema(...,
        incremental=incremental_for_table)`, move the `/tmp` scratch file into the definition's
        `full_path`, call `write_manifest`.
  4. `run()` is now a single straight-line sequence of steps with no branching over "which table(s)
     failed" — the simplest version of the "thin orchestrator" bar this design has had, gained as a
     direct consequence of moving to config rows.

### The `paging/paged` fetch algorithm (per table)

Unchanged by the config-shape revision — this was always written in terms of "the table this
execution is fetching," which is now unambiguously the row's one `table`.

This is the resolved paging contract (Phase 2, live-probed): `pageSize` is a single-call row cap,
not a page window, and there is no continuation token — the same request repeated returns the same
first N rows every time. The envelope is always
`{"key": <fresh guid, not a cursor>, "rowCount": <int, grand total>, "rowsProcessed": <int, this call>, "data": [...]}`.

**Corrected staging rule (grounding-reconciliation finding, still in force):** an earlier draft
streamed rows directly into the eventual `/data/out/tables/` output file and, on the two-call path,
truncated and rewrote that same file in place. Per `output-mapping.md` ("every file placed under
`/data/out/tables/` is uploaded — not just entries in the output mapping") and
`environment-variables.md` ("do not use `/data/` for temp files... use `/tmp/` for scratch work"),
that is unsafe: if the *second* call then failed, this row's job would have left a truncated,
half-rewritten file for Storage to upload despite the job itself failing. **Fix: every `paging/paged`
response, first or second call, streams into a fresh file under `/tmp/` (e.g.
`/tmp/ex-retain-cloud/{table}.csv`), never into `/data/out/tables/` directly.** Only after the
row's fetch fully succeeds does `component.py` move that finished `/tmp` file into
`/data/out/tables/` and write the manifest (see the `component.py` steps above); a failed second
call simply means the `/tmp` file is discarded/overwritten, nothing is ever moved, and the row's job
raises `UserException` (§ above).

1. `resp = POST paging/paged?pageSize={row.page_size}&sequential=true`, streamed (`stream=True`),
   parsed incrementally with `ijson.parse` (not `ijson.items`, since we need both the scalar
   `rowCount`/`rowsProcessed` keys *and* to stream `data` item-by-item from the same response body
   — `ijson.items` alone only yields the matched array elements and discards sibling scalars). Rows
   are written to the **`/tmp` scratch file** as they're parsed (one `csv.DictWriter.writerow` per
   row, `restval=""` for any field the schema expects but a given row omits, `extrasaction="ignore"`
   for any field a row carries that the schema didn't declare — logged once per table as a
   schema-drift warning, not per row). A `set()` of the PK column's values is accumulated as rows
   stream past, for the uniqueness check below.
2. If `rowsProcessed >= rowCount` (the common case — using the customer's own known-working default
   of `page_size=20000` as the first call's size means the large majority of the tenant's tables
   complete in exactly this one call): done. The PK is kept if a `<table>_guid` column exists
   **and** `len(pk_values_set) == rowsProcessed` (no duplicates observed this run). The `/tmp` file
   is now the complete result, ready to be moved by `component.py`.
3. Else (`rowsProcessed < rowCount` — the table is bigger than `page_size`; from the live probe,
   this affects a small minority of tables, but the ones it does affect are large): the **`/tmp`
   scratch file is discarded and reopened for writing** (truncated — safe, since it was never
   visible to Storage in the first place), the PK-values set and type-conformance trackers are
   reset, and a second call is issued:
   `POST paging/paged?pageSize={rowCount + margin}&sequential=true`, where
   `margin = max(1000, ceil(rowCount * 0.02))` — a safety buffer against rows inserted on the live,
   mutable tenant between the two calls. This second call's `data` is streamed the same way, into
   the same `/tmp` file, and is the authoritative, complete set. **No third call is issued** even
   if the second call is still short of its own reported `rowCount` (i.e. the table grew faster
   than the 2% margin covered) — log a warning and treat the run as best-effort complete; the
   source's own "no history, full reload each run" model means the next run picks up any tail rows,
   and chasing a live-growing table with unbounded extra calls would break the "at most 2 calls per
   table" contract the whole design relies on for the largest tables' runtime.
4. This is why `page_size`'s default (`20000`) matters even though the paging contract has no true
   "pages": it is both the size of the *only* call for tables at or under that count, and the
   (wasted, discarded) first call's cap for the handful of tables above it. A user with an unusually
   large table, or who wants to trade a slightly bigger first-call payload for avoiding the two-call
   path, can raise this row's `page_size`.

### Column typing — reconciling two grounding constraints

Unchanged by the config-shape revision.

Two grounding references pull in different directions here, and this design reconciles them rather
than picking one and ignoring the other:

- `keboola-context/references/native-data-types.md`: "when you have **not** confirmed a field's
  values against sampled data, do not emit native numeric/boolean" — and Retain Cloud's
  `richfieldstructure.dataType` (`Bool`/`DateTime`/`Float`/`ID`/`Int`/`String`/`Unknown`) was only
  spot-checked against real returned rows for two tables during Phase 2 research, not every table a
  row could point at.
- `component-checklist-review/checklists/output-state.md`: "an all-STRING authoritative `schema` is
  a finding" — so reflexively mapping every declared type to `STRING` "to be safe" is itself wrong.

**Resolution: verify each column's declared type against its own real, returned rows, every run,
as a side effect of the streaming pass already happening in step 1 above** — this doesn't require
trusting an unverified catalogue type, and it doesn't degrade to all-STRING either:

- `DateTime` → native `timestamp`. `native-data-types.md`'s own safe-default table explicitly
  allows `DATETIME`/`TIMESTAMP` → `timestamp` without per-row verification — dates are the one
  declared type that reference trusts outright.
- `ID` → `string`. Always correct regardless of verification — a GUID is definitionally a string,
  never at risk of the "declared numeric but really a status code" failure mode.
- `String` / `Unknown` → `string`. No ambiguity.
- `Bool` / `Int` / `Float` → checked **while streaming**: for each such column, attempt the
  matching Python coercion (`int()`, `float()`, or membership in a small boolean-token set
  including JSON `null`) on every value encountered for this table this run. If **any** value
  fails, that column is downgraded to `string` for this run's manifest (logged once, naming the
  table/column/offending value's *type*, never the value itself if it could be sensitive data). If
  **all** values conform, the column is emitted as native `integer`/`float`/`boolean`. This check
  is per-run — the manifest schema, not just the CSV content, can differ between runs for the same
  table if the source's real data composition changes, which is the correct behaviour (it means the
  verification is real, not a one-time guess baked in at design time).
- The manifest is written via `ComponentBase.create_out_table_definition_from_schema(TableSchema(...))`
  — the library's own schema-aware convenience method, which already switches between the legacy
  `column_metadata` format and the authoritative `schema` format based on `_expects_legacy_manifest()`
  internally (so this design does not hand-roll that branch). **Verified against the installed
  library (not assumed):** this method does not accept a `has_header` argument at all — when a
  schema/column list is known, its default (`TableDefinition._has_header_in_file()`) resolves to
  `has_header=False`. Rather than bypassing the convenience method just to force a header row (which
  would mean re-implementing its legacy/authoritative branch by hand, reopening exactly the class of
  bug `native-data-types.md` warns about), **the CSV is written headerless** (no
  `csv.DictWriter.writeheader()` call) — the schema already names every column, so a header row adds
  nothing Storage needs, and this is the library's own designed-for-this-case default, not a
  workaround. **Deployment prerequisite, not a code change:** the Developer Portal's `dataTypeSupport`
  property must be flipped to `authoritative` (Phase 6) for the platform to actually honor the
  `schema` manifest instead of silently downgrading it to legacy `column_metadata` hints — tracked
  in §9.
- Lookup/multi-value fields whose JSON value is an object or array (not a Retain "custom field"
  concept confirmed to exist on every table, but present per the component request) are serialized
  with `json.dumps(value, separators=(",", ":"))` into a `string` column, never exploded into
  child columns or dropped, per the original component request's explicit instruction.

### Error handling

Revised for the row-based model: a table-level failure is now this row's job failure, full stop —
there is no "other tables in this run" to protect, because there is no other table in this run.

- `UserException` (exit 1): auth failure with credentials supplied (a real `4xx` from the token
  endpoint after the retry-adapter's forcelist is exhausted — i.e. `502`/`503` are retried
  transparently, a real `400`/`401`/`403` is not); config validation failure (Pydantic, on the
  strict `Configuration` model used by `run()`); the row's `table` missing from a fresh `structure`
  call; any `403`/`404`/exhausted-retry `5xx` on `richfieldstructure` or `paging/paged` for the
  row's table; the second `paging/paged` call still short of `rowCount` **combined with** any
  subsequent I/O failure (the "still short" case alone is a logged warning + best-effort-complete
  result, not a failure — see the algorithm above; it only becomes a `UserException` if the call
  itself also errors).
- Logged warning, run still succeeds (exit 0): the second `paging/paged` call completing but still
  short of its own reported `rowCount` after the 2% margin (best-effort complete, not a failure); a
  column's declared type downgraded to `string` after a failed per-row coercion check; a row
  (CSV row, not a config row) carrying a field not present in the table's discovered schema (schema
  drift); `load_type=incremental_load` requested but this run's PK check failed (falls back to
  `incremental=false` for this run, §2/§6).
- Unexpected (exit 2): anything not covered above — a genuine bug, not a user-fixable condition.
- **No partial output on failure.** Because every `paging/paged` response streams into a `/tmp`
  scratch file and is only moved into `/data/out/tables/` after the row's fetch fully succeeds
  (staging rule above), a failed row never leaves a truncated or partially-rewritten file for
  Storage to upload — the row's job simply produces no output table this run, matching its
  `UserException` outcome.

### Key dependencies

Unchanged by the config-shape revision.

- `keboola-http-client` (already a dependency) — base HTTP client + retry/backoff, per above.
- `ijson` (**new dependency**) — streaming JSON parse for `paging/paged` responses, so the largest
  table (several hundred thousand rows, well over 1 GB as a fully materialized JSON body per the
  Phase 2 live probe) never needs to be held in memory as one Python object.
- No new JWT library — the `exp` claim is read via manual base64url-decoding of the JWT's payload
  segment (§3), avoiding a dependency for a single unauthenticated-read use case.
- Standard library `csv` for output writing (already used by the scaffold).

## 7. Testing — enumerate the cases up front

Row-based execution changes what a "run" test even means: the platform merges a row's parameters
with the root config **before** the component ever sees them (`config-rows.md`: "the component
always receives a single merged `config.json` — it never sees the root/row split"). So a config-row
test fixture is just a flat merged `config.json` with all seven fields present, exactly like the
prior single-config draft's fixtures — the difference is entirely in which fields are required/
present and what a table-level failure now means (a whole-row failure, not a "continue to the next
table" case). **There is deliberately no "two tables in one test" case** — the component cannot
distinguish "I am row 3 of 12" from "I am the only config that exists"; proving two rows genuinely
run as two independent jobs is a platform/Phase 7 concern (§8), not something a container-level
datadir/VCR test can exercise.

| Case | Kind | Covers |
|---|---|---|
| `01_testConnection_success` | sync-action ok | root-level `test_connection` with valid creds |
| `02_testConnection_badCredentials` | sync-action fail | `test_connection` with a real `4xx` from the token endpoint |
| `03_listTables_success` | sync-action ok | row-level `list_tables`, invoked **before `table` is set** on the row (proves the `RootConfig` partial-instantiation fix — §6 — actually works, not just that the happy path with everything filled in works) |
| `04_listTables_badCredentials` | sync-action fail | `list_tables` when auth fails before the table list can be fetched |
| `05_run_singleCall_fullLoad` | run | A row whose table's `rowCount <= page_size` — the common one-call path; asserts CSV content (headerless), manifest `schema`, PK |
| `06_run_twoCall_fullLoad` | run | A row whose table's first call returns `rowsProcessed < rowCount`, exercising the discard-and-refetch second call; asserts the final row count matches the *second* call, not a merge of both |
| `07_run_tokenRefresh_midRun` | run | A `401` mid-run triggers exactly one re-authenticate + retry, and the row's job still completes |
| `08_run_tokenEndpoint_transient502` | run | The token endpoint returns `502` once, then succeeds on retry — proves the retry-adapter's forcelist covers the token call, not just data calls |
| `09_run_tableFails_userException` | run fail (exit 1) | The row's table returns `403` on `paging/paged` — **changed from the prior single-config draft**: this now fails the row's job (`UserException`), it does not log-and-continue, because there is nothing else to continue to |
| `10_run_missingCreds` | run fail (exit 1) | Config validation failure (missing `#password` or `table`) never reaches the API at all |
| `11_run_emptyTable` | run edge | A row whose table has `rowCount = 0` still produces a (headerless) output table, not a hard failure or a missing manifest |
| `12_run_typeDowngrade` | run edge | A row whose table has an `Int`-declared column containing at least one non-numeric value in the fixture data — asserts that column lands as `string` in the manifest, not `integer` |
| `13_run_pkNotUnique` | run edge | A row whose table's `<table>_guid`-shaped column contains a duplicate value in the fixture data — asserts the manifest has no primary key, not a crash |
| `14_run_selectedTableGoneFromStructure` | run fail (exit 1) | The row's configured table is absent from a fresh `structure` call — **changed**: this is now a `UserException` (the row's only table doesn't exist), not a per-table-continue case |
| `15_run_incrementalLoad_pkFallback` | run edge | Row's `load_type=incremental_load`, but this run's PK check fails for its table — falls back to `incremental=false` for this run, logs a warning, **the job still succeeds** (this is a fallback, not a failure) |
| `16_run_secondCallFails_userException_noPartialOutput` | run fail (exit 1) | The row's table's first `paging/paged` call succeeds (short), the second (full) call fails (`5xx`/timeout) — asserts **no output file at all** for this row (the `/tmp` scratch file is never moved) **and** `UserException` — both changed from the prior draft's "no partial output, run still exits 0" |

VCR cassettes: sanitize `Authorization` and any literal password/username values in every
recorded interaction (the component-developer VCR skills' sanitizer wiring, not hand-edited
cassette files). The Phase 2 live-probe payloads are **not** reused directly as cassette fixtures —
they were captured against the real test tenant and contain real data; VCR fixtures for this
component must be **freshly recorded against small, synthetic, or explicitly-scrubbed table
content**, sized to exercise the single-call vs. two-call branch (case 06 needs a fixture table
whose declared `rowCount` is deliberately larger than a small test `page_size`, not a real
100k-row cassette). Full formatting/coverage rules: `component-test/references/vcr-configs-format.md`.

## 8. Deployment & validation (CF test project)

- `kbagent`-create a branch-tagged config in the `cf-dev` project (image tag pinned to the
  `initial-implementation` branch build, not `latest` — Phase 7's own runtime gate) with **at least
  two rows**: one row selecting a reference table small enough to complete in a single
  `paging/paged` call, and one row selecting a table known (from Phase 2's live count) to be large
  enough to force the two-call path — so both branches of §6's algorithm are exercised on the real
  API, not just in VCR, **and** so the smoke test proves what a component-level test cannot: that
  two rows genuinely execute as independent jobs (per `config-rows.md`, rows run sequentially by
  default — confirm that default, don't assume it, since enabling `parallelism` has the new
  concurrent-load consideration flagged in §9).
- A successful end-to-end run: each row's table lands in Storage with a native-typed manifest
  (`schema`, headerless CSV, per §6), the reference-table row's count matches what `structure`/live
  inspection shows for that table at run time, and the large table's row's count matches its
  `paging/paged` `rowCount` (not truncated at the configured `page_size`). Re-running the same
  row a second time reproduces the same row count (idempotent, per the original acceptance
  criteria) since every run is a fresh full snapshot.
- Confirm token refresh works against the *real* token endpoint (not just VCR) at least once during
  the smoke test, since the 1-hour TTL and the 5-minute-early refresh logic can't be meaningfully
  exercised by a fast VCR replay.
- Developer Portal `dataTypeSupport` must already be flipped to `authoritative` (Phase 6,
  before this smoke test) or the manifest's native types will be silently downgraded and the smoke
  test's typing assertions will be checking the wrong format.

## 9. Open risks & blockers

Ranked by how much they could derail correctness or a real run, not by document order. Two risks
from the prior single-config draft (container isolation / lost parallelism, and Load Type
coarseness across ~130 tables) are **resolved** by the config-shape revision and are not repeated
here — see §5's "What changed" box for what was gained. One **new** consideration (concurrent load
against an unverified rate limit) is introduced by the same revision and is risk #7 below.

1. **Large-table memory/time is unverified end-to-end for the biggest table.** The Phase 2 live
   probe fully fetched and hashed a table with well over 100,000 rows (~36.5s, ~235 MB as a single
   JSON response) but only read the row *count* — not a full fetch — for the tenant's single
   largest table, which exceeds that by roughly 4-5x and could be well over 1 GB as raw JSON.
   Mitigated by design (ijson streaming keeps peak memory far below the response size regardless of
   row count) but genuinely unverified until the Phase 7 real-tenant smoke test actually pulls that
   table end-to-end via its row. **Owner: Phase 7.**
2. **`uk` environment host is corroborated, not live-verified.** `app.retaincloud.com` comes from
   two independent public sources (login form + help PDF) but not a live `uk` tenant probe (the
   test tenant is `us`). **Confirm with the requester before a `uk` customer is provisioned.**
   **Owner: the original requester, before Phase 7 if a uk tenant becomes available, otherwise
   before the component's first real uk customer.**
3. **PK-uniqueness (and, by extension, the Incremental Load fallback) is checked per-run, not
   "verified once and trusted forever."** This is a deliberate simplification (§6) rather than
   tracking a stateful "have I verified this table's PK before" flag across runs (which would
   require using `state.json` for a purely full-load/full-fetch-by-default component, working
   against the "no state needed" simplicity this design otherwise relies on). Consequence: a row set
   to `Incremental Load` can silently behave as `Full Load` for a given run if the PK check fails
   that run — visible only via a warning log, not a UI indicator. Trade-off accepted: slightly more
   per-run CPU (building a set of GUID strings) in exchange for never trusting a stale verification
   from a much earlier run.
4. **Column-type verification only covers `Bool`/`Int`/`Float`; it does not (and cannot, from
   `richfieldstructure` alone) catch a `DateTime`-declared column carrying a genuinely malformed
   date string.** `native-data-types.md`'s own safe-default table trusts declared `DATETIME` without
   per-row verification, so this design follows that guidance rather than inventing an additional
   date-parsing verification pass beyond what the reference sanctions.
5. **`Incremental Fetch` / `Date Window` remain permanently infeasible until the `filter` DSL is
   solved**, and even then are capped at 1,000 rows per call — a real bulk incremental mode would
   need the vendor to either lift that cap for `filter` or document a genuine cursor/changed-since
   parameter on `paging/paged` that Phase 2's probe did not find (it tested roughly 30 parameter-name
   variants without finding one). Not a blocker; flagged as the standing future-incremental
   candidate.
6. **`dataTypeSupport=authoritative` is a Developer Portal switch, not a code change**, and new
   components default to `none`/legacy. If Phase 6 doesn't flip it before Phase 7's smoke test, the
   `schema` manifest this design writes will be silently downgraded to legacy `column_metadata`
   hints and the typing work in §6 will appear to have no effect. **Owner: Phase 6.**
7. **New with the config-shape revision: row-based config makes genuine concurrent execution
   possible, against a vendor API with no published rate limit and a known transient-502 failure
   mode on its token endpoint.** With a single config, every table was fetched strictly sequentially
   within one container by construction — no possibility of concurrent load. With config rows, the
   platform's `parallelism` setting (`config-rows.md`) can spin up multiple containers, each
   authenticating and fetching independently, at the same time. **Default/recommendation: leave
   `parallelism` unset (the platform's own default — rows execute sequentially in `rowsSortOrder`
   unless explicitly enabled) for V1**, and only raise it in the Developer Portal (Phase 6+) after
   confirming — ideally via a live multi-row smoke test — that the vendor's API tolerates concurrent
   token requests and concurrent `paging/paged` calls at whatever level is chosen. This is a
   genuinely new consideration introduced by this revision, not a carried-over one.
8. **No published rate limit; "no limit hit at this scale" is not "no limit exists."** The Phase 2
   probe saw no `429`/`Retry-After` up to a single ~235 MB response, but that is one table, one
   session, sequential. The retry-adapter's `429` handling (§6) is a safety net for a limit that may
   exist but wasn't triggered — and risk #7 above is exactly the scenario that would first surface
   it.
