# keboola.ex-retain-cloud — Design Spec

> Type: extractor
> Component ID: keboola.ex-retain-cloud
> Status: draft
> Date: 2026-09-21

## 1. Overview & source system

`keboola.ex-retain-cloud` extracts data from Retain Cloud (retaininternational.com), a
resource-planning SaaS, via its **DataAccessAPI** — a generic, tenant-scoped, table-access REST
API (JSON over HTTPS). One call lists every table a service account may read; one call reads a
table's rows; there is no history kept server-side, so a full snapshot of the selected tables *is*
the extractor's output on every run.

Primary use case: a customer that runs Retain Cloud for resource/booking/timesheet planning wants
its operational tables mirrored into Keboola Storage so they can be joined with other systems
(finance, HR, project data) in transformations, without hand-rolling API calls per table.

There is no public top-level API reference URL to link (the vendor ships its API reference as a
ReDoc-style portal export behind a customer login, plus a short help-center PDF); source material
for this spec is a vendor "Retain API Docs" export and a live-credentialed probe against a test
tenant, both recorded in the (gitignored, local-only) Phase 2 research doc.

## 2. Keboola mapping

- **Source objects → output tables.** Each selected Retain Cloud table (`api/structure` entry)
  maps 1:1 to one Keboola Storage output table, named after the source table (e.g. `booking` →
  `booking.csv` / table `booking`). No fan-out, no joins, no child tables — the source is already
  flat, prefixed rows (`<table>_<field>` columns).
- **Single config with a `tables[]` multi-select, not config rows.** This is a deliberate override
  of the Tier A "multiple objects → config rows" convention — see the **UI scope & config shape**
  decision in §5 for the full justification. In short: every selected table is fetched with
  *identical* mechanics (authenticate once → discover once → fetch full table → write CSV), there
  is no per-table configuration difference a row would carry, and there is no per-object
  incremental state to keep independent (V1 is full-load / full-fetch only, see below) — the one
  genuine benefit config rows would buy (independent per-row state and job-level retry isolation)
  doesn't apply here, so the cost (up to ~130 rows to manage in the UI, ~130x more calls to the
  vendor's own token endpoint) isn't worth paying.
- **Extraction modes (required).** `fetch_mode = full_fetch` is hardcoded/omitted from the UI (the
  sanctioned override — see below). `load_type` **is** a real, always-visible, ungated UI picker
  (`Full Load` / `Incremental Load`, default `Full Load`) — after the grounding-reconciliation pass
  on this spec, an earlier draft that hardcoded `load_type` too was corrected, because
  `extraction-modes.md` grants no such override for Load Type (unlike Fetch Mode, which explicitly
  permits omission when only one mode is possible; Load Type has no equivalent clause — it is
  "always visible, never gated" with no stated exception). `state.json` is not used regardless of
  which `load_type` is picked: Full Fetch is stateless by definition (no cursor exists to persist),
  and Incremental *Load* is a Storage-side upsert-on-primary-key mechanic (`incremental=true` in
  the manifest), not a `state.json` cursor — the two axes stay independent per the standard.

  **Why Full Fetch is omitted (sanctioned):** no verified "changed since X" filter exists. The only
  endpoint that could carry that semantic (`tableaccess/{table}/filter`) has an undocumented
  request-body DSL (never reverse-engineered — see §4, Excluded/deferred) and is capped at 1,000
  rows even if it did — useless as a bulk incremental mechanism for tables running into the hundreds
  of thousands of rows. `Incremental Fetch` and `Date Window` are both infeasible today; `Full
  Fetch` is the only implementable mode, so the field is omitted per the explicit "when only one
  mode is possible, omit the field entirely" clause.

  **Why Load Type stays a real, visible picker (not hardcoded), and how the single-config PK risk
  is handled instead:** the config is a single config covering every selected table (§5), so
  `load_type` necessarily applies *globally* per run — but not every selected table is guaranteed
  to have a stable, verified-unique `<table>_guid` this run (§6). Rather than hardcoding the field
  away to dodge that risk (which would have been an unsanctioned deviation from "always visible,
  never gated" with no textual basis in `extraction-modes.md`), the picker stays real and defaults
  to `Full Load`; when a user selects `Incremental Load`, the *per-table* application is
  conditional: a table only loads as `incremental=true` (upsert) if its PK passed this run's
  uniqueness check (§6); a table whose PK check fails or has no `<table>_guid` column falls back to
  `incremental=false` for that table only, with a per-table warning logged (`incremental_load`
  requested but PK not verified for `<table>`, falling back to full load) — the same
  verify-then-fall-back idiom the original component request already uses for PK detection itself,
  extended one step further. `Full Load` remains the strongly recommended default in the field's UI
  description, since Retain Cloud is a live, mutable, current-state system (bookings get cancelled,
  records get deleted) with **no server-kept history** — an upsert-only load never removes rows the
  source has since deleted — but `Incremental Load` is a legitimate, available V1 choice, not
  deferred to a hypothetical V2.
  - The default pairing, **Full Load + Full Fetch = "Mirror"** in the extraction-modes taxonomy, is
    the one combination where upstream deletes propagate — the desired default for a source with no
    native history. **Incremental Load + Full Fetch = "Refresh-by-upsert"** is the available
    alternative for a user who wants accumulation instead (accepting that deletes are never
    removed), gated per-table by the PK-safety fallback above.
- **Secrets → `#`-prefixed config keys.** `#password` (see §5) is the only secret; it maps to the
  API's `userpassword` field. `username`, `tenant`, `environment` are not secrets (they identify
  the account/tenant, not authenticate it) and are stored in plain config parameters, matching the
  already-provisioned `secrets.json` fixture's key names (`username`, `#password`, `tenant`).
- **Test-connection / dynamic dropdowns.** Two sync actions: `test_connection` (Tier A default —
  validates the four connection fields by authenticating) and `list_tables` (populates the `tables`
  multi-select from `api/structure`).
- **Output bucket.** Default-bucket behaviour (`in.c-{componentId}-{configId}`) — no hardcoded
  destination in the manifest; the Developer Portal's `default_bucket: true` setting (Phase 6)
  governs this per `keboola-context/references/default-bucket.md`.

## 3. Authentication & connection

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
  genuine `400`/`401`/`403` is returned).
- **Never log the token or password**, at any log level, in any exception message. This is a
  named, confirmed incident risk (a debug run of a different extractor previously leaked a full
  JWT into a Keboola job log) — see §6 for the concrete mechanism (the code never builds a log
  string containing the auth header or the config's password field; the http client's
  `_auth_header` is never dumped).

## 4. Capability inventory & scope

Every operation in the vendor's exported API reference, plus the two endpoints known only from the
live probe (`IntegrationApi/token`, `paging/paged`) and the one explicitly closed one
(`IdentityServerAPI`). This is the full DataAccessAPI menu (Phase 2's capability inventory,
verbatim) — nothing is silently narrowed.

| Capability | Verdict | Rationale |
|---|---|---|
| `POST IntegrationApi/token` | **In scope** | The only supported machine auth path (§3). |
| `POST IdentityServerAPI/.../connect/token` | Excluded | Closed to direct callers — returns `403`. Not a usable capability at all, not a scope choice. |
| `GET api/structure` | **In scope** | Table list → `list_tables` sync action; also re-checked at the start of every run to confirm selected tables still exist. |
| `GET api/structure/tablestructure` | **In scope** | Human-readable table alias/description, used to enrich the `list_tables` sync-action labels (raw table name remains the stored value). |
| `GET api/structure/fieldnames?table=` | Excluded (redundant) | Strict subset of `richfieldstructure`'s field list — no unique data, one more round trip for nothing once `richfieldstructure` is already being called per table. |
| `GET api/structure/fieldstructure?table=` | Excluded (superseded) | `richfieldstructure` is a live-verified strict superset (same 61 fields on the sampled table, plus 10 extra keys) at the same call cost — no reason to keep both. |
| `GET api/structure/richfieldstructure?table=` | **In scope** | Core discovery call: column names/order for the CSV header, `dataType` for manifest typing (§6), and detection of the `<table>_guid` primary-key candidate. |
| `GET api/tableaccess/{table}` | Excluded (superseded by design) | The design constraints allow using this for small (<1,000-row) reference tables, but it is deliberately **not** used — see the justified deviation below. |
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
rather than open; (3) one code path for all 129+ tables is simpler to reason about, test, and
review than two response shapes (bare array vs. envelope) gated on a size threshold. No capability
is lost — every table remains fully readable via `paging/paged`.

Pagination, rate limits, and the response envelope are fully covered in §6 (the mechanics are
inseparable from the algorithm, not a separate note).

## 5. Configuration & schema

### Fields

| Field | Level | Required | Purpose |
|---|---|---|---|
| `environment` | config | yes | Selects the API host (`us`/`eu`/`uk`/`aus`) via the explicit map in §3 — never a bare string interpolation. |
| `tenant` | config | yes | Case-sensitive tenant identifier, sent in the token request body. |
| `username` | config | yes | Maps to the API's `useremail` field. |
| `#password` | config | yes (secret) | Maps to the API's `userpassword` field. Platform-encrypted (`KBC::ProjectSecure::`), decrypted to plaintext at runtime, never logged. |
| `tables` | config | yes, min 1 | Multi-select of source table names, populated by the `list_tables` sync action from live `api/structure` output. Stores the raw table name (the value), not the human label. |
| `page_size` | config | no (default `20000`) | The row cap used on a table's *first* `paging/paged` call (§6) — not a true "page size," since there is no continuation mechanism; see the algorithm for exactly how it's used. |
| `load_type` | config | no (default `full_load`) | `full_load` / `incremental_load`, always visible per the extraction-modes convention (§2) — applies globally per run; per-table PK-safety fallback is handled in code, not in the schema (§6). |

`fetch_mode` is **not** a schema field in V1 (§2) — it is a fixed, internal constant in the
Pydantic model (`full_fetch`), not user-visible, not persisted as a config parameter, and not
derived from any other field (the sanctioned Fetch-Mode-omission override). `load_type` **is** a
real schema field (above) — see §2 for why it is not similarly omitted. There is also no user-facing primary-key
picker: the PK is auto-detected per table from `richfieldstructure` plus a per-run uniqueness check
(§6) — the UI-schema convention "a stable source record id is used as the PK automatically; the
picker appears only when the source has no reliable id" applies directly, and Retain Cloud's own
`<table>_guid` naming convention is exactly that reliable id.

### Sync actions

- **`test_connection`** (Tier A default — always add one). Authenticates with the four connection
  fields (`environment`, `tenant`, `username`, `#password`) and reports success/failure; does not
  need to call `structure` as well, since a successful token response already proves every
  connection field is correct (`environment`/`tenant` are part of the token request body itself, so
  a bad tenant or wrong environment host both surface as a token-endpoint failure).
- **`list_tables`**. Calls `api/structure` (the list of readable table names) and
  `api/structure/tablestructure` (aliases/descriptions) and returns one selectable option per
  table: value = raw table name, label = alias/description when available, else the raw name. This
  needs `environment`/`tenant`/`username`/`#password` already filled in (same as `test_connection`)
  — a standard "depends on the connection fields" sync action, nothing table-specific to depend on.

### UI scope & config shape

| Field | Level | Required | User-facing or internal | Default | In a NEW config's saved params? |
|---|---|---|---|---|---|
| `environment` | config | yes | user-facing (enum select) | none — user must choose | yes |
| `tenant` | config | yes | user-facing (text) | none | yes |
| `username` | config | yes | user-facing (text) | none | yes |
| `#password` | config | yes | user-facing (password widget) | none | yes |
| `tables` | config | yes | user-facing (multi-select, sync-action backed) | none | yes — the selection is the entire point of the config |
| `page_size` | config | no | user-facing, under an "Advanced options" section | `20000` | yes — a single visible, self-explanatory number field; no `dependencies` gate to hide it behind |
| `load_type` | config | no | user-facing, always visible (never gated) | `full_load` | yes — a single visible, self-explanatory enum field |
| `fetch_mode` (internal marker) | — | n/a | internal — not in the schema at all | `full_fetch` (Pydantic default) | no |

**Config-shape override — single config + `tables[]` multi-select instead of config rows.** This
is the "your call" decision the component request explicitly left open. **Honest framing, corrected
after the grounding-reconciliation pass on this spec:** this does **not** fit the Tier A
convention's own stated override clause — `architecture-conventions.md` sanctions skipping config
rows only when "there's genuinely one object, or the objects must be fetched together in a single
logical transaction," and neither is true here (this is explicitly ~130 independent objects, never
fetched together). This is a **deliberate engineering-judgment deviation beyond what the convention
itself sanctions**, not an application of an existing escape hatch — flagged plainly for the
reviewing lead/user to accept or reject, not presented as a natural fit. The reasoning for making
that deviation anyway:

1. **The main config-rows *state* benefit doesn't apply.** Config rows exist primarily to give each
   object its own independent `state.json` (incremental cursor). V1's Fetch Mode is `full_fetch`
   only (§2) — there is no per-table cursor to keep independent, so that specific benefit is moot.
   (Config rows' *other* benefits — see point 5 — are real and are being traded away, not "moot.")
2. **Every table is fetched identically.** Config rows shine when each row carries genuinely
   different settings (a different fetch field, a different output-table override, a different
   filter). Here every selected table goes through the exact same mechanics — there is no
   per-object configuration to justify a row per object.
3. **UX at this scale.** A tenant can expose over a hundred tables. Managing that as a `tables[]`
   multi-select backed by a `list_tables` sync-action dropdown is one screen; managing it as ~130
   individually-created config rows is not — and the request's own framing ("row-per-table or
   table multi-select — your call") treats this as a real, undecided choice.
4. **Avoids needless auth traffic.** Config rows would mean up to ~130 separate container runs,
   each independently calling `IntegrationApi/token` with the same shared credential — the token
   endpoint's own known `502`/`503` transient-failure mode (§3) means more calls is strictly more
   exposure to that failure mode, for a credential that isn't actually row-scoped.
5. **What's traded away, stated plainly (expanded per reconciliation):** (a) true per-table process
   isolation — one table's OOM or hang can't affect a sibling row's container, whereas here it's all
   one container/process; (b) free per-row retry-from-the-UI; (c) **the platform's built-in
   `parallelism` feature** (`config-rows.md`: "if parallelism is enabled, the platform spins up
   multiple identical container instances in batches") — with a single config, every table is
   fetched strictly sequentially within one container (§6, §9 risk #10), and there is no equivalent
   opt-in for concurrent table fetches in this design. This design compensates for (a)/(b) with
   in-process per-table isolation instead — a caught exception for one table does not abort the run
   for the others (§6) — but does **not** compensate for (c) at all; sequential wall-clock time is a
   real, accepted cost of this override, not an oversight. If per-table concurrency or truly
   independent retry becomes a real operational need, moving to config rows is the natural
   corrective path — not something this design forecloses, but also not something it approximates.

### UI presentation

| Field | Widget | Notes |
|---|---|---|
| `environment` | `enum` + `enum_titles` (`US`, `EU`, `UK`, `Australia`) | Fixed 4-value list; no free text — the host map in §3 only covers these four. |
| `tenant` | text | Case-sensitive, no enumeration possible (per-customer value). |
| `username` | text | Not a secret — the email/account name, not the credential. |
| `#password` | password | Encrypted; matches the `#password` config key exactly. |
| `tables` | creatable-off multi-select, async `select` + `autoload`, backed by `list_tables` | First thing a user must fill after the connection fields, so it autoloads once those four are present (`autoload: ["environment","tenant","username","#password"]`). Stores raw table names. |
| `load_type` | `enum` + `enum_titles` (`Full Load`, `Incremental Load`) | Always visible, no `options.dependencies` gate (per `extraction-modes.md`). Default `Full Load`; description notes `Incremental Load` upserts on a per-table basis and silently keeps deleted-upstream rows, with the per-table PK-safety fallback (§6) explained in `options.tooltip`. |
| `page_size` | number, inside an "Advanced options" (`type: object`, collapsible) section | Default `20000`; a one-line description explaining it as a fetch-sizing cap, not a page count, with the full mechanics in `options.tooltip`. |

## 6. Code architecture

### Files

- `src/configuration.py` — the Pydantic `Configuration` model (`environment` enum, `tenant`,
  `username`, `password: SecretStr = Field(alias="#password")`, `tables: list[str]`,
  `page_size: int = 20000`), `model_config = ConfigDict(extra="forbid")`. Internal-only constants
  (`load_type = "full_load"`, `fetch_mode = "full_fetch"`) live as plain class attributes / a
  comment, not as configurable fields — there is nothing for a user to set.
- `src/client.py` — `RetainCloudClient(keboola.http_client.HttpClient)`. The repo already declares
  `keboola-http-client>=1.0.1` as a dependency (confirmed by reading the installed
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
- `src/extractor.py` — the per-table fetch-and-write logic, kept separate from `run()`'s
  orchestration and from the HTTP client:
  - `build_table_schema(rich_fields)` — turns a `richfieldstructure` response into an ordered
    column list, the manifest `schema` (see typing rules below), and the PK candidate column name
    (`<table>_guid`, if present in the field list).
  - `fetch_table(client, table, page_size, scratch_dir)` — implements the two-call algorithm below,
    streaming rows into a **`/tmp` scratch file** (never directly into `/data/out/tables/` — see
    the corrected staging rule below) and returning the scratch file's path, the final row count,
    the set of encountered "declared-type still holds" flags per column, and the observed-unique
    flag for the PK column. Raises on any HTTP/parse failure; never leaves a partially-written file
    outside `/tmp`.
  - Nothing here is a config concern — it operates purely on a `RetainCloudClient`, a table name,
    a page-size cap, and a scratch directory.
- `src/component.py` — thin `Component(ComponentBase).run()`:
  1. Parse config into `Configuration`.
  2. Construct `RetainCloudClient`, call `authenticate()`.
  3. Call `client.list_tables()`; for any configured table missing from the live list, treat it as
     a per-table failure exactly like a `403`/`404` (see below) rather than aborting the run.
  4. For each remaining selected table, **sequentially** (see risk note in §9 on why this is not
     parallelized in V1): call `get_table_schema` → `fetch_table` (writes to `/tmp` only) → **on
     success**, move the finished scratch file into the `create_out_table_definition`'s
     `full_path` (under `/data/out/tables/`) and write its manifest — **on any failure, the `/tmp`
     scratch file is simply never moved and is left for the container's teardown to discard**, so
     `/data/out/tables/` never contains a partial or truncated file for a table the log reports as
     failed. Wrap the whole per-table block (schema fetch, `fetch_table`, move, manifest write) in
     a `try/except` that catches `requests.HTTPError` (any status) and any `ijson`/`OSError` parse
     or write failure; on failure, log a warning naming the table and the HTTP status/error, add
     the table to a `failed_tables` list, and `continue` to the next table.
  5. After the loop: if `failed_tables` is non-empty but shorter than the full selection, log a
     summary warning listing every failed table and exit normally (`0`) — the tables that did
     succeed are still written to Storage, matching "fail the table, not the whole run." If
     **every** selected table failed, raise `UserException` listing all of them — nothing useful
     happened, so the job should show as failed.
  6. `run()` itself stays under the checklist's informal "thin orchestrator" bar — each numbered
     step above is one call to a private method, not inline logic.

### The `paging/paged` fetch algorithm (per table)

This is the resolved paging contract (Phase 2, live-probed): `pageSize` is a single-call row cap,
not a page window, and there is no continuation token — the same request repeated returns the same
first N rows every time. The envelope is always
`{"key": <fresh guid, not a cursor>, "rowCount": <int, grand total>, "rowsProcessed": <int, this call>, "data": [...]}`.

**Corrected staging rule (grounding-reconciliation finding):** an earlier draft streamed rows
directly into the eventual `/data/out/tables/` output file and, on the two-call path, truncated and
rewrote that same file in place. Per `output-mapping.md` ("every file placed under
`/data/out/tables/` is uploaded — not just entries in the output mapping") and
`environment-variables.md` ("do not use `/data/` for temp files... use `/tmp/` for scratch work"),
that is unsafe: if the *second* call then failed, the per-table `except` block would `continue` to
the next table having left a truncated, half-rewritten file sitting in `/data/out/tables/` for a
table the log reports as failed — and it would still be uploaded. **Fix: every `paging/paged`
response, first or second call, streams into a fresh file under `/tmp/` (e.g.
`/tmp/ex-retain-cloud/{table}.csv`), never into `/data/out/tables/` directly.** Only after a
table's fetch fully succeeds does `component.py` move that finished `/tmp` file into
`/data/out/tables/` and write the manifest (see the `component.py` steps above); a failed second
call simply means the `/tmp` file is discarded/overwritten and nothing is ever moved.

1. `resp = POST paging/paged?pageSize={config.page_size}&sequential=true`, streamed
   (`stream=True`), parsed incrementally with `ijson.parse` (not `ijson.items`, since we need both
   the scalar `rowCount`/`rowsProcessed` keys *and* to stream `data` item-by-item from the same
   response body — `ijson.items` alone only yields the matched array elements and discards sibling
   scalars). Rows are written to the **`/tmp` scratch file** as they're parsed (one
   `csv.DictWriter.writerow` per row, `restval=""` for any field the schema expects but a given row
   omits, `extrasaction="ignore"` for any field a row carries that the schema didn't declare —
   logged once per table as a schema-drift warning, not per row). A `set()` of the PK column's
   values is accumulated as rows stream past, for the uniqueness check below.
2. If `rowsProcessed >= rowCount` (the common case — using the customer's own known-working default
   of `page_size=20000` as the first call's size means the large majority of a 129-table tenant's
   tables complete in exactly this one call): done. The PK is kept if a `<table>_guid` column
   exists **and** `len(pk_values_set) == rowsProcessed` (no duplicates observed this run). The
   `/tmp` file is now the complete result, ready to be moved by `component.py`.
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
   (wasted, discarded) first call's cap for the handful of tables above it. A user with unusually
   large reference tables, or who wants to trade a slightly bigger first-call payload for fewer
   two-call tables, can raise it.

### Column typing — reconciling two grounding constraints

Two grounding references pull in different directions here, and this design reconciles them rather
than picking one and ignoring the other:

- `keboola-context/references/native-data-types.md`: "when you have **not** confirmed a field's
  values against sampled data, do not emit native numeric/boolean" — and Retain Cloud's
  `richfieldstructure.dataType` (`Bool`/`DateTime`/`Float`/`ID`/`Int`/`String`/`Unknown`) was only
  spot-checked against real returned rows for two tables during Phase 2 research, not the full
  table menu.
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
  including JSON `null`) on every value encountered for that table this run. If **any** value
  fails, that column is downgraded to `string` for this run's manifest (logged once, naming the
  table/column/offending value's *type*, never the value itself if it could be sensitive data). If
  **all** values conform, the column is emitted as native `integer`/`float`/`boolean`. This check
  is per-table, per-run — the manifest schema, not just the CSV content, can differ between runs
  for the same table if the source's real data composition changes, which is the correct behaviour
  (it means the verification is real, not a one-time guess baked in at design time).
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

- `UserException` (exit 1): auth failure with credentials supplied (a real `4xx` from the token
  endpoint after the retry-adapter's forcelist is exhausted — i.e. `502`/`503` are retried
  transparently, a real `400`/`401`/`403` is not); config validation failure (Pydantic); **every**
  selected table failing during the run (nothing useful produced).
- Logged warning, run continues (exit 0 overall): one table's `structure` entry missing at run
  time; one table's `richfieldstructure`/`paging/paged` call returning `403`/`404`; one table's
  second `paging/paged` call still short of `rowCount` after the 2% margin; a column's declared
  type downgraded to `string` after a failed per-row coercion check; a row carrying a field not
  present in the table's discovered schema (schema drift); `load_type=incremental_load` requested
  but a table's PK wasn't verified unique this run (falls back to `incremental=false` for that
  table only, §2/§6).
- Unexpected (exit 2): anything not covered above — a genuine bug, not a user-fixable condition.
- **No partial output on a per-table failure.** Because every `paging/paged` response streams into
  a `/tmp` scratch file and is only moved into `/data/out/tables/` after that table's fetch fully
  succeeds (§6 staging rule), a caught exception for one table never leaves a truncated or
  partially-rewritten file for Storage to upload — the failed table simply produces no output file
  this run, which is the correct behaviour for "fail the table, not the whole run."

### Key dependencies

- `keboola-http-client` (already a dependency) — base HTTP client + retry/backoff, per above.
- `ijson` (**new dependency**) — streaming JSON parse for `paging/paged` responses, so the largest
  table (several hundred thousand rows, well over 1 GB as a fully materialized JSON body per the
  Phase 2 live probe) never needs to be held in memory as one Python object.
- No new JWT library — the `exp` claim is read via manual base64url-decoding of the JWT's payload
  segment (§3), avoiding a dependency for a single unauthenticated-read use case.
- Standard library `csv` for output writing (already used by the scaffold).

## 7. Testing — enumerate the cases up front

| Case | Kind | Covers |
|---|---|---|
| `01_testConnection_success` | sync-action ok | `test_connection` with valid creds |
| `02_testConnection_badCredentials` | sync-action fail | `test_connection` with a real `4xx` from the token endpoint |
| `03_listTables_success` | sync-action ok | `list_tables` returns the live `structure` + `tablestructure` merge |
| `04_listTables_badCredentials` | sync-action fail | `list_tables` when auth fails before the table list can be fetched |
| `05_run_singleCall_fullLoad` | run | A table whose `rowCount <= page_size` — the common one-call path; asserts CSV content (headerless), manifest `schema`, PK |
| `06_run_twoCall_fullLoad` | run | A table whose first call returns `rowsProcessed < rowCount`, exercising the discard-and-refetch second call; asserts the final row count matches the *second* call, not a merge of both |
| `07_run_multiTable_mixedSizes` | run | Two tables selected in one config, one single-call-sized and one two-call-sized, in the same run — proves the per-table loop and per-table file reset don't leak state between tables |
| `08_run_tokenRefresh_midRun` | run | A `401` on a data call mid-run triggers exactly one re-authenticate + retry, and the run still completes |
| `09_run_tokenEndpoint_transient502` | run | The token endpoint returns `502` once, then succeeds on retry — proves the retry-adapter's forcelist covers the token call, not just data calls |
| `10_run_perTableFailure_403` | run | One selected table returns `403` on `paging/paged`; the run still succeeds (exit 0), the other selected table(s) still produce output, and the failure is logged by table name |
| `11_run_allTablesFail` | run fail (exit 1) | Every selected table returns `403`/`404` — asserts `UserException`, not a silent exit 0 |
| `12_run_missingCreds` | run fail (exit 1) | Config validation failure (missing `#password` or `tables`) never reaches the API at all |
| `13_run_emptyTable` | run edge | A selected table with `rowCount = 0` still produces a header-only output table, not a hard failure or a missing manifest |
| `14_run_typeDowngrade` | run edge | A table where an `Int`-declared column contains at least one non-numeric value in the fixture data — asserts that column lands as `string` in the manifest, not `integer` |
| `15_run_pkNotUnique` | run edge | A table whose `<table>_guid`-shaped column contains a duplicate value in the fixture data — asserts the manifest has no primary key for that table, not a crash |
| `16_run_selectedTableGoneFromStructure` | run edge | A configured table name absent from a fresh `structure` call — treated as a per-table failure (like case 10), not a hard config error |
| `17_run_incrementalLoad_pkFallback` | run edge | `load_type=incremental_load` selected; one table's PK verifies unique (emitted `incremental=true` + PK) and a second table's PK fails verification (falls back to `incremental=false`, logged) in the same run |
| `18_run_secondCallFails_noPartialOutput` | run edge | A table's first `paging/paged` call succeeds (short), the second (full) call fails (`5xx`/timeout) — asserts **no file at all** appears under that table's output path this run (the `/tmp` scratch file is never moved), not a truncated table |

VCR cassettes: sanitize `Authorization` and any literal password/username values in every
recorded interaction (the component-developer VCR skills' sanitizer wiring, not hand-edited
cassette files). The Phase 2 live-probe payloads are **not** reused directly as cassette fixtures —
they were captured against the real test tenant and contain real data; VCR fixtures for this
component must be **freshly recorded against small, synthetic, or explicitly-scrubbed table
content**, sized to exercise the single-call vs. two-call branch (case 06/07 need a fixture table
whose declared `rowCount` is deliberately larger than a small test `page_size`, not a real
100k-row cassette). Full formatting/coverage rules: `component-test/references/vcr-configs-format.md`.

## 8. Deployment & validation (CF test project)

- `kbagent`-create a branch-tagged config in the `cf-dev` project (image tag pinned to the
  `initial-implementation` branch build, not `latest` — Phase 7's own runtime gate). Table
  selection for the smoke test: a small mix — at least one reference table small enough to
  complete in a single `paging/paged` call, and at least one table known (from Phase 2's live
  count) to be large enough to force the two-call path — so both branches of §6's algorithm are
  exercised on the real API, not just in VCR.
- A successful end-to-end run: every selected table lands in Storage with a native-typed manifest
  (`schema`, headerless CSV, per §6), the reference-table row count matches what `structure`/live
  inspection shows for that table at run time, and the large table's row count matches its
  `paging/paged` `rowCount` (not truncated at the configured `page_size`). Re-running the same
  config a second time reproduces the same row counts (idempotent, per the original acceptance
  criteria) since every run is a fresh full snapshot.
- Confirm token refresh works against the *real* token endpoint (not just VCR) at least once during
  the smoke test, since the 1-hour TTL and the 5-minute-early refresh logic can't be meaningfully
  exercised by a fast VCR replay.
- Developer Portal `dataTypeSupport` must already be flipped to `authoritative` (Phase 6,
  before this smoke test) or the manifest's native types will be silently downgraded and the smoke
  test's typing assertions will be checking the wrong format.

## 9. Open risks & blockers

Ranked by how much they could derail correctness or a real run, not by document order.

1. **Large-table memory/time is unverified end-to-end for the biggest table.** The Phase 2 live
   probe fully fetched and hashed a table with well over 100,000 rows (~36.5s, ~235 MB as a single
   JSON response) but only read the row *count* — not a full fetch — for the tenant's single
   largest table, which exceeds that by roughly 4-5x and could be well over 1 GB as raw JSON.
   Mitigated by design (ijson streaming keeps peak memory far below the response size regardless of
   row count) but genuinely unverified until the Phase 7 real-tenant smoke test actually pulls that
   table end-to-end. **Owner: Phase 7.**
2. **`uk` environment host is corroborated, not live-verified.** `app.retaincloud.com` comes from
   two independent public sources (login form + help PDF) but not a live `uk` tenant probe (the
   test tenant is `us`). **Confirm with the requester before a `uk` customer is provisioned.**
   **Owner: the original requester, before Phase 7 if a uk tenant becomes available, otherwise
   before the component's first real uk customer.**
3. **PK-uniqueness is checked per-run, not "verified once and trusted forever."** This is a
   deliberate simplification (§6) rather than tracking a stateful "have I verified this table's PK
   before" flag across runs (which would require using `state.json` for a purely full-load/
   full-fetch component, working against the "V1 has no state" simplicity this design otherwise
   relies on). Trade-off: slightly more per-run CPU (building a set of GUID strings) in exchange for
   never trusting a stale verification from a much earlier run. Not expected to be a real cost even
   on the largest table.
4. **Column-type verification only covers `Bool`/`Int`/`Float`; it does not (and cannot, from
   `richfieldstructure` alone) catch a `DateTime`-declared column carrying a genuinely malformed
   date string.** `native-data-types.md`'s own safe-default table trusts declared `DATETIME` without
   per-row verification, so this design follows that guidance rather than inventing an additional
   date-parsing verification pass beyond what the reference sanctions.
5. **`Incremental Fetch` / `Date Window` remain permanently infeasible until the `filter` DSL is
   solved**, and even then are capped at 1,000 rows per call — a real bulk incremental mode would
   need the vendor to either lift that cap for `filter` or document a genuine cursor/changed-since
   parameter on `paging/paged` that Phase 2's probe did not find (it tested roughly 30 parameter-name
   variants without finding one). Not a V1 blocker; flagged as the standing future-incremental
   candidate.
6. **`load_type` is a single, global picker applied per-run across every selected table, with a
   per-table PK-safety fallback rather than per-table granularity (§2).** After the
   grounding-reconciliation pass, this design keeps Load Type a real, always-visible, ungated
   picker (as `extraction-modes.md` requires, with no override clause) instead of hardcoding it —
   but it is still coarser than the row-level `load_type` `incremental-state.md` describes as the
   norm, because this spec deliberately isn't using config rows (risk #7). A user who selects
   `Incremental Load` may be surprised that some tables silently stay full-load under the hood if
   their PK didn't verify unique this run; the per-table warning log is the only signal, not a UI
   indicator. **This needs the reviewing lead/user's explicit sign-off**, since the row-level
   alternative (config rows) was available and not chosen, for the reasons in §5.
7. **The config-shape override (single config, not config rows) trades away container-level
   per-table isolation, free per-row UI retry, and the platform's built-in `parallelism` feature for
   concurrent table fetches** (§5) — confirmed as a real, not hypothetical, lost capability by the
   grounding-reconciliation pass against `config-rows.md`. Reasonable for a same-credential,
   same-mechanics, full-load-only extractor; worth revisiting if real usage wants per-table
   concurrency or independent retry-from-the-UI. This override does **not** fit
   `architecture-conventions.md`'s own stated escape hatch for skipping config rows (neither "one
   object" nor "a single logical transaction" applies) — it is accepted here as a deliberate
   engineering trade-off beyond that convention, not a natural fit for it, and needs the same
   explicit sign-off as risk #6.
8. **`dataTypeSupport=authoritative` is a Developer Portal switch, not a code change**, and new
   components default to `none`/legacy. If Phase 6 doesn't flip it before Phase 7's smoke test, the
   `schema` manifest this design writes will be silently downgraded to legacy `column_metadata`
   hints and the typing work in §6 will appear to have no effect. **Owner: Phase 6.**
9. **No published rate limit; "no limit hit at this scale" is not "no limit exists."** The Phase 2
   probe saw no `429`/`Retry-After` up to a single ~235 MB response, but that is one table, one
   session. The retry-adapter's `429` handling (§6) is a safety net for a limit that may exist but
   wasn't triggered, not a confirmed-absent risk.
10. **Sequential per-table processing means one very large or slow table blocks every table queued
    behind it**, and there is no partial-progress checkpoint if the container is killed mid-run —
    a restarted run re-fetches every table from scratch. This matches the source's own
    "full-refetch-only" model (there is no server-side resume point either), but is worth naming
    explicitly for an operator selecting all ~130 tables in one config: total wall-clock time is the
    sum of every table's fetch time, not the max.
