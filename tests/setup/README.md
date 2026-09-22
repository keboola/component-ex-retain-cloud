# VCR recording setup

Cassettes in `tests/functional/` are committed to a **public** repo, and the tenant behind
`secrets.json` is a **real customer**. Read this before recording anything.

## Why there are two definition files and three secrets combinations

`python -m keboola.datadirtest scaffold --secrets <file>` deep-merges that one file over *every*
test's `parameters` for the whole invocation (`keboola/vcr/scaffolder.py::_record_test`). There is
no per-test opt-out. So a "wrong credentials" test cannot live in the same invocation as the
success tests — the real password would silently override the deliberately-wrong one and the test
would pass instead of failing. Hence the definitions split.

The bad-credentials pass then needs a **third** file, because it needs a credential combination
that neither of the other two has: the **real** tenant with a **wrong** password.

| Definitions | Tests | Secrets |
|---|---|---|
| `configs.json` | `01_testConnection_success`, `03_listTables_success`, `05_run_fullLoad_success`, `06_run_tableNotInStructure` | `secrets.json` (real tenant, real password) |
| `configs_badpassword.json` | `02_testConnection_badCredentials`, `04_listTables_badCredentials`, `07_run_authFailure` | `secrets_badpassword.json` (real tenant, **deliberately wrong** password) |

So it is two definition files but three definitions×secrets combinations in effect: the third
combination — `configs_badpassword.json` recorded with **no** secrets at all, i.e. fully
self-contained fake values — is the one that is **wrong** and must not be used. It is what the
first recording run did, and the reason this section exists.

### Why the bad-credentials tests need a real tenant

Live-probed against the token endpoint (status codes and the API's own generic error text only):

| tenant | user + password | result | `client.authenticate()` branch |
|---|---|---|---|
| fake (`"tenant"`) | fake | **504**, an HTML gateway error page | `RequestException` → 5 retries with backoff → "API unavailable after retries" |
| **real** | fake | **`400 {"error":"invalid_request","error_description":"Your email or password is invalid."}`** | `requests.HTTPError` → `UserException` immediately |

The 504 is a routing failure for a tenant that does not exist — the gateway never reaches the
credential check. `authenticate()` still produces a `UserException`, so the test "passes", but it
exercises the wrong branch, takes five backed-off retries to do it, and duplicates what
`tests/test_client.py::test_authenticate_raises_user_exception_on_retries_exhausted` already covers
with a mock. The 400 is the intended failure path and is the live counterpart of
`test_authenticate_raises_user_exception_on_401` — which until now had no VCR coverage at all.

Only the **password** is overridden. `username` is deliberately absent from
`secrets_badpassword.json`: the merge only overrides keys present in the override file, so
`configs_badpassword.json`'s fake `not-a-real-user@example.com` stays in place, and a wrong
password alone is sufficient for the clean 400 (verified live — the tenant is what mattered).

### The real tenant now reaches those three cassettes — this is fine

Pre-sanitization, the token POST body for `02`/`04`/`07` now contains the real tenant, exactly as
it already did for the success cassettes. Nothing new needs wiring in `component.py`; the existing
chain covers it identically:

- `DefaultSanitizer(additional_sensitive_fields=[..., "tenant"])` redacts the token request body
  by **field name**, which is where the tenant appears on these three (the token URL is
  `/IntegrationApi/token` — no tenant in the path).
- `UrlPatternSanitizer` rewrites `/DataAccessAPI/<tenant>/` in the path — not reached by these
  three, since auth fails first, but it is what covers the success cassettes.
- `secrets_badpassword.json` carries its own `#vcr_redact` mirror block (written by the generator
  script) as the exact-value backstop. This mirror is **required in this file too**: pass 2 records
  with `--secrets secrets_badpassword.json`, so `secrets.json`'s mirror block is not loaded for
  that pass at all.

## Generating `secrets_badpassword.json`

Never hand-write it — derive it, so the real tenant is copied without anyone reading or pasting it:

```bash
uv run python tests/setup/make_badpassword_secrets.py
```

It reads `secrets.json`, writes `secrets_badpassword.json` at the repo root, and prints only which
keys it wrote. The file is git-ignored — it carries the real tenant, same sensitivity class as
`secrets.json`.

## Recording

```bash
# Pass 1 — real credentials
uv run python -m keboola.datadirtest scaffold \
  --definitions tests/setup/configs.json \
  --secrets secrets.json --regenerate

# Pass 2 — real tenant, deliberately wrong password
uv run python tests/setup/make_badpassword_secrets.py
uv run python -m keboola.datadirtest scaffold \
  --definitions tests/setup/configs_badpassword.json \
  --secrets secrets_badpassword.json --regenerate
```

Both write into `tests/functional/`; the numeric name prefixes keep the final ordering readable.
`--regenerate` overwrites an existing test directory, but delete the directory first when a
cassette was recorded against code that has since been fixed — a stale cassette that still matches
on method+path will simply replay its old response.

## secrets.json

Git-ignored, never committed. Required shape:

```json
{
  "parameters": {
    "tenant":    "<REAL_TENANT>",
    "username":  "<REAL_USERNAME>",
    "#password": "<REAL_PASSWORD>"
  },
  "#vcr_redact": {
    "tenant":   "<REAL_TENANT>",
    "username": "<REAL_USERNAME>"
  }
}
```

Two rules, both load-bearing:

- **`#vcr_redact` must mirror the real tenant and username.** The recorder builds an automatic
  exact-value sanitizer from every `#`-prefixed value it finds anywhere in the secrets file
  (`create_default_sanitizer` → `_collect_hash_values`). `tenant` and `username` are *not*
  `#`-prefixed in `parameters` (they can't be — `configuration.py` needs those exact key names),
  so this mirror block is what gives them blanket exact-value redaction across every URI, header
  and body, on top of the field-name and URL-path sanitizers in `component.py`. It sits at the
  config **root**, not under `parameters`, because `Configuration` is `extra="forbid"` and would
  reject an unknown key there.
- **Do NOT put `environment` in `secrets.json`.** Log capture redacts *every* string value found
  in the secrets file, `#`-prefixed or not (`LogSanitizer` → `extract_values`). A two-letter value
  like `"us"` would be substituted inside ordinary words in recorded log messages
  (`"a user error"` → `"a ***er error"`), and replay — which has no secrets file — would not
  reproduce that, breaking log comparison. `environment` is not a secret; it stays `"us"` in the
  config files.

That same `LogSanitizer` behaviour is why `tests/test_functional.py` installs
`_TENANT_PATH_NORMALIZER`: the recorded log keeps the tenant as `***` while replay logs the
placeholder `tenant`, so the comparison needs both spellings canonicalized. See the comment on
that constant.

## Choosing the table for `05_run_fullLoad_success`

`billingtype` (2 rows) is the first choice. A primary key is only declared when a
`<table>_guid` column exists *and* is unique across the fetched rows (`component.py::_to_output_schema`).
If `billingtype` has no unique `billingtype_guid`, fall back in order: `skillcategory` (11 rows),
`skilltype` (11), `workactivity` (9). Do not substitute any other table — these are small
reference/lookup tables chosen so the committed cassette and `expected/` snapshot carry no
customer operational data. `test_full_load_output_contract` is the authority on whether the PK
actually verified unique — read its assertion, do not assume.

## Scope

VCR covers exactly the seven cases above. Two-call paging, the PK-not-unique fallback,
native-type downgrade, token refresh mid-run, retries exhausted, incremental-load PK fallback and
the empty-table case are all covered by the unit tests in `tests/test_client.py`,
`tests/test_run.py` and `tests/test_extractor.py` — deliberately not duplicated here.
