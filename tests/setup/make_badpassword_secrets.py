"""Derive `secrets_badpassword.json` from `secrets.json` — the third recording secrets file.

Why this file exists at all
---------------------------
`02_testConnection_badCredentials`, `04_listTables_badCredentials` and `07_run_authFailure` are
meant to exercise `client.authenticate()`'s `requests.HTTPError` branch: the API answers
`400 {"error":"invalid_request","error_description":"Your email or password is invalid."}` and
`authenticate()` turns it straight into a `UserException`, in ONE call.

That only happens for a tenant that actually exists. Live-probed: a nonexistent tenant makes the
gateway answer `504` with an HTML error page long before the token endpoint looks at the
credentials, which `authenticate()` handles through its *other* branch (`RequestException` →
retries exhausted, 5 attempts with backoff). That is a slow recording and the wrong failure mode
for a test named `badCredentials` — and it duplicates what
`tests/test_client.py::test_authenticate_raises_user_exception_on_retries_exhausted` already
covers with a mock, while leaving the HTTPError branch (mocked in
`test_authenticate_raises_user_exception_on_401`) with no live coverage at all.

So these three need the REAL tenant with a deliberately WRONG password — a combination that exists
in neither `secrets.json` (real password, would make the tests pass) nor
`tests/setup/configs_badpassword.json` (fake tenant, gives the 504). Hence this derived file.

`username` is deliberately NOT written here: the scaffolder deep-merges, so only the keys present
in the override file win, and `configs_badpassword.json`'s fake `username` stays in place. A wrong
password alone is sufficient for the clean 400 — the tenant is what had to be real.

Usage (from the repo root, after `secrets.json` is filled in):

    uv run python tests/setup/make_badpassword_secrets.py

The script never prints the tenant value — it only reports which keys it wrote.
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE = REPO_ROOT / "secrets.json"
TARGET = REPO_ROOT / "secrets_badpassword.json"

# Any value that is not the tenant's real password. Kept obviously-fake and identical to the
# placeholder already in `configs_badpassword.json` so the two files read consistently.
WRONG_PASSWORD = "definitely-wrong"


def main() -> int:
    if not SOURCE.is_file():
        print(f"{SOURCE.name} not found — fill it in first (see tests/setup/README.md).", file=sys.stderr)
        return 1

    secrets = json.loads(SOURCE.read_text())
    tenant = secrets.get("parameters", {}).get("tenant")
    if not isinstance(tenant, str) or not tenant:
        print(f"{SOURCE.name} has no `parameters.tenant` — cannot derive the bad-password file.", file=sys.stderr)
        return 1

    derived = {
        "parameters": {
            "tenant": tenant,
            "#password": WRONG_PASSWORD,
        },
        # Mirror block, same role as in `secrets.json`: `tenant` cannot be `#`-prefixed under
        # `parameters` (configuration.py needs that exact key name), so this is what gives the real
        # tenant blanket exact-value redaction in the cassette. It matters *here* specifically
        # because pass 2 records with `--secrets secrets_badpassword.json`, which means
        # `secrets.json`'s own mirror block is not loaded for that pass at all.
        "#vcr_redact": {
            "tenant": tenant,
        },
    }

    TARGET.write_text(json.dumps(derived, indent=2) + "\n")
    print(f"Wrote {TARGET.name}: parameters.tenant (from {SOURCE.name}), parameters.#password (deliberately wrong).")
    print("It is git-ignored — it carries the real tenant. Do not commit it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
