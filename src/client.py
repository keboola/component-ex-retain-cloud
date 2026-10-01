"""HTTP client for the Retain Cloud DataAccessAPI."""

import base64
import json
import logging
import threading
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

# (connect, read) seconds, applied to every call this client makes (see `_request_raw` override
# below). Neither `HttpClient` nor `requests` sets a default, so without this an unreachable or
# hanging host blocks the job forever — and a connection that never completes never reaches the
# retry adapter either. The read side is generous headroom for a slow `create_page_result` POST or
# a slow window GET; it is NOT how large tables are handled — those are paged into many small
# windows by `extractor.fetch_table`, so no single call ever has to return a whole big table.
_DEFAULT_TIMEOUT: tuple[float, float] = (10.0, 300.0)

# Reasons surfaced only for the small set of statuses worth calling out specifically — enough for a
# user to tell "your credentials/permissions are the problem" apart from "the API had an outage",
# without echoing the (potentially arbitrary, vendor-controlled) response body.
_HTTP_STATUS_REASONS: dict[int, str] = {
    401: "permission denied",
    403: "permission denied",
    404: "not found",
}


def describe_request_error(prefix: str, error: requests.exceptions.RequestException) -> str:
    """Build a specific, secret-free `UserException` message for a failed HTTP call.

    Before this, EVERY failure shape — a real timeout, a 403 permission problem, a genuine outage —
    collapsed into the same blanket "API unavailable after retries" wording, leaving a user unable
    to tell "raise the timeout" apart from "fix your credentials" apart from "wait and retry". This
    distinguishes, in priority order:

    1. A read/connect timeout (`requests.exceptions.Timeout` — this also matches `ConnectTimeout`,
       which is BOTH a `Timeout` and a `ConnectionError` subclass, so this check must run before the
       `ConnectionError` one below) -> names the timeout explicitly and suggests raising it. This
       covers a genuinely slow connection/response, distinct from the report-table 504 case, which
       is a deterministic server-side rejection handled separately and earlier by
       `fetch_table_page`'s `fail_fast_on_http_error` path (see its docstring) — by the time a
       `RequestException` reaches this function, that path has already had its chance to build a
       more specific message.
    2. An `HTTPError` with a response -> the HTTP status code plus, for a handful of codes worth
       calling out, a short generic reason (`_HTTP_STATUS_REASONS`) — never the response body,
       which is arbitrary vendor-controlled text.
    3. A `ConnectionError` with no response at all (DNS failure, connection refused/reset) -> says
       so explicitly.
    4. Anything else (`RetryError` once retries are exhausted, or a bare `RequestException`) -> the
       original generic "API unavailable after retries" wording — still the right description for a
       sustained, non-specific outage.

    Never includes `error`'s raw exception text, a response body, or any request header/credential —
    only the exception's structural shape (type, status code) is safe to surface to a user.
    """
    if isinstance(error, requests.exceptions.Timeout):
        read_timeout = _DEFAULT_TIMEOUT[1]
        return (
            f"{prefix}: request timed out after {read_timeout:.0f}s "
            "(the table may be a slow server-side report; raise the timeout)."
        )
    if isinstance(error, requests.HTTPError):
        status = error.response.status_code if error.response is not None else None
        if status is None:
            return f"{prefix}: HTTP error."
        reason = _HTTP_STATUS_REASONS.get(status)
        return f"{prefix}: HTTP {status} — {reason}." if reason else f"{prefix}: HTTP {status}."
    if isinstance(error, requests.exceptions.ConnectionError):
        return f"{prefix}: connection error (could not reach the Retain Cloud API)."
    return f"{prefix}: API unavailable after retries."


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
        # Guards token refresh so concurrent window fetches (extractor.fetch_table uses a thread
        # pool) cannot re-authenticate at the same time and race on the shared auth header.
        self._auth_lock = threading.Lock()

    def _request_raw(self, method: str, endpoint_path: str | None = None, **kwargs) -> requests.Response:
        """Apply `_DEFAULT_TIMEOUT` to every request this client makes, unless a caller overrides it."""
        kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
        return super()._request_raw(method, endpoint_path, **kwargs)

    def authenticate(self) -> None:
        """POST the credentials to `IntegrationApi/token` and store the resulting bearer token.

        Never logs `self._password` or the response body — only the HTTP status on failure.
        """
        token_url = f"https://{self._host}/IntegrationApi/token"
        logger.debug(
            "Retain Cloud: requesting auth token for tenant %r (environment=%s).", self._tenant, self._environment
        )
        try:
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
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            # `HttpClient`'s retry adapter uses `raise_on_status=True`, so an exhausted 429/5xx (or
            # a connection failure/timeout) surfaces as `RetryError`/`ConnectionError`/`Timeout` —
            # none of which are `HTTPError` subclasses — rather than a response we could call
            # `raise_for_status()` on. `post_raw` itself is what raises these (before a response
            # even exists), which is why it's inside this same `try`. Without this clause it would
            # propagate past this method as an "unexpected" failure (exit 2) instead of the
            # retryable, user-visible outage it actually is (spec §3). `describe_request_error`
            # picks the specific wording (timeout vs HTTP status vs connection vs retries-exhausted)
            # — see its docstring.
            raise UserException(describe_request_error("Retain Cloud authentication failed", e)) from e

        logger.debug("Retain Cloud: auth token request succeeded (HTTP %s).", response.status_code)
        token_body = response.text.strip()
        try:
            self._token_exp = decode_jwt_exp(token_body)
        except (IndexError, ValueError, KeyError, TypeError) as e:
            # A `200` response whose body isn't a valid `Bearer <jwt>` (e.g. a maintenance page, or
            # a vendor contract change) must still fail as a `UserException` (exit 1), not leak a
            # raw `IndexError`/`ValueError`/`KeyError` up to `__main__`'s generic exit-2 handler —
            # this is a user-visible "the API returned something unexpected" condition, not a bug
            # in this component. Never includes `token_body` itself in the message: it is, or at
            # least resembles, a credential-bearing token.
            raise UserException(
                "Retain Cloud authentication succeeded but returned an unrecognized token format."
            ) from e
        self.update_auth_header({"Authorization": token_body}, overwrite=True)

    def _ensure_token(self) -> None:
        # Double-checked locking: the common case (token still valid) stays lock-free, but once a
        # refresh is due, only ONE of several concurrent worker threads actually re-authenticates —
        # the rest find a fresh token on the second check and skip it.
        if self._token_exp - time.time() < _TOKEN_REFRESH_MARGIN_SECONDS:
            with self._auth_lock:
                if self._token_exp - time.time() < _TOKEN_REFRESH_MARGIN_SECONDS:
                    self.authenticate()

    def _get_with_reauth(self, path: str, **kwargs):
        # Deliberately `get_raw` (undecorated), not `get` — `HttpClient.get`'s
        # `response_error_handling` decorator unconditionally logs a WARNING with a full traceback
        # for *any* `HTTPError`, including the 401 this method expects and handles gracefully via
        # reauth-and-retry below. `get_raw` does no such logging, matching the manual
        # `raise_for_status()` pattern `fetch_table_page` already uses with `post_raw`.
        logger.debug("Retain Cloud: GET %s", path)
        response = self.get_raw(path, **kwargs)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.info("Retain Cloud token expired mid-request for %s; re-authenticating and retrying.", path)
                with self._auth_lock:
                    self.authenticate()
                response = self.get_raw(path, **kwargs)
                response.raise_for_status()
            else:
                raise
        logger.debug("Retain Cloud: %s returned HTTP %s.", path, response.status_code)
        return response.json()

    def list_tables(self) -> list[str]:
        self._ensure_token()
        return self._get_with_reauth("structure")

    def list_table_labels(self) -> list[dict]:
        self._ensure_token()
        return self._get_with_reauth("structure/tablestructure")

    def get_table_schema(self, table: str) -> list[dict]:
        self._ensure_token()
        return self._get_with_reauth("structure/richfieldstructure", params={"table": table})

    def _post_json_with_reauth(self, path: str, *, params: dict | None = None, json_body: dict | None = None):
        """POST a JSON body, return the parsed JSON response, re-authenticating once on a 401.

        The POST analogue of `_get_with_reauth`. Uses `post_raw` (undecorated) for the same reason —
        to skip `HttpClient.post`'s unconditional WARNING-with-traceback on the 401 this handles
        gracefully. The 401 reauth takes `_auth_lock` so concurrent callers do not race on it.
        """
        logger.debug("Retain Cloud: POST %s", path)
        body = json_body if json_body is not None else {}
        response = self.post_raw(path, params=params, json=body)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.info("Retain Cloud token expired mid-request for %s; re-authenticating and retrying.", path)
                with self._auth_lock:
                    self.authenticate()
                response = self.post_raw(path, params=params, json=body)
                response.raise_for_status()
            else:
                raise
        logger.debug("Retain Cloud: %s returned HTTP %s.", path, response.status_code)
        return response.json()

    def create_page_result(self, table: str, fields: list[str]) -> tuple[str, int]:
        """Create a server-side paged result set; return its `(key, total_row_count)`.

        Step 1 of Retain's real paging contract. `POST tableaccess/{table}/paging/paged` with
        `pageSize=1` (we don't want a large first page here, only the handle and the total) and an
        explicit `fields` list in the body. The `key` in the response is a handle to a cached,
        materialized result set; `fetch_page_window` then reads arbitrary windows of it by GET.

        The explicit `fields` body is REQUIRED, not cosmetic. For a large table the SAME call with
        an empty body (`{}`) returns a deterministic `504 Gateway Timeout` at ~20s, while naming the
        columns returns in a few seconds. Verified live: `resourceidsdedreport` /
        `resourcenumdenreport` (which have ZERO calculated fields) 504 without the body and succeed
        with it — so it is the explicit field list itself, not the exclusion of any field category,
        that avoids the timeout.

        `fields` must contain only NON-calculated columns: listing a Retain `CalculatedField` makes
        this call return `400` (verified live on `booking`). The caller (`extractor.fetch_table`)
        already filters them out. The released single-call projection never returned calculated
        fields either, so excluding them keeps the output columns in parity.
        """
        self._ensure_token()
        path = f"tableaccess/{table}/paging/paged"
        params = {"pageSize": 1, "sequential": "false"}
        body = {"fields": [{"fieldName": name} for name in fields]}
        logger.debug("Retain Cloud: creating paged result for table %r (%d fields).", table, len(fields))
        payload = self._post_json_with_reauth(path, params=params, json_body=body)
        try:
            return payload["key"], int(payload["rowCount"])
        except (KeyError, TypeError, ValueError) as e:
            # A 200 whose body lacks `key`/`rowCount` (e.g. a vendor contract change) must fail as a
            # clean UserException for this row, not leak a raw KeyError up to `__main__`'s exit-2.
            raise UserException(
                f"Retain Cloud returned an unexpected response when creating a paged result for table '{table}'."
            ) from e

    def fetch_page_window(self, table: str, key: str, start: int, count: int) -> list[dict]:
        """Read one window `[start, start+count)` of a created page result (step 2 of the contract).

        `GET tableaccess/{table}/paging/paged?id={key}&from={start}&count={count}` returns a bare
        JSON array of row objects (NOT the create-call envelope). Different `from` values return
        different, stable windows — this is the batch-advance mechanism the old single-call model
        lacked. Safe to call concurrently: `HttpClient` builds a fresh `requests.Session` per call,
        and token refresh is guarded by `_auth_lock`.
        """
        self._ensure_token()
        path = f"tableaccess/{table}/paging/paged"
        params = {"id": key, "from": start, "count": count}
        rows = self._get_with_reauth(path, params=params)
        if not isinstance(rows, list):
            raise UserException(f"Retain Cloud returned a non-array window for table '{table}' (from={start}).")
        return rows
