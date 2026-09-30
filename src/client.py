"""HTTP client for the Retain Cloud DataAccessAPI."""

import base64
import contextlib
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

# (connect, read) seconds, applied to every call this client makes (see `_request_raw` override
# below). Neither `HttpClient` nor `requests` sets a default, so without this an unreachable or
# hanging host blocks the job forever — and a connection that never completes never reaches the
# retry adapter either. The read side is generous — raised from an original 60s — as DEFENSIVE
# headroom for a legitimately large single-call table (e.g. a ~835k-row table fetched in one
# `paging/paged` call): this is NOT the fix for the report-table 504s (see `fetch_table_page`'s
# `fail_fast_on_http_error` / `describe_request_error`'s docstrings) — a bigger CLIENT-side timeout
# cannot fix a deterministic SERVER-side `504 Gateway Timeout`, since the server has already given
# up and answered before this timeout would ever fire. For a `stream=True` response
# (`fetch_table_page`), `requests`' read timeout is the gap between individual chunks, not the
# total download time, so this stays safe even for the largest tables (spec §9 risk #1).
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

    def _request_raw(self, method: str, endpoint_path: str | None = None, **kwargs) -> requests.Response:
        """Apply `_DEFAULT_TIMEOUT` to every request this client makes, unless a caller overrides it."""
        kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
        return super()._request_raw(method, endpoint_path, **kwargs)

    @contextlib.contextmanager
    def _no_forced_status_retries(self):
        """Temporarily disable HTTP-status-based retries (`status_forcelist`) for calls made inside
        this block. Connection/read-level retries (`self.max_retries`, still applied by the
        underlying `Retry`'s `connect`/`read` budgets) are untouched — only a FORCED-status retry
        (429/500/502/503/504) is skipped.

        Used by `fetch_table_page` for the SECOND `paging/paged` call only (the one sized to a
        table's full remaining row count, per `extractor.fetch_table`'s algorithm): a report table
        with millions of rows (e.g. `resourcenumdenreport`) cannot be generated and returned by the
        API in a single request, so that call can get a DETERMINISTIC `504 Gateway Timeout` — and
        retrying a deterministic failure 5 times (`HttpClient`'s configured `max_retries`) just
        burns ~5x the wall-clock time to reach the exact same outcome. Disabling the forced-status
        retry for this one call lets the real `504` response reach `fetch_table_page` so it can
        raise a specific, actionable error (naming the status and the row count) immediately,
        instead of a generic "unavailable after retries" one after several minutes.

        `HttpClient._requests_retry_session` rebuilds its `Retry` object from `self.status_forcelist`
        on every call (it is not cached at construction time), which is what makes a plain
        set-then-restore around one call safe here — no other in-flight call is affected.
        """
        original = self.status_forcelist
        self.status_forcelist = ()
        try:
            yield
        finally:
            self.status_forcelist = original

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

    def fetch_table_page(
        self, table: str, page_size: int, *, fail_fast_on_http_error: bool = False
    ) -> requests.Response:
        """Issue one `paging/paged` call and return the raw, streamable response.

        The caller (`extractor.py`) is responsible for consuming `response.raw` with `ijson` — this
        method never reads the body itself, so the "one/two calls per table" contract (spec §6)
        stays entirely in the caller's hands.

        `json={}` is REQUIRED, not cosmetic: a bodyless POST to this endpoint returns a generic
        `400 {"status":"error","message":"invalid request"}` before the endpoint ever looks at
        `pageSize`/`sequential` — confirmed live against the real API (Phase 5 VCR recording hit
        this exact 400; a follow-up probe isolated it to the missing body/`Content-Type`, since an
        empty JSON object made the identical call return 200). `pageSize`/`sequential` still belong
        in the query string per the resolved paging contract (research §3: the endpoint has no
        body-driven DTO for them at all — an empty body's presence is what satisfies the framework's
        request-model binding, its *content* is irrelevant and any extra keys are silently ignored).

        `fail_fast_on_http_error`: when True, wraps this call in `_no_forced_status_retries` — see
        its docstring. `extractor.fetch_table` sets this for the SECOND call only (sized to a
        table's full remaining row count), where a large report table can trigger a deterministic
        `504` that retrying would not fix.

        Also note: the live API does not always honor `pageSize` (observed returning every row of a
        64k-row table for a `pageSize` of 100) — callers must not assume the response is capped at
        `page_size`, only that a `rowCount` short of what's needed triggers a second call.
        """
        self._ensure_token()
        params = {"pageSize": page_size, "sequential": "true"}
        path = f"tableaccess/{table}/paging/paged"
        logger.debug(
            "Retain Cloud: requesting paging/paged for table %r (pageSize=%d, fail_fast=%s).",
            table,
            page_size,
            fail_fast_on_http_error,
        )
        retry_scope = self._no_forced_status_retries() if fail_fast_on_http_error else contextlib.nullcontext()
        with retry_scope:
            response = self.post_raw(path, params=params, json={}, stream=True)
            try:
                response.raise_for_status()
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 401:
                    logger.info(
                        "Retain Cloud token expired mid-request for table %r; re-authenticating and retrying.", table
                    )
                    self.authenticate()
                    response = self.post_raw(path, params=params, json={}, stream=True)
                    response.raise_for_status()
                else:
                    raise
        logger.debug("Retain Cloud: paging/paged for table %r returned HTTP %s.", table, response.status_code)
        # requests does not auto-decompress `response.raw` the way it does `.content`/`.json()` —
        # without this, a gzip-compressed body would be handed to ijson as garbled raw bytes.
        response.raw.decode_content = True
        return response
