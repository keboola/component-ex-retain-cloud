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

# (connect, read) seconds, applied to every call this client makes (see `_request_raw` override
# below). Neither `HttpClient` nor `requests` sets a default, so without this an unreachable or
# hanging host blocks the job forever — and a connection that never completes never reaches the
# retry adapter either. The read side is deliberately generous: for a `stream=True` response
# (`fetch_table_page`), `requests`' read timeout is the gap between individual chunks, not the
# total download time, so this stays safe even for the largest tables (spec §9 risk #1).
_DEFAULT_TIMEOUT: tuple[float, float] = (10.0, 60.0)


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

    def authenticate(self) -> None:
        """POST the credentials to `IntegrationApi/token` and store the resulting bearer token.

        Never logs `self._password` or the response body — only the HTTP status on failure.
        """
        token_url = f"https://{self._host}/IntegrationApi/token"
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
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            raise UserException(f"Retain Cloud authentication failed (HTTP {status}).") from e
        except requests.exceptions.RequestException as e:
            # `HttpClient`'s retry adapter uses `raise_on_status=True`, so an exhausted 429/5xx (or
            # a connection failure/timeout) surfaces as `RetryError`/`ConnectionError`/`Timeout` —
            # none of which are `HTTPError` subclasses, so none would be caught above — rather than
            # a response we could call `raise_for_status()` on. `post_raw` itself is what raises
            # these (before a response even exists), which is why it's inside this same `try`.
            # Without this clause it would propagate past this method as an "unexpected" failure
            # (exit 2) instead of the retryable, user-visible outage it actually is (spec §3).
            raise UserException("Retain Cloud authentication failed: API unavailable after retries.") from e

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

    def fetch_table_page(self, table: str, page_size: int) -> requests.Response:
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
        """
        self._ensure_token()
        params = {"pageSize": page_size, "sequential": "true"}
        path = f"tableaccess/{table}/paging/paged"
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
        # requests does not auto-decompress `response.raw` the way it does `.content`/`.json()` —
        # without this, a gzip-compressed body would be handed to ijson as garbled raw bytes.
        response.raw.decode_content = True
        return response
