"""Unit tests for `component.VCR_SANITIZERS` — the chain that keeps the committed cassettes free
of the real tenant, username and password.

These exercise the sanitizers directly, without a cassette, on purpose: the recordings are made
against a live customer tenant, so a sanitizer change must be provable WITHOUT re-recording. The
chain is assembled here the way `keboola.vcr`'s recorder assembles it — its own `DefaultSanitizer`
prepended, then the component's list, applied in order by `CompositeSanitizer`.
"""

import json
import unittest

from keboola.vcr import CompositeSanitizer, DefaultSanitizer
from vcr.request import Request

from component import VCR_SANITIZERS, RequestContentLengthSanitizer

TOKEN_URL = "https://us.retaincloud.com/IntegrationApi/token"

# Values a cassette must never be able to give away, directly or by arithmetic.
REAL_TENANT = "a-real-customer-tenant-identifier"
REAL_USERNAME = "real.person@customer.example.com"
REAL_PASSWORD = "correct horse battery staple"


def _recorder_chain() -> CompositeSanitizer:
    """Mirror `VCRRecorder`'s chain: a `DefaultSanitizer` of its own, then `VCR_SANITIZERS`.

    The recorder additionally merges the two same-class `DefaultSanitizer`s via
    `_dedup_sanitizers`, which is skipped here to avoid depending on a private helper. That only
    makes this test stricter: body redaction runs twice instead of once (it is idempotent), and
    `_dedup_sanitizers` preserves relative order, so `RequestContentLengthSanitizer` is last
    either way — which is the property under test.
    """
    return CompositeSanitizer([DefaultSanitizer(), *VCR_SANITIZERS])


def _token_request(tenant: str, username: str = REAL_USERNAME, password: str = REAL_PASSWORD) -> Request:
    """The exact request `client.authenticate()` makes, headers included."""
    body = json.dumps({"useremail": username, "userpassword": password, "environment": "us", "tenant": tenant})
    return Request(
        method="POST",
        uri=TOKEN_URL,
        body=body,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body.encode("utf-8"))),
            "Accept": "*/*",
            "Authorization": "Bearer a-real-bearer-token",
        },
    )


def _as_bytes(body) -> bytes:
    return body if isinstance(body, bytes) else str(body).encode("utf-8")


class TestRequestContentLength(unittest.TestCase):
    def test_declared_length_matches_the_redacted_body(self):
        """`Content-Length` must describe what the cassette actually holds, not the original."""
        recorded = _recorder_chain().before_record_request(_token_request(REAL_TENANT))

        body = _as_bytes(recorded.body)
        self.assertNotIn(REAL_TENANT.encode(), body)
        self.assertNotIn(REAL_PASSWORD.encode(), body)
        self.assertEqual(recorded.headers["Content-Length"], str(len(body)))

    def test_declared_length_cannot_reveal_the_tenant_length(self):
        """The finding itself: two tenants differing only in length must record identically.

        With the stale pre-redaction length, the delta between the declared length and the
        redacted body's real length equalled the combined length of the three original values —
        and the deliberately-wrong-credential recordings (02/04/07) pin username and password to
        publicly-known dummies, leaving the real tenant's character count recoverable by
        subtraction. Byte-identical recordings are what closes that channel.
        """
        chain = _recorder_chain()
        short = chain.before_record_request(_token_request("t"))
        long = chain.before_record_request(_token_request("a-much-longer-tenant-identifier"))

        self.assertEqual(_as_bytes(short.body), _as_bytes(long.body))
        self.assertEqual(dict(short.headers), dict(long.headers))

    def test_authorization_header_is_still_dropped(self):
        """Guards the header whitelist the sanitizer block warns not to widen."""
        recorded = _recorder_chain().before_record_request(_token_request(REAL_TENANT))
        self.assertNotIn("Authorization", recorded.headers)

    def test_length_is_not_invented_for_a_bodyless_request(self):
        """Every `structure`/`richfieldstructure` GET declares no length; none may be added."""
        request = Request(
            method="GET",
            uri="https://us.retaincloud.com/DataAccessAPI/acme/api/structure",
            body=None,
            headers={"Accept": "*/*"},
        )
        recorded = _recorder_chain().before_record_request(request)
        self.assertNotIn("Content-Length", recorded.headers)

    def test_empty_json_post_body_is_left_consistent(self):
        """`fetch_table_page` posts `json={}`; nothing to redact, so nothing may shift."""
        request = Request(
            method="POST",
            uri="https://us.retaincloud.com/DataAccessAPI/acme/api/tableaccess/billingtype/paging/paged",
            body="{}",
            headers={"Content-Type": "application/json", "Content-Length": "2"},
        )
        recorded = _recorder_chain().before_record_request(request)
        self.assertEqual(recorded.headers["Content-Length"], "2")

    def test_sanitizer_runs_last_in_the_chain(self):
        """It measures the FINAL body, so any body rewrite added later must precede it."""
        self.assertIsInstance(VCR_SANITIZERS[-1], RequestContentLengthSanitizer)


class TestTokenResponseContentLength(unittest.TestCase):
    def test_synthetic_jwt_response_length_is_resynced(self):
        """Regression guard for sharing `_sync_content_length` between the two directions."""
        real_token = "Bearer realheader.realpayloadthatisquitelong.realsignature"
        response = {
            "status": {"code": 200, "message": "OK"},
            "headers": {"Content-Type": ["text/plain"], "content-length": [str(len(real_token))]},
            "body": {"string": real_token},
        }

        recorded = _recorder_chain().before_record_response(response)

        recorded_body = recorded["body"]["string"]
        self.assertNotIn("realpayload", recorded_body)
        self.assertEqual(recorded["headers"]["content-length"], [str(len(recorded_body.encode("utf-8")))])

    def test_non_token_response_length_is_untouched(self):
        """A JSON body no sanitizer rewrites must keep the length the server declared."""
        payload = json.dumps([{"name": "billingtype"}])
        response = {
            "status": {"code": 200, "message": "OK"},
            "headers": {"Content-Type": ["application/json"], "content-length": [str(len(payload))]},
            "body": {"string": payload},
        }

        recorded = _recorder_chain().before_record_response(response)

        self.assertEqual(recorded["body"]["string"], payload)
        self.assertEqual(recorded["headers"]["content-length"], [str(len(payload))])


if __name__ == "__main__":
    unittest.main()
