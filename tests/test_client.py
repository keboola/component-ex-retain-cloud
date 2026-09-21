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

    @mock.patch.object(RetainCloudClient, "post_raw")
    def test_authenticate_raises_user_exception_on_retries_exhausted(self, mock_post_raw):
        # `HttpClient`'s retry adapter uses `raise_on_status=True`, so a sustained 502/503 that
        # exhausts retries raises `requests.exceptions.RetryError` from *within* `post_raw` itself
        # (before any response exists to call `raise_for_status()` on) — not a `requests.HTTPError`.
        # Without the dedicated `RequestException` handling this must surface as `UserException`
        # (exit 1), not propagate unhandled to the generic exit-2 path.
        from keboola.component.exceptions import UserException

        mock_post_raw.side_effect = requests.exceptions.RetryError("too many 502 retries")
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

    @mock.patch.object(RetainCloudClient, "get_raw")
    def test_list_tables(self, mock_get_raw):
        # `_get_with_reauth` deliberately calls `get_raw` (undecorated), not `get` — see the
        # comment on `_get_with_reauth` for why (avoids a noisy library WARNING+traceback log on a
        # 401 this method already handles gracefully).
        mock_get_raw.return_value = _response(status_code=200, json_body=["booking", "resource"])
        self.assertEqual(self.client.list_tables(), ["booking", "resource"])
        mock_get_raw.assert_called_once_with("structure")

    @mock.patch.object(RetainCloudClient, "get_raw")
    def test_get_table_schema(self, mock_get_raw):
        mock_get_raw.return_value = _response(status_code=200, json_body=[{"name": "booking_guid", "dataType": "ID"}])
        result = self.client.get_table_schema("booking")
        self.assertEqual(result, [{"name": "booking_guid", "dataType": "ID"}])
        mock_get_raw.assert_called_once_with("structure/richfieldstructure", params={"table": "booking"})

    @mock.patch.object(RetainCloudClient, "authenticate")
    @mock.patch.object(RetainCloudClient, "get_raw")
    def test_discovery_reauthenticates_once_on_401_then_retries(self, mock_get_raw, mock_authenticate):
        mock_get_raw.side_effect = [
            _response(status_code=401),
            _response(status_code=200, json_body=["booking"]),
        ]
        result = self.client.list_tables()
        self.assertEqual(result, ["booking"])
        mock_authenticate.assert_called_once()
        self.assertEqual(mock_get_raw.call_count, 2)

    @mock.patch.object(RetainCloudClient, "authenticate")
    @mock.patch.object(RetainCloudClient, "get_raw")
    def test_handled_401_does_not_log_a_warning(self, mock_get_raw, mock_authenticate):
        # Regression for the cosmetic-noise finding: `HttpClient.get`'s decorator logs a WARNING
        # with a full traceback for *any* `HTTPError`, including a 401 this method handles
        # gracefully via reauth-and-retry — `_get_with_reauth` avoids that entirely by calling
        # `get_raw` instead (see its docstring comment). `assertNoLogs` fails loudly if any logger
        # emits at WARNING level or above during the block.
        mock_get_raw.side_effect = [
            _response(status_code=401),
            _response(status_code=200, json_body=["booking"]),
        ]
        with self.assertNoLogs(level="WARNING"):
            self.client.list_tables()

    @mock.patch.object(RetainCloudClient, "get_raw")
    def test_non_401_error_still_propagates_as_http_error(self, mock_get_raw):
        # `get_raw` (used here since the switch away from the decorated `get`) carries no automatic
        # `raise_for_status()` at all — confirms a non-401 failure still surfaces as a plain
        # `HTTPError` for the caller (`component.py`'s `run()`) to convert into a `UserException`
        # carrying the HTTP status, rather than being swallowed or mis-handled by this method.
        mock_get_raw.return_value = _response(status_code=403)
        with self.assertRaises(requests.HTTPError):
            self.client.list_tables()


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
