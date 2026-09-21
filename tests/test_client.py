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
