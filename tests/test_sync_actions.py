import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests
from keboola.component.exceptions import UserException

from component import Component

ROOT_PARAMS = {
    "environment": "us",
    "tenant": "acme",
    "username": "svc@example.com",
    "#password": "pw",
}


class _SyncActionTestCase(unittest.TestCase):
    """Base providing a real, isolated `KBC_DATADIR` with a `config.json` fixture per test.

    `keboola.component`'s `configuration` property re-reads `config.json` from disk on every
    access (`Configuration(self.data_folder_path)` in the installed library's
    `CommonInterface.configuration`) rather than caching an instance — so a test cannot inject
    parameters by assigning `comp.configuration.parameters = ...` after construction, that would
    only set an attribute on an ephemeral object discarded on the very next read. A real fixture
    file is the only way to control what the component sees.

    `"action": "run"` matches the platform's "called directly, not dispatched as a sync action"
    shape (see the comment in `keboola.component.base.sync_action`: "could be also called normally
    within run") — with it, a `UserException` raised by a `@sync_action`-decorated method propagates
    as a real exception here, instead of being caught and turned into `sys.exit(1)`/stderr by the
    decorator's CLI-dispatch error path, which is what happens for a *real* sync-action invocation
    (`action` set to the sync action's own name) and is the framework's own, separately-tested
    concern, not this component's.
    """

    def _component(self, params: dict) -> Component:
        data_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, data_dir, ignore_errors=True)
        config = {
            "storage": {"input": {"files": [], "tables": []}, "output": {"files": [], "tables": []}},
            "action": "run",
            "parameters": params,
        }
        (data_dir / "config.json").write_text(json.dumps(config))
        with mock.patch.dict(os.environ, {"KBC_DATADIR": str(data_dir)}):
            return Component()


class TestTestConnection(_SyncActionTestCase):
    @mock.patch("component.RetainCloudClient")
    def test_success_does_not_raise(self, mock_client_cls):
        mock_client_cls.return_value.authenticate.return_value = None
        comp = self._component(ROOT_PARAMS)
        comp.test_connection()  # must not raise

    @mock.patch("component.RetainCloudClient")
    def test_auth_failure_raises_user_exception(self, mock_client_cls):
        mock_client_cls.return_value.authenticate.side_effect = UserException("bad creds")
        comp = self._component(ROOT_PARAMS)
        with self.assertRaises(UserException):
            comp.test_connection()


class TestListTablesSyncAction(_SyncActionTestCase):
    @mock.patch("component.RetainCloudClient")
    def test_works_before_table_field_is_set(self, mock_client_cls):
        # The exact scenario a brand-new row hits: `table` isn't in the merged params at all yet.
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking", "resource"]
        client.list_table_labels.return_value = [{"name": "booking", "alias": "Bookings"}]
        comp = self._component(ROOT_PARAMS)  # no "table" key present

        options = comp.list_tables()  # must not raise a validation error

        self.assertIn({"value": "booking", "label": "Bookings"}, options)
        self.assertIn({"value": "resource", "label": "resource"}, options)  # no alias -> raw name

    @mock.patch("component.RetainCloudClient")
    def test_tolerates_partially_filled_row_fields(self, mock_client_cls):
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        client.list_table_labels.return_value = []
        comp = self._component({**ROOT_PARAMS, "table": "", "load_type": "full_load"})

        options = comp.list_tables()

        self.assertEqual(options, [{"value": "booking", "label": "booking"}])

    @mock.patch("component.RetainCloudClient")
    def test_auth_failure_raises_user_exception(self, mock_client_cls):
        mock_client_cls.return_value.authenticate.side_effect = UserException("bad creds")
        comp = self._component(ROOT_PARAMS)
        with self.assertRaises(UserException):
            comp.list_tables()

    @mock.patch("component.RetainCloudClient")
    def test_label_entry_missing_name_raises_user_exception(self, mock_client_cls):
        # Regression: `row["name"]` is a response-schema assumption, not an HTTP failure — a label
        # entry from `client.list_table_labels()` missing the `name` field used to raise a raw
        # `KeyError` here, which would escape as an unhandled exit-code-2 error instead of a clean,
        # user-visible `UserException`.
        client = mock_client_cls.return_value
        client.list_tables.return_value = ["booking"]
        client.list_table_labels.return_value = [{"alias": "Bookings"}]  # no "name" key
        comp = self._component(ROOT_PARAMS)

        with self.assertRaises(UserException) as ctx:
            comp.list_tables()

        self.assertEqual(str(ctx.exception), "Retain Cloud returned a table label entry without a 'name' field.")

    @mock.patch("component.RetainCloudClient")
    def test_post_auth_request_failure_raises_user_exception(self, mock_client_cls):
        # Regression for the error-handling gate finding: `client.list_tables()`/
        # `client.list_table_labels()` used to be called with no try/except at all here, so a
        # non-401 HTTP failure (or a `RetryError` from exhausted retries) leaked `HttpClient`'s raw
        # `requests` exception message — including the internal API URL — straight to the config
        # UI, instead of the clean `UserException` `run()` already gives for the equivalent
        # failure. Authentication itself succeeds; the failure happens on the discovery call.
        client = mock_client_cls.return_value
        client.authenticate.return_value = None
        client.list_tables.side_effect = requests.exceptions.RetryError("too many 503 retries")
        comp = self._component(ROOT_PARAMS)

        with self.assertRaises(UserException) as ctx:
            comp.list_tables()

        self.assertNotIn("https://", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
