import contextlib
import logging
import unittest
from unittest import mock

from keboola.component.exceptions import UserException

from configuration import Configuration

SECRET_PASSWORD = "super-secret-value-should-never-appear-in-logs"


class _CollectingHandler(logging.Handler):
    """A minimal, properly-typed log handler — avoids monkey-patching `Handler.emit` with a plain
    lambda, which doesn't match `emit`'s declared instance-method signature."""

    def __init__(self):
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class TestNoSecretLeakage(unittest.TestCase):
    def setUp(self):
        self.handler = _CollectingHandler()
        logging.getLogger().addHandler(self.handler)
        self.addCleanup(logging.getLogger().removeHandler, self.handler)

    def test_authenticate_failure_does_not_log_password(self):
        import requests

        from client import RetainCloudClient

        client = RetainCloudClient("us", "acme", "user@example.com", SECRET_PASSWORD)
        with mock.patch.object(RetainCloudClient, "post_raw") as mock_post_raw:
            resp = mock.Mock(spec=requests.Response, status_code=401)
            resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
            mock_post_raw.return_value = resp
            with contextlib.suppress(UserException):
                client.authenticate()

        for message in self.handler.messages:
            self.assertNotIn(SECRET_PASSWORD, message)

    def test_config_str_and_repr_never_contain_password(self):
        cfg = Configuration(
            environment="us",
            tenant="acme",
            username="user@example.com",
            **{"#password": SECRET_PASSWORD},
            table="booking",
        )
        self.assertNotIn(SECRET_PASSWORD, str(cfg))
        self.assertNotIn(SECRET_PASSWORD, repr(cfg))
        self.assertNotIn(SECRET_PASSWORD, str(vars(cfg)))


if __name__ == "__main__":
    unittest.main()
