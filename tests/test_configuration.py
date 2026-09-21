import unittest

from keboola.component.exceptions import UserException
from pydantic import SecretStr

from configuration import Configuration, Environment, LoadType, RootConfig

ROOT_PARAMS = {
    "environment": "us",
    "tenant": "acme",
    "username": "svc@example.com",
    "#password": "secret-value",
}
ROW_PARAMS = {**ROOT_PARAMS, "table": "booking"}


class TestRootConfig(unittest.TestCase):
    def test_valid_root_config_parses(self):
        cfg = RootConfig(**ROOT_PARAMS)
        self.assertEqual(cfg.environment, Environment.us)
        self.assertIsInstance(cfg.password, SecretStr)
        self.assertEqual(cfg.password.get_secret_value(), "secret-value")

    def test_tolerates_extra_row_fields_present_in_merged_config(self):
        # Simulates the platform handing a row-level sync action the merged root+row-draft
        # parameters, where row fields may be present even though RootConfig doesn't need them.
        cfg = RootConfig(**{**ROOT_PARAMS, "table": "booking", "load_type": "full_load", "page_size": 20000})
        self.assertEqual(cfg.tenant, "acme")

    def test_tolerates_missing_table_field_entirely(self):
        # The exact scenario list_tables hits on a brand-new row: `table` isn't set yet at all.
        cfg = RootConfig(**ROOT_PARAMS)  # no KeyError / ValidationError despite no `table` key
        self.assertEqual(cfg.username, "svc@example.com")

    def test_missing_required_root_field_raises_user_exception(self):
        params = dict(ROOT_PARAMS)
        del params["tenant"]
        with self.assertRaises(UserException):
            RootConfig(**params)

    def test_invalid_environment_raises_user_exception(self):
        with self.assertRaises(UserException):
            RootConfig(**{**ROOT_PARAMS, "environment": "ca"})


class TestConfiguration(unittest.TestCase):
    def test_valid_row_config_parses(self):
        cfg = Configuration(**ROW_PARAMS)
        self.assertEqual(cfg.table, "booking")
        self.assertEqual(cfg.page_size, 20000)
        self.assertEqual(cfg.load_type, LoadType.full_load)
        self.assertFalse(cfg.incremental)

    def test_incremental_load_type(self):
        cfg = Configuration(**{**ROW_PARAMS, "load_type": "incremental_load"})
        self.assertTrue(cfg.incremental)

    def test_missing_table_raises_user_exception(self):
        with self.assertRaises(UserException):
            Configuration(**ROOT_PARAMS)  # no `table` — this is the strict, run()-time model

    def test_extra_unknown_field_rejected(self):
        with self.assertRaises(UserException):
            Configuration(**{**ROW_PARAMS, "unexpected_field": "x"})

    def test_password_never_appears_in_string_representation(self):
        cfg = Configuration(**ROW_PARAMS)
        self.assertNotIn("secret-value", str(cfg))
        self.assertNotIn("secret-value", repr(cfg))


if __name__ == "__main__":
    unittest.main()
