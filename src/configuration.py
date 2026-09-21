"""Pydantic configuration models for keboola.ex-retain-cloud.

Two models, matching the root/row `configSchema.json` / `configRowSchema.json` split exactly
(spec §6):

- `RootConfig` — the four shared connection fields. Deliberately `extra="ignore"`: this model is
  also used for *partial* instantiation by sync actions that only need the connection fields
  (`test_connection`, and `list_tables` before a row's `table` is chosen) — it must tolerate
  whatever row-level keys happen to be present, absent, or blank in the merged parameters handed to
  a sync action, per the "partial instantiation only required when a sync action needs fewer fields
  than run()" pattern.
- `Configuration(RootConfig)` — adds the three row fields. `extra="forbid"`: used only by `run()`,
  where the platform guarantees the merged config is complete and valid, so unexpected keys should
  be treated as a real problem, not silently ignored.

`fetch_mode` is deliberately NOT a field on either model — V1 only implements `full_fetch` (spec
§2), which is why it isn't modeled as a constant or a field anywhere in this component; there is
nothing to configure or branch on until a second fetch mode exists.
"""

import logging
from enum import StrEnum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

logger = logging.getLogger(__name__)


class Environment(StrEnum):
    us = "us"
    eu = "eu"
    uk = "uk"
    aus = "aus"


class LoadType(StrEnum):
    full_load = "full_load"
    incremental_load = "incremental_load"


def _raise_user_exception(e: ValidationError) -> None:
    error_messages = [f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()]
    raise UserException(f"Configuration error: {', '.join(error_messages)}") from e


class RootConfig(BaseModel):
    """The shared connection fields — root config, per spec §5."""

    model_config = ConfigDict(extra="ignore")

    environment: Environment
    tenant: str
    username: str
    password: SecretStr = Field(alias="#password")

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            _raise_user_exception(e)


class Configuration(RootConfig):
    """The fully merged root+row config used by `run()` — per spec §5."""

    model_config = ConfigDict(extra="forbid")

    table: str
    load_type: LoadType = LoadType.full_load
    page_size: int = 20000

    @property
    def incremental(self) -> bool:
        """True when this row selected Incremental Load — the per-run PK-safety fallback (spec
        §2/§6) is applied later in `component.py`, not here."""
        return self.load_type == LoadType.incremental_load
