"""Deployment-over-saved precedence and immutability of setup choices."""

import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.setup.configuration import (
    ConfigurationSource,
    SetupChoices,
    SetupConfigurationStore,
    resolve_configuration,
    validate_choices,
)


class MemorySettings:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get_setting(self, key: str) -> str | None:
        return self.values.get(key)

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        self.values[key] = value


def test_no_inputs_requires_setup_form(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no deployment config and no saved choices, setup form is required."""
    monkeypatch.delenv("DQX_CATALOG", raising=False)
    monkeypatch.delenv("DQX_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_ADMIN_GROUP", raising=False)

    config = AppConfig(_env_file=None)
    resolved = resolve_configuration(config, SetupConfigurationStore(MemorySettings()))

    assert resolved.source == ConfigurationSource.NONE
    assert resolved.storage is None


def test_saved_choices_are_resolved_after_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    """Saved choices are resolved when no deployment config is present."""
    monkeypatch.delenv("DQX_CATALOG", raising=False)
    monkeypatch.delenv("DQX_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_ADMIN_GROUP", raising=False)

    store = SetupConfigurationStore(MemorySettings())
    store.save(SetupChoices("main", "studio", "data-team"), user_email="admin@example.com")

    config = AppConfig(_env_file=None)
    resolved = resolve_configuration(config, store)

    assert resolved.source == ConfigurationSource.SAVED
    assert resolved.storage is not None and resolved.storage.tmp_schema == "studio_tmp"
    assert resolved.audience is not None and resolved.audience.uc_principals == ("data-team",)


def test_deployment_configuration_wins_over_saved_choices(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deployment config takes precedence over saved choices."""
    store = SetupConfigurationStore(MemorySettings())
    store.save(SetupChoices("other", "studio", "data-team"), user_email=None)

    monkeypatch.setenv("DQX_CATALOG", "main")
    monkeypatch.setenv("DQX_PREFIX", "dqx_studio")
    monkeypatch.setenv("DQX_USER_GROUPS", '["users"]')
    monkeypatch.delenv("DQX_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)

    config = AppConfig(_env_file=None)
    resolved = resolve_configuration(config, store)

    assert resolved.source == ConfigurationSource.DEPLOYMENT
    assert resolved.storage is not None and resolved.storage.catalog == "main"
    assert resolved.audience is not None and resolved.audience.broad is True


def test_invalid_deployment_configuration_is_reported_not_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid deployment config is reported via error field, not raised."""
    monkeypatch.setenv("DQX_CATALOG", "main")
    monkeypatch.setenv("DQX_PREFIX", "Bad-Prefix")
    monkeypatch.delenv("DQX_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)

    config = AppConfig(_env_file=None)
    resolved = resolve_configuration(config, SetupConfigurationStore(MemorySettings()))

    assert resolved.source == ConfigurationSource.DEPLOYMENT
    assert resolved.storage is None
    assert resolved.error == "deployment_configuration_invalid"


def test_form_choices_reject_broad_audience() -> None:
    """Broad audience (users) is rejected in form choices."""
    with pytest.raises(InvalidParameterError):
        validate_choices(SetupChoices("main", "dqx_studio", "users"), "admins")


def test_lock_survives_reload() -> None:
    """Lock state persists across store reload."""
    settings = MemorySettings()
    SetupConfigurationStore(settings).lock(user_email=None)

    assert SetupConfigurationStore(settings).is_locked() is True


def test_deployment_derives_correct_tmp_schema_from_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deployment config with custom prefix derives correct tmp/genie schemas."""
    monkeypatch.setenv("DQX_CATALOG", "main")
    monkeypatch.setenv("DQX_PREFIX", "studio")
    monkeypatch.setenv("DQX_USER_GROUPS", '["data-team"]')
    monkeypatch.delenv("DQX_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_DEMO_SCHEMA", raising=False)

    config = AppConfig(_env_file=None)
    resolved = resolve_configuration(config, SetupConfigurationStore(MemorySettings()))

    assert resolved.source == ConfigurationSource.DEPLOYMENT
    assert resolved.storage is not None
    assert resolved.storage.schema == "studio"
    assert resolved.storage.tmp_schema == "studio_tmp"
    assert resolved.storage.genie_schema == "studio_genie"
    assert resolved.storage.demo_schema == "studio_demo"


def test_deployment_respects_explicit_tmp_schema_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit tmp_schema override is respected even with custom prefix."""
    monkeypatch.setenv("DQX_CATALOG", "main")
    monkeypatch.setenv("DQX_PREFIX", "studio")
    monkeypatch.setenv("DQX_TMP_SCHEMA", "custom_tmp")
    monkeypatch.setenv("DQX_USER_GROUPS", '["data-team"]')
    monkeypatch.delenv("DQX_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_DEMO_SCHEMA", raising=False)

    config = AppConfig(_env_file=None)
    resolved = resolve_configuration(config, SetupConfigurationStore(MemorySettings()))

    assert resolved.source == ConfigurationSource.DEPLOYMENT
    assert resolved.storage is not None
    assert resolved.storage.tmp_schema == "custom_tmp"
