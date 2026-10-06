"""Deployment-over-saved precedence and immutability of setup choices."""

import json

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


def _config(**values: str) -> AppConfig:
    # Map validation aliases to field names
    alias_map = {
        "DQX_CATALOG": "catalog",
        "DQX_PREFIX": "prefix",
        "DQX_SCHEMA": "schema_name",
        "DQX_TMP_SCHEMA": "tmp_schema_name",
        "DQX_GENIE_SCHEMA": "genie_schema_name",
        "DQX_DEMO_SCHEMA": "demo_schema_name",
        "DQX_ADMIN_GROUP": "admin_group",
        "DQX_USER_GROUPS": "user_groups",
    }

    # Use model_construct to bypass environment variable loading
    # Set defaults that match AppConfig field defaults
    defaults = {
        "app_name": "DQX Studio",
        "api_prefix": "/api",
        "catalog": "",
        "prefix": "",
        "schema_name": "",
        "tmp_schema_name": "",
        "genie_schema_name": "",
        "demo_schema_name": "",
        "default_dashboard_id": "",
        "job_id": "",
        "wheels_volume": "",
        "tag_bundle_owned_resources": False,
        "require_task_runner": False,
        "llm_endpoint": "databricks-claude-sonnet-4-5",
        "llm_max_tokens": 4096,
        "admin_group": "admins",
        "user_groups": [],
    }

    # Map aliases to field names and update defaults
    for key, value in values.items():
        field_name = alias_map.get(key, key)
        defaults[field_name] = value

    # Parse user_groups if it's a JSON string
    if isinstance(defaults.get("user_groups"), str):
        try:
            defaults["user_groups"] = json.loads(defaults["user_groups"])
        except json.JSONDecodeError:
            defaults["user_groups"] = defaults["user_groups"].split(",")

    return AppConfig.model_construct(**defaults)


def test_no_inputs_requires_setup_form() -> None:
    resolved = resolve_configuration(_config(), SetupConfigurationStore(MemorySettings()))

    assert resolved.source == ConfigurationSource.NONE
    assert resolved.storage is None


def test_saved_choices_are_resolved_after_restart() -> None:
    store = SetupConfigurationStore(MemorySettings())
    store.save(SetupChoices("main", "studio", "data-team"), user_email="admin@example.com")

    resolved = resolve_configuration(_config(), store)

    assert resolved.source == ConfigurationSource.SAVED
    assert resolved.storage is not None and resolved.storage.tmp_schema == "studio_tmp"
    assert resolved.audience is not None and resolved.audience.uc_principals == ("data-team",)


def test_deployment_configuration_wins_over_saved_choices() -> None:
    store = SetupConfigurationStore(MemorySettings())
    store.save(SetupChoices("other", "studio", "data-team"), user_email=None)

    resolved = resolve_configuration(
        _config(DQX_CATALOG="main", DQX_PREFIX="dqx_studio", DQX_USER_GROUPS='["users"]'), store
    )

    assert resolved.source == ConfigurationSource.DEPLOYMENT
    assert resolved.storage is not None and resolved.storage.catalog == "main"
    assert resolved.audience is not None and resolved.audience.broad is True


def test_invalid_deployment_configuration_is_reported_not_raised() -> None:
    resolved = resolve_configuration(_config(DQX_CATALOG="main", DQX_PREFIX="Bad-Prefix"), SetupConfigurationStore(MemorySettings()))

    assert resolved.source == ConfigurationSource.DEPLOYMENT
    assert resolved.storage is None
    assert resolved.error == "deployment_configuration_invalid"


def test_form_choices_reject_broad_audience() -> None:
    with pytest.raises(InvalidParameterError):
        validate_choices(SetupChoices("main", "dqx_studio", "users"), "admins")


def test_lock_survives_reload() -> None:
    settings = MemorySettings()
    SetupConfigurationStore(settings).lock(user_email=None)

    assert SetupConfigurationStore(settings).is_locked() is True
