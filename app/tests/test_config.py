"""Tests for AppConfig fields and env-var overrides."""

import pytest
from pydantic import ValidationError


def test_lakebase_pool_min_size_defaults_to_zero(monkeypatch):
    # Scale-to-zero: the pool must be allowed to drain to zero idle
    # connections so a suspended Lakebase endpoint isn't kept warm.
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_LAKEBASE_POOL_MIN_SIZE", raising=False)
    assert AppConfig(_env_file=None).lakebase_pool_min_size == 0


def test_lakebase_pool_min_size_env_override(monkeypatch):
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_LAKEBASE_POOL_MIN_SIZE", "2")
    assert AppConfig(_env_file=None).lakebase_pool_min_size == 2


def test_admin_group_defaults_to_workspace_admins(monkeypatch):
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_ADMIN_GROUP", raising=False)
    assert AppConfig(_env_file=None).admin_group == "admins"


def test_admin_group_rejects_whitespace_only_value() -> None:
    """A blank bootstrap group must fail clearly instead of locking every user out."""
    from databricks_labs_dqx_app.backend.config import AppConfig

    with pytest.raises(ValidationError, match="admin_group"):
        AppConfig(_env_file=None, admin_group="   ")


def test_bundle_resource_tagging_defaults_off(monkeypatch) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.delenv("DQX_TAG_BUNDLE_OWNED_RESOURCES", raising=False)
    assert AppConfig(_env_file=None).tag_bundle_owned_resources is False


def test_bundle_resource_tagging_accepts_dab_opt_in(monkeypatch) -> None:
    from databricks_labs_dqx_app.backend.config import AppConfig

    monkeypatch.setenv("DQX_TAG_BUNDLE_OWNED_RESOURCES", "1")
    assert AppConfig(_env_file=None).tag_bundle_owned_resources is True


def test_sibling_schema_names_follow_bound_volume_schema(monkeypatch):
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    monkeypatch.delenv("DQX_TMP_SCHEMA", raising=False)
    # Re-import so pydantic-settings picks up the cleared env.
    import importlib

    import databricks_labs_dqx_app.backend.config as config_module

    importlib.reload(config_module)
    from databricks_labs_dqx_app.backend.config import AppConfig

    config = AppConfig(_env_file=None, wheels_volume="/Volumes/main/dqx/wheels")
    assert config.tmp_schema_name == "dqx_tmp"
    assert config.genie_schema_name == "dqx_genie"

    monkeypatch.setenv("DQX_GENIE_SCHEMA", "custom_genie")
    monkeypatch.setenv("DQX_TMP_SCHEMA", "custom_tmp")
    configured = AppConfig(_env_file=None, wheels_volume="/Volumes/main/dqx/wheels")
    assert configured.genie_schema_name == "custom_genie"
    assert configured.tmp_schema_name == "custom_tmp"
