"""Tests for AppConfig fields and env-var overrides."""

from __future__ import annotations


def test_genie_schema_name_default_and_env(monkeypatch):
    monkeypatch.delenv("DQX_GENIE_SCHEMA", raising=False)
    # Re-import so pydantic-settings picks up the cleared env.
    import importlib

    import databricks_labs_dqx_app.backend.config as config_module

    importlib.reload(config_module)
    from databricks_labs_dqx_app.backend.config import AppConfig

    assert AppConfig().genie_schema_name == "genie"

    monkeypatch.setenv("DQX_GENIE_SCHEMA", "custom_genie")
    assert AppConfig().genie_schema_name == "custom_genie"
