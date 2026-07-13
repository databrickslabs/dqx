"""Tests for the ``/config/rules-registry-settings`` route (P21-G).

Bundles two distinct admin governance knobs behind one read/write surface:
``auto_upgrade_without_approval`` (re-approval behaviour) and
``default_auto_upgrade`` (attach-time pin default for new applications /
data-product members). See ``AppSettingsService`` for the full semantics.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.config import (
    RulesRegistrySettingsIn,
    get_rules_registry_settings,
    save_rules_registry_settings,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


def _wire_stateful_store(sql_executor_mock) -> dict[str, str]:
    store: dict[str, str] = {}

    def _upsert(_table, *, key_cols, value_cols, **_kwargs):
        store[key_cols["setting_key"]] = value_cols["setting_value"]

    def _query(sql):
        for key, value in store.items():
            if f"'{key}'" in sql:
                return [(value,)]
        return []

    sql_executor_mock.upsert.side_effect = _upsert
    sql_executor_mock.query.side_effect = _query
    return store


@pytest.fixture
def svc(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.query.return_value = []
    return AppSettingsService(sql=sql_executor_mock)


class TestGetRulesRegistrySettings:
    def test_returns_defaults_when_unset(self, svc):
        result = get_rules_registry_settings(svc)

        # auto_upgrade_without_approval defaults False (Behaviour B);
        # default_auto_upgrade defaults True (follow latest) — the two
        # settings have deliberately different defaults.
        assert result.auto_upgrade_without_approval is False
        assert result.default_auto_upgrade is True


class TestSaveRulesRegistrySettings:
    def test_requires_at_least_one_field(self, svc):
        with pytest.raises(HTTPException) as exc_info:
            save_rules_registry_settings(RulesRegistrySettingsIn(), svc, "admin@x")
        assert exc_info.value.status_code == 400

    def test_saves_default_auto_upgrade_independently(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)

        result = save_rules_registry_settings(
            RulesRegistrySettingsIn(default_auto_upgrade=False), svc, "admin@x"
        )

        assert result.default_auto_upgrade is False
        # auto_upgrade_without_approval untouched — still its default.
        assert result.auto_upgrade_without_approval is False
        assert sql_executor_mock.upsert.call_count == 1

    def test_saves_auto_upgrade_without_approval_independently(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)

        result = save_rules_registry_settings(
            RulesRegistrySettingsIn(auto_upgrade_without_approval=True), svc, "admin@x"
        )

        assert result.auto_upgrade_without_approval is True
        assert result.default_auto_upgrade is True
        assert sql_executor_mock.upsert.call_count == 1

    def test_saves_both_settings_together(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)

        result = save_rules_registry_settings(
            RulesRegistrySettingsIn(auto_upgrade_without_approval=True, default_auto_upgrade=False),
            svc,
            "admin@x",
        )

        assert result.auto_upgrade_without_approval is True
        assert result.default_auto_upgrade is False
        assert sql_executor_mock.upsert.call_count == 2
