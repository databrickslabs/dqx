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

    def test_tag_auto_apply_defaults_false(self, svc):
        assert get_rules_registry_settings(svc).tag_auto_apply is False


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

    def test_saves_tag_auto_apply_independently(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)
        result = save_rules_registry_settings(
            RulesRegistrySettingsIn(tag_auto_apply=True), svc, "admin@x"
        )
        assert result.tag_auto_apply is True
        assert result.auto_upgrade_without_approval is False
        assert result.default_auto_upgrade is True
        assert sql_executor_mock.upsert.call_count == 1


class TestRulesRegistrySettingsThresholdFields:
    """GET exposes default_pass_threshold + pass_threshold_enabled; PUT can update them."""

    def test_get_returns_default_pass_threshold_70(self, svc):
        result = get_rules_registry_settings(svc)
        assert result.default_pass_threshold == 70

    def test_get_returns_pass_threshold_enabled_true_by_default(self, svc):
        result = get_rules_registry_settings(svc)
        assert result.pass_threshold_enabled is True

    def test_put_pass_threshold_enabled_false_persists(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)
        result = save_rules_registry_settings(
            RulesRegistrySettingsIn(pass_threshold_enabled=False), svc, "admin@x"
        )
        assert result.pass_threshold_enabled is False

    def test_put_default_pass_threshold_persists_via_rules_registry(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)
        result = save_rules_registry_settings(
            RulesRegistrySettingsIn(default_pass_threshold=85), svc, "admin@x"
        )
        assert result.default_pass_threshold == 85

    def test_put_default_pass_threshold_also_readable_via_standalone_endpoint(
        self, svc, sql_executor_mock
    ):
        from databricks_labs_dqx_app.backend.routes.v1.config import get_default_pass_threshold

        _wire_stateful_store(sql_executor_mock)
        save_rules_registry_settings(
            RulesRegistrySettingsIn(default_pass_threshold=85), svc, "admin@x"
        )
        out = get_default_pass_threshold(svc)
        assert out.default_pass_threshold == 85

    def test_put_default_pass_threshold_above_100_rejected(self, svc):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RulesRegistrySettingsIn(default_pass_threshold=150)

    def test_put_default_pass_threshold_below_0_rejected(self, svc):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RulesRegistrySettingsIn(default_pass_threshold=-1)

    def test_get_has_no_admin_role_dependency(self):
        """GET /rules-registry-settings must NOT require ADMIN — it's VIEWER+."""
        import databricks_labs_dqx_app.backend.routes.v1.config as config_routes
        from databricks_labs_dqx_app.backend.common.authorization import UserRole

        for route in config_routes.router.routes:
            if getattr(route, "operation_id", None) != "getRulesRegistrySettings":
                continue
            for dep in route.dependencies:
                for cell in getattr(dep.dependency, "__closure__", None) or ():
                    val = cell.cell_contents
                    if isinstance(val, tuple) and val and all(isinstance(v, UserRole) for v in val):
                        assert UserRole.ADMIN not in val, "GET must not be ADMIN-only"
            return  # route found; no require_role dep = no role guard = VIEWER+ ok
        # If we get here, route was not found — acceptable (no role guard = open to all authenticated)

    def test_put_requires_admin(self):
        """PUT /rules-registry-settings must still require ADMIN."""
        import databricks_labs_dqx_app.backend.routes.v1.config as config_routes
        from databricks_labs_dqx_app.backend.common.authorization import UserRole

        for route in config_routes.router.routes:
            if getattr(route, "operation_id", None) != "saveRulesRegistrySettings":
                continue
            for dep in route.dependencies:
                for cell in getattr(dep.dependency, "__closure__", None) or ():
                    val = cell.cell_contents
                    if isinstance(val, tuple) and val and all(isinstance(v, UserRole) for v in val):
                        assert UserRole.ADMIN in val
                        return
        raise AssertionError("No require_role(ADMIN) found on saveRulesRegistrySettings")
