"""Tests for the ``/config/global-results-settings`` route (issue B2-20).

An admin toggle that enables the app-wide, all-tables Results surface.
OFF by default; read at VIEWER+ (the sidebar / homepage gate on it) and
written ADMIN-only. See ``AppSettingsService.get_global_results_enabled``.
"""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.routes.v1.config import (
    GlobalResultsSettingsIn,
    get_global_results_settings,
    save_global_results_settings,
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


class TestGetGlobalResultsSettings:
    def test_defaults_to_disabled_when_unset(self, svc):
        result = get_global_results_settings(svc)

        assert result.global_results_enabled is False


class TestSaveGlobalResultsSettings:
    def test_enable_round_trips(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)

        result = save_global_results_settings(
            GlobalResultsSettingsIn(global_results_enabled=True), svc, "admin@x"
        )

        assert result.global_results_enabled is True
        assert get_global_results_settings(svc).global_results_enabled is True

    def test_disable_round_trips(self, svc, sql_executor_mock):
        _wire_stateful_store(sql_executor_mock)

        save_global_results_settings(GlobalResultsSettingsIn(global_results_enabled=True), svc, "admin@x")
        result = save_global_results_settings(
            GlobalResultsSettingsIn(global_results_enabled=False), svc, "admin@x"
        )

        assert result.global_results_enabled is False
        assert get_global_results_settings(svc).global_results_enabled is False
