"""Tests for the share-tables-with-workspace-users admin setting."""

from __future__ import annotations

import pytest

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
def sql_executor_mock(mocker):
    m = mocker.MagicMock()
    m.fqn.side_effect = lambda t: t
    m.ts_text.side_effect = lambda c: c
    m.query.return_value = []
    return m


@pytest.fixture
def settings_svc(sql_executor_mock):
    return AppSettingsService(sql=sql_executor_mock)


class TestAppSettingsShareTablesWithWorkspaceUsers:
    def test_defaults_to_false(self, settings_svc):
        assert settings_svc.get_share_tables_with_workspace_users() is False

    @pytest.mark.parametrize("enabled", [True, False])
    def test_round_trip(self, settings_svc, sql_executor_mock, enabled):
        _wire_stateful_store(sql_executor_mock)
        assert settings_svc.save_share_tables_with_workspace_users(enabled, user_email="admin@x") is enabled
        assert settings_svc.get_share_tables_with_workspace_users() is enabled

    def test_corrupt_value_reads_as_false(self, settings_svc, sql_executor_mock):
        store = _wire_stateful_store(sql_executor_mock)
        store["share_tables_with_workspace_users"] = "banana"
        assert settings_svc.get_share_tables_with_workspace_users() is False


class TestShareTablesWithWorkspaceUsersRoute:
    def test_get_returns_default_false(self, settings_svc):
        from databricks_labs_dqx_app.backend.routes.v1.config import get_share_tables_with_workspace_users

        assert get_share_tables_with_workspace_users(settings_svc).share_tables_with_workspace_users is False

    def test_put_saves_value(self, settings_svc, sql_executor_mock):
        from databricks_labs_dqx_app.backend.routes.v1.config import (
            ShareTablesWithWorkspaceUsersIn,
            get_share_tables_with_workspace_users,
            save_share_tables_with_workspace_users,
        )

        _wire_stateful_store(sql_executor_mock)
        out = save_share_tables_with_workspace_users(
            ShareTablesWithWorkspaceUsersIn(share_tables_with_workspace_users=True),
            settings_svc,
            "admin@x",
        )
        assert out.share_tables_with_workspace_users is True
        assert get_share_tables_with_workspace_users(settings_svc).share_tables_with_workspace_users is True
