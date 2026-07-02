"""Tests for the ``/config/ai-settings`` admin route (Rules Registry Phase 4A)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.config import AiSettingsIn, get_ai_settings, save_ai_settings
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


@pytest.fixture
def svc(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.query.return_value = []
    return AppSettingsService(sql=sql_executor_mock)


class TestGetAiSettings:
    def test_returns_defaults_when_unset(self, svc):
        result = get_ai_settings(svc)

        assert result.ai_enabled is False
        assert result.ai_endpoint_name == ""
        assert result.ai_rate_limit_per_user_per_hour == 30
        assert result.ai_rate_limit_default == 30


class TestSaveAiSettings:
    def test_requires_at_least_one_field(self, svc):
        with pytest.raises(HTTPException) as exc_info:
            save_ai_settings(AiSettingsIn(), svc, "admin@x")
        assert exc_info.value.status_code == 400

    def test_rejects_negative_rate_limit(self, svc):
        with pytest.raises(HTTPException) as exc_info:
            save_ai_settings(AiSettingsIn(ai_rate_limit_per_user_per_hour=-1), svc, "admin@x")
        assert exc_info.value.status_code == 400

    def test_saves_and_echoes_effective_settings(self, svc, sql_executor_mock):
        # Backstop the mocked executor with a tiny in-memory key/value store so the
        # GET-after-PUT round trip inside the route reflects the just-saved values,
        # not the fixture's static ``[]`` stub.
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

        result = save_ai_settings(
            AiSettingsIn(ai_enabled=True, ai_endpoint_name="my-endpoint", ai_rate_limit_per_user_per_hour=5),
            svc,
            "admin@x",
        )

        assert sql_executor_mock.upsert.call_count == 3
        assert result.ai_enabled is True
        assert result.ai_endpoint_name == "my-endpoint"
        assert result.ai_rate_limit_per_user_per_hour == 5
