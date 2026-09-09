"""Tests for the ``/config/ai-settings`` admin route (Rules Registry Phase 4A)."""

import threading
import time
from unittest.mock import create_autospec

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.config import AiSettingsIn, get_ai_settings, save_ai_settings
from databricks_labs_dqx_app.backend.services.ai_bootstrap import AiBootstrap
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


@pytest.fixture
def svc(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.query.return_value = []
    return AppSettingsService(sql=sql_executor_mock)


@pytest.fixture
def bootstrap():
    """Autospec'd bootstrap whose ``ensure_ai_ready`` is awaitable and inert."""

    async def _noop() -> None:
        return None

    mock = create_autospec(AiBootstrap, instance=True)
    mock.ensure_ai_ready.side_effect = _noop
    return mock


class TestGetAiSettings:
    def test_returns_defaults_when_unset(self, svc):
        result = get_ai_settings(svc)

        # AI is ON by default (per explicit product request); the admin
        # kill-switch (an explicit "false") is what turns it off.
        assert result.ai_enabled is True
        assert result.ai_endpoint_name == "databricks-claude-sonnet-4-5"
        assert result.ai_endpoint_name_default == "databricks-claude-sonnet-4-5"
        assert result.ai_rate_limit_per_user_per_hour == 30
        assert result.ai_rate_limit_default == 30
        # Auto-derived (Rules Registry Phase 8B) — no separate admin
        # input for this; see ``AppSettingsService.EMBEDDING_ENDPOINT_NAME_DEFAULT``.
        assert result.embedding_endpoint_name == "databricks-gte-large-en"


def _wire_stateful_store(sql_executor_mock) -> dict[str, str]:
    """Backstop the mocked executor with a tiny in-memory key/value store so the
    GET-after-PUT round trip inside the route reflects the just-saved values,
    not the fixture's static ``[]`` stub."""
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


class TestSaveAiSettings:
    def test_requires_at_least_one_field(self, svc, bootstrap):
        with pytest.raises(HTTPException) as exc_info:
            save_ai_settings(AiSettingsIn(), svc, bootstrap, "admin@x")
        assert exc_info.value.status_code == 400

    def test_rejects_negative_rate_limit(self, svc, bootstrap):
        with pytest.raises(HTTPException) as exc_info:
            save_ai_settings(AiSettingsIn(ai_rate_limit_per_user_per_hour=-1), svc, bootstrap, "admin@x")
        assert exc_info.value.status_code == 400

    def test_saves_and_echoes_effective_settings(self, svc, sql_executor_mock, bootstrap):
        _wire_stateful_store(sql_executor_mock)

        result = save_ai_settings(
            AiSettingsIn(ai_enabled=True, ai_endpoint_name="my-endpoint", ai_rate_limit_per_user_per_hour=5),
            svc,
            bootstrap,
            "admin@x",
        )

        assert sql_executor_mock.upsert.call_count == 3
        assert result.ai_enabled is True
        assert result.ai_endpoint_name == "my-endpoint"
        assert result.ai_rate_limit_per_user_per_hour == 5

    def test_saves_embedding_endpoint_name(self, svc, sql_executor_mock, bootstrap):
        _wire_stateful_store(sql_executor_mock)

        result = save_ai_settings(
            AiSettingsIn(embedding_endpoint_name="embed-endpoint"),
            svc,
            bootstrap,
            "admin@x",
        )

        assert result.embedding_endpoint_name == "embed-endpoint"


class TestSaveAiSettingsTriggersEnsureAiReady:
    """``save_ai_settings`` kicks off ``ensure_ai_ready`` — non-blocking, off the
    request thread, and without letting a bootstrap failure affect the save
    response.
    """

    def test_enabling_ai_invokes_ensure_ai_ready_off_request_thread(self, svc, sql_executor_mock, bootstrap):
        _wire_stateful_store(sql_executor_mock)
        request_thread_id = threading.get_ident()
        done = threading.Event()
        captured: dict[str, int] = {}

        async def _record() -> None:
            captured["thread_id"] = threading.get_ident()
            done.set()

        bootstrap.ensure_ai_ready.side_effect = _record

        result = save_ai_settings(AiSettingsIn(ai_enabled=True), svc, bootstrap, "admin@x")

        assert result.ai_enabled is True
        assert done.wait(timeout=2), "ensure_ai_ready was not invoked in time"
        bootstrap.ensure_ai_ready.assert_called_once()
        assert captured["thread_id"] != request_thread_id

    def test_does_not_invoke_when_ai_stays_disabled(self, svc, sql_executor_mock, bootstrap):
        # AI now defaults ON, so "stays disabled" means the kill-switch is
        # explicitly held off — pass ai_enabled=False alongside the edit.
        _wire_stateful_store(sql_executor_mock)

        result = save_ai_settings(
            AiSettingsIn(ai_enabled=False, ai_rate_limit_per_user_per_hour=10),
            svc,
            bootstrap,
            "admin@x",
        )

        assert result.ai_enabled is False
        bootstrap.ensure_ai_ready.assert_not_called()

    def test_ensure_ai_ready_failure_does_not_affect_save_response(
        self, svc, sql_executor_mock, bootstrap, monkeypatch
    ):
        # The app's shared ``logger`` singleton disables propagation to the
        # root logger (see ``backend/logger.py``), so ``caplog`` can't see
        # it; capture the warning call directly instead.
        from databricks_labs_dqx_app.backend.routes.v1 import config as config_mod

        warnings: list[str] = []
        monkeypatch.setattr(config_mod.logger, "warning", lambda msg, *a, **k: warnings.append(msg), raising=False)

        _wire_stateful_store(sql_executor_mock)
        done = threading.Event()

        async def _boom() -> None:
            done.set()
            raise RuntimeError("Serving control plane unreachable")

        bootstrap.ensure_ai_ready.side_effect = _boom

        result = save_ai_settings(AiSettingsIn(ai_enabled=True), svc, bootstrap, "admin@x")

        assert result.ai_enabled is True
        assert done.wait(timeout=2), "ensure_ai_ready was not invoked in time"
        # Give the background thread's exception handler a moment to log
        # before asserting — the save call itself must already have
        # returned successfully regardless.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not any("non-fatal" in msg for msg in warnings):
            time.sleep(0.01)
        assert any("non-fatal" in msg for msg in warnings)
