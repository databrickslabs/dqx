"""Tests for ``backend.app._maybe_start_ai_bootstrap``.

Wires ``AiBootstrap.ensure_ai_ready`` at app startup: fire-and-forget via
``asyncio.create_task`` when AI is enabled, a no-op when it isn't, and
never allowed to raise or block startup.
"""

import asyncio
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI

from databricks_labs_dqx_app.backend.app import _maybe_start_ai_bootstrap
from databricks_labs_dqx_app.backend.services.ai_bootstrap import AiBootstrap
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


@pytest.fixture
def app() -> FastAPI:
    return FastAPI()


@pytest.fixture
def sp_ws() -> WorkspaceClient:
    return create_autospec(WorkspaceClient, instance=True)


def _wire_ai_enabled(sql_executor_mock, enabled: bool) -> None:
    sql_executor_mock.query.return_value = [("true" if enabled else "false",)]


class TestStartsWhenAiEnabled:
    async def test_creates_a_background_task_and_calls_ensure_ai_ready(
        self, app, sp_ws, sql_executor_mock, monkeypatch
    ):
        _wire_ai_enabled(sql_executor_mock, enabled=True)

        bootstrap = create_autospec(AiBootstrap, instance=True)
        done = asyncio.Event()

        async def _record() -> None:
            done.set()

        bootstrap.ensure_ai_ready.side_effect = _record
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.AiBootstrap",
            lambda **_kwargs: bootstrap,
        )

        _maybe_start_ai_bootstrap(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        assert hasattr(app.state, "ai_bootstrap_startup_task")
        await asyncio.wait_for(done.wait(), timeout=2)
        bootstrap.ensure_ai_ready.assert_called_once()
        # The task was stashed on app.state and doesn't block the caller —
        # `_maybe_start_ai_bootstrap` itself is synchronous.
        await app.state.ai_bootstrap_startup_task

    async def test_prefers_pg_executor_over_delta_when_provided(self, app, sp_ws, sql_executor_mock, monkeypatch):
        pg_executor = create_autospec(SqlExecutor, instance=True)
        pg_executor.query.return_value = [("true",)]
        # Delta executor would report AI disabled — proves pg_executor,
        # not sp_sql, is the one actually consulted when both are present.
        sql_executor_mock.query.return_value = [("false",)]

        bootstrap = create_autospec(AiBootstrap, instance=True)

        async def _noop() -> None:
            return None

        bootstrap.ensure_ai_ready.side_effect = _noop
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.AiBootstrap",
            lambda **_kwargs: bootstrap,
        )

        _maybe_start_ai_bootstrap(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=pg_executor)

        assert hasattr(app.state, "ai_bootstrap_startup_task")
        await app.state.ai_bootstrap_startup_task
        bootstrap.ensure_ai_ready.assert_called_once()


class TestSkipsWhenAiDisabled:
    async def test_does_not_create_a_task_when_ai_is_disabled(self, app, sp_ws, sql_executor_mock, monkeypatch):
        _wire_ai_enabled(sql_executor_mock, enabled=False)

        bootstrap = create_autospec(AiBootstrap, instance=True)
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.AiBootstrap",
            lambda **_kwargs: bootstrap,
        )

        _maybe_start_ai_bootstrap(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        assert not hasattr(app.state, "ai_bootstrap_startup_task")
        bootstrap.ensure_ai_ready.assert_not_called()


class TestNeverRaisesOrBlocksStartup:
    async def test_swallows_errors_raised_while_kicking_off_bootstrap(self, app, sp_ws, sql_executor_mock):
        # Force an error in the setup path itself (before any task is
        # created) by making the settings query blow up.
        sql_executor_mock.query.side_effect = RuntimeError("warehouse unreachable")

        # Must not raise — startup has to continue regardless.
        _maybe_start_ai_bootstrap(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        assert not hasattr(app.state, "ai_bootstrap_startup_task")

    async def test_task_failure_does_not_propagate_to_the_caller(self, app, sp_ws, sql_executor_mock, monkeypatch):
        _wire_ai_enabled(sql_executor_mock, enabled=True)

        bootstrap = create_autospec(AiBootstrap, instance=True)

        async def _boom() -> None:
            raise RuntimeError("Serving control plane unreachable")

        bootstrap.ensure_ai_ready.side_effect = _boom
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.AiBootstrap",
            lambda **_kwargs: bootstrap,
        )

        # Synchronous call returns cleanly even though the task it kicked
        # off will fail once the event loop gets around to it.
        _maybe_start_ai_bootstrap(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        with pytest.raises(RuntimeError, match="Serving control plane unreachable"):
            await app.state.ai_bootstrap_startup_task
