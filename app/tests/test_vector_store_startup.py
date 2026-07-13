"""Tests for ``backend.app._maybe_start_vector_store_provisioning``.

``VectorStoreProvisioner.ensure_vector_store`` was implemented (Rules
Registry Phase 7F) but was never actually invoked anywhere — this helper
(extracted from the ``lifespan`` startup sequence so it can be unit tested
in isolation) is what wires it up at app startup: fire-and-forget via
``asyncio.create_task`` when AI is enabled, a no-op when it isn't, and
never allowed to raise or block startup.
"""

from __future__ import annotations

import asyncio
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI

from databricks_labs_dqx_app.backend.app import _maybe_start_vector_store_provisioning
from databricks_labs_dqx_app.backend.services.vector_store import VectorStoreProvisioner
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
    async def test_creates_a_background_task_and_calls_ensure_vector_store(
        self, app, sp_ws, sql_executor_mock, monkeypatch
    ):
        _wire_ai_enabled(sql_executor_mock, enabled=True)

        provisioner = create_autospec(VectorStoreProvisioner, instance=True)
        done = asyncio.Event()

        async def _record() -> None:
            done.set()

        provisioner.ensure_vector_store.side_effect = _record
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.VectorStoreProvisioner",
            lambda **_kwargs: provisioner,
        )

        _maybe_start_vector_store_provisioning(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        assert hasattr(app.state, "vector_store_startup_task")
        await asyncio.wait_for(done.wait(), timeout=2)
        provisioner.ensure_vector_store.assert_called_once()
        # The task was stashed on app.state and doesn't block the caller —
        # `_maybe_start_vector_store_provisioning` itself is synchronous.
        await app.state.vector_store_startup_task

    async def test_prefers_pg_executor_over_delta_when_provided(self, app, sp_ws, sql_executor_mock, monkeypatch):
        pg_executor = create_autospec(SqlExecutor, instance=True)
        pg_executor.query.return_value = [("true",)]
        # Delta executor would report AI disabled — proves pg_executor,
        # not sp_sql, is the one actually consulted when both are present.
        sql_executor_mock.query.return_value = [("false",)]

        provisioner = create_autospec(VectorStoreProvisioner, instance=True)

        async def _noop() -> None:
            return None

        provisioner.ensure_vector_store.side_effect = _noop
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.VectorStoreProvisioner",
            lambda **_kwargs: provisioner,
        )

        _maybe_start_vector_store_provisioning(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=pg_executor)

        assert hasattr(app.state, "vector_store_startup_task")
        await app.state.vector_store_startup_task
        provisioner.ensure_vector_store.assert_called_once()


class TestSkipsWhenAiDisabled:
    async def test_does_not_create_a_task_when_ai_is_disabled(self, app, sp_ws, sql_executor_mock, monkeypatch):
        _wire_ai_enabled(sql_executor_mock, enabled=False)

        provisioner = create_autospec(VectorStoreProvisioner, instance=True)
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.VectorStoreProvisioner",
            lambda **_kwargs: provisioner,
        )

        _maybe_start_vector_store_provisioning(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        assert not hasattr(app.state, "vector_store_startup_task")
        provisioner.ensure_vector_store.assert_not_called()


class TestNeverRaisesOrBlocksStartup:
    async def test_swallows_errors_raised_while_kicking_off_provisioning(self, app, sp_ws, sql_executor_mock):
        # Force an error in the setup path itself (before any task is
        # created) by making the settings query blow up.
        sql_executor_mock.query.side_effect = RuntimeError("warehouse unreachable")

        # Must not raise — startup has to continue regardless.
        _maybe_start_vector_store_provisioning(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        assert not hasattr(app.state, "vector_store_startup_task")

    async def test_task_failure_does_not_propagate_to_the_caller(self, app, sp_ws, sql_executor_mock, monkeypatch):
        _wire_ai_enabled(sql_executor_mock, enabled=True)

        provisioner = create_autospec(VectorStoreProvisioner, instance=True)

        async def _boom() -> None:
            raise RuntimeError("Vector Search control plane unreachable")

        provisioner.ensure_vector_store.side_effect = _boom
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.app.VectorStoreProvisioner",
            lambda **_kwargs: provisioner,
        )

        # Synchronous call returns cleanly even though the task it kicked
        # off will fail once the event loop gets around to it.
        _maybe_start_vector_store_provisioning(app, sp_ws=sp_ws, sp_sql=sql_executor_mock, pg_executor=None)

        with pytest.raises(RuntimeError, match="Vector Search control plane unreachable"):
            await app.state.vector_store_startup_task
