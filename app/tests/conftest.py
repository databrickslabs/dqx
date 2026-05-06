"""Shared pytest fixtures and config for the DQX Studio backend.

These tests are pure unit tests — no live Databricks workspace,
no SQL warehouse. Anything that talks to a remote service is replaced
with a ``unittest.mock.MagicMock`` (or ``create_autospec``) so the
suite runs offline in <1s.
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock, create_autospec

import pytest

# ---------------------------------------------------------------------------
# Module-level env shim: ensure the AppConfig import has predictable values.
# pydantic-settings honours real env vars over defaults, so we want to land
# at a known-good baseline before ``backend.config`` is imported by anything
# under test. Each test that needs different values can monkeypatch.
# ---------------------------------------------------------------------------

os.environ.setdefault("DQX_CATALOG", "dqx_test")
os.environ.setdefault("DQX_SCHEMA", "dqx_app_test")
os.environ.setdefault("DQX_TMP_SCHEMA", "dqx_app_test_tmp")
os.environ.setdefault("DQX_ADMIN_GROUP", "test-admins")
os.environ.setdefault("DQX_JOB_ID", "")
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "test-warehouse")


# ---------------------------------------------------------------------------
# Async-cache reset — the module-level ``app_cache`` is a process singleton.
# Without resetting between tests, the OBO/SP caches pollute each other.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
async def _reset_app_cache():
    from databricks_labs_dqx_app.backend.cache import app_cache

    await app_cache.clear()
    yield
    await app_cache.clear()


# ---------------------------------------------------------------------------
# Common mocks
# ---------------------------------------------------------------------------


@pytest.fixture
def sql_executor_mock() -> MagicMock:
    """Spec-bound mock of ``SqlExecutor`` so misuse fails loudly."""
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

    mock = create_autospec(SqlExecutor, instance=True)
    mock.catalog = "dqx_test"
    mock.schema = "dqx_app_test"
    mock.warehouse_id = "test-warehouse"
    return mock


@pytest.fixture
def workspace_client_mock() -> MagicMock:
    """A flexible WorkspaceClient mock — most tests only need a couple of attrs."""
    return MagicMock(name="WorkspaceClient")


@pytest.fixture
def app_config():
    """Return the live AppConfig singleton (driven by the env shim above)."""
    from databricks_labs_dqx_app.backend.config import conf

    return conf


@pytest.fixture
def make_user_factory():
    """Build a SCIM-shaped user object compatible with ``WorkspaceClient.current_user.me()``.

    Usage:
        user = make_user_factory(email="alice@example.com", groups=["data-eng", "admins"])
    """

    def _make(email: str = "user@example.com", groups: list[str] | None = None) -> Any:
        user = MagicMock()
        user.user_name = email
        user.id = "u-" + email.split("@")[0]
        user.groups = [MagicMock(display=g) for g in (groups or [])]
        return user

    return _make
