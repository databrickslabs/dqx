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


@pytest.fixture
def make_scheduler():
    """Factory that constructs a real :class:`SchedulerService` for tests.

    Why this exists
    ---------------
    Earlier scheduler tests skipped ``__init__`` via
    ``SchedulerService.__new__(SchedulerService)`` and hand-attached
    private attributes (``_sql``, ``_oltp_sql``, ``_settings_table``,
    ``_catalog``, ...). That pattern couples the test suite to internal
    field names: a refactor of ``_oltp_sql`` → ``_oltp_executor`` would
    silently turn every fixture into AttributeError at access time
    rather than a loud TypeError at construction.

    This factory routes through the real public constructor instead.
    The constructor parameter ``oltp_sql=`` IS the seam — renaming it
    fails fast in CI; renaming the resulting ``_oltp_sql`` private
    attribute only matters for the one test that introspects Delta-side
    ``execute`` calls separately (which we document explicitly via the
    ``distinct_sql`` opt-in).

    The constructor and ``SqlExecutor.__init__`` are both pure
    attribute-assignment — no IO, no warehouse calls, no event-loop
    binding — so wrapping them in a unit test is safe.

    Returns
    -------
    A callable that builds a ``(service, mocks)`` pair. ``mocks.oltp``
    is the OLTP executor passed via the public ``oltp_sql=`` parameter;
    ``mocks.sql`` is the analytical Delta executor. With
    ``distinct_sql=True``, ``mocks.sql`` is a fresh ``MagicMock``
    swapped onto the service so tests can distinguish Delta DELETE
    calls from OLTP ones. With ``distinct_sql=False`` (the default,
    matching legacy single-backend deployments), ``mocks.sql`` aliases
    the constructor-built ``SqlExecutor`` — tests in that mode only
    interact with ``mocks.oltp``.

    Usage::

        svc, mocks = make_scheduler(oltp_dialect="postgres")
        mocks.oltp.query.return_value = [("90",)]
        assert svc._resolve_retention_days() == 90
    """
    from types import SimpleNamespace

    def _make(
        *,
        oltp_dialect: str = "delta",
        oltp_query_return: list[tuple[Any, ...]] | None = None,
        catalog: str = "dqx",
        schema: str = "public",
        tmp_schema: str | None = None,
        distinct_sql: bool = False,
        distinct_tmp_sql: bool = False,
        oltp_spec: list[str] | None = None,
        data_product_service: Any | None = None,
        binding_run_service: Any | None = None,
        score_cache_service: Any | None = None,
    ) -> tuple[Any, SimpleNamespace]:
        from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService

        if oltp_spec is not None:
            # spec-bound mock — used by tests that need attribute
            # access on the mock to be restricted (so any code path
            # that touches an unlisted attribute fails loudly). The
            # spec list MUST cover every attribute the constructor +
            # tested method touches: ``fqn``, ``query``, ``execute``
            # at minimum.
            oltp = MagicMock(spec=oltp_spec, name="oltp_sql")
        else:
            oltp = MagicMock(name="oltp_sql")
            oltp.dialect = oltp_dialect

        # Production-realistic FQN shape: the constructor calls
        # ``oltp_sql.fqn("dq_app_settings")`` etc., so a side_effect
        # that returns ``catalog.schema.<name>`` produces a meaningful
        # ``_settings_table`` for tests that later inspect it.
        oltp.fqn.side_effect = lambda t: f"{catalog}.{schema}.{t}"
        oltp.query.return_value = [] if oltp_query_return is None else oltp_query_return

        # ``OltpExecutorProtocol`` methods that services depend on for
        # dialect-agnostic SQL — wire concrete side_effects matching
        # the real implementations so tests that assert on rendered
        # SQL (e.g. retention's INTERVAL literal) see real strings,
        # not MagicMock reprs. Only attached when the spec permits.
        if oltp_spec is None or "interval_days_expr" in oltp_spec:
            if oltp_dialect == "postgres":
                oltp.interval_days_expr.side_effect = lambda d: f"INTERVAL '{int(d)} days'"
            else:
                oltp.interval_days_expr.side_effect = lambda d: f"INTERVAL {int(d)} DAY"
        if oltp_spec is None or "select_json_text" in oltp_spec:
            if oltp_dialect == "postgres":
                oltp.select_json_text.side_effect = lambda c: c
            else:
                oltp.select_json_text.side_effect = lambda c: f"to_json({c})"

        svc = SchedulerService(
            ws=MagicMock(name="WorkspaceClient"),
            warehouse_id="test-wh",
            catalog=catalog,
            schema=schema,
            tmp_schema=tmp_schema if tmp_schema is not None else f"{schema}_tmp",
            job_id="test-job-0",
            oltp_sql=oltp,
            data_product_service=data_product_service,
            binding_run_service=binding_run_service,
            score_cache_service=score_cache_service,
        )

        mocks = SimpleNamespace(oltp=oltp)
        # The analytical and tmp executors have no constructor seam
        # today (the constructor builds them from ``ws + warehouse_id +
        # {schema,tmp_schema}``). Tests that need to assert on their
        # calls swap them here explicitly. These two attribute writes
        # are the entire remaining internal-name surface — collapsing
        # them into one helper means a future rename of either field
        # fails in one place rather than scattered across the suite.
        if distinct_sql:
            delta = MagicMock(name="delta_sql")
            svc._sql = delta  # noqa: SLF001 - see helper docstring
            mocks.sql = delta
        else:
            mocks.sql = svc._sql  # noqa: SLF001 - alias for legacy single-backend tests
        if distinct_tmp_sql:
            tmp = MagicMock(name="tmp_sql")
            svc._tmp_sql = tmp  # noqa: SLF001 - see helper docstring
            mocks.tmp = tmp
        else:
            mocks.tmp = svc._tmp_sql  # noqa: SLF001

        return svc, mocks

    return _make
