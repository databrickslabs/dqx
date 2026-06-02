"""Tests for the retention sweep + admin settings surface.

Three layers exercised:
* ``AppSettingsService.get_retention_days`` /
  ``get_quarantine_retention_days`` and their setters — round-trip the
  integer-day setting through ``dq_app_settings``.
* ``backend.routes.v1.config._validate_retention_days`` — request-side
  bounds checking for the admin PUT endpoint.
* ``SchedulerService._resolve_retention_days`` and
  ``_resolve_quarantine_retention_days`` — fall-back behaviour when the
  setting is missing / unparseable / read fails, plus the safety floor.

The retention defaults differ on purpose — global is 90 days (trend
dashboards) and quarantine is 30 days (tighter PII window) — so the
tests exercise both resolvers separately.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# AppSettingsService — storage layer
# ---------------------------------------------------------------------------


class TestAppSettingsRetention:
    @pytest.fixture
    def svc(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

        return AppSettingsService(sql_executor_mock), sql_executor_mock

    def test_get_returns_none_when_unset(self, svc):
        s, sql = svc
        sql.query.return_value = []
        assert s.get_retention_days() is None
        assert s.get_quarantine_retention_days() is None

    def test_get_returns_none_when_blank(self, svc):
        s, sql = svc
        sql.query.return_value = [(None,)]
        assert s.get_retention_days() is None
        sql.query.return_value = [("",)]
        assert s.get_retention_days() is None

    def test_get_parses_integer(self, svc):
        s, sql = svc
        sql.query.return_value = [("45",)]
        assert s.get_retention_days() == 45
        sql.query.return_value = [("14",)]
        assert s.get_quarantine_retention_days() == 14

    def test_get_returns_none_on_garbage_value(self, svc):
        s, sql = svc
        sql.query.return_value = [("not-a-number",)]
        assert s.get_retention_days() is None

    def test_save_persists_integer_string(self, svc):
        s, sql = svc
        assert s.save_retention_days(60) == 60
        assert sql.upsert.called
        kwargs = sql.upsert.call_args.kwargs
        if "value_cols" in kwargs:
            payload = kwargs["value_cols"]["setting_value"]
        else:
            payload = sql.upsert.call_args.args[2]["setting_value"]
        assert payload == "60"

    def test_save_quarantine_persists_integer_string(self, svc):
        s, sql = svc
        assert s.save_quarantine_retention_days(21) == 21
        # The most recent upsert should carry the new key.
        kwargs = sql.upsert.call_args.kwargs
        if "key_cols" in kwargs:
            keys = kwargs["key_cols"]
        else:
            keys = sql.upsert.call_args.args[1]
        assert keys["setting_key"] == "quarantine_retention_days"


# ---------------------------------------------------------------------------
# Route-level validator
# ---------------------------------------------------------------------------


class TestRetentionDaysValidator:
    @pytest.fixture
    def validate(self):
        from databricks_labs_dqx_app.backend.routes.v1.config import _validate_retention_days

        return _validate_retention_days

    def test_min_floor_enforced(self, validate):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            validate(1, field="retention_days")
        assert exc.value.status_code == 400
        assert "at least" in exc.value.detail.lower()

    def test_max_ceiling_enforced(self, validate):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            validate(10_000, field="retention_days")
        assert exc.value.status_code == 400
        assert "at most" in exc.value.detail.lower()

    def test_valid_value_passes_through(self, validate):
        assert validate(30, field="quarantine_retention_days") == 30
        assert validate(90, field="retention_days") == 90

    def test_field_label_appears_in_message(self, validate):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            validate(2, field="quarantine_retention_days")
        assert "quarantine_retention_days" in exc.value.detail


# ---------------------------------------------------------------------------
# Scheduler service — both resolvers honour the floor and fall back safely
# ---------------------------------------------------------------------------


class TestSchedulerResolveRetention:
    """Scheduler must never crash on a missing / corrupt retention setting.

    Built via the real ``SchedulerService(...)`` constructor through
    the shared :func:`make_scheduler` factory so a rename of the
    public ``oltp_sql=`` constructor parameter fails at construction
    time rather than silently producing AttributeErrors later.
    """

    @pytest.fixture
    def scheduler(self, make_scheduler):
        return make_scheduler()

    def test_global_falls_back_to_default_when_unset(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import _RETENTION_DAYS_DEFAULT

        svc, mocks = scheduler
        mocks.oltp.query.return_value = []
        assert svc._resolve_retention_days() == _RETENTION_DAYS_DEFAULT

    def test_quarantine_falls_back_to_its_own_default(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
        )

        svc, mocks = scheduler
        mocks.oltp.query.return_value = []
        assert svc._resolve_quarantine_retention_days() == _QUARANTINE_RETENTION_DAYS_DEFAULT

    def test_quarantine_default_is_tighter_than_global(self):
        """Sanity check on the constants: quarantine (PII) ages out faster."""
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _RETENTION_DAYS_DEFAULT,
        )

        assert _QUARANTINE_RETENTION_DAYS_DEFAULT < _RETENTION_DAYS_DEFAULT

    def test_returns_persisted_value(self, scheduler):
        svc, mocks = scheduler
        mocks.oltp.query.return_value = [("45",)]
        assert svc._resolve_retention_days() == 45

    def test_floor_protects_against_too_small_value(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import _RETENTION_DAYS_MIN

        svc, mocks = scheduler
        mocks.oltp.query.return_value = [("1",)]
        # Stored value of 1 must be raised to the floor — never wipe inside the safety window.
        assert svc._resolve_retention_days() == _RETENTION_DAYS_MIN
        assert svc._resolve_quarantine_retention_days() == _RETENTION_DAYS_MIN

    def test_swallows_query_exception(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _RETENTION_DAYS_DEFAULT,
        )

        svc, mocks = scheduler
        mocks.oltp.query.side_effect = RuntimeError("warehouse offline")
        # Must not propagate — sweep retries on the next tick.
        assert svc._resolve_retention_days() == _RETENTION_DAYS_DEFAULT
        assert svc._resolve_quarantine_retention_days() == _QUARANTINE_RETENTION_DAYS_DEFAULT

    def test_unparseable_value_returns_default(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import _RETENTION_DAYS_DEFAULT

        svc, mocks = scheduler
        mocks.oltp.query.return_value = [("not-a-number",)]
        assert svc._resolve_retention_days() == _RETENTION_DAYS_DEFAULT


# ---------------------------------------------------------------------------
# Scheduler service — _run_retention applies the right cutoff per table
# ---------------------------------------------------------------------------


class TestRunRetentionUsesQuarantineCutoff:
    """The Delta sweep must use the quarantine-specific cutoff for that one table."""

    @pytest.fixture
    def scheduler(self, make_scheduler):
        # ``distinct_sql=True`` so we can introspect Delta-side
        # ``execute`` calls independently of OLTP ones below.
        return make_scheduler(distinct_sql=True)

    def test_quarantine_table_uses_its_own_default(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _RETENTION_DAYS_DEFAULT,
        )

        svc, mocks = scheduler
        svc._run_retention()

        # Collect every DELETE statement that actually fired.
        delta_stmts = [c.args[0] for c in mocks.sql.execute.call_args_list]
        quarantine_stmts = [s for s in delta_stmts if "dq_quarantine_records" in s]
        non_quarantine_delta = [s for s in delta_stmts if "dq_quarantine_records" not in s]

        assert quarantine_stmts, "Expected at least one DELETE for the quarantine table"
        # Every quarantine DELETE should use the *quarantine* default,
        # not the global one.
        for stmt in quarantine_stmts:
            assert f"INTERVAL {_QUARANTINE_RETENTION_DAYS_DEFAULT} DAY" in stmt
            assert f"INTERVAL {_RETENTION_DAYS_DEFAULT} DAY" not in stmt

        # And the other Delta tables should still use the global default.
        for stmt in non_quarantine_delta:
            assert f"INTERVAL {_RETENTION_DAYS_DEFAULT} DAY" in stmt


# ---------------------------------------------------------------------------
# Scheduler service — _run_retention uses dialect-specific INTERVAL syntax
# ---------------------------------------------------------------------------


class TestRunRetentionPostgresDialect:
    """OLTP retention must emit Postgres-flavoured ``INTERVAL`` syntax.

    Delta speaks ``INTERVAL 90 DAY`` (no quotes, uppercase singular);
    Postgres speaks ``INTERVAL '90 days'`` (single-quoted literal,
    lowercase plural). The two syntaxes are mutually incompatible —
    Postgres throws ``ERROR: syntax error at or near "DAY"`` on the
    Delta form, and Delta throws on the quoted form — so the
    scheduler branches on ``self._oltp_sql.dialect``. This class
    parallels :class:`TestRunRetentionUsesQuarantineCutoff` (which
    covers the Delta dialect) for the Postgres branch.

    The reviewer's specific contract to lock down:

    1. OLTP DELETEs use ``INTERVAL '<N> days'`` (and NEVER
       ``INTERVAL <N> DAY``) when the executor reports
       ``dialect == "postgres"``.
    2. Cutoff routing is correct: OLTP tables today use the *global*
       ``days`` (no OLTP quarantine table), while the Delta loop still
       routes ``dq_quarantine_records`` to ``quarantine_days``.
    3. The Delta loop is untouched by the OLTP dialect flip — its
       statements keep using the Delta ``INTERVAL N DAY`` form.
    """

    @pytest.fixture
    def scheduler(self, make_scheduler):
        # ``distinct_sql=True`` keeps ``mocks.sql`` (analytical Delta)
        # and ``mocks.oltp`` (Postgres-flavoured) addressable as
        # separate mocks so the test can assert which loop emitted
        # which DELETE.
        return make_scheduler(oltp_dialect="postgres", catalog="dqx", schema="public", distinct_sql=True)

    @staticmethod
    def _delta_stmts(mocks) -> list[str]:
        return [c.args[0] for c in mocks.sql.execute.call_args_list]

    @staticmethod
    def _oltp_stmts(mocks) -> list[str]:
        return [c.args[0] for c in mocks.oltp.execute.call_args_list]

    def test_oltp_loop_uses_postgres_interval_syntax(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _OLTP_RETENTION_TABLES,
            _RETENTION_DAYS_DEFAULT,
        )

        svc, mocks = scheduler
        svc._run_retention()

        oltp_stmts = self._oltp_stmts(mocks)
        # One DELETE per OLTP retention table.
        assert len(oltp_stmts) == len(_OLTP_RETENTION_TABLES)

        for stmt in oltp_stmts:
            # Postgres form (with single quotes, lowercase plural "days").
            assert (
                f"INTERVAL '{_RETENTION_DAYS_DEFAULT} days'" in stmt
            ), f"OLTP DELETE missing Postgres INTERVAL literal: {stmt!r}"
            # Delta form must NOT appear — Postgres would reject it.
            assert (
                f"INTERVAL {_RETENTION_DAYS_DEFAULT} DAY" not in stmt
            ), f"OLTP DELETE leaked Delta INTERVAL syntax into Postgres: {stmt!r}"

    def test_oltp_loop_uses_global_cutoff_not_quarantine(self, scheduler):
        """No OLTP table is in the quarantine list today — every OLTP DELETE uses ``days``."""
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _QUARANTINE_TABLE_NAME,
            _OLTP_RETENTION_TABLES,
            _RETENTION_DAYS_DEFAULT,
        )

        # Guard the test's premise: if anyone adds the quarantine
        # table to the OLTP list, this assertion forces them to
        # extend the cutoff-routing logic for the OLTP loop too.
        assert _QUARANTINE_TABLE_NAME not in {t for t, _ in _OLTP_RETENTION_TABLES}, (
            "Quarantine table moved to OLTP — extend _run_retention to route "
            "quarantine_days for the OLTP branch and update this test."
        )

        svc, mocks = scheduler
        svc._run_retention()

        oltp_stmts = self._oltp_stmts(mocks)
        for stmt in oltp_stmts:
            assert f"INTERVAL '{_RETENTION_DAYS_DEFAULT} days'" in stmt
            assert f"INTERVAL '{_QUARANTINE_RETENTION_DAYS_DEFAULT} days'" not in stmt

    def test_delta_loop_untouched_by_postgres_oltp_dialect(self, scheduler):
        """Flipping ``_oltp_sql.dialect`` MUST NOT change the Delta loop's syntax."""
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _RETENTION_DAYS_DEFAULT,
        )

        svc, mocks = scheduler
        svc._run_retention()

        delta_stmts = self._delta_stmts(mocks)
        for stmt in delta_stmts:
            # Delta keeps its no-quotes, uppercase-DAY form regardless
            # of what the OLTP executor speaks.
            if "dq_quarantine_records" in stmt:
                assert f"INTERVAL {_QUARANTINE_RETENTION_DAYS_DEFAULT} DAY" in stmt
            else:
                assert f"INTERVAL {_RETENTION_DAYS_DEFAULT} DAY" in stmt
            # No Postgres syntax should ever appear in a Delta DELETE.
            assert "days'" not in stmt
            assert "'" not in stmt.split("INTERVAL", 1)[1] if "INTERVAL" in stmt else True

    def test_oltp_delete_targets_use_executor_fqn(self, scheduler):
        """OLTP loop must build the table path via ``self._oltp_sql.fqn(...)``.

        Hard-coding ``catalog.schema.table`` would break Postgres (no
        catalog) and bypass the executor's dialect-aware quoting.
        """
        from databricks_labs_dqx_app.backend.services.scheduler_service import _OLTP_RETENTION_TABLES

        svc, mocks = scheduler
        svc._run_retention()

        oltp_stmts = self._oltp_stmts(mocks)
        # ``make_scheduler`` configures ``oltp.fqn`` to return
        # ``catalog.schema.table`` — the same shape :class:`SqlExecutor`
        # produces. Any code change that bypasses ``fqn()`` and
        # hardcodes its own FQN shape will fail this assertion.
        for table_name, _ in _OLTP_RETENTION_TABLES:
            assert any(
                f"DELETE FROM dqx.public.{table_name} " in s for s in oltp_stmts
            ), f"No OLTP DELETE used fqn()'d path for {table_name}"
            # A hardcoded Delta-quoted three-part form must not leak in.
            assert not any(f"`dqx`.`public`.{table_name}" in s for s in oltp_stmts)

    def test_oltp_failure_does_not_abort_remaining_tables(self, scheduler):
        """Parity with the Delta loop — one bad DELETE must not skip the others."""
        from databricks_labs_dqx_app.backend.services.scheduler_service import _OLTP_RETENTION_TABLES

        svc, mocks = scheduler
        # First OLTP DELETE blows up; the rest must still fire.
        call_count = {"n": 0}

        def flaky_execute(*_, **__):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("transient Lakebase failure")

        mocks.oltp.execute.side_effect = flaky_execute
        # Must not propagate — _run_retention catches per-table.
        svc._run_retention()
        # All OLTP tables were attempted despite the early failure.
        assert call_count["n"] == len(_OLTP_RETENTION_TABLES)


class TestRunRetentionExecutorContract:
    """``_run_retention`` is dialect-agnostic — it just calls
    :meth:`OltpExecutorProtocol.interval_days_expr` and trusts the
    executor to render the right form.

    The pre-Protocol design used a defensive
    ``getattr(oltp, "dialect", "delta") == "postgres"`` branch inside
    ``_run_retention``. That fallback no longer exists — every OLTP
    executor must implement the Protocol — so the contract worth
    locking down now is *the service must NOT inspect ``dialect``
    directly* and *must call ``interval_days_expr`` exactly once per
    retention sweep* (DRY: don't re-derive the literal per row).
    """

    def test_interval_literal_is_sourced_from_executor_not_a_dialect_branch(self, make_scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _OLTP_RETENTION_TABLES,
            _RETENTION_DAYS_DEFAULT,
        )

        # Configure the OLTP mock to return a clearly identifiable
        # sentinel interval — if any DELETE statement is missing the
        # sentinel, the service is hand-building the literal instead
        # of delegating to the executor.
        svc, mocks = make_scheduler(oltp_dialect="postgres", distinct_sql=True)
        mocks.oltp.interval_days_expr.side_effect = None
        mocks.oltp.interval_days_expr.return_value = "INTERVAL_SENTINEL_42"

        svc._run_retention()

        # interval_days_expr is called exactly once with the resolved
        # retention-day count (DRY: not re-derived per OLTP table).
        mocks.oltp.interval_days_expr.assert_called_once_with(_RETENTION_DAYS_DEFAULT)

        # Every OLTP DELETE statement embedded the sentinel literally.
        oltp_stmts = [c.args[0] for c in mocks.oltp.execute.call_args_list]
        assert len(oltp_stmts) == len(_OLTP_RETENTION_TABLES)
        for stmt in oltp_stmts:
            assert "INTERVAL_SENTINEL_42" in stmt, f"OLTP DELETE bypassed executor.interval_days_expr: {stmt!r}"
            # And the service must NOT have synthesised either dialect's
            # literal on its own — both would be a regression to the
            # branch-on-dialect pattern.
            assert f"INTERVAL '{_RETENTION_DAYS_DEFAULT} days'" not in stmt
            assert f"INTERVAL {_RETENTION_DAYS_DEFAULT} DAY" not in stmt
