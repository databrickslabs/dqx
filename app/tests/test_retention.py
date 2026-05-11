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

from unittest.mock import MagicMock

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
    """Scheduler must never crash on a missing / corrupt retention setting."""

    @pytest.fixture
    def scheduler(self):
        from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService

        # Bypass __init__ — only the attributes the resolver touches.
        svc = SchedulerService.__new__(SchedulerService)
        svc._oltp_sql = MagicMock()
        svc._sql = svc._oltp_sql
        svc._settings_table = "dqx.public.dq_app_settings"
        return svc

    def test_global_falls_back_to_default_when_unset(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import _RETENTION_DAYS_DEFAULT

        scheduler._oltp_sql.query.return_value = []
        assert scheduler._resolve_retention_days() == _RETENTION_DAYS_DEFAULT

    def test_quarantine_falls_back_to_its_own_default(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
        )

        scheduler._oltp_sql.query.return_value = []
        assert scheduler._resolve_quarantine_retention_days() == _QUARANTINE_RETENTION_DAYS_DEFAULT

    def test_quarantine_default_is_tighter_than_global(self):
        """Sanity check on the constants: quarantine (PII) ages out faster."""
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _RETENTION_DAYS_DEFAULT,
        )

        assert _QUARANTINE_RETENTION_DAYS_DEFAULT < _RETENTION_DAYS_DEFAULT

    def test_returns_persisted_value(self, scheduler):
        scheduler._oltp_sql.query.return_value = [("45",)]
        assert scheduler._resolve_retention_days() == 45

    def test_floor_protects_against_too_small_value(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import _RETENTION_DAYS_MIN

        scheduler._oltp_sql.query.return_value = [("1",)]
        # Stored value of 1 must be raised to the floor — never wipe inside the safety window.
        assert scheduler._resolve_retention_days() == _RETENTION_DAYS_MIN
        assert scheduler._resolve_quarantine_retention_days() == _RETENTION_DAYS_MIN

    def test_swallows_query_exception(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _RETENTION_DAYS_DEFAULT,
        )

        scheduler._oltp_sql.query.side_effect = RuntimeError("warehouse offline")
        # Must not propagate — sweep retries on the next tick.
        assert scheduler._resolve_retention_days() == _RETENTION_DAYS_DEFAULT
        assert scheduler._resolve_quarantine_retention_days() == _QUARANTINE_RETENTION_DAYS_DEFAULT

    def test_unparseable_value_returns_default(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import _RETENTION_DAYS_DEFAULT

        scheduler._oltp_sql.query.return_value = [("not-a-number",)]
        assert scheduler._resolve_retention_days() == _RETENTION_DAYS_DEFAULT


# ---------------------------------------------------------------------------
# Scheduler service — _run_retention applies the right cutoff per table
# ---------------------------------------------------------------------------


class TestRunRetentionUsesQuarantineCutoff:
    """The Delta sweep must use the quarantine-specific cutoff for that one table."""

    @pytest.fixture
    def scheduler(self):
        from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService

        svc = SchedulerService.__new__(SchedulerService)
        svc._sql = MagicMock()
        svc._oltp_sql = MagicMock()
        # ``_oltp_sql`` is also the one resolvers query for the
        # configured day values; stub a missing row so both resolvers
        # fall back to their respective defaults.
        svc._oltp_sql.query.return_value = []
        # ``dialect`` lookup happens in the OLTP loop.
        svc._oltp_sql.dialect = "delta"
        svc._settings_table = "dqx.public.dq_app_settings"
        svc._catalog = "dqx"
        svc._schema = "public"
        # ``_qualify_oltp`` is invoked for the OLTP DELETE loop.
        svc._qualify_oltp = lambda t: f"dqx.public.{t}"  # type: ignore[method-assign]
        return svc

    def test_quarantine_table_uses_its_own_default(self, scheduler):
        from databricks_labs_dqx_app.backend.services.scheduler_service import (
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
            _RETENTION_DAYS_DEFAULT,
        )

        scheduler._run_retention()

        # Collect every DELETE statement that actually fired.
        delta_stmts = [c.args[0] for c in scheduler._sql.execute.call_args_list]
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
