"""Tests for the custom-metrics admin surface.

Two layers exercised:
* ``AppSettingsService.get_custom_metrics`` / ``save_custom_metrics`` —
  storage layer that round-trips the JSON list through the settings table.
* ``backend.routes.v1.config._validate_custom_metric_expr`` — request-side
  validation for the admin PUT endpoint.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# AppSettingsService — storage layer
# ---------------------------------------------------------------------------


class TestAppSettingsCustomMetrics:
    @pytest.fixture
    def svc(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

        # ``get_setting`` is what we'll stub on a per-test basis. The full
        # service still routes through ``self._sql.query`` / ``execute``.
        return AppSettingsService(sql_executor_mock), sql_executor_mock

    def test_returns_empty_when_setting_unset(self, svc):
        s, sql = svc
        sql.query.return_value = []
        assert s.get_custom_metrics() == []

    def test_returns_empty_when_blank_string(self, svc):
        s, sql = svc
        sql.query.return_value = [(None,)]
        assert s.get_custom_metrics() == []
        sql.query.return_value = [("",)]
        assert s.get_custom_metrics() == []

    def test_returns_parsed_list(self, svc):
        s, sql = svc
        payload = json.dumps(["sum(amount) as total_amount", "count(*) as row_count"])
        sql.query.return_value = [(payload,)]
        assert s.get_custom_metrics() == ["sum(amount) as total_amount", "count(*) as row_count"]

    def test_invalid_json_falls_back_to_empty(self, svc):
        s, sql = svc
        sql.query.return_value = [("{not json}",)]
        assert s.get_custom_metrics() == []

    def test_non_list_top_level_falls_back_to_empty(self, svc):
        s, sql = svc
        sql.query.return_value = [(json.dumps({"foo": "bar"}),)]
        assert s.get_custom_metrics() == []

    def test_filters_non_string_and_blank_entries(self, svc):
        s, sql = svc
        payload = json.dumps(["good as g", "", None, 42, "  "])
        sql.query.return_value = [(payload,)]
        assert s.get_custom_metrics() == ["good as g"]

    def test_save_normalises_and_persists(self, svc):
        s, sql = svc
        result = s.save_custom_metrics(["  sum(x) as total  ", None, "", "count(*) as n"])
        assert result == ["sum(x) as total", "count(*) as n"]
        # ``save_setting`` performs a MERGE — confirm the JSON payload
        # matches what we expect.
        assert sql.execute.called
        sql_arg = sql.execute.call_args.args[0]
        assert "sum(x) as total" in sql_arg
        assert "count(*) as n" in sql_arg

    def test_save_empty_list_writes_empty_json(self, svc):
        s, sql = svc
        assert s.save_custom_metrics([]) == []
        assert sql.execute.called


# ---------------------------------------------------------------------------
# Route-level validator
# ---------------------------------------------------------------------------


class TestCustomMetricExprValidator:
    @pytest.fixture
    def validate(self):
        from databricks_labs_dqx_app.backend.routes.v1.config import _validate_custom_metric_expr

        return _validate_custom_metric_expr

    def test_strips_and_returns_valid_expression(self, validate):
        assert validate("  sum(amount) as total_amount  ") == "sum(amount) as total_amount"

    def test_blank_raises_400(self, validate):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            validate("")
        assert exc.value.status_code == 400

    @pytest.mark.parametrize(
        "expr",
        [
            "sum(amount)",  # no alias
            "sum(amount) as 1bad",  # alias starts with digit
            "sum(amount) as bad-name",  # hyphen invalid in alias
        ],
    )
    def test_malformed_expressions_rejected(self, validate, expr):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            validate(expr)
        assert exc.value.status_code == 400

    def test_safe_aggregations_accepted(self, validate):
        good = [
            "count(*) as total",
            "sum(amount) as total_amount",
            "avg(price) as avg_price",
            "count(distinct id) as uniques",
            "count(case when status = 'OK' then 1 end) as ok_count",
        ]
        for expr in good:
            assert validate(expr) == expr

    def test_dml_keywords_rejected_when_safe_helper_available(self, validate, monkeypatch):
        # If the DQX helper is importable, dangerous keywords should be
        # rejected. We mock ``is_sql_query_safe`` to ensure deterministic
        # behaviour even when the underlying denylist grows.
        from databricks.labs.dqx import utils as dqx_utils
        from fastapi import HTTPException

        monkeypatch.setattr(dqx_utils, "is_sql_query_safe", lambda _: False)
        with pytest.raises(HTTPException) as exc:
            validate("drop table x as y")
        assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# Scheduler service — _load_custom_metrics swallows failures
# ---------------------------------------------------------------------------


class TestSchedulerLoadCustomMetrics:
    """The scheduler's hot path must never crash on a missing/blank setting."""

    @pytest.fixture
    def scheduler(self):
        from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService

        # __init__ wires SqlExecutor for itself; we bypass it and inject
        # the minimal attributes ``_load_custom_metrics`` actually touches.
        svc = SchedulerService.__new__(SchedulerService)
        svc._sql = MagicMock()
        svc._settings_table = "dqx.public.dq_app_settings"
        return svc

    def test_returns_empty_when_no_rows(self, scheduler):
        scheduler._sql.query.return_value = []
        assert scheduler._load_custom_metrics() == []

    def test_returns_empty_when_value_is_null(self, scheduler):
        scheduler._sql.query.return_value = [(None,)]
        assert scheduler._load_custom_metrics() == []

    def test_parses_json_list(self, scheduler):
        scheduler._sql.query.return_value = [(json.dumps(["sum(x) as total"]),)]
        assert scheduler._load_custom_metrics() == ["sum(x) as total"]

    def test_swallows_query_exception(self, scheduler):
        scheduler._sql.query.side_effect = RuntimeError("warehouse offline")
        # Must not propagate — the scheduler tick keeps running.
        assert scheduler._load_custom_metrics() == []

    def test_filters_invalid_entries(self, scheduler):
        scheduler._sql.query.return_value = [(json.dumps(["good as g", 42, "", None]),)]
        assert scheduler._load_custom_metrics() == ["good as g"]
