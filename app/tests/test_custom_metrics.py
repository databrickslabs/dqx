"""Tests for the custom-metrics admin surface.

Two layers exercised:
* ``AppSettingsService.get_custom_metrics`` / ``save_custom_metrics`` —
  storage layer that round-trips the JSON list through the settings table.
* ``backend.routes.v1.config._validate_custom_metric_expr`` — request-side
  validation for the admin PUT endpoint.
"""

from __future__ import annotations

import json

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
        # ``save_setting`` now goes through SqlExecutor.upsert(); the
        # JSON payload lands on the value-cols dict.
        assert sql.upsert.called
        kwargs = sql.upsert.call_args.kwargs
        # Either positional (table, key_cols, value_cols) or keyword form.
        if "value_cols" in kwargs:
            payload = kwargs["value_cols"]["setting_value"]
        else:
            payload = sql.upsert.call_args.args[2]["setting_value"]
        assert "sum(x) as total" in payload
        assert "count(*) as n" in payload

    def test_save_empty_list_writes_empty_json(self, svc):
        s, sql = svc
        assert s.save_custom_metrics([]) == []
        assert sql.upsert.called


# ---------------------------------------------------------------------------
# AppSettingsService.get_config — WorkspaceConfig round-trip
# ---------------------------------------------------------------------------


class TestAppSettingsGetConfig:
    """``get_config`` deserialization must not depend on private blueprint APIs.

    The pre-refactor code called :func:`Installation._unmarshal_type`
    (leading underscore — private). The fix routes through Pydantic's
    public :class:`~pydantic.TypeAdapter` instead. These tests pin the
    new contract:

    1. **No private-API regressions.** ``Installation._unmarshal_type``
       must not be touched even if a stale import survives —
       :mod:`backend.services.app_settings_service` must not import
       ``Installation`` at all.
    2. **Round-trip fidelity.** A rich, nested ``WorkspaceConfig``
       written via ``as_dict()`` re-emerges from ``get_config()``
       byte-equivalent on the ``as_dict()`` side. The point of the
       fallback is to never lose user data.
    3. **Safe fallbacks.** Corrupt rows (bad JSON, wrong shape) yield
       an empty-default ``WorkspaceConfig`` instead of crashing the
       admin UI. A regression here means a single bad settings row
       takes down the whole config page.
    """

    @pytest.fixture
    def svc(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

        return AppSettingsService(sql_executor_mock), sql_executor_mock

    def test_module_does_not_import_private_blueprint_api(self):
        # Belt-and-suspenders against the reviewer-flagged anti-pattern.
        # If anyone re-imports Installation here, this test fails — a
        # regression would re-couple us to a private blueprint API.
        import importlib

        mod = importlib.import_module("databricks_labs_dqx_app.backend.services.app_settings_service")
        assert not hasattr(mod, "Installation"), (
            "app_settings_service must not import blueprint's Installation — "
            "deserialization goes through pydantic.TypeAdapter instead."
        )

    def test_returns_default_when_no_row(self, svc):
        from databricks.labs.dqx.config import WorkspaceConfig

        s, sql = svc
        sql.query.return_value = []
        result = s.get_config()
        assert isinstance(result, WorkspaceConfig)
        assert result.run_configs == []

    def test_round_trips_a_rich_nested_config_via_pydantic(self, svc):
        """The critical contract: nothing on the as_dict() round-trip is lost.

        Any future schema change in WorkspaceConfig must continue
        working without touching THIS service's code — that's the whole
        point of delegating to TypeAdapter rather than hand-rolling
        ``dataclasses.fields + getattr``.
        """
        from databricks.labs.dqx.config import (
            AnomalyConfig,
            ExtraParams,
            InputConfig,
            LLMConfig,
            LLMModelConfig,
            OutputConfig,
            ProfilerConfig,
            RunConfig,
            WorkspaceConfig,
        )

        s, sql = svc
        original = WorkspaceConfig(
            run_configs=[
                RunConfig(
                    name="prod",
                    input_config=InputConfig(location="c.s.src"),
                    output_config=OutputConfig(location="c.s.out"),
                    quarantine_config=OutputConfig(location="c.s.q"),
                    profiler_config=ProfilerConfig(),
                    checks_location="checks.yml",
                    warehouse_id="wh-prod",
                    reference_tables={"r1": InputConfig(location="c.s.ref")},
                    custom_check_functions={"fn": "sql"},
                    anomaly_config=AnomalyConfig(),
                )
            ],
            log_level="DEBUG",
            serverless_clusters=False,
            extra_params=ExtraParams(
                result_column_names={"flag": "is_bad"},
                user_metadata={"team": "data"},
                suppress_skipped=True,
                variables={},
            ),
            profiler_override_clusters={"a": "c1"},
            quality_checker_override_clusters={"b": "c2"},
            e2e_override_clusters={},
            anomaly_override_clusters={},
            profiler_spark_conf={"spark.x": "1"},
            quality_checker_spark_conf={},
            e2e_spark_conf={},
            anomaly_spark_conf={},
            custom_metrics=["count(*) AS n"],
            llm_config=LLMConfig(
                model=LLMModelConfig(model_name="m", api_key="k", api_base="https://example.cloud.databricks.com")
            ),
        )
        sql.query.return_value = [(json.dumps(original.as_dict()),)]

        loaded = s.get_config()

        assert isinstance(loaded, WorkspaceConfig)
        # The exact dict shape must match — every nested
        # dataclass (RunConfig, ExtraParams, LLMConfig.model, etc.)
        # round-trips back to its concrete type, not to a dict.
        assert loaded.as_dict() == original.as_dict()
        # Spot-check that the result is fully-typed (not bare dicts).
        assert isinstance(loaded.run_configs[0], RunConfig)
        assert isinstance(loaded.run_configs[0].input_config, InputConfig)
        assert isinstance(loaded.extra_params, ExtraParams)
        assert isinstance(loaded.llm_config, LLMConfig)
        assert isinstance(loaded.llm_config.model, LLMModelConfig)

    def test_invalid_json_falls_back_to_default(self, svc, caplog):
        from databricks.labs.dqx.config import WorkspaceConfig

        s, sql = svc
        sql.query.return_value = [("{this is not valid json",)]
        with caplog.at_level("WARNING"):
            result = s.get_config()
        assert isinstance(result, WorkspaceConfig)
        assert result.run_configs == []
        # Operator-visible signal — a regression that swallows the warning
        # silently would let corrupt rows hide indefinitely.
        assert any("not valid JSON" in r.getMessage() for r in caplog.records)

    def test_schema_mismatch_falls_back_to_default(self, svc, caplog):
        """ValidationError path: well-formed JSON but wrong shape."""
        from databricks.labs.dqx.config import WorkspaceConfig

        s, sql = svc
        # ``run_configs`` must be a list of RunConfig dicts; a bare
        # string will trip Pydantic's ValidationError.
        sql.query.return_value = [(json.dumps({"run_configs": "definitely-not-a-list"}),)]
        with caplog.at_level("WARNING"):
            result = s.get_config()
        assert isinstance(result, WorkspaceConfig)
        assert result.run_configs == []
        assert any("schema validation" in r.getMessage() for r in caplog.records)


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
    def scheduler(self, make_scheduler):
        # Built via the real ``SchedulerService(...)`` constructor —
        # see :func:`make_scheduler` in conftest.py for the rationale.
        # ``_load_custom_metrics`` reads through ``self._oltp_sql.query``,
        # which the factory exposes as ``mocks.oltp``.
        return make_scheduler()

    def test_returns_empty_when_no_rows(self, scheduler):
        svc, mocks = scheduler
        mocks.oltp.query.return_value = []
        assert svc._load_custom_metrics() == []

    def test_returns_empty_when_value_is_null(self, scheduler):
        svc, mocks = scheduler
        mocks.oltp.query.return_value = [(None,)]
        assert svc._load_custom_metrics() == []

    def test_parses_json_list(self, scheduler):
        svc, mocks = scheduler
        mocks.oltp.query.return_value = [(json.dumps(["sum(x) as total"]),)]
        assert svc._load_custom_metrics() == ["sum(x) as total"]

    def test_swallows_query_exception(self, scheduler):
        svc, mocks = scheduler
        mocks.oltp.query.side_effect = RuntimeError("warehouse offline")
        # Must not propagate — the scheduler tick keeps running.
        assert svc._load_custom_metrics() == []

    def test_filters_invalid_entries(self, scheduler):
        svc, mocks = scheduler
        mocks.oltp.query.return_value = [(json.dumps(["good as g", 42, "", None]),)]
        assert svc._load_custom_metrics() == ["good as g"]
