"""Unit tests for ``backend.routes.v1.metrics``.

Exercises only the pure helpers (``_safe_int``, ``_parse_check_metrics``,
``_pivot_rows``, ``_check_metrics_to_error_breakdown``) — the FastAPI
endpoints themselves are thin wrappers around SQL execution and would
require a live warehouse to exercise end-to-end.
"""

from __future__ import annotations

import json

import pytest


@pytest.fixture
def metrics_module():
    from databricks_labs_dqx_app.backend.routes.v1 import metrics

    return metrics


class TestSafeInt:
    """``_safe_int`` is the universal coercion for stringified observation values."""

    def test_returns_none_for_blank_inputs(self, metrics_module):
        assert metrics_module._safe_int(None) is None
        assert metrics_module._safe_int("") is None

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("123", 123),
            ("123.0", 123),
            ("0", 0),
            (123, 123),
            (123.7, 123),  # float→int truncates as int(float()) does
        ],
    )
    def test_coerces_numeric_inputs(self, metrics_module, value, expected):
        assert metrics_module._safe_int(value) == expected

    @pytest.mark.parametrize("value", ["abc", "12abc", [], {}, object()])
    def test_returns_none_for_invalid(self, metrics_module, value):
        assert metrics_module._safe_int(value) is None


class TestParseCheckMetrics:
    """``check_metrics`` arrives as a JSON-encoded string from Spark Observation."""

    def test_empty_inputs_yield_empty_list(self, metrics_module):
        assert metrics_module._parse_check_metrics(None) == []
        assert metrics_module._parse_check_metrics("") == []
        assert metrics_module._parse_check_metrics("[]") == []

    def test_parses_well_formed_json_string(self, metrics_module):
        raw = json.dumps(
            [
                {"check_name": "not_null", "error_count": 5, "warning_count": 0},
                {"check_name": "in_range", "error_count": 0, "warning_count": 3},
            ]
        )
        out = metrics_module._parse_check_metrics(raw)
        assert len(out) == 2
        assert out[0].check_name == "not_null"
        assert out[0].error_count == 5
        assert out[0].warning_count == 0
        assert out[1].warning_count == 3

    def test_accepts_already_parsed_list(self, metrics_module):
        items = [{"check_name": "x", "error_count": 1, "warning_count": 2}]
        out = metrics_module._parse_check_metrics(items)
        assert len(out) == 1
        assert out[0].error_count == 1

    def test_invalid_json_yields_empty(self, metrics_module):
        assert metrics_module._parse_check_metrics("{not json}") == []

    def test_non_list_top_level_yields_empty(self, metrics_module):
        assert metrics_module._parse_check_metrics(json.dumps({"foo": "bar"})) == []

    def test_skips_non_dict_items(self, metrics_module):
        out = metrics_module._parse_check_metrics(
            json.dumps(["not a dict", {"check_name": "ok", "error_count": 1, "warning_count": 0}])
        )
        assert len(out) == 1
        assert out[0].check_name == "ok"

    def test_defaults_missing_fields(self, metrics_module):
        out = metrics_module._parse_check_metrics(json.dumps([{"check_name": "x"}]))
        assert len(out) == 1
        assert out[0].error_count == 0
        assert out[0].warning_count == 0

    def test_unknown_name_falls_back_to_unknown(self, metrics_module):
        out = metrics_module._parse_check_metrics(json.dumps([{"error_count": 4}]))
        assert out[0].check_name == "unknown"


class TestCheckMetricsToErrorBreakdown:
    """The legacy ``error_breakdown`` shape is recovered from per-check rows."""

    def test_empty_returns_none(self, metrics_module):
        assert metrics_module._check_metrics_to_error_breakdown([]) is None

    def test_drops_zero_count_rows(self, metrics_module):
        from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown

        out = metrics_module._check_metrics_to_error_breakdown(
            [
                CheckMetricBreakdown(check_name="zero", error_count=0, warning_count=0),
                CheckMetricBreakdown(check_name="ok", error_count=2, warning_count=1),
            ]
        )
        assert out is not None
        assert len(out) == 1
        assert out[0]["error"] == "ok"
        assert out[0]["count"] == 3

    def test_orders_by_count_desc_and_caps_at_20(self, metrics_module):
        from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown

        items = [CheckMetricBreakdown(check_name=f"c{i}", error_count=i, warning_count=0) for i in range(1, 25)]
        out = metrics_module._check_metrics_to_error_breakdown(items)
        assert out is not None
        assert len(out) == 20
        # Highest count first
        assert out[0]["count"] == 24
        assert out[-1]["count"] == 5


class TestPivotRows:
    """Long-format → wide-format pivot is the core transformation of the route."""

    def _row(self, *, run_id: str, name: str, value: str, **extra) -> dict:
        # Mirrors the column names yielded by the SELECT in get_metrics_trend.
        return {
            "run_id": run_id,
            "input_location": extra.get("input_location", "main.public.orders"),
            "metric_name": name,
            "metric_value": value,
            "rule_set_fingerprint": extra.get("rule_set_fingerprint", "fp-abc"),
            "run_type": extra.get("run_type", "scheduled"),
            "requesting_user": extra.get("requesting_user", "alice@example.com"),
            "created_at": extra.get("created_at", "2025-01-01T00:00:00Z"),
        }

    def test_empty_input_yields_empty_output(self, metrics_module):
        assert metrics_module._pivot_rows([]) == []

    def test_groups_metrics_by_run_id(self, metrics_module):
        rows = [
            self._row(run_id="r1", name="input_row_count", value="100"),
            self._row(run_id="r1", name="error_row_count", value="5"),
            self._row(run_id="r1", name="warning_row_count", value="2"),
            self._row(run_id="r1", name="valid_row_count", value="93"),
        ]
        out = metrics_module._pivot_rows(rows)
        assert len(out) == 1
        snap = out[0]
        assert snap.run_id == "r1"
        assert snap.total_rows == 100
        assert snap.error_row_count == 5
        assert snap.warning_row_count == 2
        assert snap.valid_rows == 93
        # invalid_rows is the legacy alias for error_row_count
        assert snap.invalid_rows == 5
        # 93/100 → 93.0 with 4-decimal rounding
        assert snap.pass_rate == pytest.approx(93.0)

    def test_preserves_input_order_across_runs(self, metrics_module):
        # Rows arrive in run_time DESC order — newest first.
        rows = [
            self._row(run_id="r_new", name="input_row_count", value="50"),
            self._row(run_id="r_old", name="input_row_count", value="80"),
        ]
        out = metrics_module._pivot_rows(rows)
        assert [s.run_id for s in out] == ["r_new", "r_old"]

    def test_pass_rate_handles_zero_total_safely(self, metrics_module):
        rows = [
            self._row(run_id="r1", name="input_row_count", value="0"),
            self._row(run_id="r1", name="valid_row_count", value="0"),
        ]
        out = metrics_module._pivot_rows(rows)
        assert out[0].pass_rate is None  # avoids division-by-zero

    def test_pass_rate_handles_missing_inputs(self, metrics_module):
        # No metrics at all for the run — pass_rate must not crash.
        rows = [self._row(run_id="r1", name="input_row_count", value=None)]
        out = metrics_module._pivot_rows(rows)
        assert out[0].total_rows is None
        assert out[0].pass_rate is None

    def test_check_metrics_round_trip(self, metrics_module):
        cm_payload = json.dumps([{"check_name": "not_null", "error_count": 3, "warning_count": 0}])
        rows = [
            self._row(run_id="r1", name="input_row_count", value="10"),
            self._row(run_id="r1", name="valid_row_count", value="7"),
            self._row(run_id="r1", name="check_metrics", value=cm_payload),
        ]
        out = metrics_module._pivot_rows(rows)
        snap = out[0]
        assert snap.check_metrics is not None
        assert snap.check_metrics[0].check_name == "not_null"
        assert snap.check_metrics[0].error_count == 3
        # Legacy error_breakdown is also derived from check_metrics so the
        # frontend list view keeps showing entries even after the migration.
        assert snap.error_breakdown is not None
        assert snap.error_breakdown[0]["error"] == "not_null"

    def test_custom_metrics_separated_from_builtins(self, metrics_module):
        rows = [
            self._row(run_id="r1", name="input_row_count", value="10"),
            self._row(run_id="r1", name="total_revenue", value="42500"),
            self._row(run_id="r1", name="distinct_customers", value="123"),
        ]
        out = metrics_module._pivot_rows(rows)
        assert out[0].total_rows == 10
        assert out[0].custom_metrics == {
            "total_revenue": "42500",
            "distinct_customers": "123",
        }

    def test_custom_metrics_none_when_only_builtins(self, metrics_module):
        rows = [self._row(run_id="r1", name="input_row_count", value="1")]
        assert metrics_module._pivot_rows(rows)[0].custom_metrics is None

    def test_first_rows_metadata_used_for_grouped_run(self, metrics_module):
        # All rows of a run share the same denormalised metadata; we only
        # need to read it from the first one.
        rows = [
            self._row(run_id="r1", name="input_row_count", value="10", run_type="scheduled"),
            self._row(run_id="r1", name="valid_row_count", value="9", run_type="dryrun"),  # ignored
        ]
        out = metrics_module._pivot_rows(rows)
        assert out[0].run_type == "scheduled"
