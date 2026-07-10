"""Unit tests for ``backend.services.score_view_service``.

The service owns the two SP-created UC objects that back the dq-score
endpoints (dqlake-parity architecture):

- ``v_dq_check_results`` — plain shaping view that explodes the
  long-format ``dq_metrics`` table into one row per
  (run_id, input_location, check_name);
- ``mv_dq_scores`` — a UC metric view (``WITH METRICS LANGUAGE YAML``)
  over the shaping view exposing ``failed_tests`` / ``total_tests`` /
  ``score`` measures.

Unit tests cannot reach a warehouse, so they pin the DDL *contract*:
object names, CREATE OR REPLACE idempotency, single-statement shape,
YAML validity, and the measure formulas' numerical agreement with the
pure-Python ``ScoreService`` specification. Live MEASURE() behaviour is
covered by the plan's manual verification pass.
"""

from __future__ import annotations

import yaml

import pytest

from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown
from databricks_labs_dqx_app.backend.services.score_service import ScoreService
from databricks_labs_dqx_app.backend.services.score_view_service import (
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
    ScoreViewService,
    metric_view_fqn,
)


@pytest.fixture
def svc(sql_executor_mock) -> ScoreViewService:
    sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
    return ScoreViewService(sql=sql_executor_mock)


def _yaml_body(metric_ddl: str) -> dict:
    """Extract and parse the $$-delimited YAML body of the metric-view DDL."""
    parts = metric_ddl.split("$$")
    assert len(parts) == 3, "metric view DDL must carry exactly one $$...$$ body"
    return yaml.safe_load(parts[1])


class TestObjectNames:
    def test_view_name_constants(self):
        assert SHAPING_VIEW_NAME == "v_dq_check_results"
        assert METRIC_VIEW_NAME == "mv_dq_scores"

    def test_metric_view_fqn_helper(self):
        assert metric_view_fqn("cat", "sch") == "cat.sch.mv_dq_scores"


class TestShapingViewDdl:
    def test_is_idempotent_create_or_replace(self, svc):
        assert svc.shaping_view_ddl().startswith("CREATE OR REPLACE VIEW ")

    def test_targets_shaping_view_and_reads_dq_metrics(self, svc):
        ddl = svc.shaping_view_ddl()
        assert f"`dqx_test`.`dqx_app_test`.{SHAPING_VIEW_NAME}" in ddl
        assert "`dqx_test`.`dqx_app_test`.dq_metrics" in ddl

    def test_explodes_check_metrics_json(self, svc):
        ddl = svc.shaping_view_ddl()
        assert "from_json" in ddl
        assert "check_metrics" in ddl
        assert "input_row_count" in ddl
        # No-check runs must still be present (placeholder row) so the
        # per-table endpoint can surface latest_run_id with a null score.
        assert "LATERAL VIEW OUTER" in ddl

    def test_flags_latest_run_per_input_location(self, svc):
        ddl = svc.shaping_view_ddl()
        assert "is_latest_run" in ddl
        assert "ROW_NUMBER() OVER (PARTITION BY input_location ORDER BY run_time DESC)" in ddl

    def test_is_a_single_statement(self, svc):
        # The Statement Execution API accepts one statement per call —
        # a ';' would have to go through a splitter and none exists here.
        assert ";" not in svc.shaping_view_ddl()

    def test_quotes_hyphenated_catalog(self, sql_executor_mock):
        sql_executor_mock.catalog = "prod-east"
        sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
        ddl = ScoreViewService(sql=sql_executor_mock).shaping_view_ddl()
        assert "`prod-east`" in ddl
        assert " prod-east." not in ddl


class TestMetricViewDdl:
    def test_is_idempotent_create_or_replace_with_metrics_yaml(self, svc):
        ddl = svc.metric_view_ddl()
        assert ddl.startswith("CREATE OR REPLACE VIEW ")
        assert f"`dqx_test`.`dqx_app_test`.{METRIC_VIEW_NAME}" in ddl
        assert "WITH METRICS" in ddl
        assert "LANGUAGE YAML" in ddl

    def test_is_a_single_statement(self, svc):
        assert ";" not in svc.metric_view_ddl()

    def test_yaml_body_parses_and_sources_the_shaping_view(self, svc):
        body = _yaml_body(svc.metric_view_ddl())
        assert str(body["version"]) == "1.1"
        assert body["source"] == f"`dqx_test`.`dqx_app_test`.{SHAPING_VIEW_NAME}"

    def test_yaml_dimensions(self, svc):
        body = _yaml_body(svc.metric_view_ddl())
        dims = {d["name"]: d["expr"] for d in body["dimensions"]}
        assert dims == {
            "input_location": "input_location",
            "run_id": "run_id",
            "run_time": "run_time",
            "is_latest_run": "is_latest_run",
            "check_name": "check_name",
        }

    def test_yaml_measures_match_the_score_formula(self, svc):
        body = _yaml_body(svc.metric_view_ddl())
        measures = {m["name"]: m["expr"] for m in body["measures"]}
        assert measures["failed_tests"] == "SUM(error_count + warning_count)"
        assert measures["total_tests"] == "SUM(input_row_count)"
        # TRY_DIVIDE (not /) so a zero/NULL denominator yields NULL —
        # the SQL equivalent of ScoreService's None-when-no-rows.
        assert measures["score"] == "1 - TRY_DIVIDE(SUM(error_count + warning_count), SUM(input_row_count))"


class TestEnsureViews:
    def test_creates_shaping_view_before_metric_view(self, svc, sql_executor_mock):
        svc.ensure_views()
        executed = [call.args[0] for call in sql_executor_mock.execute.call_args_list]
        assert executed == [svc.shaping_view_ddl(), svc.metric_view_ddl()]


class TestStartupWiring:
    """``backend.app._ensure_score_views`` — the lifespan step that owns the DDL."""

    def test_creates_both_views(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _ensure_score_views

        sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
        _ensure_score_views(sql_executor_mock)
        executed = [call.args[0] for call in sql_executor_mock.execute.call_args_list]
        assert len(executed) == 2
        assert SHAPING_VIEW_NAME in executed[0]
        assert METRIC_VIEW_NAME in executed[1]

    def test_is_best_effort_and_never_raises(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _ensure_score_views

        sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
        sql_executor_mock.execute.side_effect = RuntimeError("warehouse cannot create metric views")
        _ensure_score_views(sql_executor_mock)  # must not propagate


# ---------------------------------------------------------------------------
# Formula parity: the MEASURE() path must agree with ScoreService
# ---------------------------------------------------------------------------


def _simulate_measure_path(
    check_metrics: list[CheckMetricBreakdown], input_row_count: int
) -> tuple[float | None, int, int]:
    """Evaluate the metric-view measures over the shaping-view row shape.

    Mirrors exactly what the warehouse computes: the shaping view emits
    one row per check carrying (error_count, warning_count,
    input_row_count); the measures are ``SUM(error_count +
    warning_count)``, ``SUM(input_row_count)``, and ``1 -
    TRY_DIVIDE(failed, total)`` (NULL on zero/NULL denominator). A run
    with no checks emits a single all-NULL placeholder row, so every
    SUM is NULL (-> None/0 here).
    """
    rows = [(m.error_count, m.warning_count, input_row_count) for m in check_metrics]
    failed = sum(e + w for e, w, _ in rows) if rows else 0
    total = sum(irc for _, _, irc in rows) if rows else 0
    score = None if total <= 0 else 1.0 - failed / total
    return score, failed, total


class TestMeasureFormulaMatchesScoreService:
    """The SQL measure math and ScoreService.compute_table_score agree.

    ScoreService stays the formula's unit-tested specification (see
    test_score_service.py); this pins the metric-view translation to it
    on shared fixture data, including the approved filter approximation
    (total_tests = input_row_count per check, not filter-scoped) and
    the None-when-no-rows semantics.
    """

    @pytest.mark.parametrize(
        ("check_metrics", "input_row_count"),
        [
            ([CheckMetricBreakdown(check_name="a", error_count=10, warning_count=0)], 100),
            (
                [
                    CheckMetricBreakdown(check_name="a", error_count=10, warning_count=0),
                    CheckMetricBreakdown(check_name="b", error_count=20, warning_count=10),
                ],
                100,
            ),
            ([CheckMetricBreakdown(check_name="warn_only", error_count=0, warning_count=7)], 50),
            ([CheckMetricBreakdown(check_name="clean", error_count=0, warning_count=0)], 10),
            ([], 100),  # run with no check_metrics -> None
            ([CheckMetricBreakdown(check_name="a", error_count=1, warning_count=0)], 0),  # empty table -> None
            ([CheckMetricBreakdown(check_name="all_bad", error_count=100, warning_count=0)], 100),
        ],
    )
    def test_paths_agree(self, check_metrics: list[CheckMetricBreakdown], input_row_count: int):
        expected = ScoreService.compute_table_score(check_metrics, input_row_count)
        simulated_score, simulated_failed, simulated_total = _simulate_measure_path(check_metrics, input_row_count)
        if expected is None:
            assert simulated_score is None
        else:
            assert simulated_score == pytest.approx(expected)
            # And the supporting measures match the Python aggregates.
            assert simulated_failed == sum(m.error_count + m.warning_count for m in check_metrics)
            assert simulated_total == input_row_count * len(check_metrics)
