"""Unit tests for ``backend.services.score_view_service``.

The service owns the three SP-created UC objects that back the dq-score
endpoints (dqlake-parity architecture):

- ``v_dq_check_attribution`` — as-of-the-run rule attribution parsed
  from the frozen ``dq_validation_runs.checks_json`` rendered rule set
  (severity/dimension reserved tags, criticality, mapped columns,
  registry rule id) — one row per (run_id, source_table_fqn, check_name);
- ``v_dq_check_results`` — plain shaping view that explodes the
  long-format ``dq_metrics`` table into one row per
  (run_id, input_location, check_name), LEFT JOINed to the attribution
  view so every check row carries the metadata it ran with;
- ``mv_dq_scores`` — a UC metric view (``WITH METRICS LANGUAGE YAML``)
  over the shaping view exposing ``failed_tests`` / ``total_tests`` /
  ``score`` measures over attribution-aware dimensions.

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
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
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
        assert ATTRIBUTION_VIEW_NAME == "v_dq_check_attribution"
        assert SHAPING_VIEW_NAME == "v_dq_check_results"
        assert ASOF_VIEW_NAME == "v_dq_check_results_asof"
        assert METRIC_VIEW_NAME == "mv_dq_scores"

    def test_metric_view_fqn_helper_quotes_catalog_and_schema(self):
        # Quoted per part, consistent with the DDL side's sql.q convention.
        assert metric_view_fqn("cat", "sch") == "`cat`.`sch`.mv_dq_scores"

    def test_metric_view_fqn_helper_supports_hyphenated_catalog(self):
        # Read paths must stay parseable for exotic UC names end-to-end.
        assert metric_view_fqn("prod-east", "dqx-studio") == "`prod-east`.`dqx-studio`.mv_dq_scores"


class TestAttributionViewDdl:
    """``v_dq_check_attribution`` — as-of-run attribution from checks_json.

    The frozen runner (READ-ONLY ``app/tasks/.../runner.py``) writes the
    complete rendered rule set for every run into
    ``dq_validation_runs.checks_json``; this view is what makes the
    severity/dimension/columns attribution VERSION-ACCURATE — editing or
    renaming a tag today must never rewrite historical results.
    """

    def test_is_idempotent_create_or_replace(self, svc):
        assert svc.attribution_view_ddl().startswith("CREATE OR REPLACE VIEW ")

    def test_targets_attribution_view_and_reads_validation_runs(self, svc):
        ddl = svc.attribution_view_ddl()
        assert f"`dqx_test`.`dqx_app_test`.{ATTRIBUTION_VIEW_NAME}" in ddl
        assert "`dqx_test`.`dqx_app_test`.dq_validation_runs" in ddl

    def test_is_a_single_statement(self, svc):
        assert ";" not in svc.attribution_view_ddl()

    def test_parses_the_materializer_rendered_check_shape(self, svc):
        # The from_json schema must address exactly the paths the
        # materializer renders (see test_render_check_* fixtures):
        # name / criticality / check.arguments.column|columns /
        # user_metadata (string map with the reserved tags).
        ddl = svc.attribution_view_ddl()
        assert "from_json" in ddl
        assert "name: STRING" in ddl
        assert "criticality: STRING" in ddl
        assert "column: STRING" in ddl
        assert "columns: ARRAY<STRING>" in ddl
        assert "user_metadata: MAP<STRING, STRING>" in ddl

    def test_extracts_reserved_tags_and_registry_rule_id(self, svc):
        ddl = svc.attribution_view_ddl()
        assert "user_metadata['severity'] AS severity" in ddl
        assert "user_metadata['dimension'] AS dimension" in ddl
        assert "user_metadata['registry_rule_id'] AS registry_rule_id" in ddl

    def test_columns_stay_an_array_merging_column_and_columns(self, svc):
        # A single-column check renders arguments.column, a multi-column
        # one arguments.columns — the view exposes ONE array column.
        # The COALESCE also falls back to user_metadata['mapped_columns']
        # for sql_query checks (see test_attribution_view_columns_fall_back_*).
        ddl = svc.attribution_view_ddl()
        assert "COALESCE(" in ddl
        assert "arg_columns" in ddl
        assert "CASE WHEN arg_column IS NOT NULL THEN array(arg_column) END" in ddl

    def test_only_rows_with_checks_json_participate(self, svc):
        # The app inserts a RUNNING lifecycle row without checks_json; the
        # runner appends the result row WITH it. Legacy runs may have none.
        ddl = svc.attribution_view_ddl()
        assert "checks_json IS NOT NULL" in ddl

    def test_dedupes_lifecycle_rows_and_duplicate_check_names(self, svc):
        # One attribution row per (run_id, table, check_name): latest
        # lifecycle row wins; duplicate names in one rendered set (should
        # not happen — names are unique per rule set) keep the first.
        ddl = svc.attribution_view_ddl()
        assert "ROW_NUMBER() OVER (PARTITION BY run_id, source_table_fqn ORDER BY created_at DESC)" in ddl
        assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id, source_table_fqn, check_name ORDER BY pos) = 1" in ddl

    def test_unnamed_checks_are_excluded(self, svc):
        # A check without a name gets a DQX-generated check name at run
        # time — it can never join back to the metrics row.
        assert "check_name IS NOT NULL" in svc.attribution_view_ddl()

    def test_attribution_view_exposes_rule_name_from_metadata(self, svc):
        ddl = svc.attribution_view_ddl()
        assert "user_metadata['name'] AS rule_name" in ddl

    def test_attribution_view_columns_fall_back_to_metadata_mapped_columns(self, svc):
        ddl = svc.attribution_view_ddl()
        # sql_query checks carry columns only in user_metadata['mapped_columns'];
        # the COALESCE must recover them so by-column populates for sql_query.
        assert "from_json(user_metadata['mapped_columns'], 'ARRAY<STRING>')" in ddl
        # existing arg-based sources still come first
        assert "arg_columns" in ddl and "arg_column" in ddl
        # Order matters: metadata fallback must come AFTER the argument-based sources.
        assert ddl.index("arg_columns") < ddl.index("from_json(user_metadata['mapped_columns']")


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

    def test_left_joins_as_of_run_attribution(self, svc):
        # LEFT (not INNER): a legacy run with NULL checks_json still keeps
        # its check rows — they land in the untagged (NULL) bucket.
        ddl = svc.shaping_view_ddl()
        assert f"LEFT JOIN `dqx_test`.`dqx_app_test`.{ATTRIBUTION_VIEW_NAME} a" in ddl
        assert "ON a.run_id = e.run_id" in ddl
        assert "AND a.source_table_fqn = e.input_location" in ddl
        assert "AND a.check_name = e.check_name" in ddl

    def test_carries_attribution_columns(self, svc):
        ddl = svc.shaping_view_ddl()
        assert "a.criticality" in ddl
        assert "a.severity" in ddl
        assert "a.dimension" in ddl
        assert "a.registry_rule_id" in ddl
        assert "a.columns" in ddl  # stays an ARRAY<STRING> column

    def test_reads_run_provenance_tags_from_the_run_level_metadata_map(self, svc):
        # dq_metrics is long-format: the run-level user_metadata map repeats
        # per metric row, so MAX over the grouped rows picks it from any row.
        ddl = svc.shaping_view_ddl()
        assert "MAX(user_metadata['run_mode']) AS run_mode_tag" in ddl
        assert "MAX(user_metadata['binding_version']) AS binding_version_tag" in ddl

    def test_run_mode_tag_wins_and_untagged_legacy_runs_are_published(self, svc):
        # Untagged (legacy) runs classify as 'published', full stop — the
        # draft concept did not exist when they ran, and preview runs never
        # persist metrics. A run_type-based heuristic was tried and
        # reverted: pre-tag app runs were ALL submitted task_type='dryrun'
        # (only promoted to 'scheduled' when sample_size == 0), so it
        # reclassified the entire pre-upgrade run history as drafts and hid
        # it under the endpoints' published-only default.
        ddl = svc.shaping_view_ddl()
        assert "COALESCE(e.run_mode_tag, 'published') AS run_mode" in ddl
        # No run_type fallback: the dq_validation_runs join existed only
        # for the reverted heuristic and must stay gone.
        assert "run_type" not in ddl
        assert "dq_validation_runs" not in ddl

    def test_binding_version_is_tag_only_and_cast_to_int(self, svc):
        # No heuristic for binding_version — legacy runs keep NULL.
        ddl = svc.shaping_view_ddl()
        assert "TRY_CAST(e.binding_version_tag AS INT) AS binding_version" in ddl


class TestAsofViewDdl:
    """``v_dq_check_results_asof`` — the table-agnostic AS-OF expansion that
    moved the carry-forward consolidation out of Python and into the UC
    view layer. For every distinct run instant per read scope, each table
    with a run at-or-before it repeats its latest such run's check rows.
    """

    def test_is_idempotent_create_or_replace(self, svc):
        assert svc.asof_view_ddl().startswith("CREATE OR REPLACE VIEW ")

    def test_targets_asof_view_and_reads_the_shaping_view(self, svc):
        ddl = svc.asof_view_ddl()
        assert f"`dqx_test`.`dqx_app_test`.{ASOF_VIEW_NAME}" in ddl
        assert f"`dqx_test`.`dqx_app_test`.{SHAPING_VIEW_NAME}" in ddl

    def test_is_a_single_statement(self, svc):
        assert ";" not in svc.asof_view_ddl()

    def test_builds_two_partitions_published_runs_land_in_both(self, svc):
        # The include_drafts discriminator: the false partition is built
        # over published runs only; the true partition over ALL runs. A
        # published run therefore joins both scopes; a draft only the
        # drafts-inclusive one. Post-hoc run_mode filtering CANNOT
        # replace this (it would leave draft instants in the published
        # series and drop tables whose latest run is a draft).
        ddl = svc.asof_view_ddl()
        assert "SELECT explode(array(false, true)) AS include_drafts" in ddl
        assert "ON s.include_drafts OR r.run_mode = 'published'" in ddl

    def test_instants_are_per_scope_run_times(self, svc):
        ddl = svc.asof_view_ddl()
        assert "SELECT DISTINCT include_drafts, run_time AS as_of_time FROM scoped" in ddl

    def test_picks_each_tables_latest_run_at_or_before_every_instant(self, svc):
        # The as-of window: latest run per (scope, instant, table), rows
        # kept only for the picked run.
        ddl = svc.asof_view_ddl()
        assert "PARTITION BY i.include_drafts, i.as_of_time, s.input_location" in ddl
        assert "ORDER BY s.run_time DESC" in ddl
        assert "s.run_time <= i.as_of_time" in ddl
        assert ddl.rstrip().endswith("WHERE a.rn = 1")

    def test_joins_carried_run_rows_back_at_check_grain(self, svc):
        ddl = svc.asof_view_ddl()
        assert "ON c.run_id = a.run_id AND c.input_location = a.input_location" in ddl

    def test_emits_as_of_time_plus_the_shaping_row_shape(self, svc):
        ddl = svc.asof_view_ddl()
        for column in (
            "a.include_drafts",
            "a.as_of_time",
            "c.run_id",
            "c.input_location",
            "c.run_time",
            "c.check_name",
            "c.error_count",
            "c.warning_count",
            "c.input_row_count",
            "c.run_mode",
            "c.severity",
            "c.dimension",
            "c.registry_rule_id",
            "c.columns",
        ):
            assert column in ddl, column

    def test_null_run_times_never_enter_the_expansion(self, svc):
        assert "WHERE run_time IS NOT NULL" in svc.asof_view_ddl()

    def test_quotes_hyphenated_catalog(self, sql_executor_mock):
        sql_executor_mock.catalog = "prod-east"
        sql_executor_mock.schema = "dqx-studio"
        sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
        ddl = ScoreViewService(sql=sql_executor_mock).asof_view_ddl()
        assert f"`prod-east`.`dqx-studio`.{ASOF_VIEW_NAME}" in ddl


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
            # Run provenance ('draft' | 'published') — the stamped tag,
            # untagged legacy runs resolved to 'published' in the
            # shaping view.
            "run_mode": "run_mode",
            "check_name": "check_name",
            # As-of-run attribution dimensions (frozen at materialization
            # time in checks_json — never rewritten by later tag edits).
            "severity": "severity",
            "dimension": "dimension",
            "criticality": "criticality",
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
    def test_creates_attribution_shaping_asof_then_metric_view(self, svc, sql_executor_mock):
        svc.ensure_views()
        executed = [call.args[0] for call in sql_executor_mock.execute.call_args_list]
        assert executed == [
            svc.attribution_view_ddl(),
            svc.shaping_view_ddl(),
            svc.asof_view_ddl(),
            svc.metric_view_ddl(),
        ]


class TestStartupWiring:
    """``backend.app._ensure_score_views`` — the lifespan step that owns the DDL."""

    def test_creates_all_four_views(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _ensure_score_views

        sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
        _ensure_score_views(sql_executor_mock)
        executed = [call.args[0] for call in sql_executor_mock.execute.call_args_list]
        assert len(executed) == 4
        assert ATTRIBUTION_VIEW_NAME in executed[0]
        assert SHAPING_VIEW_NAME in executed[1]
        assert ASOF_VIEW_NAME in executed[2]
        assert METRIC_VIEW_NAME in executed[3]

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


# ---------------------------------------------------------------------------
# Run-mode resolution: the COALESCE's intended truth table
# ---------------------------------------------------------------------------


def _simulate_run_mode_resolution(run_mode_tag: str | None) -> str:
    """Evaluate the shaping view's run_mode COALESCE over one run.

    Mirrors the SQL exactly: the stamped run-level tag wins; untagged
    (legacy) runs classify as published. No run_type heuristic — pre-tag
    app runs were ALL submitted task_type='dryrun' (promoted to
    'scheduled' only when sample_size == 0), so any run_type-based
    reclassification hides the entire pre-upgrade run history under the
    endpoints' published-only default.
    """
    if run_mode_tag is not None:
        return run_mode_tag
    return "published"


class TestRunModeResolutionTruthTable:
    @pytest.mark.parametrize(
        ("run_mode_tag", "expected"),
        [
            # The stamped tag always wins.
            ("draft", "draft"),
            ("published", "published"),
            # Untagged (legacy) runs: the draft concept didn't exist when
            # they ran, and preview runs never persist — published.
            (None, "published"),
        ],
    )
    def test_resolution(self, run_mode_tag: str | None, expected: str):
        assert _simulate_run_mode_resolution(run_mode_tag) == expected


# ---------------------------------------------------------------------------
# Attribution contract: the from_json schema vs the materializer's real output
# ---------------------------------------------------------------------------


def _simulate_attribution_extraction(check: dict) -> dict[str, object]:
    """Evaluate the attribution view's per-check extraction over one check dict.

    Mirrors what the warehouse computes from ``checks_json`` via the
    from_json schema: name / criticality at the top level, the reserved
    tags + registry_rule_id out of the ``user_metadata`` string map, and
    the merged column array from ``check.arguments.column|columns``.
    """
    metadata = check.get("user_metadata") or {}
    arguments = (check.get("check") or {}).get("arguments") or {}
    arg_column = arguments.get("column") if isinstance(arguments.get("column"), str) else None
    arg_columns = arguments.get("columns") if isinstance(arguments.get("columns"), list) else None
    columns = arg_columns if arg_columns is not None else ([arg_column] if arg_column is not None else None)
    return {
        "check_name": check.get("name"),
        "criticality": check.get("criticality"),
        "severity": metadata.get("severity"),
        "dimension": metadata.get("dimension"),
        "registry_rule_id": metadata.get("registry_rule_id"),
        "columns": columns,
    }


class TestChecksJsonAttributionContract:
    """A REALISTIC rendered check (the materializer's own ``render_check``
    output — exactly what the frozen runner json-dumps into
    ``dq_validation_runs.checks_json``) must expose every path the
    attribution view's from_json schema addresses, with string-typed
    values so the MAP<STRING, STRING> cast holds."""

    def _rendered_check(self) -> dict:
        from unittest.mock import create_autospec

        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition, RuleVersion
        from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
        from databricks_labs_dqx_app.backend.services.materializer import render_check

        app_settings = create_autospec(AppSettingsService, instance=True)
        app_settings.get_label_definitions.return_value = []
        definition = RuleDefinition.model_validate(
            {
                "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
                "slots": [{"name": "column", "family": "any", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        )
        version = RuleVersion(
            rule_id="r1",
            version=1,
            definition=definition,
            polarity=None,
            user_metadata={"name": "Not Null Check", "dimension": "Completeness", "severity": "High"},
        )
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=app_settings,
        )
        return check

    def test_rendered_check_yields_full_attribution(self):
        extracted = _simulate_attribution_extraction(self._rendered_check())
        assert extracted == {
            "check_name": "Not Null Check",
            "criticality": "error",
            "severity": "High",
            "dimension": "Completeness",
            "registry_rule_id": "r1",
            "columns": ["customer_id"],
        }

    def test_rendered_user_metadata_is_a_pure_string_map(self):
        # The view casts user_metadata to MAP<STRING, STRING>; a non-string
        # value would null the map and silently drop the run's tags.
        metadata = self._rendered_check()["user_metadata"]
        assert metadata and all(isinstance(k, str) and isinstance(v, str) for k, v in metadata.items())

    def test_untagged_hand_authored_check_degrades_to_null_attribution(self):
        # A hand-authored (non-registry) check has no user_metadata at all
        # — every attribution field must come back None (untagged bucket).
        extracted = _simulate_attribution_extraction(
            {"criticality": "warn", "check": {"function": "sql_expression", "arguments": {"expression": "a > 0"}}}
        )
        assert extracted["check_name"] is None
        assert extracted["severity"] is None
        assert extracted["dimension"] is None
        assert extracted["registry_rule_id"] is None
        assert extracted["columns"] is None
