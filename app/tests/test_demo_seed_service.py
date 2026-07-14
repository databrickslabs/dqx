"""Unit tests for the DemoSeedService orchestrator.

All dependencies are mocked (``create_autospec`` / ``MagicMock``): these tests
assert the orchestration wiring (rules created, bindings approved, products
approved, terminal status written) rather than warehouse behaviour. The
``weeks=0`` short-circuit builds every governed object and writes a terminal
status but skips the validation-gate real-run and the weekly history loop, so
the whole pipeline is exercisable without a live SQL warehouse.
"""

import json

from unittest.mock import create_autospec, MagicMock

import pytest

from databricks_labs_dqx_app.backend.demo import manifest
from databricks_labs_dqx_app.backend.demo.seed_service import DemoSeedService
from databricks_labs_dqx_app.backend.demo.status import DemoStatusStore
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


def _svc(**over):
    from databricks.sdk import WorkspaceClient
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
    from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
    from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
    from databricks_labs_dqx_app.backend.services.materializer import Materializer
    from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
    from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
    from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
    from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
    from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService

    deps = dict(
        demo_sql=create_autospec(SqlExecutor, instance=True),
        app_sql=create_autospec(SqlExecutor, instance=True),
        oltp=MagicMock(),
        sp_ws=create_autospec(WorkspaceClient, instance=True),
        registry=create_autospec(RegistryService, instance=True),
        monitored_tables=create_autospec(MonitoredTableService, instance=True),
        apply_rules=create_autospec(ApplyRulesService, instance=True),
        materializer=create_autospec(Materializer, instance=True),
        rules_catalog=create_autospec(RulesCatalogService, instance=True),
        version_service=create_autospec(MonitoredTableVersionService, instance=True),
        data_products=create_autospec(DataProductService, instance=True),
        binding_run=create_autospec(BindingRunService, instance=True),
        score_cache=create_autospec(ScoreCacheService, instance=True),
        status=create_autospec(DemoStatusStore, instance=True),
    )
    deps.update(over)
    return DemoSeedService(**deps), deps


def test_run_creates_all_rules_and_writes_terminal_status():
    svc, deps = _svc()
    # registry returns a fake approved rule with an id for every create
    rule = MagicMock()
    rule.rule_id = "r1"
    deps["registry"].match_or_create_approved_rule.return_value = (rule, True)
    # make binding registration + run + score reads no-op-friendly
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    # short-circuit the wait+validate+weekly loop via a 0-week run for the unit test
    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    assert deps["registry"].match_or_create_approved_rule.call_count >= 10
    # terminal status written
    assert deps["status"].set.called
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state in {"succeeded", "failed"}


def test_wipe_first_calls_reset_service():
    reset = MagicMock()
    svc, deps = _svc(reset_service=reset)
    deps["registry"].match_or_create_approved_rule.return_value = (MagicMock(rule_id="r1"), True)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    svc.run(user_email="admin@example.com", wipe_first=True, weeks=0)
    reset.reset_all_data.assert_called_once()


def test_build_source_data_assigns_governed_column_tags_via_sdk():
    # Governed dotted-key tags (class.location, class.credit_card) can't be set
    # via ALTER TABLE SQL — they must go through the Entity Tag Assignments API.
    svc, deps = _svc()
    svc._build_source_data()

    create = deps["sp_ws"].entity_tag_assignments.create
    assert create.call_count == len(manifest.COLUMN_TAGS)

    from databricks.sdk.service.catalog import EntityTagAssignment

    seen: dict[str, EntityTagAssignment] = {}
    for call in create.call_args_list:
        assignment = call.args[0] if call.args else call.kwargs["tag_assignment"]
        assert isinstance(assignment, EntityTagAssignment)
        assert assignment.entity_type == "columns"
        seen[assignment.entity_name] = assignment

    for tag in manifest.COLUMN_TAGS:
        entity_name = f"dqx.{manifest.SOURCE_SCHEMA}.{tag.table}.{tag.column}"
        assert entity_name in seen
        assert seen[entity_name].tag_key == tag.tag  # the FULL dotted key
        assert "." in seen[entity_name].tag_key


def test_build_source_data_swallows_tag_assignment_errors():
    # A missing ASSIGN privilege / undefined governed tag must NOT abort the
    # ~1h seed — the failure is logged best-effort and seeding continues.
    svc, deps = _svc()
    deps["sp_ws"].entity_tag_assignments.create.side_effect = RuntimeError("no ASSIGN privilege")
    deps["registry"].match_or_create_approved_rule.return_value = (MagicMock(rule_id="r1"), True)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")

    # run(weeks=0) invokes _build_source_data; the raised error must be swallowed.
    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)

    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "succeeded"


def test_run_marks_failed_status_and_reraises_on_error():
    svc, deps = _svc()
    deps["registry"].match_or_create_approved_rule.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "failed"


def test_run_approves_bindings_and_products_not_relying_on_auto_publish():
    # The app has approvals enabled; the seeder must drive approval explicitly.
    svc, deps = _svc()
    deps["registry"].match_or_create_approved_rule.return_value = (MagicMock(rule_id="r1"), True)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    # bindings materialized + frozen (approved), products submitted + approved
    assert deps["materializer"].materialize_binding.called
    assert deps["version_service"].freeze_new_version.called
    assert deps["data_products"].approve.called


def test_current_rule_ids_returns_string_keyed_map_without_typeerror():
    # FIX 1 regression: RuleSpec is a frozen dataclass with dict fields, so it
    # is UNHASHABLE — the old code keyed the map by the RuleSpec object and
    # raised TypeError at runtime. The map must be keyed by the stable
    # RuleSpec.key string instead.
    svc, deps = _svc()
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _definition: MagicMock(rule_id="rid")

    resolved = svc._current_rule_ids()

    assert resolved  # every manifest rule resolved
    assert all(isinstance(k, str) for k in resolved)
    assert all(isinstance(v, str) for v in resolved.values())
    # keys are the manifest rule keys, never RuleSpec objects
    assert set(resolved) == {spec.key for spec in manifest.RULES}
    assert isinstance(manifest.RULES[0].key, str)


def _metrics_rows(input_rows: int, unique_failed: int):
    """dq_metrics rows for one run: input_row_count + a unique-check breakdown."""
    check_metrics = json.dumps(
        [{"check_name": manifest.RULES_BY_KEY["unique"].name, "error_count": unique_failed, "warning_count": 0}]
    )
    return [
        {"metric_name": "input_row_count", "metric_value": str(input_rows)},
        {"metric_name": "check_metrics", "metric_value": check_metrics},
    ]


def test_assert_no_misfire_raises_when_unique_count_outside_band():
    # FIX 2: a unique check whose failed-row count is outside UNIQUE_EXPECT_ROWS
    # is a misfire even below the catastrophic rate.
    svc, deps = _svc()
    _low, high = manifest.UNIQUE_EXPECT_ROWS
    out_of_band = high + 1
    input_rows = 50_000  # rate stays far below _MISFIRE_RATE
    deps["app_sql"].query_dicts.return_value = _metrics_rows(input_rows, out_of_band)

    with pytest.raises(RuntimeError, match="uniqueness"):
        svc._assert_no_misfire("customers", "run1")


def test_assert_no_misfire_passes_when_unique_count_inside_band():
    svc, deps = _svc()
    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2
    deps["app_sql"].query_dicts.return_value = _metrics_rows(50_000, in_band)

    # inside the band and below the catastrophic rate — must not raise
    svc._assert_no_misfire("customers", "run1")


def test_assert_no_misfire_logs_when_unique_check_name_missing(caplog):
    # FIX 2: when a binding has the uniqueness rule applied but no check with the
    # reserved unique-rule name appears in metrics, the band silently never
    # fires. That skip must be observable (logged), not passing mutely — but it
    # must NOT hard-fail (the catastrophic-rate gate still protects correctness).
    svc, deps = _svc()
    # products has the unique rule applied at week 0 (no lifecycle window), so a
    # run with NO unique check in its metrics is a loggable silent skip.
    deps["app_sql"].query_dicts.return_value = [
        {"metric_name": "input_row_count", "metric_value": "5000"},
        {
            "metric_name": "check_metrics",
            "metric_value": json.dumps([{"check_name": "some_other_check", "error_count": 5, "warning_count": 0}]),
        },
    ]
    with caplog.at_level("WARNING", logger="databricks_labs_dqx_app.backend.demo.seed_service"):
        svc._assert_no_misfire("products", "run1")  # must not raise
    assert "uniqueness rule applied" in caplog.text
    assert "band was skipped" in caplog.text


def test_weekly_trend_strips_polluting_now_history_but_keeps_cache_current():
    # BUG: the final "truthful now" refresh_all_for_tables updates dq_score_cache
    # but ScoreCacheService ALSO appends one un-re-dated dq_score_history row per
    # scope at real wall-clock now() — those clustered real-now points pollute the
    # back-dated weekly trend. The seed must (a) still call refresh_all_for_tables
    # so the current cache is truthful, and (b) issue a history cleanup that
    # deletes every history row appended after the shared "now" cutoff.
    svc, deps = _svc()
    deps["registry"].match_or_create_approved_rule.return_value = (MagicMock(rule_id="r1"), True)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    svc.run(user_email="admin@example.com", wipe_first=False, weeks=1)

    # (a) the current cache is still refreshed truthfully
    assert deps["score_cache"].refresh_all_for_tables.called

    # (b) a history cleanup deletes rows appended after the "now" cutoff, so no
    # un-re-dated real-now history row survives to pollute the trend
    executed = [call.args[0] for call in deps["oltp"].execute.call_args_list]
    cleanup = [s for s in executed if s.startswith("DELETE FROM") and "dq_score_history" in s and "computed_at >" in s]
    assert cleanup, "expected a dq_score_history post-cutoff cleanup DELETE"

    # terminal status succeeded
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "succeeded"


def test_validation_gate_deletes_gate_runs_from_both_tables():
    # FIX 1: the validation-gate runs execute at real "now" and are never
    # re-dated, so they must be deleted from dq_metrics AND dq_validation_runs
    # after the misfire assertions pass — otherwise a gate run can outrank the
    # re-dated (past) weekly runs in the score cache's latest-run selection.
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="gate-run")
    # wait-for-run sees SUCCESS; misfire read sees an in-band unique count.
    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    binding_map = {"customers": "b-customers", "orders": "b-orders"}
    svc._validation_gate(binding_map, "admin@example.com")

    executed = [call.args[0] for call in deps["app_sql"].execute.call_args_list]
    deletes = [sql for sql in executed if sql.startswith("DELETE FROM")]
    # one DELETE per gate run against each of the two tables
    assert any(s.startswith("DELETE FROM") and "dq_metrics" in s and "gate-run" in s for s in deletes)
    assert any(s.startswith("DELETE FROM") and "dq_validation_runs" in s and "gate-run" in s for s in deletes)
