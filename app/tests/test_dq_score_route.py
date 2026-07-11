"""Unit tests for ``backend.routes.v1.dq_score``.

Follows ``test_check_functions_routes.py``'s TestClient convention:
stand up a minimal FastAPI app with just the router under test and
override the SQL/auth dependencies (the full ``backend.app`` lifespan
needs a live workspace). SQL execution is a spec-bound
``create_autospec(SqlExecutor)`` so no warehouse is touched.

The endpoint queries the ``mv_dq_scores`` UC metric view (see
``services.score_view_service``) via ``MEASURE()`` — the mocked
executor returns Statement-Execution-shaped string rows and these
tests pin the query text and the response mapping. The measure math
itself is pinned against ``ScoreService`` in
``test_score_view_service.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_apply_rules_service,
    get_conf,
    get_monitored_table_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    get_user_role,
)
from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.score_view_service import METRIC_VIEW_NAME
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

ACCESSIBLE_CATALOGS = frozenset({"main", "dev"})


@pytest.fixture
def sql_mock() -> MagicMock:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def apply_rules_mock() -> MagicMock:
    mock = create_autospec(ApplyRulesService, instance=True)
    mock.list_bindings_for_rule.return_value = []
    return mock


@pytest.fixture
def monitored_tables_mock() -> MagicMock:
    mock = create_autospec(MonitoredTableService, instance=True)
    mock.get.return_value = None
    return mock


@pytest.fixture
def client(sql_mock, apply_rules_mock, monitored_tables_mock, app_config) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.dq_score import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/dq-score")
    app.dependency_overrides[get_sp_sql_executor] = lambda: sql_mock
    app.dependency_overrides[get_conf] = lambda: app_config
    app.dependency_overrides[get_user_catalog_names] = lambda: ACCESSIBLE_CATALOGS
    app.dependency_overrides[get_user_role] = lambda: UserRole.VIEWER
    app.dependency_overrides[get_apply_rules_service] = lambda: apply_rules_mock
    app.dependency_overrides[get_monitored_table_service] = lambda: monitored_tables_mock
    return TestClient(app)


def make_applied_rule(applied_id: str, binding_id: str, rule_id: str = "r1") -> AppliedRule:
    return AppliedRule(id=applied_id, binding_id=binding_id, rule_id=rule_id)


def make_binding_detail(binding_id: str, table_fqn: str) -> MonitoredTableDetail:
    return MonitoredTableDetail(table=MonitoredTable(binding_id=binding_id, table_fqn=table_fqn))


def measure_row(
    run_id: str | None,
    score: float | None,
    failed_tests: int | None,
    total_tests: int | None,
) -> dict[str, str | None]:
    """One mv_dq_scores MEASURE() result row, Statement-Execution shaped.

    The Statement Execution API returns every value as a string (or
    None for SQL NULL) — mirror that so the response-mapping parsing is
    what's actually under test.
    """
    return {
        "run_id": run_id,
        "score": None if score is None else str(score),
        "failed_tests": None if failed_tests is None else str(failed_tests),
        "total_tests": None if total_tests is None else str(total_tests),
    }


class TestRuleScore:
    def test_reports_zero_applications(self, client, sql_mock, apply_rules_mock):
        apply_rules_mock.list_bindings_for_rule.return_value = []
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["rule_id"] == "r1"
        assert body["applied_to_count"] == 0
        assert body["overall_score"] is None
        assert body["per_table"] == []
        sql_mock.query_dicts.assert_not_called()
        apply_rules_mock.list_bindings_for_rule.assert_called_once_with("r1")

    def test_aggregates_across_tables(self, client, sql_mock, apply_rules_mock, monitored_tables_mock):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            make_applied_rule("ar1", "b1"),
            make_applied_rule("ar2", "b2"),
        ]
        details = {
            "b1": make_binding_detail("b1", "main.sales.orders"),
            "b2": make_binding_detail("b2", "dev.sales.items"),
        }
        monitored_tables_mock.get.side_effect = details.get

        def per_table_rows(stmt: str) -> list[dict[str, str | None]]:
            if "'main.sales.orders'" in stmt:
                return [measure_row("r1", 0.9, 10, 100)]
            return [measure_row("r2", 0.7, 30, 100)]

        sql_mock.query_dicts.side_effect = per_table_rows
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["applied_to_count"] == 2
        assert body["overall_score"] == pytest.approx(0.8)  # unweighted mean of 0.9 and 0.7
        assert [t["source_table_fqn"] for t in body["per_table"]] == ["main.sales.orders", "dev.sales.items"]
        assert [t["score"] for t in body["per_table"]] == [pytest.approx(0.9), pytest.approx(0.7)]

    def test_per_table_query_measures_the_metric_view_and_escapes_fqn(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock, app_config
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [make_applied_rule("ar1", "b1")]
        monitored_tables_mock.get.return_value = make_binding_detail("b1", "main.sales.orders")
        client.get("/api/v1/dq-score/rule/r1")
        stmt = sql_mock.query_dicts.call_args[0][0]
        # Catalog/schema are backtick-quoted (hyphenated-catalog support).
        assert f"`{app_config.catalog}`.`{app_config.schema_name}`.{METRIC_VIEW_NAME}" in stmt
        assert "'main.sales.orders'" in stmt
        assert "MEASURE(score)" in stmt
        assert "MEASURE(failed_tests)" in stmt
        assert "MEASURE(total_tests)" in stmt
        # Latest run within the selected mode set — NOT the view's
        # is_latest_run flag, which is computed over ALL runs regardless of
        # run_mode (a newer draft would otherwise blank the published score).
        assert "ORDER BY run_time DESC LIMIT 1" in stmt
        assert "is_latest_run" not in stmt

    def test_scores_default_to_the_latest_published_run(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [make_applied_rule("ar1", "b1")]
        monitored_tables_mock.get.return_value = make_binding_detail("b1", "main.sales.orders")
        sql_mock.query_dicts.return_value = [measure_row("r1", 0.9, 10, 100)]
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert "run_mode = 'published'" in stmt

    def test_include_drafts_drops_the_run_mode_filter(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [make_applied_rule("ar1", "b1")]
        monitored_tables_mock.get.return_value = make_binding_detail("b1", "main.sales.orders")
        sql_mock.query_dicts.return_value = [measure_row("r1", 0.9, 10, 100)]
        resp = client.get("/api/v1/dq-score/rule/r1", params={"include_drafts": "true"})
        assert resp.status_code == 200
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert "run_mode" not in stmt
        # The latest run overall is still what gets scored.
        assert "ORDER BY run_time DESC LIMIT 1" in stmt

    def test_applied_to_count_includes_inaccessible_tables(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        """The total application count is permission-independent.

        A rule applied to 3 tables is "applied to 3 tables" even when the
        viewer can only see 2 of them — the frontend disables the Results
        view on ``applied_to_count == 0``, so a viewer without catalog
        access must still see the rule as applied (with a filtered
        ``per_table``), never as "not applied anywhere".
        """
        apply_rules_mock.list_bindings_for_rule.return_value = [
            make_applied_rule("ar1", "b1"),
            make_applied_rule("ar2", "b2"),
            make_applied_rule("ar3", "b3"),
        ]
        details = {
            "b1": make_binding_detail("b1", "main.sales.orders"),
            "b2": make_binding_detail("b2", "dev.sales.items"),
            "b3": make_binding_detail("b3", "secret.hr.salaries"),
        }
        monitored_tables_mock.get.side_effect = details.get
        sql_mock.query_dicts.return_value = [measure_row("r1", 0.9, 10, 100)]
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        body = resp.json()
        # Total count reflects ALL applications, including the one the
        # viewer cannot access...
        assert body["applied_to_count"] == 3
        # ...while per_table is filtered to accessible catalogs only.
        assert [t["source_table_fqn"] for t in body["per_table"]] == ["main.sales.orders", "dev.sales.items"]
        for call in sql_mock.query_dicts.call_args_list:
            assert "secret.hr.salaries" not in call[0][0]

    def test_missing_binding_is_skipped_but_still_counted(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            make_applied_rule("ar1", "b1"),
            make_applied_rule("ar2", "b-deleted"),
        ]
        monitored_tables_mock.get.side_effect = lambda binding_id: (
            make_binding_detail("b1", "main.sales.orders") if binding_id == "b1" else None
        )
        sql_mock.query_dicts.return_value = [measure_row("r1", 0.9, 10, 100)]
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["applied_to_count"] == 2
        assert [t["source_table_fqn"] for t in body["per_table"]] == ["main.sales.orders"]

    def test_binding_lookup_error_is_skipped_not_500(self, client, sql_mock, apply_rules_mock, monitored_tables_mock):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            make_applied_rule("ar1", "b1"),
            make_applied_rule("ar2", "b-broken"),
        ]

        def get_binding(binding_id: str) -> MonitoredTableDetail:
            if binding_id == "b1":
                return make_binding_detail("b1", "main.sales.orders")
            raise RuntimeError("lakebase hiccup")

        monitored_tables_mock.get.side_effect = get_binding
        sql_mock.query_dicts.return_value = [measure_row("r1", 0.9, 10, 100)]
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["applied_to_count"] == 2
        assert [t["source_table_fqn"] for t in body["per_table"]] == ["main.sales.orders"]

    def test_duplicate_table_fqns_are_scored_once(self, client, sql_mock, apply_rules_mock, monitored_tables_mock):
        # Two applications on the SAME binding (different mapping hashes)
        # count as 2 applications but the table is scored once.
        apply_rules_mock.list_bindings_for_rule.return_value = [
            make_applied_rule("ar1", "b1"),
            make_applied_rule("ar2", "b1"),
        ]
        monitored_tables_mock.get.return_value = make_binding_detail("b1", "main.sales.orders")
        sql_mock.query_dicts.return_value = [measure_row("r1", 0.9, 10, 100)]
        resp = client.get("/api/v1/dq-score/rule/r1")
        body = resp.json()
        assert body["applied_to_count"] == 2
        assert [t["source_table_fqn"] for t in body["per_table"]] == ["main.sales.orders"]
        assert sql_mock.query_dicts.call_count == 1

    def test_unscored_tables_excluded_from_mean_but_listed(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            make_applied_rule("ar1", "b1"),
            make_applied_rule("ar2", "b2"),
        ]
        details = {
            "b1": make_binding_detail("b1", "main.sales.orders"),
            "b2": make_binding_detail("b2", "main.sales.new_table"),
        }
        monitored_tables_mock.get.side_effect = details.get

        def per_table_rows(stmt: str) -> list[dict[str, str | None]]:
            if "'main.sales.orders'" in stmt:
                return [measure_row("r1", 0.9, 10, 100)]
            return []  # never run -> no score

        sql_mock.query_dicts.side_effect = per_table_rows
        resp = client.get("/api/v1/dq-score/rule/r1")
        body = resp.json()
        assert body["overall_score"] == pytest.approx(0.9)
        assert len(body["per_table"]) == 2
        assert body["per_table"][1]["score"] is None

    def test_hyphenated_app_catalog_is_quoted_in_the_measure_query(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock, app_config
    ):
        client.app.dependency_overrides[get_conf] = lambda: app_config.model_copy(
            update={"catalog": "prod-east", "schema_name": "dqx-studio"}
        )
        apply_rules_mock.list_bindings_for_rule.return_value = [make_applied_rule("ar1", "b1")]
        monitored_tables_mock.get.return_value = make_binding_detail("b1", "main.sales.orders")
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert f"`prod-east`.`dqx-studio`.{METRIC_VIEW_NAME}" in stmt

    def test_binding_with_invalid_fqn_is_skipped_before_interpolation(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        """Defense-in-depth: app-DB-sourced FQNs are re-validated on read.

        ``escape_sql_string`` relies on ``validate_fqn`` having rejected
        backslashes, so a corrupted/adversarial binding row must never
        reach the SQL string literal — it is skipped, never 500."""
        apply_rules_mock.list_bindings_for_rule.return_value = [
            make_applied_rule("ar1", "b1"),
            make_applied_rule("ar2", "b-bad"),
        ]
        details = {
            "b1": make_binding_detail("b1", "main.sales.orders"),
            "b-bad": make_binding_detail("b-bad", "main.sales.evil\\"),
        }
        monitored_tables_mock.get.side_effect = details.get
        sql_mock.query_dicts.return_value = [measure_row("r1", 0.9, 10, 100)]
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 200
        body = resp.json()
        assert [t["source_table_fqn"] for t in body["per_table"]] == ["main.sales.orders"]
        for call in sql_mock.query_dicts.call_args_list:
            assert "evil" not in call[0][0]

    def test_applications_lookup_failure_maps_to_500(self, client, apply_rules_mock):
        apply_rules_mock.list_bindings_for_rule.side_effect = RuntimeError("lakebase down")
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 500

    def test_sql_failure_maps_to_500(self, client, sql_mock, apply_rules_mock, monitored_tables_mock):
        apply_rules_mock.list_bindings_for_rule.return_value = [make_applied_rule("ar1", "b1")]
        monitored_tables_mock.get.return_value = make_binding_detail("b1", "main.sales.orders")
        sql_mock.query_dicts.side_effect = RuntimeError("warehouse down")
        resp = client.get("/api/v1/dq-score/rule/r1")
        assert resp.status_code == 500


class TestRbac:
    def test_route_is_gated_for_all_roles(self):
        """Score reads are viewer-visible, like the metrics routes."""
        from databricks_labs_dqx_app.backend.routes.v1 import dq_score

        for route in dq_score.router.routes:
            if getattr(route, "operation_id", None) != "getRuleScore":
                continue
            for dep in route.dependencies:
                for cell in getattr(dep.dependency, "__closure__", None) or ():
                    val = cell.cell_contents
                    if isinstance(val, tuple) and val and all(isinstance(v, UserRole) for v in val):
                        assert set(val) == {
                            UserRole.ADMIN,
                            UserRole.RULE_APPROVER,
                            UserRole.RULE_AUTHOR,
                            UserRole.VIEWER,
                        }
                        return
        raise AssertionError("No require_role dependency found for getRuleScore")
