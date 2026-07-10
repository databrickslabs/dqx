"""Unit tests for ``backend.routes.v1.dq_score``.

Follows ``test_check_functions_routes.py``'s TestClient convention:
stand up a minimal FastAPI app with just the router under test and
override the SQL/auth dependencies (the full ``backend.app`` lifespan
needs a live workspace). SQL execution is a spec-bound
``create_autospec(SqlExecutor)`` so no warehouse is touched.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_conf,
    get_sp_sql_executor,
    get_user_catalog_names,
    get_user_role,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

ACCESSIBLE_CATALOGS = frozenset({"main", "dev"})


@pytest.fixture
def sql_mock() -> MagicMock:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def client(sql_mock, app_config) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.dq_score import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/dq-score")
    app.dependency_overrides[get_sp_sql_executor] = lambda: sql_mock
    app.dependency_overrides[get_conf] = lambda: app_config
    app.dependency_overrides[get_user_catalog_names] = lambda: ACCESSIBLE_CATALOGS
    app.dependency_overrides[get_user_role] = lambda: UserRole.VIEWER
    return TestClient(app)


class TestCatalogGating:
    def test_returns_403_for_inaccessible_catalog(self, client, sql_mock):
        resp = client.get("/api/v1/dq-score/table/secret.sales.orders")
        assert resp.status_code == 403
        # Gate fires before any SQL is issued.
        sql_mock.query_dicts.assert_not_called()

    def test_returns_400_for_malformed_fqn(self, client):
        resp = client.get("/api/v1/dq-score/table/not-a-three-part-name")
        assert resp.status_code == 400


class TestScoreComputation:
    def test_computes_from_latest_run(self, client, sql_mock):
        sql_mock.query_dicts.return_value = [
            {"run_id": "r1", "metric_name": "input_row_count", "metric_value": "100"},
            {
                "run_id": "r1",
                "metric_name": "check_metrics",
                "metric_value": json.dumps([{"check_name": "rule_a", "error_count": 10, "warning_count": 0}]),
            },
        ]
        resp = client.get("/api/v1/dq-score/table/main.sales.orders")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source_table_fqn"] == "main.sales.orders"
        assert body["score"] == pytest.approx(0.9)
        assert body["latest_run_id"] == "r1"
        assert body["total_tests"] == 100
        assert body["failed_tests"] == 10

    def test_multiple_checks_use_row_weighted_denominator(self, client, sql_mock):
        sql_mock.query_dicts.return_value = [
            {"run_id": "r9", "metric_name": "input_row_count", "metric_value": "100"},
            {
                "run_id": "r9",
                "metric_name": "check_metrics",
                "metric_value": json.dumps(
                    [
                        {"check_name": "rule_a", "error_count": 10, "warning_count": 0},
                        {"check_name": "rule_b", "error_count": 20, "warning_count": 10},
                    ]
                ),
            },
        ]
        resp = client.get("/api/v1/dq-score/table/main.sales.orders")
        body = resp.json()
        # failed=40, total=2 rules * 100 rows = 200 -> 0.8
        assert body["score"] == pytest.approx(0.8)
        assert body["total_tests"] == 200
        assert body["failed_tests"] == 40

    def test_no_runs_yields_null_score(self, client, sql_mock):
        sql_mock.query_dicts.return_value = []
        resp = client.get("/api/v1/dq-score/table/main.sales.orders")
        assert resp.status_code == 200
        body = resp.json()
        assert body["score"] is None
        assert body["latest_run_id"] is None
        assert body["total_tests"] == 0
        assert body["failed_tests"] == 0

    def test_run_without_check_metrics_yields_null_score(self, client, sql_mock):
        # Runs predating check_metrics emission still return the run id
        # but no score.
        sql_mock.query_dicts.return_value = [
            {"run_id": "r1", "metric_name": "input_row_count", "metric_value": "100"},
        ]
        resp = client.get("/api/v1/dq-score/table/main.sales.orders")
        body = resp.json()
        assert body["score"] is None
        assert body["latest_run_id"] == "r1"
        assert body["total_tests"] == 0

    def test_query_targets_metrics_table_and_escapes_fqn(self, client, sql_mock, app_config):
        client.get("/api/v1/dq-score/table/main.sales.orders")
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert f"{app_config.catalog}.{app_config.schema_name}.dq_metrics" in stmt
        assert "'main.sales.orders'" in stmt

    def test_sql_failure_maps_to_500(self, client, sql_mock):
        sql_mock.query_dicts.side_effect = RuntimeError("warehouse down")
        resp = client.get("/api/v1/dq-score/table/main.sales.orders")
        assert resp.status_code == 500


class TestRbac:
    def test_route_is_gated_for_all_roles(self):
        """Score reads are viewer-visible, like the metrics routes."""
        from databricks_labs_dqx_app.backend.routes.v1 import dq_score

        for route in dq_score.router.routes:
            if getattr(route, "operation_id", None) != "getTableScore":
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
        raise AssertionError("No require_role dependency found for getTableScore")
