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
    get_data_product_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    get_user_role,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductDetail,
    DataProductMemberDetail,
    DataProductService,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

ACCESSIBLE_CATALOGS = frozenset({"main", "dev"})


@pytest.fixture
def sql_mock() -> MagicMock:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def data_products_mock() -> MagicMock:
    mock = create_autospec(DataProductService, instance=True)
    mock.get.return_value = None
    return mock


@pytest.fixture
def client(sql_mock, data_products_mock, app_config) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.dq_score import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/dq-score")
    app.dependency_overrides[get_sp_sql_executor] = lambda: sql_mock
    app.dependency_overrides[get_conf] = lambda: app_config
    app.dependency_overrides[get_user_catalog_names] = lambda: ACCESSIBLE_CATALOGS
    app.dependency_overrides[get_user_role] = lambda: UserRole.VIEWER
    app.dependency_overrides[get_data_product_service] = lambda: data_products_mock
    return TestClient(app)


def make_product_detail(product_id: str, table_fqns: list[str]) -> DataProductDetail:
    """Build a DataProductDetail whose members carry the given table FQNs."""
    members = [
        DataProductMemberDetail(
            id=f"m{i}",
            binding_id=f"b{i}",
            table_fqn=fqn,
            binding_status="approved",
            binding_version=1,
            pinned_version=None,
            rules_count=1,
            checks_count=1,
            runnable=True,
        )
        for i, fqn in enumerate(table_fqns)
    ]
    return DataProductDetail(product=MagicMock(name="DataProduct"), members=members, member_count=len(members))


def metrics_rows(run_id: str, row_count: int, error_count: int) -> list[dict[str, str]]:
    """Long-format dq_metrics rows for one run with a single check."""
    return [
        {"run_id": run_id, "metric_name": "input_row_count", "metric_value": str(row_count)},
        {
            "run_id": run_id,
            "metric_name": "check_metrics",
            "metric_value": json.dumps([{"check_name": "rule_a", "error_count": error_count, "warning_count": 0}]),
        },
    ]


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


class TestProductScore:
    def test_averages_member_tables(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = make_product_detail(
            "p1", ["main.sales.orders", "dev.sales.items"]
        )

        def per_table_rows(stmt: str) -> list[dict[str, str]]:
            if "'main.sales.orders'" in stmt:
                return metrics_rows("r1", 100, 10)  # score 0.9
            return metrics_rows("r2", 100, 30)  # score 0.7

        sql_mock.query_dicts.side_effect = per_table_rows
        resp = client.get("/api/v1/dq-score/product/p1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["product_id"] == "p1"
        assert body["score"] == pytest.approx(0.8)  # unweighted mean of 0.9 and 0.7
        assert [t["source_table_fqn"] for t in body["member_table_scores"]] == [
            "main.sales.orders",
            "dev.sales.items",
        ]
        assert [t["score"] for t in body["member_table_scores"]] == [pytest.approx(0.9), pytest.approx(0.7)]
        data_products_mock.get.assert_called_once_with("p1")

    def test_unknown_product_returns_404(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = None
        resp = client.get("/api/v1/dq-score/product/nope")
        assert resp.status_code == 404
        sql_mock.query_dicts.assert_not_called()

    def test_inaccessible_catalogs_are_filtered_not_403(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = make_product_detail(
            "p1", ["main.sales.orders", "secret.hr.salaries"]
        )
        sql_mock.query_dicts.return_value = metrics_rows("r1", 100, 10)
        resp = client.get("/api/v1/dq-score/product/p1")
        assert resp.status_code == 200
        body = resp.json()
        # The inaccessible member is silently excluded from both the mean
        # and the member breakdown.
        assert [t["source_table_fqn"] for t in body["member_table_scores"]] == ["main.sales.orders"]
        assert body["score"] == pytest.approx(0.9)
        for call in sql_mock.query_dicts.call_args_list:
            assert "secret.hr.salaries" not in call[0][0]

    def test_unscored_members_excluded_from_mean_but_listed(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = make_product_detail(
            "p1", ["main.sales.orders", "main.sales.new_table"]
        )

        def per_table_rows(stmt: str) -> list[dict[str, str]]:
            if "'main.sales.orders'" in stmt:
                return metrics_rows("r1", 100, 10)  # score 0.9
            return []  # never run -> no score

        sql_mock.query_dicts.side_effect = per_table_rows
        resp = client.get("/api/v1/dq-score/product/p1")
        body = resp.json()
        assert body["score"] == pytest.approx(0.9)  # mean over scored members only
        assert len(body["member_table_scores"]) == 2
        assert body["member_table_scores"][1]["score"] is None

    def test_product_with_no_scored_members_yields_null_score(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = make_product_detail("p1", ["main.sales.orders"])
        sql_mock.query_dicts.return_value = []
        resp = client.get("/api/v1/dq-score/product/p1")
        assert resp.status_code == 200
        assert resp.json()["score"] is None

    def test_member_lookup_failure_maps_to_500(self, client, data_products_mock):
        data_products_mock.get.side_effect = RuntimeError("lakebase down")
        resp = client.get("/api/v1/dq-score/product/p1")
        assert resp.status_code == 500

    def test_empty_product_yields_null_score(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = make_product_detail("p1", [])
        resp = client.get("/api/v1/dq-score/product/p1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["score"] is None
        assert body["member_table_scores"] == []
        sql_mock.query_dicts.assert_not_called()


class TestRbac:
    @pytest.mark.parametrize("operation_id", ["getTableScore", "getProductScore"])
    def test_route_is_gated_for_all_roles(self, operation_id):
        """Score reads are viewer-visible, like the metrics routes."""
        from databricks_labs_dqx_app.backend.routes.v1 import dq_score

        for route in dq_score.router.routes:
            if getattr(route, "operation_id", None) != operation_id:
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
        raise AssertionError(f"No require_role dependency found for {operation_id}")
