"""Unit tests for ``backend.routes.v1.home`` — the homepage stats endpoint.

Follows ``test_dq_score_route.py``'s TestClient convention: stand up a
minimal FastAPI app with just the router under test and override the
service/auth dependencies. Everything is served from the app DB (counts)
and the ``dq_score_cache`` 'global' row (P3.4) — the endpoint never
touches the warehouse, so there is nothing warehouse-shaped to mock.

The count methods themselves (``RegistryService.count`` /
``MonitoredTableService.count`` / ``DataProductService.count``) are pinned
against spec-bound executor mocks in ``TestCountMethods`` below.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_data_product_service,
    get_monitored_table_service,
    get_registry_service,
    get_score_cache_service,
    get_user_role,
)
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.score_cache_service import (
    GLOBAL_SCOPE_KEY,
    SCOPE_GLOBAL,
    CachedScore,
    ScoreCacheService,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


@pytest.fixture
def registry_mock() -> MagicMock:
    mock = create_autospec(RegistryService, instance=True)
    mock.count.return_value = 0
    return mock


@pytest.fixture
def monitored_tables_mock() -> MagicMock:
    mock = create_autospec(MonitoredTableService, instance=True)
    mock.count.return_value = 0
    return mock


@pytest.fixture
def products_mock() -> MagicMock:
    mock = create_autospec(DataProductService, instance=True)
    mock.count.return_value = 0
    return mock


@pytest.fixture
def score_cache_mock() -> MagicMock:
    mock = create_autospec(ScoreCacheService, instance=True)
    mock.get_many.return_value = {}
    return mock


@pytest.fixture
def client(registry_mock, monitored_tables_mock, products_mock, score_cache_mock) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.home import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/home")
    app.dependency_overrides[get_registry_service] = lambda: registry_mock
    app.dependency_overrides[get_monitored_table_service] = lambda: monitored_tables_mock
    app.dependency_overrides[get_data_product_service] = lambda: products_mock
    app.dependency_overrides[get_score_cache_service] = lambda: score_cache_mock
    app.dependency_overrides[get_user_role] = lambda: UserRole.VIEWER
    return TestClient(app)


class TestHomeStats:
    def test_composes_counts_and_cached_global_score(
        self, client, registry_mock, monitored_tables_mock, products_mock, score_cache_mock
    ):
        registry_mock.count.return_value = 12
        monitored_tables_mock.count.return_value = 5
        products_mock.count.return_value = 3
        score_cache_mock.get_many.return_value = {
            GLOBAL_SCOPE_KEY: CachedScore(
                score=0.9134,
                failed_tests=42,
                total_tests=500,
                computed_at="2026-07-10 09:00:00",
            )
        }

        resp = client.get("/api/v1/home/stats")

        assert resp.status_code == 200
        assert resp.json() == {
            "rule_count": 12,
            "monitored_table_count": 5,
            "table_space_count": 3,
            "score": 0.9134,
            "failed_tests": 42,
            "total_tests": 500,
            "computed_at": "2026-07-10 09:00:00",
        }
        score_cache_mock.get_many.assert_called_once_with(SCOPE_GLOBAL, [GLOBAL_SCOPE_KEY])

    def test_never_computed_cache_serves_null_score(self, client, registry_mock, score_cache_mock):
        registry_mock.count.return_value = 7
        score_cache_mock.get_many.return_value = {}

        resp = client.get("/api/v1/home/stats")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rule_count"] == 7
        assert body["score"] is None
        assert body["failed_tests"] is None
        assert body["total_tests"] is None
        assert body["computed_at"] is None

    def test_cached_row_with_null_score_still_serves_computed_at(self, client, score_cache_mock):
        # 'computed, nothing found yet' — distinguishable from 'never computed'.
        score_cache_mock.get_many.return_value = {
            GLOBAL_SCOPE_KEY: CachedScore(score=None, computed_at="2026-07-10 09:00:00")
        }

        resp = client.get("/api/v1/home/stats")

        assert resp.status_code == 200
        body = resp.json()
        assert body["score"] is None
        assert body["computed_at"] == "2026-07-10 09:00:00"

    def test_count_failure_maps_to_500(self, client, registry_mock):
        registry_mock.count.side_effect = RuntimeError("db down")
        resp = client.get("/api/v1/home/stats")
        assert resp.status_code == 500


class TestCountMethods:
    """Pin the COUNT(*) SQL and the stringified-cell coercion per service."""

    @staticmethod
    def _executor(count_cell: str | None) -> MagicMock:
        mock = create_autospec(SqlExecutor, instance=True)
        mock.dialect = "delta"
        mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
        mock.q.side_effect = lambda ident: f"`{ident}`"
        mock.ts_text.side_effect = lambda col: f"CAST({col} AS STRING)"
        mock.select_json_text.side_effect = lambda col: f"CAST({col} AS STRING)"
        mock.query.return_value = [[count_cell]] if count_cell is not None else []
        return mock

    def test_registry_service_counts_all_rules(self):
        sql = self._executor("12")
        service = RegistryService(sql)
        assert service.count() == 12
        stmt = sql.query.call_args[0][0]
        assert "COUNT(*)" in stmt
        assert "dqx_test.dqx_app_test.dq_rules" in stmt

    def test_monitored_table_service_counts_all_bindings(self):
        sql = self._executor("5")
        service = MonitoredTableService(sql, profiling_sql=self._executor(None))
        assert service.count() == 5
        stmt = sql.query.call_args[0][0]
        assert "COUNT(*)" in stmt
        assert "dqx_test.dqx_app_test.dq_monitored_tables" in stmt

    def test_data_product_service_counts_all_products(self):
        sql = self._executor("3")
        service = DataProductService(
            sql,
            monitored_tables=create_autospec(MonitoredTableService, instance=True),
            run_set_service=MagicMock(),
            binding_run_service=MagicMock(),
            version_service=MagicMock(),
            app_settings=MagicMock(),
        )
        assert service.count() == 3
        stmt = sql.query.call_args[0][0]
        assert "COUNT(*)" in stmt
        assert "dqx_test.dqx_app_test.dq_data_products" in stmt

    def test_empty_result_coerces_to_zero(self):
        sql = self._executor(None)
        service = RegistryService(sql)
        assert service.count() == 0
