"""Tests for the compute routes (P22-B) — settings, listings, warehouse access/grant."""

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_obo_ws,
    get_setup_orchestrator,
    get_user_role,
)
from databricks_labs_dqx_app.backend.routes.v1.compute import (
    ComputeSettingsIn,
    GrantWarehouseAccessIn,
    JobsComputeModel,
    get_compute_settings,
    get_warehouse_access,
    grant_warehouse_access,
    list_clusters,
    list_warehouses,
    router,
    save_compute_settings,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService, WarehouseInfo, ClusterInfo
from databricks_labs_dqx_app.backend.setup.checks import ResourceCheckers
from databricks_labs_dqx_app.backend.setup.models import SetupStep, SetupStepId, StepState


@pytest.fixture
def app_settings(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.query.return_value = []
    return AppSettingsService(sql=sql_executor_mock)


@pytest.fixture
def compute_svc():
    return create_autospec(ComputeService, instance=True)


@pytest.fixture
def route_app_settings() -> MagicMock:
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_sql_warehouse_id.return_value = "previous-warehouse"
    settings.get_jobs_compute.return_value = {"kind": "serverless"}
    return settings


@pytest.fixture
def checkers() -> MagicMock:
    checker = create_autospec(ResourceCheckers, instance=True)
    checker.check_warehouse.return_value = SetupStep(id=SetupStepId.WAREHOUSE, state=StepState.PASSED)
    return checker


@pytest.fixture
def obo_ws() -> MagicMock:
    return MagicMock(name="obo_ws")


@pytest.fixture
def client(route_app_settings: MagicMock, checkers: MagicMock, obo_ws: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/compute")
    app.dependency_overrides[get_app_settings_service] = lambda: route_app_settings
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws
    app.dependency_overrides[get_setup_orchestrator] = lambda: SimpleNamespace(checkers=checkers)
    app.dependency_overrides[get_user_email] = lambda: "admin@x"
    app.dependency_overrides[get_user_role] = lambda: UserRole.ADMIN
    return TestClient(app)


@pytest.fixture
def unavailable_client(route_app_settings: MagicMock, obo_ws: MagicMock) -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/compute")
    app.dependency_overrides[get_app_settings_service] = lambda: route_app_settings
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws
    app.dependency_overrides[get_user_email] = lambda: "admin@x"
    app.dependency_overrides[get_user_role] = lambda: UserRole.ADMIN
    return TestClient(app, raise_server_exceptions=False)


class TestListings:
    @pytest.mark.asyncio
    async def test_list_warehouses_maps(self, compute_svc):
        async def _w(*, lister_ws):
            return [WarehouseInfo(id="w1", name="A", serverless=True, running=False)]

        compute_svc.list_warehouses_async.side_effect = _w
        result = await list_warehouses(compute_svc, MagicMock())
        assert result[0].id == "w1" and result[0].serverless is True

    @pytest.mark.asyncio
    async def test_list_warehouses_runs_under_obo(self, compute_svc):
        """The acting user's OBO client — not the app SP — drives listing."""
        captured: dict[str, object] = {}

        async def _w(*, lister_ws):
            captured["lister_ws"] = lister_ws
            return []

        compute_svc.list_warehouses_async.side_effect = _w
        obo = MagicMock(name="obo_ws")
        await list_warehouses(compute_svc, obo)
        assert captured["lister_ws"] is obo

    @pytest.mark.asyncio
    async def test_list_warehouses_degrades_to_empty(self, compute_svc):
        async def _boom(*, lister_ws):
            raise RuntimeError("boom")

        compute_svc.list_warehouses_async.side_effect = _boom
        assert await list_warehouses(compute_svc, MagicMock()) == []

    @pytest.mark.asyncio
    async def test_list_clusters_maps(self, compute_svc):
        async def _c(*, lister_ws):
            return [ClusterInfo(cluster_id="c1", cluster_name="B", state="RUNNING")]

        compute_svc.list_clusters_async.side_effect = _c
        result = await list_clusters(compute_svc, MagicMock())
        assert result[0].cluster_id == "c1"

    @pytest.mark.asyncio
    async def test_list_clusters_degrades_to_empty(self, compute_svc):
        async def _boom(*, lister_ws):
            raise RuntimeError("boom")

        compute_svc.list_clusters_async.side_effect = _boom
        assert await list_clusters(compute_svc, MagicMock()) == []


class TestSettings:
    def test_get_defaults(self, app_settings, sql_executor_mock, monkeypatch):
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "env-wh")
        result = get_compute_settings(app_settings)
        assert result.sql_warehouse_id == ""
        assert result.effective_warehouse_id == "env-wh"
        assert result.warehouse_is_override is False
        assert result.jobs_compute.kind == "serverless"

    @pytest.mark.asyncio
    async def test_save_requires_a_field(self, app_settings):
        with pytest.raises(HTTPException) as exc:
            await save_compute_settings(ComputeSettingsIn(), app_settings, "admin@x", MagicMock(), MagicMock())
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_save_persists_warehouse_and_jobs_compute(self, app_settings, sql_executor_mock):
        store: dict[str, str] = {}

        def _upsert(_table, *, key_cols, value_cols, **_kwargs):
            store[key_cols["setting_key"]] = value_cols["setting_value"]

        def _query(sql):
            for key, value in store.items():
                if f"'{key}'" in sql:
                    return [(value,)]
            return []

        sql_executor_mock.upsert.side_effect = _upsert
        sql_executor_mock.query.side_effect = _query

        orchestrator = MagicMock()
        orchestrator.checkers.check_warehouse.return_value = SetupStep(id=SetupStepId.WAREHOUSE, state=StepState.PASSED)
        result = await save_compute_settings(
            ComputeSettingsIn(
                sql_warehouse_id="wh-9",
                jobs_compute=JobsComputeModel(kind="existing_cluster", cluster_id="c-1"),
            ),
            app_settings,
            "admin@x",
            MagicMock(),
            orchestrator,
        )
        assert result.sql_warehouse_id == "wh-9"
        assert result.warehouse_is_override is True
        assert result.jobs_compute.kind == "existing_cluster"
        assert result.jobs_compute.cluster_id == "c-1"

    def test_save_warehouse_rejects_candidate_before_persist(
        self, client: TestClient, checkers: MagicMock, route_app_settings: MagicMock, obo_ws: MagicMock
    ) -> None:
        """Saving before a failed readiness check would replace the working warehouse."""
        checkers.check_warehouse.return_value = SetupStep(
            id=SetupStepId.WAREHOUSE,
            state=StepState.ACTION_REQUIRED,
            code="warehouse_permissions_missing",
            summary="The app service principal needs CAN_USE on the SQL warehouse.",
        )

        response = client.put("/api/v1/compute/settings", json={"sql_warehouse_id": "new-warehouse"})

        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "warehouse_permissions_missing"
        route_app_settings.save_sql_warehouse_id.assert_not_called()
        checkers.check_warehouse.assert_called_once_with("new-warehouse", reader_ws=obo_ws)

    def test_save_warehouse_persists_candidate_after_readiness_passes(
        self, client: TestClient, checkers: MagicMock, route_app_settings: MagicMock, obo_ws: MagicMock
    ) -> None:
        """Removing the successful readiness check would activate an unverified warehouse."""
        response = client.put("/api/v1/compute/settings", json={"sql_warehouse_id": "new-warehouse"})

        assert response.status_code == 200
        checkers.check_warehouse.assert_called_once_with("new-warehouse", reader_ws=obo_ws)
        route_app_settings.save_sql_warehouse_id.assert_called_once_with("new-warehouse", user_email="admin@x")

    def test_save_warehouse_accepts_candidate_when_acl_is_unreadable(
        self, client: TestClient, checkers: MagicMock, route_app_settings: MagicMock
    ) -> None:
        """An inconclusive ACL read must not reject a potentially usable warehouse."""
        checkers.check_warehouse.return_value = SetupStep(
            id=SetupStepId.WAREHOUSE,
            state=StepState.ACTION_REQUIRED,
            code="warehouse_permission_unknown",
            summary="Could not determine app service principal access to the SQL warehouse.",
        )

        response = client.put("/api/v1/compute/settings", json={"sql_warehouse_id": "new-warehouse"})

        assert response.status_code == 200
        route_app_settings.save_sql_warehouse_id.assert_called_once_with("new-warehouse", user_email="admin@x")

    def test_clear_warehouse_override_skips_validation_and_returns_to_bound_default(
        self, client: TestClient, checkers: MagicMock, route_app_settings: MagicMock
    ) -> None:
        """Validating an empty override would prevent an administrator returning to the bound warehouse."""
        route_app_settings.get_sql_warehouse_id.return_value = None

        response = client.put("/api/v1/compute/settings", json={"sql_warehouse_id": ""})

        assert response.status_code == 200
        assert response.json()["sql_warehouse_id"] == ""
        assert response.json()["warehouse_is_override"] is False
        checkers.check_warehouse.assert_not_called()
        route_app_settings.save_sql_warehouse_id.assert_called_once_with("", user_email="admin@x")

    def test_save_warehouse_returns_curated_unavailable_response_without_setup_orchestrator(
        self, unavailable_client: TestClient
    ) -> None:
        """A missing setup orchestrator must not turn a settings request into an internal error."""
        response = unavailable_client.put("/api/v1/compute/settings", json={"sql_warehouse_id": "new-warehouse"})

        assert response.status_code == 503
        assert response.json()["detail"] == "DQX Studio setup is unavailable."


class TestWarehouseAccess:
    @pytest.mark.asyncio
    async def test_get_access_status(self, compute_svc):
        async def _status(wid, reader_ws):
            return "missing"

        compute_svc.warehouse_access_status_async.side_effect = _status
        compute_svc.sp_application_id.return_value = "sp-1"

        result = await get_warehouse_access(compute_svc, MagicMock(), "wh-1")
        assert result.status == "missing"
        assert result.warehouse_id == "wh-1"
        assert result.sp_application_id == "sp-1"

    @pytest.mark.asyncio
    async def test_get_access_requires_warehouse_id(self, compute_svc):
        with pytest.raises(HTTPException) as exc:
            await get_warehouse_access(compute_svc, MagicMock(), "  ")
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_grant_success_returns_status(self, compute_svc):
        async def _grant(wid, grantor_ws):
            return None

        async def _status(wid, reader_ws):
            return "granted"

        compute_svc.grant_warehouse_can_use_async.side_effect = _grant
        compute_svc.warehouse_access_status_async.side_effect = _status
        compute_svc.sp_application_id.return_value = "sp-1"

        result = await grant_warehouse_access(GrantWarehouseAccessIn(warehouse_id="wh-1"), compute_svc, MagicMock())
        assert result.status == "granted"

    @pytest.mark.asyncio
    async def test_grant_failure_maps_to_502(self, compute_svc):
        async def _boom(wid, grantor_ws):
            raise PermissionError("no manage")

        compute_svc.grant_warehouse_can_use_async.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await grant_warehouse_access(GrantWarehouseAccessIn(warehouse_id="wh-1"), compute_svc, MagicMock())
        assert exc.value.status_code == 502
