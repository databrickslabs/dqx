"""Tests for the compute routes (P22-B) — settings, listings, warehouse access/grant."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1 import compute as compute_mod
from databricks_labs_dqx_app.backend.routes.v1.compute import (
    ComputeSettingsIn,
    GrantWarehouseAccessIn,
    JobsComputeModel,
    get_compute_settings,
    get_warehouse_access,
    grant_warehouse_access,
    list_clusters,
    list_warehouses,
    save_compute_settings,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService, WarehouseInfo, ClusterInfo


@pytest.fixture
def app_settings(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.query.return_value = []
    return AppSettingsService(sql=sql_executor_mock)


@pytest.fixture
def compute_svc():
    return create_autospec(ComputeService, instance=True)


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

    def test_save_requires_a_field(self, app_settings):
        with pytest.raises(HTTPException) as exc:
            save_compute_settings(ComputeSettingsIn(), app_settings, "admin@x")
        assert exc.value.status_code == 400

    def test_save_persists_warehouse_and_jobs_compute(self, app_settings, sql_executor_mock):
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

        result = save_compute_settings(
            ComputeSettingsIn(
                sql_warehouse_id="wh-9",
                jobs_compute=JobsComputeModel(kind="existing_cluster", cluster_id="c-1"),
            ),
            app_settings,
            "admin@x",
        )
        assert result.sql_warehouse_id == "wh-9"
        assert result.warehouse_is_override is True
        assert result.jobs_compute.kind == "existing_cluster"
        assert result.jobs_compute.cluster_id == "c-1"


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
