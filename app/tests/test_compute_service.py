"""Tests for ComputeService — warehouse/cluster discovery + SP access check/grant (P22-B)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService, resolve_warehouse_id


@pytest.fixture
def app_settings(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.query.return_value = []
    return AppSettingsService(sql=sql_executor_mock)


@pytest.fixture
def sp_ws():
    return create_autospec(WorkspaceClient, instance=True)


@pytest.fixture
def service(sp_ws, app_settings):
    svc = ComputeService(sp_ws=sp_ws, app_settings=app_settings)
    # Deterministic SP identity for grant/check tests.
    svc.sp_application_id = MagicMock(return_value="sp-app-id")  # type: ignore[method-assign]
    return svc


class TestListWarehouses:
    def test_maps_and_sorts(self, service, sp_ws):
        sp_ws.warehouses.list.return_value = iter(
            [
                SimpleNamespace(id="w2", name="Zeta", enable_serverless_compute=True, state=SimpleNamespace(value="RUNNING")),
                SimpleNamespace(id="w1", name="Alpha", enable_serverless_compute=False, state=SimpleNamespace(value="STOPPED")),
                SimpleNamespace(id=None, name="skip", enable_serverless_compute=False, state=None),
            ]
        )

        result = service.list_warehouses()

        assert [w.name for w in result] == ["Alpha", "Zeta"]
        assert result[0].serverless is False and result[0].running is False
        assert result[1].serverless is True and result[1].running is True

    def test_lists_via_obo_client_when_supplied(self, service, sp_ws):
        """When an OBO client is passed it — not the app SP — is used to list."""
        obo = MagicMock(name="obo_ws")
        obo.warehouses.list.return_value = iter(
            [SimpleNamespace(id="w1", name="Alpha", enable_serverless_compute=False, state=None)]
        )

        result = service.list_warehouses(lister_ws=obo)

        assert [w.id for w in result] == ["w1"]
        obo.warehouses.list.assert_called_once()
        sp_ws.warehouses.list.assert_not_called()


class TestListClusters:
    def test_maps_all_purpose_clusters(self, service, sp_ws):
        sp_ws.clusters.list.return_value = iter(
            [
                SimpleNamespace(cluster_id="c1", cluster_name="Beta", state=SimpleNamespace(value="RUNNING")),
                SimpleNamespace(cluster_id=None, cluster_name="skip", state=None),
            ]
        )

        result = service.list_clusters()

        assert [c.cluster_id for c in result] == ["c1"]
        assert result[0].state == "RUNNING"

    def test_lists_via_obo_client_when_supplied(self, service, sp_ws):
        obo = MagicMock(name="obo_ws")
        obo.clusters.list.return_value = iter(
            [SimpleNamespace(cluster_id="c1", cluster_name="Beta", state=SimpleNamespace(value="RUNNING"))]
        )

        result = service.list_clusters(lister_ws=obo)

        assert [c.cluster_id for c in result] == ["c1"]
        obo.clusters.list.assert_called_once()
        sp_ws.clusters.list.assert_not_called()


class TestWarehouseAccessStatus:
    def _acl(self, principal, level):
        return SimpleNamespace(
            access_control_list=[
                SimpleNamespace(
                    service_principal_name=principal,
                    all_permissions=[SimpleNamespace(permission_level=SimpleNamespace(value=level))],
                )
            ]
        )

    def test_granted_when_sp_has_can_use(self, service, sp_ws):
        sp_ws.warehouses.get_permissions.return_value = self._acl("sp-app-id", "CAN_USE")

        assert service.warehouse_access_status("wh", reader_ws=MagicMock()) == "granted"

    def test_missing_when_sp_absent_from_acl(self, service, sp_ws):
        sp_ws.warehouses.get_permissions.return_value = self._acl("someone-else", "CAN_MANAGE")

        assert service.warehouse_access_status("wh", reader_ws=MagicMock()) == "missing"

    def test_falls_back_to_reader_when_sp_cannot_read_acl(self, service, sp_ws):
        sp_ws.warehouses.get_permissions.side_effect = PermissionError("no manage")
        reader = MagicMock()
        reader.warehouses.get_permissions.return_value = self._acl("sp-app-id", "CAN_USE")

        assert service.warehouse_access_status("wh", reader_ws=reader) == "granted"

    def test_unknown_when_neither_client_can_read(self, service, sp_ws):
        sp_ws.warehouses.get_permissions.side_effect = PermissionError("no manage")
        reader = MagicMock()
        reader.warehouses.get_permissions.side_effect = PermissionError("no manage")

        assert service.warehouse_access_status("wh", reader_ws=reader) == "unknown"

    def test_unknown_when_sp_identity_unresolved(self, sp_ws, app_settings):
        svc = ComputeService(sp_ws=sp_ws, app_settings=app_settings)
        svc.sp_application_id = MagicMock(return_value="")  # type: ignore[method-assign]

        assert svc.warehouse_access_status("wh", reader_ws=MagicMock()) == "unknown"


class TestGrantWarehouseCanUse:
    def test_grants_via_grantor(self, service):
        grantor = MagicMock()

        service.grant_warehouse_can_use("wh", grantor_ws=grantor)

        grantor.warehouses.update_permissions.assert_called_once()
        args, kwargs = grantor.warehouses.update_permissions.call_args
        assert args[0] == "wh"
        acl = kwargs["access_control_list"]
        assert acl[0].service_principal_name == "sp-app-id"

    def test_raises_when_sp_identity_unresolved(self, sp_ws, app_settings):
        svc = ComputeService(sp_ws=sp_ws, app_settings=app_settings)
        svc.sp_application_id = MagicMock(return_value="")  # type: ignore[method-assign]

        with pytest.raises(RuntimeError):
            svc.grant_warehouse_can_use("wh", grantor_ws=MagicMock())


class TestResolveWarehouseId:
    def test_prefers_configured_override(self, app_settings, sql_executor_mock):
        sql_executor_mock.query.return_value = [["wh-override"]]

        assert resolve_warehouse_id(app_settings) == "wh-override"

    def test_falls_back_to_env(self, app_settings, sql_executor_mock, monkeypatch):
        sql_executor_mock.query.return_value = []  # unset
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "env-wh")

        assert resolve_warehouse_id(app_settings) == "env-wh"
