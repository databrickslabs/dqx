"""Route-level tests for the schedule-grant gate on save (Task 12).

Exercises the hard-block (403) and grant-on-manageable paths of the
monitored-table and data-product schedule saves by calling the handlers
directly with mocked services (the established route-test pattern).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.models import (
    UpdateDataProductIn,
    UpdateMonitoredTableScheduleIn,
)
from databricks_labs_dqx_app.backend.routes.v1.data_products import update_data_product
from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import update_monitored_table_schedule
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.schedule_grant_service import CannotManageError, ScheduleGrantService

FQN = "cat.sch.tbl"


@pytest.fixture
def obo_ws():
    ws = MagicMock(name="obo_ws")
    ws.current_user.me.return_value = SimpleNamespace(user_name="alice@example.com")
    return ws


@pytest.fixture
def perms():
    p = create_autospec(PermissionsService, instance=True)
    p.require_object.return_value = None
    return p


@pytest.fixture
def grant_svc():
    return create_autospec(ScheduleGrantService, instance=True)


class TestMonitoredTableScheduleGate:
    def _detail(self):
        return SimpleNamespace(table=SimpleNamespace(table_fqn=FQN))

    def test_blocks_with_403_when_cannot_manage(self, obo_ws, perms, grant_svc):
        svc = create_autospec(MonitoredTableService, instance=True)
        svc.get.return_value = self._detail()
        grant_svc.grant_select_to_schedulers.side_effect = CannotManageError(
            FQN, [{"principal": "bob@example.com", "type": "user"}]
        )
        body = UpdateMonitoredTableScheduleIn(schedule_cron="0 0 * * *", schedule_tz="UTC")

        with pytest.raises(HTTPException) as exc:
            update_monitored_table_schedule(
                "b1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc
            )

        assert exc.value.status_code == 403
        assert exc.value.detail["code"] == "cannot_manage_schedule_tables"
        assert exc.value.detail["tables"][0]["fqn"] == FQN
        svc.update_schedule.assert_not_called()

    def test_grants_then_saves_when_manageable(self, obo_ws, perms, grant_svc):
        svc = create_autospec(MonitoredTableService, instance=True)
        svc.get.return_value = self._detail()
        svc.update_schedule.return_value = MagicMock()
        body = UpdateMonitoredTableScheduleIn(schedule_cron="0 0 * * *", schedule_tz="UTC")

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "databricks_labs_dqx_app.backend.routes.v1.monitored_tables.MonitoredTableOut",
                SimpleNamespace(from_domain=lambda t: "out"),
            )
            result = update_monitored_table_schedule(
                "b1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc
            )

        grant_svc.grant_select_to_schedulers.assert_called_once_with(FQN)
        svc.update_schedule.assert_called_once()
        assert result == "out"

    def test_skips_gate_when_clearing_schedule(self, obo_ws, perms, grant_svc):
        svc = create_autospec(MonitoredTableService, instance=True)
        svc.update_schedule.return_value = MagicMock()
        body = UpdateMonitoredTableScheduleIn(schedule_cron=None)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "databricks_labs_dqx_app.backend.routes.v1.monitored_tables.MonitoredTableOut",
                SimpleNamespace(from_domain=lambda t: "out"),
            )
            update_monitored_table_schedule("b1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

        grant_svc.grant_select_to_schedulers.assert_not_called()
        svc.get.assert_not_called()


class TestDataProductScheduleGate:
    def test_blocks_with_403_listing_all_unmanageable_tables(self, obo_ws, perms, grant_svc):
        svc = create_autospec(DataProductService, instance=True)
        svc.member_table_fqns.return_value = ["cat.sch.t1", "cat.sch.t2"]
        # t1 manageable, t2 not.
        grant_svc.user_can_manage.side_effect = lambda fqn: fqn == "cat.sch.t1"
        grant_svc.manage_holders.return_value = [{"principal": "bob@example.com", "type": "user"}]
        body = UpdateDataProductIn(schedule_cron="0 0 * * *")

        with pytest.raises(HTTPException) as exc:
            update_data_product("p1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

        assert exc.value.status_code == 403
        blocked = {t["fqn"] for t in exc.value.detail["tables"]}
        assert blocked == {"cat.sch.t2"}
        svc.update.assert_not_called()

    def test_grants_all_members_then_saves(self, obo_ws, perms, grant_svc):
        svc = create_autospec(DataProductService, instance=True)
        svc.member_table_fqns.return_value = ["cat.sch.t1", "cat.sch.t2"]
        svc.get.return_value = MagicMock()
        grant_svc.user_can_manage.return_value = True
        body = UpdateDataProductIn(schedule_cron="0 0 * * *")

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "databricks_labs_dqx_app.backend.routes.v1.data_products.DataProductOut",
                SimpleNamespace(from_domain=lambda d: "out"),
            )
            result = update_data_product("p1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

        assert grant_svc.grant_select_to_schedulers.call_count == 2
        svc.update.assert_called_once()
        assert result == "out"

    def test_skips_gate_when_no_schedule_field(self, obo_ws, perms, grant_svc):
        svc = create_autospec(DataProductService, instance=True)
        svc.get.return_value = MagicMock()
        body = UpdateDataProductIn(name="new name")  # no schedule_cron

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "databricks_labs_dqx_app.backend.routes.v1.data_products.DataProductOut",
                SimpleNamespace(from_domain=lambda d: "out"),
            )
            update_data_product("p1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

        grant_svc.user_can_manage.assert_not_called()
        grant_svc.grant_select_to_schedulers.assert_not_called()
