"""Route-level tests for the schedule-grant gate on save (Task 12).

Exercises the hard-block (403) and grant-on-manageable paths of the
monitored-table and data-product schedule saves by calling the handlers
directly with mocked services (the established route-test pattern).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk.service.catalog import Privilege
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.models import (
    ScheduleConfigIn,
    UpdateDataProductIn,
    UpdateMonitoredTableScheduleIn,
)
from databricks_labs_dqx_app.backend.routes.v1.data_products import update_data_product
from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import update_monitored_table_schedule
from databricks_labs_dqx_app.backend.routes.v1.schedules import save_schedule
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.schedule_config_service import ScheduleConfigService
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
            update_monitored_table_schedule("b1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

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

        # Precleared grant: every member is MANAGE-gated once above, so the grant
        # must NOT re-run the ownership + effective-privilege round-trips.
        assert grant_svc.grant_select_precleared.call_count == 2
        grant_svc.grant_select_to_schedulers.assert_not_called()
        svc.update.assert_called_once()
        assert result == "out"

    def test_checks_manage_exactly_once_per_member_table(self, obo_ws, perms, grant_svc):
        """The gate and the grant together must cost ONE MANAGE check per table.

        Regression guard for the 2xN Unity Catalog round-trips this path used to
        make (``user_can_manage`` in the gate, then again inside
        ``grant_select_to_schedulers``).
        """
        svc = create_autospec(DataProductService, instance=True)
        svc.member_table_fqns.return_value = ["cat.sch.t1", "cat.sch.t2", "cat.sch.t3"]
        svc.get.return_value = MagicMock()
        grant_svc.user_can_manage.return_value = True
        body = UpdateDataProductIn(schedule_cron="0 0 * * *")

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "databricks_labs_dqx_app.backend.routes.v1.data_products.DataProductOut",
                SimpleNamespace(from_domain=lambda d: "out"),
            )
            update_data_product("p1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

        assert grant_svc.user_can_manage.call_count == 3
        assert grant_svc.grant_select_precleared.call_count == 3

    def test_primes_identities_once_for_the_whole_save(self, obo_ws, perms, grant_svc):
        """Caller + scheduler identities are resolved once, not per table."""
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
            update_data_product("p1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

        grant_svc.prime_caller_identity.assert_called_once()
        grant_svc.prime_scheduler_sp_identities.assert_called_once()

    def test_does_not_grant_when_blocked(self, obo_ws, perms, grant_svc):
        """A blocked member must stop the save before any grant is attempted."""
        svc = create_autospec(DataProductService, instance=True)
        svc.member_table_fqns.return_value = ["cat.sch.t1", "cat.sch.t2"]
        grant_svc.user_can_manage.return_value = False
        grant_svc.manage_holders.return_value = [{"principal": "bob@example.com", "type": "user"}]
        body = UpdateDataProductIn(schedule_cron="0 0 * * *")

        with pytest.raises(HTTPException):
            update_data_product("p1", body, svc, obo_ws, UserRole.ADMIN, frozenset(), perms, grant_svc)

        grant_svc.grant_select_precleared.assert_not_called()

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
        grant_svc.grant_select_precleared.assert_not_called()


def _me(user_name="alice@example.com", emails=("alice@example.com",), groups=()):
    return SimpleNamespace(
        user_name=user_name,
        emails=[SimpleNamespace(value=e) for e in emails],
        groups=[SimpleNamespace(display=d, value=v) for (d, v) in groups],
        id="u-1",
    )


def _eff(assignments):
    return SimpleNamespace(privilege_assignments=list(assignments), next_page_token=None)


class TestScopeConfigScheduleGate:
    """POST /schedules — the ``scope_mode='all'`` performance-fix path (#C3).

    Uses a *real* ScheduleGrantService over mocked workspace clients so the
    identity/round-trip call counts are observable: the fix must make per-table
    work fast (identity resolved once, MANAGE checked once) while keeping full
    coverage (every in-scope table gated + granted on every save).
    """

    @pytest.fixture
    def obo(self):
        ws = MagicMock(name="obo_ws")
        ws.current_user.me.return_value = _me()
        ws.tables.get.return_value = SimpleNamespace(owner=None)
        ws.schemas.get.return_value = SimpleNamespace(owner=None)
        ws.catalogs.get.return_value = SimpleNamespace(owner=None)
        ws.grants.get_effective.return_value = _eff([])
        return ws

    @pytest.fixture
    def sp(self):
        ws = MagicMock(name="sp_ws")
        ws.jobs.get.return_value = SimpleNamespace(
            settings=SimpleNamespace(run_as=SimpleNamespace(service_principal_name="task-runner-sp"))
        )
        return ws

    @pytest.fixture
    def grant_svc(self, obo, sp, monkeypatch):  # type: ignore[override]
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-id")
        return ScheduleGrantService(obo_ws=obo, sp_ws=sp, job_id="123")

    @staticmethod
    def _config_svc(fqns):
        svc = create_autospec(ScheduleConfigService, instance=True)
        svc.resolve_scope_table_fqns.return_value = fqns
        svc.save.return_value = SimpleNamespace(
            schedule_name="nightly",
            config={"scope_mode": "all"},
            version=1,
            created_by="alice@example.com",
            created_at=None,
            updated_by="alice@example.com",
            updated_at=None,
        )
        return svc

    async def test_all_tables_gated_and_granted_with_identity_resolved_once(self, obo, sp, grant_svc):
        # Every table is manageable (alice owns each). scope_mode='all' resolves
        # to many tables — the classic slow path.
        fqns = [f"cat.sch.t{i}" for i in range(20)]
        obo.tables.get.return_value = SimpleNamespace(owner="alice@example.com")
        svc = self._config_svc(fqns)
        body = ScheduleConfigIn(schedule_name="nightly", config={"scope_mode": "all"})

        result = await save_schedule(body, obo, svc, grant_svc)

        assert result.schedule_name == "nightly"
        # Full coverage: SELECT granted to app SP + task-runner SP on EVERY table.
        assert obo.grants.update.call_count == 2 * len(fqns)
        granted_principals = {c.kwargs["changes"][0].principal for c in obo.grants.update.call_args_list}
        assert granted_principals == {"app-sp-id", "task-runner-sp"}
        # MANAGE checked exactly once per table (owner read once each, not twice).
        assert obo.tables.get.call_count == len(fqns)
        # Identity round-trips do NOT scale with the table count: the OBO caller
        # identity is resolved once for the gate + once for the persisted audit
        # row, and the task-runner SP is derived once.
        assert obo.current_user.me.call_count == 2
        assert sp.jobs.get.call_count == 1
        svc.save.assert_called_once()

    async def test_single_unmanageable_table_hard_blocks_listing_all_blocked(self, obo, sp, grant_svc):
        fqns = ["cat.sch.t0", "cat.sch.t1", "cat.sch.t2"]

        # t1 is manageable (owned by alice); t0 and t2 are not.
        def _tables_get(fqn):
            return SimpleNamespace(owner="alice@example.com" if fqn == "cat.sch.t1" else None)

        obo.tables.get.side_effect = _tables_get
        # A different principal holds MANAGE on the blocked tables (surfaced to UI).
        obo.grants.get_effective.return_value = _eff(
            [SimpleNamespace(principal="bob@example.com", privileges=[SimpleNamespace(privilege=Privilege.MANAGE)])]
        )
        svc = self._config_svc(fqns)
        body = ScheduleConfigIn(schedule_name="nightly", config={"scope_mode": "all"})

        with pytest.raises(HTTPException) as exc:
            await save_schedule(body, obo, svc, grant_svc)

        assert exc.value.status_code == 403
        assert exc.value.detail["code"] == "cannot_manage_schedule_tables"
        blocked = {t["fqn"] for t in exc.value.detail["tables"]}
        assert blocked == {"cat.sch.t0", "cat.sch.t2"}  # ALL blocked tables aggregated
        # Hard block: nothing granted and nothing saved.
        obo.grants.update.assert_not_called()
        svc.save.assert_not_called()

    async def test_noop_when_scope_resolves_to_no_tables(self, obo, grant_svc):
        svc = self._config_svc([])
        body = ScheduleConfigIn(schedule_name="nightly", config={"scope_mode": "all"})

        result = await save_schedule(body, obo, svc, grant_svc)

        assert result.schedule_name == "nightly"
        obo.grants.update.assert_not_called()
        svc.save.assert_called_once()

    async def test_disabled_schedule_skips_gate(self, obo, grant_svc):
        svc = self._config_svc(["cat.sch.t0"])
        body = ScheduleConfigIn(schedule_name="nightly", config={"scope_mode": "all", "enabled": False})

        result = await save_schedule(body, obo, svc, grant_svc)

        assert result.schedule_name == "nightly"
        svc.resolve_scope_table_fqns.assert_not_called()
        obo.grants.update.assert_not_called()
        svc.save.assert_called_once()
