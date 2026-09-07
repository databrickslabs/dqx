"""Tests for ScheduleGrantService — grantability checks + scheduler grants (Task 12)."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import Privilege

from databricks_labs_dqx_app.backend.services.schedule_grant_service import (
    CannotManageError,
    ScheduleGrantService,
    manage_block_detail,
)

FQN = "cat.sch.tbl"


def _me(user_name="alice@example.com", emails=("alice@example.com",), groups=()):
    return SimpleNamespace(
        user_name=user_name,
        emails=[SimpleNamespace(value=e) for e in emails],
        groups=[SimpleNamespace(display=d, value=v) for (d, v) in groups],
        id="u-1",
    )


def _assignment(principal, privileges):
    return SimpleNamespace(principal=principal, privileges=[SimpleNamespace(privilege=p) for p in privileges])


def _eff(assignments):
    return SimpleNamespace(privilege_assignments=list(assignments))


@pytest.fixture
def obo():
    ws = MagicMock(name="obo_ws")
    ws.current_user.me.return_value = _me()
    # No owners / no grants by default → not manageable.
    ws.tables.get.return_value = SimpleNamespace(owner=None)
    ws.schemas.get.return_value = SimpleNamespace(owner=None)
    ws.catalogs.get.return_value = SimpleNamespace(owner=None)
    ws.grants.get_effective.return_value = _eff([])
    return ws


@pytest.fixture
def sp():
    ws = MagicMock(name="sp_ws")
    # Task-runner SP derived from the bound job's run_as.
    ws.jobs.get.return_value = SimpleNamespace(
        settings=SimpleNamespace(run_as=SimpleNamespace(service_principal_name="task-runner-sp"))
    )
    return ws


@pytest.fixture
def service(obo, sp, monkeypatch):
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-id")
    return ScheduleGrantService(obo_ws=obo, sp_ws=sp, job_id="123")


class TestUserCanManage:
    def test_true_when_user_owns_table(self, service, obo):
        obo.tables.get.return_value = SimpleNamespace(owner="alice@example.com")
        assert service.user_can_manage(FQN) is True

    def test_true_when_group_owns_table(self, service, obo):
        obo.current_user.me.return_value = _me(groups=[("data-eng", "g1")])
        obo.tables.get.return_value = SimpleNamespace(owner="data-eng")
        assert service.user_can_manage(FQN) is True

    def test_true_when_parent_schema_owner_is_user(self, service, obo):
        obo.schemas.get.return_value = SimpleNamespace(owner="alice@example.com")
        assert service.user_can_manage(FQN) is True

    def test_true_via_effective_manage_for_user(self, service, obo):
        def _ge(_type, _fqn, *, principal=None, page_token=None):
            if principal == "alice@example.com":
                return _eff([_assignment("alice@example.com", [Privilege.MANAGE])])
            return _eff([])

        obo.grants.get_effective.side_effect = _ge
        assert service.user_can_manage(FQN) is True

    def test_true_via_group_manage(self, service, obo):
        obo.current_user.me.return_value = _me(groups=[("data-eng", "g1")])

        def _ge(_type, _fqn, *, principal=None, page_token=None):
            if principal is None:
                return _eff([_assignment("data-eng", [Privilege.MANAGE])])
            return _eff([])

        obo.grants.get_effective.side_effect = _ge
        assert service.user_can_manage(FQN) is True

    def test_false_with_only_all_privileges_or_select(self, service, obo):
        # ALL PRIVILEGES / SELECT alone does NOT confer re-grant — only MANAGE does.
        obo.grants.get_effective.return_value = _eff(
            [_assignment("alice@example.com", [Privilege.ALL_PRIVILEGES, Privilege.SELECT])]
        )
        assert service.user_can_manage(FQN) is False

    def test_false_when_no_owner_no_manage(self, service):
        assert service.user_can_manage(FQN) is False

    def test_false_for_synthetic_fqn(self, service):
        assert service.user_can_manage("__sql_check__/my_check") is False

    def test_false_safe_default_when_me_raises(self, service, obo):
        # Identity unresolved (me() raises) → block. Even though another
        # principal (bob) owns the table and holds MANAGE, the caller must NOT
        # inherit it, and we must short-circuit before consulting any grant.
        obo.current_user.me.side_effect = RuntimeError("no identity")
        obo.tables.get.return_value = SimpleNamespace(owner="bob@example.com")
        obo.grants.get_effective.return_value = _eff([_assignment("bob@example.com", [Privilege.MANAGE])])

        assert service.user_can_manage(FQN) is False
        # The safe-default guard short-circuits before any securable/grant read,
        # so another principal's MANAGE can never be mis-attributed to the caller.
        obo.grants.get_effective.assert_not_called()
        obo.tables.get.assert_not_called()

    def test_false_safe_default_when_identity_empty(self, service, obo):
        # me() returns an empty/None user_name with no groups and no emails →
        # empty principal set → block, again without consulting any grant.
        obo.current_user.me.return_value = _me(user_name="", emails=(), groups=())
        obo.tables.get.return_value = SimpleNamespace(owner="bob@example.com")
        obo.grants.get_effective.return_value = _eff([_assignment("bob@example.com", [Privilege.MANAGE])])

        assert service.user_can_manage(FQN) is False
        obo.grants.get_effective.assert_not_called()
        obo.tables.get.assert_not_called()

    def test_true_via_group_manage_on_later_page(self, service, obo):
        # A group's MANAGE on a SECOND page of effective grants must still count —
        # otherwise a legitimate MANAGE-via-group user is falsely hard-blocked.
        obo.current_user.me.return_value = _me(groups=[("data-eng", "g1")])

        def _ge(_type, _fqn, *, principal=None, page_token=None):
            if principal is not None:
                return SimpleNamespace(privilege_assignments=[], next_page_token=None)
            if page_token is None:
                return SimpleNamespace(
                    privilege_assignments=[_assignment("someone@example.com", [Privilege.SELECT])],
                    next_page_token="p2",
                )
            return SimpleNamespace(
                privilege_assignments=[_assignment("data-eng", [Privilege.MANAGE])],
                next_page_token=None,
            )

        obo.grants.get_effective.side_effect = _ge
        assert service.user_can_manage(FQN) is True


class TestManageHolders:
    def test_enumerates_owners_and_manage_holders(self, service, obo):
        obo.tables.get.return_value = SimpleNamespace(owner="owner-group")
        obo.grants.get_effective.return_value = _eff(
            [
                _assignment("bob@example.com", [Privilege.MANAGE]),
                _assignment("someone@example.com", [Privilege.SELECT]),  # not a manager
            ]
        )
        holders = service.manage_holders(FQN)
        principals = {h["principal"] for h in holders}
        assert principals == {"owner-group", "bob@example.com"}
        by_p = {h["principal"]: h["type"] for h in holders}
        assert by_p["bob@example.com"] == "user"
        assert by_p["owner-group"] == "group"

    def test_empty_for_synthetic_fqn(self, service):
        assert service.manage_holders("__sql_check__/x") == []


class TestGrant:
    def test_grants_select_to_app_and_task_runner(self, service, obo):
        obo.tables.get.return_value = SimpleNamespace(owner="alice@example.com")  # manageable

        granted = service.grant_select_to_schedulers(FQN)

        assert granted == ["app-sp-id", "task-runner-sp"]
        principals = [c.kwargs["changes"][0].principal for c in obo.grants.update.call_args_list]
        assert principals == ["app-sp-id", "task-runner-sp"]
        for call in obo.grants.update.call_args_list:
            change = call.kwargs["changes"][0]
            assert change.add == [Privilege.SELECT]

    def test_grants_app_sp_only_when_task_runner_underivable(self, service, obo, sp):
        obo.tables.get.return_value = SimpleNamespace(owner="alice@example.com")
        sp.jobs.get.side_effect = RuntimeError("no job")

        granted = service.grant_select_to_schedulers(FQN)

        assert granted == ["app-sp-id"]
        assert obo.grants.update.call_count == 1

    def test_task_runner_grant_best_effort_does_not_fail(self, service, obo):
        obo.tables.get.return_value = SimpleNamespace(owner="alice@example.com")
        # First grant (app SP) succeeds; second (task-runner) raises.
        obo.grants.update.side_effect = [None, PermissionError("nope")]

        granted = service.grant_select_to_schedulers(FQN)

        assert granted == ["app-sp-id"]  # task-runner failure swallowed

    def test_raises_cannot_manage_when_user_lacks_manage(self, service, obo):
        # Only bob holds MANAGE — the caller (alice) has nothing on the table.
        def _ge(_type, _fqn, *, principal=None, page_token=None):
            if principal is None:
                return _eff([_assignment("bob@example.com", [Privilege.MANAGE])])
            return _eff([])  # alice's own effective privileges: none

        obo.grants.get_effective.side_effect = _ge
        with pytest.raises(CannotManageError) as exc:
            service.grant_select_to_schedulers(FQN)
        assert exc.value.fqn == FQN
        assert {h["principal"] for h in exc.value.manage_holders} == {"bob@example.com"}
        obo.grants.update.assert_not_called()

    def test_app_sp_grant_failure_propagates(self, service, obo):
        obo.tables.get.return_value = SimpleNamespace(owner="alice@example.com")
        obo.grants.update.side_effect = PermissionError("denied")
        with pytest.raises(PermissionError):
            service.grant_select_to_schedulers(FQN)

    def test_synthetic_fqn_is_noop(self, service, obo):
        assert service.grant_select_to_schedulers("__sql_check__/x") == []
        obo.grants.update.assert_not_called()


class TestTaskRunnerDerivation:
    def test_derives_from_job_run_as(self, service):
        assert service.task_runner_sp_id() == "task-runner-sp"

    def test_none_when_no_job_id(self, obo, sp):
        svc = ScheduleGrantService(obo_ws=obo, sp_ws=sp, job_id="")
        assert svc.task_runner_sp_id() is None


class TestManageBlockDetail:
    def test_shape(self):
        detail = manage_block_detail([(FQN, [{"principal": "bob@example.com", "type": "user"}])])
        assert detail["code"] == "cannot_manage_schedule_tables"
        assert detail["tables"] == [{"fqn": FQN, "manage_holders": [{"principal": "bob@example.com", "type": "user"}]}]


class TestIdentityMemoization:
    """Per-request identity round-trips must not scale with the table count."""

    def test_caller_identity_resolved_once_across_many_checks(self, service, obo):
        obo.tables.get.return_value = SimpleNamespace(owner="alice@example.com")  # manageable
        for i in range(25):
            assert service.user_can_manage(f"cat.sch.t{i}") is True
        # current_user.me() (the OBO caller identity) fires exactly once, not per table.
        assert obo.current_user.me.call_count == 1

    def test_prime_caller_identity_is_single_round_trip(self, service, obo):
        service.prime_caller_identity()
        service.prime_caller_identity()
        for i in range(10):
            service.user_can_manage(f"cat.sch.t{i}")
        assert obo.current_user.me.call_count == 1

    async def test_task_runner_derivation_memoized_across_grants(self, service, sp):
        service.prime_scheduler_sp_identities()
        for i in range(15):
            await service.grant_select_precleared_async(f"cat.sch.t{i}")
        # jobs.get (task-runner SP derivation) fires once, not per table.
        assert sp.jobs.get.call_count == 1

    def test_app_sp_me_fallback_memoized(self, obo, sp, monkeypatch):
        # No DATABRICKS_CLIENT_ID → app SP id falls back to the SP's own me().
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        sp.current_user.me.return_value = SimpleNamespace(user_name="app-sp", id="app-sp")
        svc = ScheduleGrantService(obo_ws=obo, sp_ws=sp, job_id="")
        assert svc.app_sp_id() == "app-sp"
        assert svc.app_sp_id() == "app-sp"
        assert sp.current_user.me.call_count == 1


class TestPreclearedGrant:
    """The pre-gated grant path trusts the caller's MANAGE decision (checked once)."""

    async def test_precleared_grant_does_not_recheck_manage(self, service, obo):
        # Deliberately *not* manageable: no owner, no MANAGE. The gate is the
        # caller's responsibility; the pre-gated grant must not re-verify it, so
        # it grants without ever reading ownership / effective privileges.
        granted = await service.grant_select_precleared_async(FQN)

        assert granted == ["app-sp-id", "task-runner-sp"]
        obo.grants.get_effective.assert_not_called()
        obo.tables.get.assert_not_called()
        obo.schemas.get.assert_not_called()
        obo.catalogs.get.assert_not_called()

    async def test_precleared_grant_synthetic_is_noop(self, service, obo):
        assert await service.grant_select_precleared_async("__sql_check__/x") == []
        obo.grants.update.assert_not_called()
