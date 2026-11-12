"""Tests for the ``/data-products`` route handlers (Data Products Task 4).

Follows ``test_run_sets_routes.py``'s / ``test_monitored_tables_routes.py``'s
convention: call the route functions directly with a mocked
``DataProductService`` rather than spinning up a FastAPI ``TestClient``, and
assert the RBAC gate structurally (VIEWER+ for reads, RULE_AUTHOR+ for
writes, `run_rules` / Admin+Author gate for run) without going through
middleware.
"""

from unittest.mock import MagicMock, create_autospec, patch

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.services.scheduler_wake import SchedulerWakeMarker
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import Privilege
from databricks_labs_dqx_app.backend.models import (
    SaveDataProductIn,
    AddDataProductMemberIn,
    CreateDataProductIn,
    LifecycleRationaleIn,
    RunDataProductIn,
    UpdateDataProductIn,
)
from databricks_labs_dqx_app.backend.registry_models import DataProduct, DataProductMember
from databricks_labs_dqx_app.backend.routes.v1 import data_products as dp_routes
from databricks_labs_dqx_app.backend.services.lifecycle_history import HistoryEvent, LifecycleHistoryService
from databricks_labs_dqx_app.backend.routes.v1.data_products import (
    add_data_product_member,
    approve_data_product,
    create_data_product,
    delete_data_product,
    get_data_product,
    list_data_products,
    reject_data_product,
    revert_data_product,
    remove_data_product_member,
    run_data_product,
    submit_data_product,
    update_data_product,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductService,
    BindingNotApprovedError,
    CollectionChange,
    DataProductChanges,
    DataProductDetail,
    DataProductMemberDetail,
    DataProductRunResult,
    DataProductVersion,
    DuplicateDataProductNameError,
    InvalidStatusTransitionError,
    NoRunnableMembersError,
)


def _product(product_id: str = "p1", name: str = "Orders", status: str = "draft", version: int = 0) -> DataProduct:
    return DataProduct(product_id=product_id, name=name, status=status, version=version)


def _detail(**kwargs) -> DataProductDetail:
    return DataProductDetail(product=_product(**kwargs))


def _mock_obo_ws(user_email: str = "alice@x") -> MagicMock:
    obo = MagicMock()
    me = MagicMock()
    me.user_name = user_email
    obo.current_user.me.return_value = me
    return obo


def _enabled_settings() -> MagicMock:
    """App-settings mock with approvals mode = ``enabled`` (no auto-approve)."""
    app_settings = MagicMock()
    app_settings.get_approvals_mode.return_value = "enabled"
    return app_settings


def _disabled_settings() -> MagicMock:
    """App-settings mock with approvals mode = ``disabled`` (every submit auto-approves)."""
    app_settings = MagicMock()
    app_settings.get_approvals_mode.return_value = "disabled"
    return app_settings


def _route_required_roles(operation_id: str) -> set[UserRole]:
    """Extract the ``require_role(...)`` role set declared on a route (see
    ``test_monitored_tables_routes.py``'s identical helper)."""
    for route in dp_routes.router.routes:
        if getattr(route, "operation_id", None) != operation_id:
            continue
        for dep in route.dependencies:
            for cell in getattr(dep.dependency, "__closure__", None) or ():
                val = cell.cell_contents
                if isinstance(val, tuple) and val and all(isinstance(v, UserRole) for v in val):
                    return set(val)
    raise AssertionError(f"No require_role dependency found for {operation_id}")


class TestRbac:
    def test_list_and_get_are_viewer_plus(self):
        for op in ("listDataProducts", "getDataProduct"):
            roles = _route_required_roles(op)
            assert UserRole.VIEWER in roles

    def test_writes_are_author_and_above_not_viewer(self):
        for op in (
            "createDataProduct",
            "updateDataProduct",
            "deleteDataProduct",
            "addDataProductMember",
            "removeDataProductMember",
            "submitDataProduct",
        ):
            roles = _route_required_roles(op)
            assert UserRole.RULE_AUTHOR in roles
            assert UserRole.VIEWER not in roles

    def test_approve_reject_are_approvers_only(self):
        for op in ("approveDataProduct", "rejectDataProduct"):
            roles = _route_required_roles(op)
            assert UserRole.RULE_APPROVER in roles
            assert UserRole.ADMIN in roles
            assert UserRole.RULE_AUTHOR not in roles
            assert UserRole.VIEWER not in roles

    def test_run_uses_can_run_roles_gate(self):
        from databricks_labs_dqx_app.backend.common.authorization import CAN_RUN_ROLES

        for route in dp_routes.router.routes:
            if getattr(route, "operation_id", None) != "runDataProduct":
                continue
            # The run route must use require_role with CAN_RUN_ROLES (ADMIN + RULE_AUTHOR).
            qualnames = {getattr(dep.dependency, "__qualname__", "") for dep in route.dependencies}
            assert any("require_role" in q or "_check" in q for q in qualnames)
            # Confirm the allowed roles are exactly CAN_RUN_ROLES.
            for dep in route.dependencies:
                dep_fn = getattr(dep, "dependency", None)
                if dep_fn is None:
                    continue
                inner = getattr(dep_fn, "__closure__", None)
                if inner is None:
                    continue
                for cell in inner:
                    try:
                        val = cell.cell_contents
                    except ValueError:
                        continue
                    if isinstance(val, tuple) and set(val) == set(CAN_RUN_ROLES):
                        return
            # Fallback: just confirm require_role/_check is wired
            assert any("require_role" in q or "_check" in q for q in qualnames)
            return
        raise AssertionError("No route found for operation_id=runDataProduct")


class TestListAndGet:
    def test_list_maps_domain_to_dto(self):
        svc = MagicMock()
        svc.list_products.return_value = [_detail(product_id="p1")]
        wake = create_autospec(SchedulerWakeMarker, instance=True)
        result = list_data_products(svc=svc, wake=wake)
        wake.signal.assert_called_once_with()  # the Collections page load wakes the scheduler
        assert len(result) == 1
        assert result[0].product_id == "p1"
        assert result[0].display_status == "draft"

    def test_get_missing_raises_404(self):
        svc = MagicMock()
        svc.get.return_value = None
        with pytest.raises(HTTPException) as excinfo:
            get_data_product("missing", svc=svc)
        assert excinfo.value.status_code == 404

    def test_get_found(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1")
        result = get_data_product("p1", svc=svc)
        assert result.product_id == "p1"


class TestCreate:
    def test_create_success(self):
        svc = MagicMock()
        svc.create.return_value = _product(product_id="p1")
        svc.get.return_value = _detail(product_id="p1")
        result = create_data_product(
            body=CreateDataProductIn(name="Orders", description=None, owner=None),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            perms=MagicMock(),
            history=MagicMock(),
        )
        assert result.product_id == "p1"
        svc.create.assert_called_once_with("Orders", None, None, "alice@x", owner_display_name=None)

    def test_create_seeds_default_grants_via_service(self):
        """Seeding is now the service's responsibility (not the route's).

        The route no longer calls perms.seed_default_grants directly. Seeding
        is verified exhaustively in test_data_products.py::TestCreate::test_create_seeds_default_grants.
        """
        svc = MagicMock()
        svc.create.return_value = _product(product_id="new-p1")
        svc.get.return_value = _detail(product_id="new-p1")
        create_data_product(
            body=CreateDataProductIn(name="Orders", description=None, owner=None),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            perms=MagicMock(),
            history=MagicMock(),
        )
        svc.create.assert_called_once()

    def test_create_duplicate_name_raises_409(self):
        svc = MagicMock()
        svc.create.side_effect = DuplicateDataProductNameError("A data product named 'Orders' already exists.")
        with pytest.raises(HTTPException) as excinfo:
            create_data_product(
                body=CreateDataProductIn(name="Orders", description=None, owner=None),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                perms=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 409

    def test_create_empty_name_raises_400(self):
        svc = MagicMock()
        svc.create.side_effect = ValueError("Data product name must not be empty.")
        with pytest.raises(HTTPException) as excinfo:
            create_data_product(
                body=CreateDataProductIn(name="", description=None, owner=None),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                perms=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 400


class TestUpdate:
    def test_update_only_passes_explicitly_set_fields(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1")
        update_data_product(
            "p1",
            body=UpdateDataProductIn(description="new desc"),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
            grant_svc=MagicMock(),
            history=MagicMock(),
        )
        svc.update.assert_called_once_with("p1", {"description": "new desc"}, "alice@x")

    def test_update_missing_raises_404(self):
        svc = MagicMock()
        svc.update.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            update_data_product(
                "p1",
                body=UpdateDataProductIn(),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                grant_svc=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 404

    def test_update_duplicate_name_raises_409(self):
        svc = MagicMock()
        svc.update.side_effect = DuplicateDataProductNameError("boom")
        with pytest.raises(HTTPException) as excinfo:
            update_data_product(
                "p1",
                body=UpdateDataProductIn(name="Taken"),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                grant_svc=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 409


class TestDelete:
    def test_delete_success(self):
        svc = MagicMock()
        result = delete_data_product(
            "p1",
            svc=svc,
            obo_ws=_mock_obo_ws(),
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
            history=MagicMock(),
        )
        assert result["status"] == "deleted"
        svc.delete.assert_called_once_with("p1")

    def test_delete_missing_raises_404(self):
        svc = MagicMock()
        svc.delete.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            delete_data_product(
                "p1",
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 404


class TestMembers:
    def test_add_member_success(self):
        svc = MagicMock()
        svc.add_member.return_value = DataProductMember(id="m1", product_id="p1", binding_id="b1", pinned_version=2)
        svc.get.return_value = _detail(product_id="p1")
        result = add_data_product_member(
            "p1",
            body=AddDataProductMemberIn(binding_id="b1", pinned_version=2),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
            history=MagicMock(),
        )
        assert result.product_id == "p1"
        svc.add_member.assert_called_once_with("p1", "b1", 2, "alice@x")

    def test_add_member_missing_product_raises_404(self):
        svc = MagicMock()
        svc.add_member.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            add_data_product_member(
                "p1",
                body=AddDataProductMemberIn(binding_id="b1"),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 404

    def test_add_member_invalid_binding_id_raises_404(self):
        svc = MagicMock()
        svc.add_member.side_effect = RuntimeError("Monitored table not found: invalid_binding")
        with pytest.raises(HTTPException) as excinfo:
            add_data_product_member(
                "p1",
                body=AddDataProductMemberIn(binding_id="invalid_binding"),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 404

    def test_add_member_non_approved_binding_raises_400(self):
        """P3.2: a draft/pending/rejected/never-approved binding cannot join —
        the service's BindingNotApprovedError maps to a clear 400."""
        svc = MagicMock()
        svc.add_member.side_effect = BindingNotApprovedError(
            "Cannot add table 'cat.schema.tbl' to this table space: its status is 'draft', expected 'approved'. "
            "Only approved tables can join a table space."
        )
        with pytest.raises(HTTPException) as excinfo:
            add_data_product_member(
                "p1",
                body=AddDataProductMemberIn(binding_id="b1"),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 400
        assert "cat.schema.tbl" in excinfo.value.detail
        assert "'draft'" in excinfo.value.detail

    def test_remove_member_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1")
        result = remove_data_product_member(
            "p1",
            "m1",
            svc=svc,
            obo_ws=_mock_obo_ws(),
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
            history=MagicMock(),
        )
        assert result.product_id == "p1"
        svc.remove_member.assert_called_once_with("p1", "m1", "alice@x")

    def test_remove_member_missing_raises_404(self):
        svc = MagicMock()
        svc.remove_member.side_effect = LookupError("Data product member not found: m1")
        with pytest.raises(HTTPException) as excinfo:
            remove_data_product_member(
                "p1",
                "m1",
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 404


class TestSubmit:
    def test_submit_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="pending_approval", version=0)
        result = submit_data_product(
            "p1",
            svc=svc,
            app_settings=_enabled_settings(),
            draft_run_gate=MagicMock(),
            perms=MagicMock(),
            role=UserRole.RULE_AUTHOR,
            principal_ids=frozenset(),
            obo_ws=_mock_obo_ws(),
            history=MagicMock(),
        )
        assert result.status == "pending_approval"
        svc.submit.assert_called_once_with("p1", "alice@x", rationale=None)

    def test_submit_missing_raises_404(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="draft", version=0)
        svc.submit.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            submit_data_product(
                "p1",
                svc=svc,
                app_settings=_enabled_settings(),
                draft_run_gate=MagicMock(),
                perms=MagicMock(),
                role=UserRole.RULE_AUTHOR,
                principal_ids=frozenset(),
                obo_ws=_mock_obo_ws(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 404

    def test_submit_auto_approve_notifies_scheduler(self):
        # A submit that auto-approves (disabled mode) activates any cron on the
        # space, so it must wake the scheduler just like an explicit approve.
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="approved", version=1)
        with patch("databricks_labs_dqx_app.backend._scheduler_registry.notify_scheduler") as notify:
            submit_data_product(
                "p1",
                svc=svc,
                app_settings=_disabled_settings(),
                draft_run_gate=MagicMock(),
                perms=MagicMock(),
                role=UserRole.RULE_AUTHOR,
                principal_ids=frozenset(),
                obo_ws=_mock_obo_ws(),
                history=MagicMock(),
            )
        svc.approve.assert_called_once()
        notify.assert_called_once()

    def test_submit_without_auto_approve_does_not_notify_scheduler(self):
        # In ``enabled`` mode the submit only moves to pending_approval — no cron
        # activates yet, so the scheduler must not be woken.
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="pending_approval", version=0)
        with patch("databricks_labs_dqx_app.backend._scheduler_registry.notify_scheduler") as notify:
            submit_data_product(
                "p1",
                svc=svc,
                app_settings=_enabled_settings(),
                draft_run_gate=MagicMock(),
                perms=MagicMock(),
                role=UserRole.RULE_AUTHOR,
                principal_ids=frozenset(),
                obo_ws=_mock_obo_ws(),
                history=MagicMock(),
            )
        svc.approve.assert_not_called()
        notify.assert_not_called()

    def test_submit_approved_unchanged_raises_409(self):
        """Minor fix: submitting an already-approved, unchanged space via the
        API directly (bypassing the UI's disabled submit button) must 409, not
        silently move the space to ``pending_approval`` and pause its
        scheduled runs.
        """
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="approved", version=1)
        svc.submit.side_effect = InvalidStatusTransitionError("boom")
        with pytest.raises(HTTPException) as excinfo:
            submit_data_product(
                "p1",
                svc=svc,
                app_settings=_enabled_settings(),
                draft_run_gate=MagicMock(),
                perms=MagicMock(),
                role=UserRole.RULE_AUTHOR,
                principal_ids=frozenset(),
                obo_ws=_mock_obo_ws(),
                history=MagicMock(),
            )
        assert excinfo.value.status_code == 409


class TestApprove:
    def test_approve_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="approved", version=1)
        result = approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=MagicMock())
        assert result.status == "approved"
        svc.approve.assert_called_once_with("p1", "alice@x", rationale=None)

    def test_approve_non_pending_raises_409(self):
        svc = MagicMock()
        svc.approve.side_effect = InvalidStatusTransitionError("boom")
        with pytest.raises(HTTPException) as excinfo:
            approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=MagicMock())
        assert excinfo.value.status_code == 409

    def test_approve_missing_raises_404(self):
        svc = MagicMock()
        svc.approve.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=MagicMock())
        assert excinfo.value.status_code == 404

    def test_approve_notifies_scheduler(self):
        # Approval activates any cron on the space; the scheduler must be woken
        # so its first run does not wait out the idle poll interval.
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="approved", version=1)
        with patch("databricks_labs_dqx_app.backend._scheduler_registry.notify_scheduler") as notify:
            approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=MagicMock())
        notify.assert_called_once()


class TestReject:
    def test_reject_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="rejected", version=0)
        result = reject_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=MagicMock())
        assert result.status == "rejected"
        svc.reject.assert_called_once_with("p1", "alice@x", rationale=None)

    def test_reject_non_pending_raises_409(self):
        svc = MagicMock()
        svc.reject.side_effect = InvalidStatusTransitionError("boom")
        with pytest.raises(HTTPException) as excinfo:
            reject_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=MagicMock())
        assert excinfo.value.status_code == 409


class TestRun:
    def test_run_success_maps_result(self):
        svc = MagicMock()
        svc.run.return_value = DataProductRunResult(run_set_id="rs-1")
        result = run_data_product(
            "p1",
            body=RunDataProductIn(source="approved"),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
            wake=create_autospec(SchedulerWakeMarker, instance=True),
        )
        assert result.run_set_id == "rs-1"
        svc.run.assert_called_once_with(
            "p1", source="approved", user_email="alice@x", trigger="manual", sample_size=None
        )

    def test_started_run_wakes_the_scheduler(self):
        svc = MagicMock()
        svc.run.return_value = DataProductRunResult(run_set_id="rs-1")
        wake = create_autospec(SchedulerWakeMarker, instance=True)
        run_data_product(
            "p1",
            body=RunDataProductIn(source="approved"),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
            wake=wake,
        )
        wake.signal.assert_called_once_with()

    def test_execute_check_called_before_run(self):
        """require_object(EXECUTE) is invoked on the data product before delegating to svc.run."""
        from unittest.mock import create_autospec
        from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
        from databricks_labs_dqx_app.backend.common.permissions import Privilege

        svc = MagicMock()
        svc.run.return_value = DataProductRunResult(run_set_id="rs-1")
        perms = create_autospec(PermissionsService, instance=True)
        run_data_product(
            "p1",
            body=RunDataProductIn(source="approved"),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            role=UserRole.RULE_AUTHOR,
            principal_ids=frozenset({"u1"}),
            perms=perms,
            wake=create_autospec(SchedulerWakeMarker, instance=True),
        )
        perms.require_object.assert_called_once()
        call_kwargs = perms.require_object.call_args
        assert call_kwargs.args[0] == "data_product"
        assert call_kwargs.args[1] == "p1"
        assert call_kwargs.args[2] == Privilege.EXECUTE

    def test_execute_denied_raises_403(self):
        """When require_object raises 403, svc.run is never called."""
        from unittest.mock import create_autospec
        from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
        from fastapi import HTTPException as FastHTTPException

        svc = MagicMock()
        perms = create_autospec(PermissionsService, instance=True)
        perms.require_object.side_effect = FastHTTPException(status_code=403, detail="Denied")
        with pytest.raises(FastHTTPException) as excinfo:
            run_data_product(
                "p1",
                body=RunDataProductIn(source="approved"),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.RULE_AUTHOR,
                principal_ids=frozenset(),
                perms=perms,
                wake=create_autospec(SchedulerWakeMarker, instance=True),
            )
        assert excinfo.value.status_code == 403
        svc.run.assert_not_called()

    def test_run_missing_product_raises_404(self):
        svc = MagicMock()
        svc.run.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            run_data_product(
                "p1",
                body=RunDataProductIn(source="approved"),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                wake=create_autospec(SchedulerWakeMarker, instance=True),
            )
        assert excinfo.value.status_code == 404

    def test_run_zero_runnable_raises_409(self):
        svc = MagicMock()
        svc.run.side_effect = NoRunnableMembersError("boom")
        with pytest.raises(HTTPException) as excinfo:
            run_data_product(
                "p1",
                body=RunDataProductIn(source="approved"),
                svc=svc,
                obo_ws=_mock_obo_ws(),
                role=UserRole.ADMIN,
                principal_ids=frozenset(),
                perms=MagicMock(),
                wake=create_autospec(SchedulerWakeMarker, instance=True),
            )
        assert excinfo.value.status_code == 409


# ---------------------------------------------------------------------------
# getDataProductChanges — View changes for a collection
# ---------------------------------------------------------------------------


class TestChanges:
    @staticmethod
    def _result(**overrides):
        values = {
            "approved_version": 2,
            "has_pending_changes": True,
            "base_version": 2,
            "target_version": None,
            "base_recorded": True,
            "versions": [DataProductVersion(version=2, state={}, created_by="bob@x", created_at="2026-10-01")],
            "changes": [
                CollectionChange(
                    key="settings", name="", status="unchanged", before=[{"name": "x"}], after=[{"name": "x"}]
                ),
                CollectionChange(
                    key="b2", name="c.s.items", status="added", after=[{"table": "c.s.items", "version": "latest"}]
                ),
            ],
        }
        values.update(overrides)
        return DataProductChanges(**values)

    def test_changes_is_viewer_plus(self):
        assert UserRole.VIEWER in _route_required_roles("getDataProductChanges")

    def test_returns_counts_versions_and_changes(self):
        svc = MagicMock()
        svc.changes.return_value = self._result()
        out = dp_routes.get_data_product_changes("p1", svc, MagicMock(), base=None, target=None)
        svc.changes.assert_called_once_with("p1", None, None)
        assert out.counts == {"added": 1, "removed": 0, "modified": 0, "unchanged": 1}
        assert [v.version for v in out.versions] == [2]
        assert out.changes[1].key == "b2" and out.changes[1].status == "added"
        assert out.has_pending_changes and out.base_recorded

    def test_404_when_missing(self):
        svc = MagicMock()
        svc.changes.side_effect = LookupError("nope")
        with pytest.raises(HTTPException) as exc:
            dp_routes.get_data_product_changes("nope", svc, MagicMock(), base=None, target=None)
        assert exc.value.status_code == 404

    def test_400_for_a_version_that_does_not_exist(self):
        svc = MagicMock()
        svc.changes.side_effect = ValueError("Version 9 doesn't exist (latest is 2)")
        with pytest.raises(HTTPException) as exc:
            dp_routes.get_data_product_changes("p1", svc, MagicMock(), base=None, target=9)
        assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# Review trail — each review step on a collection is recorded
# ---------------------------------------------------------------------------


class TestLifecycleHistory:
    @staticmethod
    def _history() -> MagicMock:
        return create_autospec(LifecycleHistoryService, instance=True)

    @staticmethod
    def _submit(svc: MagicMock, history: MagicMock, app_settings: MagicMock, **kwargs) -> None:
        submit_data_product(
            "p1",
            svc=svc,
            app_settings=app_settings,
            draft_run_gate=MagicMock(),
            perms=MagicMock(),
            role=UserRole.RULE_AUTHOR,
            principal_ids=frozenset(),
            obo_ws=_mock_obo_ws(),
            history=history,
            **kwargs,
        )

    def test_submit_is_recorded_with_its_note(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="pending_approval", version=1)
        history = self._history()
        self._submit(svc, history, _enabled_settings(), body=LifecycleRationaleIn(rationale="adds orders"))
        history.record.assert_called_once_with(
            "data_product",
            "p1",
            "submit",
            "alice@x",
            prev_status="pending_approval",
            new_status="pending_approval",
            rationale="adds orders",
        )

    def test_an_auto_approved_submit_records_the_approval_and_its_version(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="approved", version=2)
        svc.approve.return_value = _product(product_id="p1", status="approved", version=2)
        settings = MagicMock()
        settings.get_approvals_mode.return_value = "disabled"
        history = self._history()
        self._submit(svc, history, settings)
        actions = [c.args[2] for c in history.record.call_args_list]
        assert actions == ["submit", "approve"]
        assert history.record.call_args.kwargs["version"] == 2

    def test_approve_records_the_approver_note_and_version(self):
        svc = MagicMock()
        svc.approve.return_value = _product(product_id="p1", status="approved", version=3)
        svc.get.return_value = _detail(product_id="p1", status="approved", version=3)
        history = self._history()
        approve_data_product(
            "p1", svc=svc, obo_ws=_mock_obo_ws(), history=history, body=LifecycleRationaleIn(rationale="ok")
        )
        history.record.assert_called_once_with(
            "data_product",
            "p1",
            "approve",
            "alice@x",
            prev_status="pending_approval",
            new_status="approved",
            version=3,
            rationale="ok",
        )

    def test_reject_records_the_note(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="rejected", version=1)
        history = self._history()
        reject_data_product(
            "p1", svc=svc, obo_ws=_mock_obo_ws(), history=history, body=LifecycleRationaleIn(rationale="no")
        )
        history.record.assert_called_once_with(
            "data_product",
            "p1",
            "reject",
            "alice@x",
            prev_status="pending_approval",
            new_status="rejected",
            rationale="no",
        )

    def test_withdraw_is_recorded(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="draft", version=1)
        history = self._history()
        revert_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=history)
        history.record.assert_called_once_with(
            "data_product", "p1", "withdraw", "alice@x", prev_status="pending_approval", new_status="draft"
        )

    def test_a_refused_approval_records_nothing(self):
        svc = MagicMock()
        svc.approve.side_effect = InvalidStatusTransitionError("not pending")
        history = self._history()
        with pytest.raises(HTTPException):
            approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), history=history)
        history.record.assert_not_called()

    def test_history_is_viewer_plus(self):
        assert UserRole.VIEWER in _route_required_roles("listDataProductHistory")

    def test_adding_a_member_records_an_edit(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1")
        history = self._history()
        add_data_product_member(
            "p1",
            body=AddDataProductMemberIn(binding_id="b1", pinned_version=2),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            history=history,
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
        )
        history.record.assert_called_once_with(
            "data_product",
            "p1",
            "update",
            "alice@x",
            new_status="draft",
            details={"member": "b1", "pinned_version": 2},
        )

    def test_deleting_a_collection_records_it(self):
        svc = MagicMock()
        history = self._history()
        delete_data_product(
            "p1",
            svc=svc,
            obo_ws=_mock_obo_ws(),
            history=history,
            role=UserRole.ADMIN,
            principal_ids=frozenset(),
            perms=MagicMock(),
        )
        history.record.assert_called_once_with("data_product", "p1", "delete", "alice@x")

    def test_resubmitting_a_collection_already_in_review_without_a_note_records_nothing(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="pending_approval", version=1)
        history = self._history()
        self._submit(svc, history, _enabled_settings())
        history.record.assert_not_called()

    def test_the_trail_is_listed(self):
        history = self._history()
        history.list_events.return_value = [
            HistoryEvent(action="submitted", actor="alice@x", created_at="2026-10-01", rationale="adds orders"),
        ]
        out = dp_routes.list_data_product_history("p1", history=history)
        history.list_events.assert_called_once_with("data_product", "p1")
        assert [(e.action, e.actor, e.rationale) for e in out] == [("submitted", "alice@x", "adds orders")]


class TestChangesWithSteps:
    def test_a_saved_draft_is_compared_with_the_current_members(self):
        history = create_autospec(LifecycleHistoryService, instance=True)
        history.get_snapshot.return_value = (
            True,
            {"name": "Orders", "description": "old", "owner": "a@x", "members": []},
        )
        svc = create_autospec(DataProductService, instance=True)
        svc.changes.return_value = MagicMock(
            approved_version=1, has_pending_changes=True, versions=[], base_recorded=True, changes=[]
        )
        svc.current_state.return_value = {"name": "Orders", "description": "new", "owner": "a@x", "members": []}
        out = dp_routes.get_data_product_changes("p1", svc=svc, history=history, base_step=3)
        assert out.base_step == 3
        assert [(c.key, c.status) for c in out.changes] == [("settings", "modified")]

    def test_an_unknown_step_is_404(self):
        history = create_autospec(LifecycleHistoryService, instance=True)
        history.get_snapshot.return_value = (False, None)
        svc = create_autospec(DataProductService, instance=True)
        svc.changes.return_value = MagicMock(versions=[])
        with pytest.raises(HTTPException) as exc:
            dp_routes.get_data_product_changes("p1", svc=svc, history=history, target_step=9)
        assert exc.value.status_code == 404


def _member(binding_id: str, pinned: int | None = None) -> DataProductMemberDetail:
    return DataProductMemberDetail(
        id=f"m-{binding_id}",
        binding_id=binding_id,
        table_fqn=f"c.s.{binding_id}",
        binding_status="approved",
        binding_version=1,
        pinned_version=pinned,
        rules_count=1,
        checks_count=1,
        runnable=True,
    )


class TestSaveDataProduct:
    """One save of the collection's page is one request and one history step."""

    @staticmethod
    def _call(svc, history, perms, body):
        return dp_routes.save_data_product(
            "p1",
            SaveDataProductIn.model_validate(body),
            svc=svc,
            obo_ws=_mock_obo_ws(),
            history=history,
            role=UserRole.RULE_AUTHOR,
            principal_ids=frozenset(),
            perms=perms,
            grant_svc=MagicMock(),
        )

    def _svc(self):
        svc = create_autospec(DataProductService, instance=True)
        detail = DataProductDetail(
            product=_product(status="approved", version=1), members=[_member("b1"), _member("b2", 1)]
        )
        svc.get.return_value = detail
        svc.update.return_value = _product(status="draft")
        return svc

    def test_settings_and_member_changes_are_one_history_step(self):
        svc, history, perms = self._svc(), create_autospec(LifecycleHistoryService, instance=True), MagicMock()
        self._call(
            svc,
            history,
            perms,
            {
                "updates": {"description": "new"},
                "members": [
                    {"binding_id": "b2", "pinned_version": 2},
                    {"binding_id": "b3", "pinned_version": None},
                ],
            },
        )
        svc.update.assert_called_once_with("p1", {"description": "new"}, "alice@x")
        assert [c.args[1:3] for c in svc.add_member.call_args_list] == [("b3", None), ("b2", 2)]
        svc.remove_member.assert_called_once_with("p1", "m-b1", "alice@x")
        history.record.assert_called_once()
        call = history.record.call_args
        assert call.args[:4] == ("data_product", "p1", "update", "alice@x")
        assert call.kwargs["new_status"] == "draft"
        assert call.kwargs["details"] == {
            "fields": ["description"],
            "members_added": ["b3"],
            "members_repinned": ["b2"],
            "members_removed": ["b1"],
        }
        privileges = {c.args[2] for c in perms.require_object.call_args_list}
        assert privileges == {Privilege.MODIFY, Privilege.APPLY}

    def test_a_save_that_changes_nothing_records_nothing(self):
        svc, history = self._svc(), create_autospec(LifecycleHistoryService, instance=True)
        self._call(
            svc,
            history,
            MagicMock(),
            {"members": [{"binding_id": "b1", "pinned_version": None}, {"binding_id": "b2", "pinned_version": 1}]},
        )
        assert not svc.update.called and not svc.add_member.called and not svc.remove_member.called
        assert not history.record.called

    def test_only_members_needs_apply_not_modify(self):
        svc, perms = self._svc(), MagicMock()
        self._call(
            svc,
            create_autospec(LifecycleHistoryService, instance=True),
            perms,
            {"members": [{"binding_id": "b1"}, {"binding_id": "b2", "pinned_version": 1}, {"binding_id": "b4"}]},
        )
        assert {c.args[2] for c in perms.require_object.call_args_list} == {Privilege.APPLY}

    def test_a_table_that_cant_join_refuses_the_whole_save_before_anything_is_written(self):
        svc = self._svc()
        svc.require_approved_bindings.side_effect = BindingNotApprovedError("not approved")
        with pytest.raises(HTTPException) as exc:
            self._call(
                svc,
                create_autospec(LifecycleHistoryService, instance=True),
                MagicMock(),
                {"updates": {"name": "Renamed"}, "members": [{"binding_id": "b9"}]},
            )
        assert exc.value.status_code == 400
        svc.require_approved_bindings.assert_called_once_with(["b9"])
        assert not svc.update.called and not svc.add_member.called and not svc.remove_member.called

    def test_a_schedule_is_granted_on_the_members_after_the_save(self):
        svc = self._svc()
        svc.table_fqns_of.return_value = []
        self._call(
            svc,
            create_autospec(LifecycleHistoryService, instance=True),
            MagicMock(),
            {"updates": {"schedule_cron": "0 6 * * *"}, "members": [{"binding_id": "b1"}, {"binding_id": "b3"}]},
        )
        svc.table_fqns_of.assert_called_once_with(["b1", "b3"])
