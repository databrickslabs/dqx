"""Tests for the ``/data-products`` route handlers (Data Products Task 4).

Follows ``test_run_sets_routes.py``'s / ``test_monitored_tables_routes.py``'s
convention: call the route functions directly with a mocked
``DataProductService`` rather than spinning up a FastAPI ``TestClient``, and
assert the RBAC gate structurally (VIEWER+ for reads, RULE_AUTHOR+ for
writes, the orthogonal RUNNER gate for run) without going through
middleware.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.models import (
    AddDataProductMemberIn,
    CreateDataProductIn,
    RunDataProductIn,
    UpdateDataProductIn,
)
from databricks_labs_dqx_app.backend.registry_models import DataProduct, DataProductMember
from databricks_labs_dqx_app.backend.routes.v1 import data_products as dp_routes
from databricks_labs_dqx_app.backend.routes.v1.data_products import (
    add_data_product_member,
    approve_data_product,
    create_data_product,
    delete_data_product,
    get_data_product,
    list_data_products,
    reject_data_product,
    remove_data_product_member,
    run_data_product,
    submit_data_product,
    update_data_product,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    BindingNotApprovedError,
    DataProductDetail,
    DataProductRunResult,
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

    def test_run_uses_orthogonal_runner_gate_not_require_role(self):
        for route in dp_routes.router.routes:
            if getattr(route, "operation_id", None) != "runDataProduct":
                continue
            qualnames = {getattr(dep.dependency, "__qualname__", "") for dep in route.dependencies}
            assert any("require_runner" in q for q in qualnames)
            return
        raise AssertionError("No route found for operation_id=runDataProduct")


class TestListAndGet:
    def test_list_maps_domain_to_dto(self):
        svc = MagicMock()
        svc.list_products.return_value = [_detail(product_id="p1")]
        result = list_data_products(svc=svc)
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
            body=CreateDataProductIn(name="Orders", description=None, steward=None),
            svc=svc,
            obo_ws=_mock_obo_ws(),
        )
        assert result.product_id == "p1"
        svc.create.assert_called_once_with("Orders", None, None, "alice@x")

    def test_create_duplicate_name_raises_409(self):
        svc = MagicMock()
        svc.create.side_effect = DuplicateDataProductNameError("A data product named 'Orders' already exists.")
        with pytest.raises(HTTPException) as excinfo:
            create_data_product(
                body=CreateDataProductIn(name="Orders", description=None, steward=None),
                svc=svc,
                obo_ws=_mock_obo_ws(),
            )
        assert excinfo.value.status_code == 409

    def test_create_empty_name_raises_400(self):
        svc = MagicMock()
        svc.create.side_effect = ValueError("Data product name must not be empty.")
        with pytest.raises(HTTPException) as excinfo:
            create_data_product(
                body=CreateDataProductIn(name="", description=None, steward=None),
                svc=svc,
                obo_ws=_mock_obo_ws(),
            )
        assert excinfo.value.status_code == 400


class TestUpdate:
    def test_update_only_passes_explicitly_set_fields(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1")
        update_data_product("p1", body=UpdateDataProductIn(description="new desc"), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        svc.update.assert_called_once_with("p1", {"description": "new desc"}, "alice@x")

    def test_update_missing_raises_404(self):
        svc = MagicMock()
        svc.update.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            update_data_product("p1", body=UpdateDataProductIn(), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404

    def test_update_duplicate_name_raises_409(self):
        svc = MagicMock()
        svc.update.side_effect = DuplicateDataProductNameError("boom")
        with pytest.raises(HTTPException) as excinfo:
            update_data_product("p1", body=UpdateDataProductIn(name="Taken"), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 409


class TestDelete:
    def test_delete_success(self):
        svc = MagicMock()
        result = delete_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result["status"] == "deleted"
        svc.delete.assert_called_once_with("p1")

    def test_delete_missing_raises_404(self):
        svc = MagicMock()
        svc.delete.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            delete_data_product("p1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


class TestMembers:
    def test_add_member_success(self):
        svc = MagicMock()
        svc.add_member.return_value = DataProductMember(id="m1", product_id="p1", binding_id="b1", pinned_version=2)
        svc.get.return_value = _detail(product_id="p1")
        result = add_data_product_member(
            "p1", body=AddDataProductMemberIn(binding_id="b1", pinned_version=2), svc=svc, obo_ws=_mock_obo_ws()
        , role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result.product_id == "p1"
        svc.add_member.assert_called_once_with("p1", "b1", 2, "alice@x")

    def test_add_member_missing_product_raises_404(self):
        svc = MagicMock()
        svc.add_member.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            add_data_product_member("p1", body=AddDataProductMemberIn(binding_id="b1"), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404

    def test_add_member_invalid_binding_id_raises_404(self):
        svc = MagicMock()
        svc.add_member.side_effect = RuntimeError("Monitored table not found: invalid_binding")
        with pytest.raises(HTTPException) as excinfo:
            add_data_product_member("p1", body=AddDataProductMemberIn(binding_id="invalid_binding"), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
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
            add_data_product_member("p1", body=AddDataProductMemberIn(binding_id="b1"), svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 400
        assert "cat.schema.tbl" in excinfo.value.detail
        assert "'draft'" in excinfo.value.detail

    def test_remove_member_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1")
        result = remove_data_product_member("p1", "m1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert result.product_id == "p1"
        svc.remove_member.assert_called_once_with("p1", "m1", "alice@x")

    def test_remove_member_missing_raises_404(self):
        svc = MagicMock()
        svc.remove_member.side_effect = LookupError("Data product member not found: m1")
        with pytest.raises(HTTPException) as excinfo:
            remove_data_product_member("p1", "m1", svc=svc, obo_ws=_mock_obo_ws(), role=UserRole.ADMIN, principal_ids=frozenset(), perms=MagicMock())
        assert excinfo.value.status_code == 404


class TestSubmit:
    def test_submit_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="pending_approval", version=0)
        result = submit_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "pending_approval"
        svc.submit.assert_called_once_with("p1", "alice@x")

    def test_submit_missing_raises_404(self):
        svc = MagicMock()
        svc.submit.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            submit_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404

    def test_submit_approved_unchanged_raises_409(self):
        """Minor fix: submitting an already-approved, unchanged space via the
        API directly (bypassing the UI's disabled submit button) must 409, not
        silently move the space to ``pending_approval`` and pause its
        scheduled runs.
        """
        svc = MagicMock()
        svc.submit.side_effect = InvalidStatusTransitionError("boom")
        with pytest.raises(HTTPException) as excinfo:
            submit_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 409


class TestApprove:
    def test_approve_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="approved", version=1)
        result = approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "approved"
        svc.approve.assert_called_once_with("p1", "alice@x")

    def test_approve_non_pending_raises_409(self):
        svc = MagicMock()
        svc.approve.side_effect = InvalidStatusTransitionError("boom")
        with pytest.raises(HTTPException) as excinfo:
            approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 409

    def test_approve_missing_raises_404(self):
        svc = MagicMock()
        svc.approve.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            approve_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404


class TestReject:
    def test_reject_success(self):
        svc = MagicMock()
        svc.get.return_value = _detail(product_id="p1", status="rejected", version=0)
        result = reject_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert result.status == "rejected"
        svc.reject.assert_called_once_with("p1", "alice@x")

    def test_reject_non_pending_raises_409(self):
        svc = MagicMock()
        svc.reject.side_effect = InvalidStatusTransitionError("boom")
        with pytest.raises(HTTPException) as excinfo:
            reject_data_product("p1", svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 409


class TestRun:
    def test_run_success_maps_result(self):
        svc = MagicMock()
        svc.run.return_value = DataProductRunResult(run_set_id="rs-1")
        result = run_data_product("p1", body=RunDataProductIn(source="approved"), svc=svc, obo_ws=_mock_obo_ws())
        assert result.run_set_id == "rs-1"
        svc.run.assert_called_once_with("p1", source="approved", user_email="alice@x", trigger="manual")

    def test_run_missing_product_raises_404(self):
        svc = MagicMock()
        svc.run.side_effect = LookupError("Data product not found: p1")
        with pytest.raises(HTTPException) as excinfo:
            run_data_product("p1", body=RunDataProductIn(source="approved"), svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 404

    def test_run_zero_runnable_raises_409(self):
        svc = MagicMock()
        svc.run.side_effect = NoRunnableMembersError("boom")
        with pytest.raises(HTTPException) as excinfo:
            run_data_product("p1", body=RunDataProductIn(source="approved"), svc=svc, obo_ws=_mock_obo_ws())
        assert excinfo.value.status_code == 409
