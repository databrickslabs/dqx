"""Route tests for /api/v1/saved-views."""

from unittest.mock import create_autospec

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import get_saved_views_service, get_user_role
from databricks_labs_dqx_app.backend.routes.v1.saved_views import router
from databricks_labs_dqx_app.backend.saved_views_models import (
    SavedViewListOut,
    SavedViewOut,
    SavedViewPage,
    SavedViewScope,
)
from databricks_labs_dqx_app.backend.services.saved_views_service import (
    SavedViewConflictError,
    SavedViewForbiddenError,
    SavedViewNotFoundError,
    SavedViewsService,
    SavedViewValidationError,
)

PAYLOAD = {"v": 1, "filters": {"order": [], "visibility": {}}, "values": {}}


def _view(view_id: str = "v1") -> SavedViewOut:
    return SavedViewOut(
        id=view_id,
        page=SavedViewPage.MONITORED_TABLES,
        name="Mine",
        scope=SavedViewScope.USER,
        payload=None,
        is_builtin=False,
        is_org_default=False,
        is_my_default=False,
        starred=False,
        can_edit=True,
        can_set_org_default=False,
    )


def _listing() -> SavedViewListOut:
    return SavedViewListOut(views=[_view()], effective_default_id="v1")


@pytest.fixture
def svc() -> SavedViewsService:
    mock = create_autospec(SavedViewsService, instance=True)
    mock.list_views.return_value = _listing()
    mock.create_view.return_value = _view()
    mock.set_my_default.return_value = _listing()
    mock.set_org_default.return_value = _listing()
    return mock


def _client(svc: SavedViewsService, role: UserRole = UserRole.VIEWER) -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/saved-views")
    app.dependency_overrides[get_saved_views_service] = lambda: svc
    app.dependency_overrides[get_user_email] = lambda: "u@x"
    app.dependency_overrides[get_user_role] = lambda: role
    return TestClient(app)


def _create_body(**overrides: object) -> dict[str, object]:
    return {"page": "monitored_tables", "name": "Mine", "scope": "user", "payload": PAYLOAD, **overrides}


def test_list_returns_effective_default(svc):
    resp = _client(svc).get("/api/v1/saved-views", params={"page": "monitored_tables"})
    assert resp.status_code == 200
    assert resp.json()["effective_default_id"] == "v1"
    svc.list_views.assert_called_once_with(SavedViewPage.MONITORED_TABLES, user="u@x", is_admin=False)


def test_list_invalid_page_is_422(svc):
    assert _client(svc).get("/api/v1/saved-views", params={"page": "nope"}).status_code == 422


def test_admin_flag_is_passed_through(svc):
    _client(svc, UserRole.ADMIN).get("/api/v1/saved-views", params={"page": "collections"})
    svc.list_views.assert_called_once_with(SavedViewPage.COLLECTIONS, user="u@x", is_admin=True)


def test_create_ok(svc):
    resp = _client(svc).post("/api/v1/saved-views", json=_create_body())
    assert resp.status_code == 200
    assert resp.json()["id"] == "v1"


def test_non_admin_org_create_is_403(svc):
    svc.create_view.side_effect = SavedViewForbiddenError("secret db detail")
    resp = _client(svc).post("/api/v1/saved-views", json=_create_body(scope="org"))
    assert resp.status_code == 403
    assert "secret" not in resp.text


def test_create_duplicate_is_409(svc):
    svc.create_view.side_effect = SavedViewConflictError("dup")
    assert _client(svc).post("/api/v1/saved-views", json=_create_body()).status_code == 409


def test_update_foreign_view_is_404(svc):
    svc.update_view.side_effect = SavedViewNotFoundError("nope")
    resp = _client(svc).put("/api/v1/saved-views/other", json={"payload": PAYLOAD})
    assert resp.status_code == 404


def test_admin_update_builtin_is_422(svc):
    svc.update_view.side_effect = SavedViewValidationError("The Default view cannot be saved over")
    resp = _client(svc, UserRole.ADMIN).put("/api/v1/saved-views/builtin:monitored_tables", json={"payload": PAYLOAD})
    assert resp.status_code == 422


def test_rename_builtin_is_422(svc):
    svc.rename_view.side_effect = SavedViewValidationError("builtin")
    resp = _client(svc).patch("/api/v1/saved-views/builtin:collections", json={"name": "x"})
    assert resp.status_code == 422


def test_delete_builtin_is_422_even_for_admin(svc):
    svc.delete_view.side_effect = SavedViewValidationError("The Default view cannot be deleted")
    resp = _client(svc, UserRole.ADMIN).delete("/api/v1/saved-views/builtin:monitored_tables")
    assert resp.status_code == 422
    svc.delete_view.assert_called_once_with("builtin:monitored_tables", user="u@x", is_admin=True)


def test_delete_returns_204(svc):
    resp = _client(svc).delete("/api/v1/saved-views/v1")
    assert resp.status_code == 204
    svc.delete_view.assert_called_once_with("v1", user="u@x", is_admin=False)


def test_star_and_unstar(svc):
    svc.set_starred.return_value = _view()
    client = _client(svc)
    assert client.put("/api/v1/saved-views/v1/star").status_code == 200
    assert client.delete("/api/v1/saved-views/v1/star").status_code == 200
    assert [c.args[1] for c in svc.set_starred.call_args_list] == [True, False]


def test_my_default_accepts_null_view_id(svc):
    resp = _client(svc).put("/api/v1/saved-views/defaults/me", json={"page": "schedules", "view_id": None})
    assert resp.status_code == 200
    svc.set_my_default.assert_called_once_with(SavedViewPage.SCHEDULES, None, user="u@x", is_admin=False)


def test_defaults_route_not_shadowed_by_view_id(svc):
    resp = _client(svc).put("/api/v1/saved-views/defaults/me", json={"page": "schedules", "view_id": "v1"})
    assert resp.status_code == 200
    svc.update_view.assert_not_called()


def test_org_default_requires_admin(svc):
    body = {"page": "schedules", "view_id": "v1"}
    assert _client(svc).put("/api/v1/saved-views/defaults/org", json=body).status_code == 403
    assert _client(svc, UserRole.ADMIN).put("/api/v1/saved-views/defaults/org", json=body).status_code == 200
