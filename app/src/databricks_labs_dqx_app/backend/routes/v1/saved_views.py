"""Saved views API: named column/filter presets per page."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Annotated, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import CurrentUserRole, get_saved_views_service, require_role
from databricks_labs_dqx_app.backend.saved_views_models import (
    SavedViewCreateIn,
    SavedViewListOut,
    SavedViewMyDefaultIn,
    SavedViewOrgDefaultIn,
    SavedViewOut,
    SavedViewPage,
    SavedViewRenameIn,
    SavedViewUpdateIn,
)
from databricks_labs_dqx_app.backend.services.saved_views_service import (
    SavedViewConflictError,
    SavedViewForbiddenError,
    SavedViewNotFoundError,
    SavedViewsService,
    SavedViewValidationError,
)

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_ROLES = [require_role(*_ALL_ROLES)]

ServiceDep = Annotated[SavedViewsService, Depends(get_saved_views_service)]
EmailDep = Annotated[str, Depends(get_user_email)]

_T = TypeVar("_T")


@contextmanager
def _mapped_errors() -> Iterator[None]:
    """Translate service errors into HTTP errors with fixed messages."""
    try:
        yield
    except SavedViewNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Saved view not found") from exc
    except SavedViewForbiddenError as exc:
        raise HTTPException(status_code=403, detail="You do not have permission to do that") from exc
    except SavedViewConflictError as exc:
        raise HTTPException(status_code=409, detail="A view with that name already exists") from exc
    except SavedViewValidationError as exc:
        raise HTTPException(status_code=422, detail="The saved view request is not valid") from exc


def _call(action: Callable[[], _T]) -> _T:
    with _mapped_errors():
        return action()


@router.get("", response_model=SavedViewListOut, operation_id="listSavedViews", dependencies=_ROLES)
def list_saved_views(
    svc: ServiceDep,
    email: EmailDep,
    role: CurrentUserRole,
    page: Annotated[SavedViewPage, Query(description="Page key the views belong to")],
) -> SavedViewListOut:
    """List the org views and the caller's personal views for a page."""
    return _call(lambda: svc.list_views(page, user=email, is_admin=role == UserRole.ADMIN))


@router.post("", response_model=SavedViewOut, operation_id="createSavedView", dependencies=_ROLES)
def create_saved_view(body: SavedViewCreateIn, svc: ServiceDep, email: EmailDep, role: CurrentUserRole) -> SavedViewOut:
    """Create a saved view (org scope is admin-only)."""
    return _call(lambda: svc.create_view(body, user=email, is_admin=role == UserRole.ADMIN))


# Static /defaults/* routes are declared before the /{view_id} routes.
@router.put("/defaults/me", response_model=SavedViewListOut, operation_id="setMySavedViewDefault", dependencies=_ROLES)
def set_my_saved_view_default(
    body: SavedViewMyDefaultIn, svc: ServiceDep, email: EmailDep, role: CurrentUserRole
) -> SavedViewListOut:
    """Set or clear the caller's personal default view for a page."""
    return _call(lambda: svc.set_my_default(body.page, body.view_id, user=email, is_admin=role == UserRole.ADMIN))


@router.put(
    "/defaults/org",
    response_model=SavedViewListOut,
    operation_id="setOrgSavedViewDefault",
    dependencies=[require_role(UserRole.ADMIN)],
)
def set_org_saved_view_default(
    body: SavedViewOrgDefaultIn, svc: ServiceDep, email: EmailDep, role: CurrentUserRole
) -> SavedViewListOut:
    """Make an org view the org default for a page (admin only)."""
    return _call(lambda: svc.set_org_default(body.page, body.view_id, user=email, is_admin=role == UserRole.ADMIN))


@router.put("/{view_id}", response_model=SavedViewOut, operation_id="updateSavedView", dependencies=_ROLES)
def update_saved_view(
    view_id: str, body: SavedViewUpdateIn, svc: ServiceDep, email: EmailDep, role: CurrentUserRole
) -> SavedViewOut:
    """Replace the payload of a saved view."""
    return _call(lambda: svc.update_view(view_id, body.payload, user=email, is_admin=role == UserRole.ADMIN))


@router.patch("/{view_id}", response_model=SavedViewOut, operation_id="renameSavedView", dependencies=_ROLES)
def rename_saved_view(
    view_id: str, body: SavedViewRenameIn, svc: ServiceDep, email: EmailDep, role: CurrentUserRole
) -> SavedViewOut:
    """Rename a saved view."""
    return _call(lambda: svc.rename_view(view_id, body.name, user=email, is_admin=role == UserRole.ADMIN))


@router.delete("/{view_id}", status_code=204, operation_id="deleteSavedView", dependencies=_ROLES)
def delete_saved_view(view_id: str, svc: ServiceDep, email: EmailDep, role: CurrentUserRole) -> Response:
    """Delete a saved view."""
    _call(lambda: svc.delete_view(view_id, user=email, is_admin=role == UserRole.ADMIN))
    return Response(status_code=204)


@router.put("/{view_id}/star", response_model=SavedViewOut, operation_id="starSavedView", dependencies=_ROLES)
def star_saved_view(view_id: str, svc: ServiceDep, email: EmailDep, role: CurrentUserRole) -> SavedViewOut:
    """Star a saved view for the caller."""
    return _call(lambda: svc.set_starred(view_id, True, user=email, is_admin=role == UserRole.ADMIN))


@router.delete("/{view_id}/star", response_model=SavedViewOut, operation_id="unstarSavedView", dependencies=_ROLES)
def unstar_saved_view(view_id: str, svc: ServiceDep, email: EmailDep, role: CurrentUserRole) -> SavedViewOut:
    """Remove the caller's star from a saved view."""
    return _call(lambda: svc.set_starred(view_id, False, user=email, is_admin=role == UserRole.ADMIN))
