from typing import Annotated

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User as UserOut
from fastapi import APIRouter, Depends

from databricks_labs_dqx_app.backend.common.authorization import CurrentUser, get_permissions_for_role
from databricks_labs_dqx_app.backend.dependencies import CurrentUserRole, get_obo_ws
from databricks_labs_dqx_app.backend.models import UserRoleOut, VersionOut

router = APIRouter()


@router.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@router.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return obo_ws.current_user.me()


@router.get("/current-user/role", response_model=UserRoleOut, operation_id="currentUserRole")
def me_role(email: CurrentUser, role: CurrentUserRole):
    return UserRoleOut(
        email=email,
        role=role.value,
        permissions=get_permissions_for_role(role),
    )
