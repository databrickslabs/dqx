from typing import Annotated

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User as UserOut
from fastapi import APIRouter, Depends

from databricks_labs_dqx_app.backend.common.authorization import (
    CurrentUser,
    get_permissions_for_role,
)
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentUserRole,
    get_obo_ws,
)
from databricks_labs_dqx_app.backend.models import UserRoleOut, VersionOut

router = APIRouter()


@router.get("/version", response_model=VersionOut, operation_id="getVersion")
async def version():
    return VersionOut.from_metadata()


@router.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return obo_ws.current_user.me()


@router.get("/current-user/role", response_model=UserRoleOut, operation_id="currentUserRole")
def me_role(email: CurrentUser, role: CurrentUserRole):
    permissions = list(get_permissions_for_role(role))
    return UserRoleOut(
        email=email,
        role=role.value,
        permissions=permissions,
        # Backward-compat: frontend reads is_runner to decide whether to show
        # run controls. Derive it from run_rules in the permission list so
        # existing clients work without a model change.
        is_runner=("run_rules" in permissions),
    )
