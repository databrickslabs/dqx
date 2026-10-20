"""Pre-migration setup readiness and bootstrap reconciliation APIs."""

import asyncio
import json
import logging
from typing import Annotated

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, NotFound, PermissionDenied
from databricks.sdk.errors.base import DatabricksError
from fastapi import APIRouter, Depends, HTTPException, Request, status

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    SetupAccess,
    get_conf,
    get_obo_ws,
    get_setup_access,
    get_setup_configuration_store,
    get_setup_orchestrator,
    get_sp_ws,
    get_setup_sql_reader_factory,
    require_setup_admin,
    sanitize_setup_display,
)
from databricks_labs_dqx_app.backend.setup.audience import (
    ACCOUNT_USERS,
    BROAD_AUDIENCE,
    WORKSPACE_ADMINS,
    AudienceGroupRequiredError,
    BuiltInAudienceGroupError,
)
from databricks_labs_dqx_app.backend.setup.configuration import SetupChoices, SetupConfigurationStore, validate_choices
from databricks_labs_dqx_app.backend.setup.errors import (
    AppServicePrincipalChosenError,
    TaskRunnerJobUnavailableError,
    TaskRunnerManagedByDeploymentError,
)
from databricks_labs_dqx_app.backend.setup.job_manager import list_task_runner_principals
from databricks_labs_dqx_app.backend.setup.storage import InvalidCatalogNameError
from databricks_labs_dqx_app.backend.setup.models import (
    SetupApplyGrantsResponse,
    SetupConfigurationRequest,
    SetupGroupsResponse,
    SetupOverrideRequest,
    SetupReport,
    SetupStatusResponse,
    SetupStepId,
    TaskRunnerAccessResponse,
    TaskRunnerGrantsResponse,
    TaskRunnerPrincipalRequest,
    TaskRunnerPrincipalsResponse,
)
from databricks_labs_dqx_app.backend.setup.orchestrator import ReaderSqlFactory, SetupOrchestrator
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/status", response_model=SetupStatusResponse, operation_id="getSetupStatus")
async def get_setup_status(
    access: Annotated[SetupAccess, Depends(get_setup_access)],
    config: Annotated[AppConfig, Depends(get_conf)],
    request: Request,
) -> SetupStatusResponse:
    """Return readiness, the caller's bootstrap setup access and the resolved configuration."""
    orchestrator = getattr(request.app.state, "setup_orchestrator", None)
    report = setup_runtime.report()
    return SetupStatusResponse(
        report=report,
        can_manage=access.can_manage,
        admin_group=sanitize_setup_display(config.admin_group) or "",
        user_name=access.user_name or "",
        task_runner_job_id=setup_runtime.job_id if access.can_manage else None,
        configuration=orchestrator.configuration_view() if orchestrator is not None else None,
    )


def _error(status_code: int, code: str, detail: str | None = None) -> HTTPException:
    body = {"code": code} if detail is None else {"code": code, "detail": detail}
    return HTTPException(status_code=status_code, detail=body)


def _catalog_exists(reader_ws: WorkspaceClient, catalog: str) -> bool:
    """Whether the caller can see *catalog*; unexpected SDK failures raise HTTP 502."""
    try:
        reader_ws.catalogs.get(catalog)
    except (NotFound, PermissionDenied):
        return False
    except DatabricksError as error:
        logger.warning(f"Catalog existence check failed: {type(error).__name__}")
        raise _error(status.HTTP_502_BAD_GATEWAY, "catalog_check_failed") from None
    return True


def _group_exists(sp_ws: WorkspaceClient, group: str) -> bool:
    """Whether a workspace group named *group* exists; unexpected SDK failures raise HTTP 502."""
    try:
        matches = list(
            sp_ws.groups.list(
                filter=f"displayName eq {json.dumps(group, ensure_ascii=False)}", attributes="id,displayName"
            )
        )
    except NotFound:
        return False
    except DatabricksError as error:
        logger.warning(f"Group existence check failed: {type(error).__name__}")
        raise _error(status.HTTP_502_BAD_GATEWAY, "group_check_failed") from None
    return any((match.display_name or "").casefold() == group.casefold() for match in matches)


MAX_LISTED_GROUPS = 1000
_BUILT_IN_GROUP_NAMES = frozenset({BROAD_AUDIENCE, ACCOUNT_USERS, WORKSPACE_ADMINS})


@router.get("/groups", response_model=SetupGroupsResponse, operation_id="listSetupGroups")
async def list_setup_groups(
    _access: Annotated[SetupAccess, require_setup_admin()],
    config: Annotated[AppConfig, Depends(get_conf)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
) -> SetupGroupsResponse:
    """List workspace groups for the default user group picker, without the built-in ones.

    Listed as the app, like the group existence check: no user token scope covers groups.
    A deployment that supplies storage also supplies the group, so there is nothing to choose.
    """
    if config.has_deployment_storage:
        raise _error(status.HTTP_409_CONFLICT, "configuration_managed_by_deployment")
    try:
        names, truncated = await asyncio.to_thread(_choosable_group_names, sp_ws, MAX_LISTED_GROUPS)
    except DatabricksError as error:
        logger.warning(f"Group listing failed: {type(error).__name__}")
        raise _error(status.HTTP_502_BAD_GATEWAY, "group_list_failed") from None
    return SetupGroupsResponse(groups=tuple(sorted(names)), truncated=truncated)


def _choosable_group_names(sp_ws: WorkspaceClient, limit: int) -> tuple[set[str], bool]:
    """Read up to *limit* non-built-in group names, and whether the workspace has more.

    Paging stops at the first name past *limit*, so a workspace with many groups isn't read
    in full. A page is sized to hold the whole limit plus the built-in groups left out.
    """
    names: set[str] = set()
    pages = sp_ws.groups.list(attributes="id,displayName", count=limit + len(_BUILT_IN_GROUP_NAMES) + 1)
    for group in pages:
        name = group.display_name
        if not name or name.casefold() in _BUILT_IN_GROUP_NAMES or name in names:
            continue
        if len(names) == limit:
            return names, True
        names.add(name)
    return names, False


@router.post("/configuration", response_model=SetupReport, operation_id="configureSetup")
async def configure_setup(
    body: SetupConfigurationRequest,
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    store: Annotated[SetupConfigurationStore, Depends(get_setup_configuration_store)],
    config: Annotated[AppConfig, Depends(get_conf)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    reader_sql_factory: Annotated[ReaderSqlFactory, Depends(get_setup_sql_reader_factory)],
) -> SetupReport:
    """Validate and save catalog, prefix and audience group, then run the setup workflow."""
    if config.has_deployment_storage:
        raise _error(status.HTTP_409_CONFLICT, "configuration_managed_by_deployment")
    try:
        storage, audience = validate_choices(
            SetupChoices(catalog=body.catalog, prefix=body.prefix, audience_group=body.audience_group),
            config.admin_group,
        )
    except BuiltInAudienceGroupError as error:
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "audience_group_builtin", str(error)) from None
    except AudienceGroupRequiredError as error:
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "audience_group_required", str(error)) from None
    except InvalidCatalogNameError as error:
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "catalog_invalid", str(error)) from None
    except InvalidParameterError as error:
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "configuration_invalid", str(error)) from None
    choices = SetupChoices(catalog=storage.catalog, prefix=storage.schema, audience_group=audience.groups[0])
    if not await asyncio.to_thread(_catalog_exists, reader_ws, choices.catalog):
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "catalog_not_found")
    if not await asyncio.to_thread(_group_exists, sp_ws, choices.audience_group):
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "audience_group_not_found")
    outcome = await orchestrator.save_configuration(store, choices, user_email=access.user_name)
    if outcome == "locked":
        raise _error(status.HTTP_409_CONFLICT, "configuration_locked")
    # Reconcile binds the storage for the choices just saved, then builds the
    # administrator SQL reader for that storage, so the first check can inspect grants.
    return await orchestrator.reconcile(
        setup_user=access.user_name, reader_ws=reader_ws, reader_sql_factory=reader_sql_factory
    )


@router.post("/reconcile", response_model=SetupReport, operation_id="reconcileSetup")
async def reconcile_setup(
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    reader_sql_factory: Annotated[ReaderSqlFactory, Depends(get_setup_sql_reader_factory)],
) -> SetupReport:
    """Run the serialized setup workflow as a bootstrap administrator."""
    return await orchestrator.reconcile(
        setup_user=access.user_name, reader_ws=reader_ws, reader_sql_factory=reader_sql_factory
    )


@router.post("/override", response_model=SetupReport, operation_id="overrideSetupStep")
async def override_setup_step(
    body: SetupOverrideRequest,
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    reader_sql_factory: Annotated[ReaderSqlFactory, Depends(get_setup_sql_reader_factory)],
) -> SetupReport:
    """Continue past a step an administrator confirmed is set up in a way Studio cannot see."""
    report = await orchestrator.override(
        body.step_id,
        setup_user=access.user_name,
        reader_ws=reader_ws,
        reader_sql_factory=reader_sql_factory,
    )
    if report is None:
        raise _error(status.HTTP_409_CONFLICT, "override_not_available")
    return report


@router.post(
    "/steps/{step_id}/apply-grants",
    response_model=SetupApplyGrantsResponse,
    operation_id="applySetupStepGrants",
)
async def apply_setup_step_grants(
    step_id: SetupStepId,
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    reader_sql_factory: Annotated[ReaderSqlFactory, Depends(get_setup_sql_reader_factory)],
) -> SetupApplyGrantsResponse:
    """Run a step's GRANT statements as the signed-in administrator, then re-verify setup.

    The request carries only the step; the statements are re-derived on the server. A failure
    while re-deriving them or re-checking setup afterwards is reported as *setup_grants_failed*,
    without the error's text.
    """
    try:
        result = await orchestrator.apply_grants(
            step_id,
            setup_user=access.user_name,
            reader_ws=reader_ws,
            reader_sql_factory=reader_sql_factory,
        )
    except Exception as error:
        logger.warning(f"Could not run a setup step's grants ({type(error).__name__})")
        raise _error(status.HTTP_502_BAD_GATEWAY, "setup_grants_failed") from None
    if result is None:
        raise _error(status.HTTP_409_CONFLICT, "nothing_to_apply")
    return result


@router.get(
    "/task-runner/service-principals",
    response_model=TaskRunnerPrincipalsResponse,
    operation_id="listTaskRunnerServicePrincipals",
)
async def list_task_runner_service_principals(
    _access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    config: Annotated[AppConfig, Depends(get_conf)],
) -> TaskRunnerPrincipalsResponse:
    """List the service principals the task-runner job could run as, without the app's own.

    Listed as the app. When the app can't list them, *available* is false so the
    administrator pastes an Application ID instead. A job from the deployment bundle gets
    its service principal from the deployment, so there is nothing to choose.
    """
    if orchestrator.job_managed_by_deployment:
        raise _error(status.HTTP_409_CONFLICT, "task_runner_managed_by_deployment")
    fallback_name = sanitize_setup_display(config.app_slug_name) or ""
    try:
        listing = await asyncio.to_thread(list_task_runner_principals, sp_ws, orchestrator.app_sp_id)
    except DatabricksError as error:
        logger.warning(f"Could not list service principals for the task runner ({type(error).__name__})")
        return TaskRunnerPrincipalsResponse(available=False, app_name=fallback_name)
    return TaskRunnerPrincipalsResponse(
        available=True,
        principals=listing.principals,
        app_name=listing.app_display_name or fallback_name,
        truncated=listing.truncated,
    )


_TASK_RUNNER_REFUSALS = (TaskRunnerManagedByDeploymentError, TaskRunnerJobUnavailableError, InvalidParameterError)


def _task_runner_refusal(error: Exception) -> HTTPException:
    """Map a refused task-runner choice (one of *_TASK_RUNNER_REFUSALS*) to an HTTP error."""
    if isinstance(error, TaskRunnerManagedByDeploymentError):
        return _error(status.HTTP_409_CONFLICT, "task_runner_managed_by_deployment")
    if isinstance(error, TaskRunnerJobUnavailableError):
        return _error(status.HTTP_409_CONFLICT, "task_runner_job_unavailable")
    if isinstance(error, AppServicePrincipalChosenError):
        return _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "task_runner_principal_is_app")
    return _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "task_runner_principal_invalid")


@router.post("/task-runner/access", response_model=TaskRunnerAccessResponse, operation_id="checkTaskRunnerAccess")
async def check_task_runner_access(
    body: TaskRunnerPrincipalRequest,
    _access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    reader_sql_factory: Annotated[ReaderSqlFactory, Depends(get_setup_sql_reader_factory)],
) -> TaskRunnerAccessResponse:
    """Check a picked service principal: whether the app may use it, and the data access it still needs."""
    try:
        return await orchestrator.check_task_runner_access(
            body.application_id, reader_ws=reader_ws, reader_sql_factory=reader_sql_factory
        )
    except _TASK_RUNNER_REFUSALS as error:
        raise _task_runner_refusal(error) from None
    except DatabricksError as error:
        logger.warning(f"Could not check the task-runner service principal ({type(error).__name__})")
        raise _error(status.HTTP_502_BAD_GATEWAY, "task_runner_access_failed") from None


@router.post("/task-runner/grants", response_model=TaskRunnerGrantsResponse, operation_id="applyTaskRunnerGrants")
async def apply_task_runner_grants(
    body: TaskRunnerPrincipalRequest,
    _access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    reader_sql_factory: Annotated[ReaderSqlFactory, Depends(get_setup_sql_reader_factory)],
) -> TaskRunnerGrantsResponse:
    """Run a picked service principal's GRANT statements as the signed-in administrator.

    The request carries only the service principal; the statements are re-derived on the server.
    """
    try:
        return await orchestrator.apply_task_runner_grants(
            body.application_id, reader_ws=reader_ws, reader_sql_factory=reader_sql_factory
        )
    except _TASK_RUNNER_REFUSALS as error:
        raise _task_runner_refusal(error) from None
    except DatabricksError as error:
        logger.warning(f"Could not run the task-runner service principal's grants ({type(error).__name__})")
        raise _error(status.HTTP_502_BAD_GATEWAY, "task_runner_grants_failed") from None


@router.post("/task-runner/run-as", response_model=SetupReport, operation_id="setTaskRunnerRunAs")
async def set_task_runner_run_as(
    body: TaskRunnerPrincipalRequest,
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    reader_sql_factory: Annotated[ReaderSqlFactory, Depends(get_setup_sql_reader_factory)],
) -> SetupReport:
    """Make the task-runner job run as the picked service principal, then re-run setup.

    The job is updated as the app, which manages the job it created; Databricks refuses until
    the app may use the service principal. A job from the deployment bundle is never changed.
    """
    try:
        return await orchestrator.set_task_runner_run_as(
            body.application_id,
            setup_user=access.user_name,
            reader_ws=reader_ws,
            reader_sql_factory=reader_sql_factory,
        )
    except _TASK_RUNNER_REFUSALS as error:
        raise _task_runner_refusal(error) from None
    except PermissionDenied:
        raise _error(status.HTTP_403_FORBIDDEN, "task_runner_run_as_forbidden") from None
    except (NotFound, InvalidParameterValue):
        raise _error(status.HTTP_404_NOT_FOUND, "task_runner_principal_not_found") from None
    except DatabricksError as error:
        logger.warning(f"Could not set the task-runner job's service principal ({type(error).__name__})")
        raise _error(status.HTTP_502_BAD_GATEWAY, "task_runner_run_as_failed") from None
