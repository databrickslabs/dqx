"""Tests for application dependency boundaries."""

import pytest
from unittest.mock import create_autospec
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ComplexValue, User
from databricks.sdk.service.jobs import Job, JobSettings, JobRunAs
from databricks.sdk.service.sql import StatementResponse, StatementState, StatementStatus
from fastapi import HTTPException

from databricks_labs_dqx_app.backend import dependencies
from databricks_labs_dqx_app.backend.dependencies import get_sp_oltp_executor, set_oltp_executor, setup_access
from databricks_labs_dqx_app.backend.runtime import rt
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime


@pytest.mark.asyncio
async def test_oltp_dependency_fails_closed_until_lakebase_is_registered() -> None:
    set_oltp_executor(None)

    with pytest.raises(HTTPException) as raised:
        await get_sp_oltp_executor()

    assert raised.value.status_code == 503
    assert raised.value.detail == "DQX Studio setup is not ready."


@pytest.mark.asyncio
async def test_scheduler_view_dependency_uses_temporary_schema() -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.current_user.me.return_value = User(user_name="app-sp")
    workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner-sp")))
    workspace.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )
    previous_job_id = setup_runtime.job_id
    setup_runtime.job_id = 42
    try:
        main_sql = await dependencies.get_sp_sql_executor(workspace)
        service = await dependencies.get_scheduler_view_service(workspace, main_sql)
        view = service.create_view("source.schema.table")
    finally:
        setup_runtime.job_id = previous_job_id

    resources = rt.require_resources()
    assert view.startswith(f"{resources.volume.catalog}.{resources.tmp_schema}.tmp_view_")
    statements = [call.kwargs["statement"] for call in workspace.statement_execution.execute_statement.call_args_list]
    assert any(statement.endswith("TO `runner-sp`") for statement in statements)
    assert any(statement.endswith("TO `app-sp`") for statement in statements)


def test_workspace_admins_can_manage_setup_with_custom_admin_group() -> None:
    user = User(user_name="admin@example.com", groups=[ComplexValue(display="admins")])

    assert setup_access(user, "dqx-admins").can_manage is True


def test_audience_member_cannot_manage_setup() -> None:
    user = User(user_name="a@example.com", groups=[ComplexValue(display="data-team")])

    assert setup_access(user, "dqx-admins").can_manage is False


def test_configured_admin_group_membership_is_case_insensitive() -> None:
    user = User(user_name="a@example.com", groups=[ComplexValue(display="DQX-Admins")])

    assert setup_access(user, "dqx-admins").can_manage is True


def test_workspace_admins_membership_ignores_control_characters_and_case() -> None:
    user = User(user_name="a@example.com", groups=[ComplexValue(display=" Admins\n")])

    assert setup_access(user, "dqx-admins").can_manage is True
