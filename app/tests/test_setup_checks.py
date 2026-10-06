"""Behavior tests for deployment-agnostic setup resource capability checks."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    EffectivePermissionsList,
    EffectivePrivilege,
    EffectivePrivilegeAssignment,
    Privilege,
)
from databricks.sdk.service.jobs import Job, JobRunAs, JobSettings

from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.checks import ResourceCheckers, required_catalog_grants
from databricks_labs_dqx_app.backend.setup.grants import GrantInspector
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _effective_permissions(*privileges: Privilege, principal: str = "app-sp-id") -> EffectivePermissionsList:
    """Build the SDK's complete effective-permissions response shape."""
    return EffectivePermissionsList(
        privilege_assignments=[
            EffectivePrivilegeAssignment(
                principal=principal,
                privileges=[EffectivePrivilege(privilege=privilege) for privilege in privileges],
            )
        ]
    )


@pytest.fixture
def resources() -> ActiveResources:
    return ActiveResources(
        volume=VolumeLocation(
            catalog="main",
            schema="dqx_studio",
            volume="wheels",
            path="/Volumes/main/dqx_studio/wheels",
        ),
        lakebase=LakebaseConnection(
            endpoint="projects/project/branches/main/endpoints/primary",
            host=None,
            port=5432,
            database="databricks_postgres",
            username=None,
            password=None,
            schema="dqx_studio",
        ),
        warehouse_id="warehouse-id",
        job_id=None,
        tmp_schema="dqx_studio_tmp",
        genie_schema="genie",
        demo_schema="studio_demo",
        audience=resolve_audience(["data-team"], "admins", allow_broad=False),
    )


@pytest.fixture
def workspace() -> MagicMock:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp-id", id=None)
    return workspace


@pytest.fixture
def sql() -> MagicMock:
    executor = create_autospec(SqlExecutor, instance=True)
    executor.q.side_effect = lambda identifier: "`" + identifier.replace("`", "``") + "`"
    return executor


@pytest.fixture
def compute() -> MagicMock:
    service = create_autospec(ComputeService, instance=True)
    service.warehouse_access_status.return_value = "granted"
    return service


@pytest.fixture
def checkers(
    resources: ActiveResources,
    workspace: MagicMock,
    sql: MagicMock,
    compute: MagicMock,
) -> ResourceCheckers:
    return ResourceCheckers(resources=resources, workspace=workspace, sql=sql, compute=compute, app_sp_id="app-sp-id")


def _simulate_creation(workspace: MagicMock, sql: MagicMock, owner: str = "app-sp-id") -> None:
    """Make schemas and the volume appear, owned by *owner*, once their CREATE statement runs."""
    created: set[str] = set()

    def execute(statement: str) -> None:
        created.add(statement.split(" EXISTS ")[1].replace("`", ""))

    def get_schema(full_name: str) -> SimpleNamespace:
        if full_name not in created:
            raise NotFound("missing")
        return SimpleNamespace(owner=owner)

    def read_volume(full_name: str) -> SimpleNamespace:
        if full_name not in created:
            raise NotFound("missing")
        return SimpleNamespace(owner=owner)

    sql.execute_no_schema.side_effect = execute
    workspace.schemas.get.side_effect = get_schema
    workspace.volumes.read.side_effect = read_volume
    workspace.grants.get_effective.return_value = _effective_permissions()


@pytest.fixture
def runner_checkers(checkers: ResourceCheckers, workspace: MagicMock) -> ResourceCheckers:
    workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner-sp-id")))
    permissions = {
        "CATALOG": _effective_permissions(Privilege.USE_CATALOG, principal="runner-sp-id"),
        "SCHEMA": _effective_permissions(Privilege.USE_SCHEMA, principal="runner-sp-id"),
        "VOLUME": _effective_permissions(Privilege.READ_VOLUME, principal="runner-sp-id"),
        "TABLE": _effective_permissions(Privilege.SELECT, Privilege.MODIFY, principal="runner-sp-id"),
    }
    workspace.grants.get_effective.side_effect = lambda securable_type, full_name, *, principal: (
        _effective_permissions(Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY, principal=principal)
        if principal == "runner-sp-id" and (securable_type, full_name) == ("SCHEMA", "main.dqx_studio")
        else permissions[securable_type] if principal == "runner-sp-id" else _effective_permissions()
    )
    return checkers


@pytest.mark.parametrize(
    ("securable_type", "full_name", "instruction"),
    [
        ("CATALOG", "main", "GRANT USE CATALOG ON CATALOG `main` TO `runner-sp-id`;"),
        ("SCHEMA", "main.dqx_studio", "GRANT USE SCHEMA ON SCHEMA `main`.`dqx_studio` TO `runner-sp-id`;"),
        (
            "VOLUME",
            "main.dqx_studio.wheels",
            "GRANT READ VOLUME ON VOLUME `main`.`dqx_studio`.`wheels` TO `runner-sp-id`;",
        ),
    ],
)
def test_runner_missing_wheel_access_requires_specific_grant(
    runner_checkers: ResourceCheckers,
    workspace: MagicMock,
    securable_type: str,
    full_name: str,
    instruction: str,
) -> None:
    available_permissions = workspace.grants.get_effective.side_effect
    workspace.grants.get_effective.side_effect = lambda kind, name, *, principal: (
        _effective_permissions(principal=principal)
        if (kind, name) == (securable_type, full_name)
        else available_permissions(kind, name, principal=principal)
    )

    result = runner_checkers.check_runner_access(42)

    assert result.id == SetupStepId.TASK_RUNNER
    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permissions_missing"
    assert result.instructions == (instruction,)
    assert result.actions == (SetupActionId.VERIFY_AGAIN,)


def test_runner_wheel_read_access_passes_without_write_access(runner_checkers: ResourceCheckers) -> None:
    result = runner_checkers.check_runner_access(42)

    assert result.state == StepState.PASSED


def test_runner_requires_temporary_schema_usage(runner_checkers: ResourceCheckers, workspace: MagicMock) -> None:
    available = workspace.grants.get_effective.side_effect
    workspace.grants.get_effective.side_effect = lambda kind, name, *, principal: (
        _effective_permissions(principal=principal)
        if (kind, name) == ("SCHEMA", "main.dqx_studio_tmp")
        else available(kind, name, principal=principal)
    )

    result = runner_checkers.check_runner_access(42)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.instructions == ("GRANT USE SCHEMA ON SCHEMA `main`.`dqx_studio_tmp` TO `runner-sp-id`;",)


@pytest.mark.parametrize("missing", [Privilege.SELECT, Privilege.MODIFY])
def test_runner_missing_schema_data_permission_blocks_readiness(
    runner_checkers: ResourceCheckers, workspace: MagicMock, missing: Privilege
) -> None:
    available_permissions = workspace.grants.get_effective.side_effect
    workspace.grants.get_effective.side_effect = lambda kind, name, *, principal: (
        _effective_permissions(
            *(
                privilege
                for privilege in (Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY)
                if privilege != missing
            ),
            principal=principal,
        )
        if (kind, name) == ("SCHEMA", "main.dqx_studio")
        else available_permissions(kind, name, principal=principal)
    )

    result = runner_checkers.check_runner_access(42, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.instructions == (f"GRANT {missing.value} ON SCHEMA `main`.`dqx_studio` TO `runner-sp-id`;",)


def test_runner_schema_permissions_pass_without_individual_table_inspection(
    runner_checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    available_permissions = workspace.grants.get_effective.side_effect

    def permissions(kind: str, name: str, *, principal: str) -> EffectivePermissionsList:
        if kind == "TABLE":
            raise PermissionError("individual table grants cannot be inspected")
        return available_permissions(kind, name, principal=principal)

    workspace.grants.get_effective.side_effect = permissions

    assert runner_checkers.check_runner_access(42, include_outputs=True).state == StepState.PASSED
    assert all(call.args[0] != "TABLE" for call in workspace.grants.get_effective.call_args_list)


def test_runner_schema_permission_lookup_failure_is_not_reported_as_missing_grant(
    runner_checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    available_permissions = workspace.grants.get_effective.side_effect

    def permissions(kind: str, name: str, *, principal: str) -> EffectivePermissionsList:
        if (kind, name) == ("SCHEMA", "main.dqx_studio"):
            raise PermissionError("sensitive schema payload")
        return available_permissions(kind, name, principal=principal)

    workspace.grants.get_effective.side_effect = permissions

    result = runner_checkers.check_runner_access(42, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permission_check_failed"
    assert "sensitive schema payload" not in str(result)
    assert not any(instruction.startswith("GRANT") for instruction in result.instructions)


@pytest.mark.parametrize("include_outputs", [False, True])
def test_runner_all_privileges_passes(
    runner_checkers: ResourceCheckers, workspace: MagicMock, include_outputs: bool
) -> None:
    workspace.grants.get_effective.side_effect = None
    workspace.grants.get_effective.return_value = _effective_permissions(
        Privilege.ALL_PRIVILEGES, principal="runner-sp-id"
    )

    assert runner_checkers.check_runner_access(42, include_outputs=include_outputs).state == StepState.PASSED


def test_runner_owner_passes_without_explicit_grants(runner_checkers: ResourceCheckers, workspace: MagicMock) -> None:
    workspace.grants.get_effective.side_effect = None
    workspace.grants.get_effective.return_value = _effective_permissions(principal="runner-sp-id")
    workspace.catalogs.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.schemas.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.volumes.read.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.tables.get.return_value = SimpleNamespace(owner="runner-sp-id")

    assert runner_checkers.check_runner_access(42).state == StepState.PASSED


def test_runner_schema_owner_still_requires_data_grants(
    runner_checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    workspace.grants.get_effective.side_effect = None
    workspace.grants.get_effective.return_value = _effective_permissions(principal="runner-sp-id")
    workspace.catalogs.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.schemas.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.volumes.read.return_value = SimpleNamespace(owner="runner-sp-id")

    result = runner_checkers.check_runner_access(42, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permissions_missing"
    assert result.instructions == (
        "GRANT SELECT ON SCHEMA `main`.`dqx_studio` TO `runner-sp-id`;",
        "GRANT MODIFY ON SCHEMA `main`.`dqx_studio` TO `runner-sp-id`;",
    )


def test_runner_schema_owner_cannot_bypass_uninspectable_data_grants(
    runner_checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    workspace.grants.get_effective.side_effect = PermissionError("grants unavailable")
    workspace.catalogs.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.schemas.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.volumes.read.return_value = SimpleNamespace(owner="runner-sp-id")

    result = runner_checkers.check_runner_access(42, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permission_check_failed"


def test_runner_ownership_can_verify_access_when_grants_are_unavailable(
    runner_checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    workspace.grants.get_effective.side_effect = RuntimeError("grants unavailable")
    workspace.catalogs.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.schemas.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.volumes.read.return_value = SimpleNamespace(owner="runner-sp-id")

    assert runner_checkers.check_runner_access(42).state == StepState.PASSED


def test_runner_permission_lookup_failure_blocks_setup_without_raw_error(
    runner_checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    workspace.grants.get_effective.side_effect = RuntimeError("sensitive platform payload")

    result = runner_checkers.check_runner_access(42)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permission_check_failed"
    assert "sensitive platform payload" not in str(result)
    assert "Verify" in " ".join(result.instructions)
    assert "READ METADATA" in " ".join(result.instructions)
    assert not any(instruction.startswith("GRANT") for instruction in result.instructions)


@pytest.fixture
def runner_reader(workspace: MagicMock) -> MagicMock:
    reader = create_autospec(WorkspaceClient, instance=True)
    reader.grants.get_effective.side_effect = workspace.grants.get_effective.side_effect
    return reader


@pytest.fixture
def runner_sql(workspace: MagicMock, sql: MagicMock) -> MagicMock:
    workspace.grants.get_effective.side_effect = PermissionError("app cannot inspect another principal")
    workspace.service_principals.list.return_value = [SimpleNamespace(application_id="runner-sp-id", groups=[])]
    return sql


def test_runner_admin_inspection_uses_sql_scope_instead_of_grants_api(
    runner_checkers: ResourceCheckers, workspace: MagicMock, runner_reader: MagicMock, runner_sql: MagicMock
) -> None:
    workspace.grants.get_effective.side_effect = PermissionError("app cannot inspect another principal")
    runner_reader.grants.get_effective.side_effect = PermissionError("unsupported OAuth scope")
    runner_sql.query_dicts.return_value = [
        {"principal": "runner-sp-id", "actionType": "ALL PRIVILEGES", "objectType": "SCHEMA", "objectKey": "main"}
    ]

    result = runner_checkers.check_runner_access(
        42, reader_ws=runner_reader, reader_sql=runner_sql, include_outputs=True
    )

    assert result.state == StepState.PASSED
    runner_reader.grants.get_effective.assert_not_called()
    runner_reader.jobs.get.assert_not_called()
    assert "SHOW GRANTS ON SCHEMA `main`.`dqx_studio`" in [
        call.args[0] for call in runner_sql.query_dicts.call_args_list
    ]
    assert all(" ON TABLE " not in call.args[0] for call in runner_sql.query_dicts.call_args_list)


def test_runner_sql_inspection_requires_complete_results(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock
) -> None:
    def grants(_query: str, *, require_complete: bool = False) -> list[dict[str, str]]:
        if not require_complete:
            return []
        return [{"principal": "runner-sp-id", "actionType": "ALL PRIVILEGES"}]

    runner_sql.query_dicts.side_effect = grants

    assert (
        runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True).state == StepState.PASSED
    )


@pytest.mark.parametrize(
    ("principal_column", "action_column"),
    [("principal", "actionType"), ("Principal", "ActionType"), ("PRINCIPAL", "ACTIONTYPE")],
)
def test_runner_sql_grants_accept_column_casing(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock, principal_column: str, action_column: str
) -> None:
    runner_sql.query_dicts.return_value = [{principal_column: "runner-sp-id", action_column: "ALL PRIVILEGES"}]

    result = runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True)

    assert result.state == StepState.PASSED


def test_runner_sql_grants_include_parent_inheritance(runner_checkers: ResourceCheckers, runner_sql: MagicMock) -> None:
    runner_sql.query_dicts.side_effect = lambda statement, require_complete: (
        [{"Principal": "runner-sp-id", "ActionType": "ALL PRIVILEGES"}]
        if statement == "SHOW GRANTS ON CATALOG `main`"
        else []
    )

    result = runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True)

    assert result.state == StepState.PASSED
    assert [call.args[0] for call in runner_sql.query_dicts.call_args_list].count("SHOW GRANTS ON CATALOG `main`") == 1


def test_runner_sql_grants_include_verified_group_inheritance(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock, workspace: MagicMock
) -> None:
    workspace.service_principals.list.return_value = [
        SimpleNamespace(
            application_id="runner-sp-id", groups=[SimpleNamespace(display="runner-group", value="group-id")]
        )
    ]
    runner_sql.query_dicts.side_effect = lambda statement, require_complete: (
        [{"Principal": "runner-group", "ActionType": "ALL PRIVILEGES"}]
        if statement == "SHOW GRANTS ON CATALOG `main`"
        else []
    )

    assert (
        runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True).state == StepState.PASSED
    )


def test_runner_sql_unknown_result_columns_do_not_report_missing_grants(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock
) -> None:
    runner_sql.query_dicts.return_value = [{"unexpected": "runner-sp-id"}]

    result = runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permission_check_failed"
    assert not any(instruction.startswith("GRANT") for instruction in result.instructions)


def test_runner_schema_permissions_are_rechecked_after_success(
    runner_checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    assert runner_checkers.check_runner_access(42, include_outputs=True).state == StepState.PASSED
    available = workspace.grants.get_effective.side_effect
    workspace.grants.get_effective.side_effect = lambda kind, name, *, principal: (
        _effective_permissions(Privilege.USE_SCHEMA, Privilege.SELECT, principal=principal)
        if (kind, name) == ("SCHEMA", "main.dqx_studio")
        else available(kind, name, principal=principal)
    )

    result = runner_checkers.check_runner_access(42, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.instructions == ("GRANT MODIFY ON SCHEMA `main`.`dqx_studio` TO `runner-sp-id`;",)


def test_runner_sql_membership_is_rechecked_after_success(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock, workspace: MagicMock
) -> None:
    workspace.service_principals.list.return_value = [
        SimpleNamespace(
            application_id="runner-sp-id", groups=[SimpleNamespace(display="runner-group", value="group-id")]
        )
    ]
    runner_sql.query_dicts.return_value = [{"Principal": "runner-group", "ActionType": "ALL PRIVILEGES"}]
    assert (
        runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True).state == StepState.PASSED
    )
    workspace.service_principals.list.return_value = [SimpleNamespace(application_id="runner-sp-id", groups=[])]

    result = runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permissions_missing"


@pytest.mark.parametrize(
    "row",
    [
        {"Principal": "other", "principal": "runner-sp-id", "ActionType": "ALL PRIVILEGES"},
        {"Principal": "runner-sp-id", "ActionType": ""},
    ],
)
def test_runner_sql_ambiguous_grant_rows_are_unknown(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock, row: dict[str, str]
) -> None:
    runner_sql.query_dicts.return_value = [row]

    result = runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permission_check_failed"


@pytest.mark.parametrize("rows", [[], [{"principal": "other-sp", "actionType": "ALL PRIVILEGES"}]])
def test_runner_sql_inspection_does_not_accept_another_principals_grants(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock, rows: list[dict[str, str]]
) -> None:
    runner_sql.query_dicts.return_value = rows

    result = runner_checkers.check_runner_access(42, reader_sql=runner_sql)

    assert result.state == StepState.ACTION_REQUIRED
    assert any("GRANT READ VOLUME" in instruction for instruction in result.instructions)


def test_runner_sql_inspection_failure_reports_unknown_permissions(
    runner_checkers: ResourceCheckers, runner_sql: MagicMock
) -> None:
    runner_sql.query_dicts.side_effect = PermissionError("sensitive SQL payload")

    result = runner_checkers.check_runner_access(42, reader_sql=runner_sql, include_outputs=True)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_permission_check_failed"
    assert "sensitive SQL payload" not in str(result)
    assert not any(instruction.startswith("GRANT") for instruction in result.instructions)


def test_runner_ownership_uses_app_metadata_when_obo_volume_scope_is_unavailable(
    runner_checkers: ResourceCheckers, workspace: MagicMock, runner_reader: MagicMock, runner_sql: MagicMock
) -> None:
    runner_sql.query_dicts.return_value = [
        {"principal": "runner-sp-id", "actionType": "SELECT"},
        {"principal": "runner-sp-id", "actionType": "MODIFY"},
    ]
    runner_reader.volumes.read.side_effect = PermissionError("unsupported OAuth scope")
    workspace.catalogs.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.schemas.get.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.volumes.read.return_value = SimpleNamespace(owner="runner-sp-id")
    workspace.tables.get.return_value = SimpleNamespace(owner="runner-sp-id")

    result = runner_checkers.check_runner_access(
        42, reader_ws=runner_reader, reader_sql=runner_sql, include_outputs=True
    )

    assert result.state == StepState.PASSED


@pytest.mark.parametrize(
    "run_as",
    [
        None,
        JobRunAs(user_name="runner@example.com"),
        JobRunAs(service_principal_name="app-sp-id"),
        JobRunAs(service_principal_name="runner-sp\nid"),
    ],
)
def test_runner_volume_check_rejects_invalid_identity(
    runner_checkers: ResourceCheckers, workspace: MagicMock, run_as: JobRunAs | None
) -> None:
    workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=run_as))

    result = runner_checkers.check_runner_access(42)

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "task_runner_identity_unresolved"
    workspace.grants.get_effective.assert_not_called()


def test_catalog_check_requires_app_sp_catalog_privileges(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    def effective(kind: str, name: str, *, principal: str) -> EffectivePermissionsList:
        if principal == "app-sp-id":
            return _effective_permissions(Privilege.USE_CATALOG)
        return _effective_permissions(Privilege.USE_CATALOG, principal=principal)

    workspace.grants.get_effective.side_effect = effective

    step = checkers.check_unity_catalog()

    assert step.id == SetupStepId.UNITY_CATALOG
    assert step.code == "catalog_permissions_missing"
    assert step.instructions == ("GRANT CREATE SCHEMA ON CATALOG `main` TO `app-sp-id`;",)


def test_catalog_check_reports_missing_audience_usage(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    def effective(kind: str, name: str, *, principal: str) -> EffectivePermissionsList:
        if principal == "app-sp-id":
            return _effective_permissions(Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA)
        return _effective_permissions(principal=principal)

    workspace.grants.get_effective.side_effect = effective

    step = checkers.check_unity_catalog()

    assert step.code == "catalog_permissions_missing"
    assert step.instructions == ("GRANT USE CATALOG ON CATALOG `main` TO `data-team`;",)


def test_catalog_check_grants_audience_usage_best_effort(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)
    sql.execute_no_schema.side_effect = RuntimeError("denied")

    step = checkers.check_unity_catalog()

    assert step.state == StepState.PASSED
    sql.execute_no_schema.assert_called_once_with("GRANT USE CATALOG ON CATALOG `main` TO `data-team`")


def test_catalog_owner_passes_without_explicit_privileges(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    def effective(kind: str, name: str, *, principal: str) -> EffectivePermissionsList:
        if principal == "app-sp-id":
            return _effective_permissions()
        return _effective_permissions(Privilege.USE_CATALOG, principal=principal)

    workspace.grants.get_effective.side_effect = effective
    workspace.catalogs.get.return_value = SimpleNamespace(owner="app-sp-id")

    assert checkers.check_unity_catalog().state == StepState.PASSED


def test_catalog_check_unknown_when_grants_unreadable(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    workspace.grants.get_effective.side_effect = PermissionError("denied")

    step = checkers.check_unity_catalog()

    assert step.code == "catalog_permission_check_failed"
    assert "READ METADATA" in "\n".join(step.instructions)


def test_catalog_check_accepts_request_scoped_reader_sql(
    checkers: ResourceCheckers, sql: MagicMock, workspace: MagicMock
) -> None:
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)

    assert checkers.check_unity_catalog(reader_sql=sql).state == StepState.PASSED


def test_storage_provisions_missing_schemas_and_volume(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    _simulate_creation(workspace, sql)

    step = checkers.ensure_storage(provision=True)

    statements = [call.args[0] for call in sql.execute_no_schema.call_args_list]
    assert step.state == StepState.PASSED
    assert statements == [
        "CREATE SCHEMA IF NOT EXISTS `main`.`dqx_studio`",
        "CREATE SCHEMA IF NOT EXISTS `main`.`dqx_studio_tmp`",
        "CREATE SCHEMA IF NOT EXISTS `main`.`genie`",
        "CREATE SCHEMA IF NOT EXISTS `main`.`studio_demo`",
        "CREATE VOLUME IF NOT EXISTS `main`.`dqx_studio`.`wheels`",
    ]


def test_storage_creation_failure_requests_reconcile(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    workspace.schemas.get.side_effect = NotFound("missing")
    workspace.volumes.read.side_effect = NotFound("missing")
    sql.execute_no_schema.side_effect = RuntimeError("permission denied")

    step = checkers.ensure_storage(provision=True)

    assert step.code == "storage_creation_failed"
    assert step.actions == (SetupActionId.RECONCILE,)
    assert "permission denied" not in step.summary


def test_storage_refuses_unmanaged_existing_schema(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="someone-else")
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.USE_SCHEMA)

    step = checkers.ensure_storage(provision=True)

    assert step.code == "storage_collision"
    assert "`main`.`dqx_studio_tmp`" in "\n".join(step.instructions)
    sql.execute_no_schema.assert_not_called()


def test_storage_schema_with_manage_only_requires_usage_grants(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="deployer@example.com")
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.MANAGE)

    step = checkers.ensure_storage(provision=False)

    assert step.code == "storage_permissions_missing"
    assert step.instructions[0] == "GRANT USE SCHEMA, CREATE TABLE ON SCHEMA `main`.`dqx_studio` TO `app-sp-id`;"
    sql.execute_no_schema.assert_not_called()


def test_storage_schema_with_all_privileges_but_no_manage_is_a_collision(
    checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="someone-else")
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)

    assert checkers.ensure_storage(provision=True).code == "storage_collision"


def test_storage_schema_with_owner_unknown_relies_on_manage(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner=None)
    workspace.volumes.read.return_value = SimpleNamespace(owner="app-sp-id")
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)

    assert checkers.ensure_storage(provision=False).code == "storage_collision"
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES, Privilege.MANAGE)
    assert checkers.ensure_storage(provision=False).state == StepState.PASSED


def test_storage_schema_lookup_error_is_unknown(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    workspace.schemas.get.side_effect = PermissionError("denied")

    step = checkers.ensure_storage(provision=True)

    assert step.code == "storage_permission_check_failed"
    sql.execute_no_schema.assert_not_called()


def test_storage_volume_lookup_error_is_unknown(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="app-sp-id")
    workspace.volumes.read.side_effect = PermissionError("denied")

    step = checkers.ensure_storage(provision=True)

    assert step.code == "storage_permission_check_failed"
    sql.execute_no_schema.assert_not_called()


def test_partial_provisioning_still_verifies_existing_volume_access(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    def get_schema(full_name: str) -> SimpleNamespace:
        if full_name == "main.dqx_studio_tmp" and not sql.execute_no_schema.called:
            raise NotFound("missing")
        return SimpleNamespace(owner="app-sp-id")

    workspace.schemas.get.side_effect = get_schema
    workspace.volumes.read.return_value = SimpleNamespace(owner="foreign")
    workspace.grants.get_effective.return_value = _effective_permissions()

    step = checkers.ensure_storage(provision=True)

    assert step.code == "volume_permissions_missing"


def test_created_schema_that_is_not_studio_managed_is_a_collision(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    _simulate_creation(workspace, sql, owner="someone-else")

    step = checkers.ensure_storage(provision=True)

    assert step.code == "storage_collision"


def test_catalog_with_unsafe_name_skips_best_effort_grants(
    resources: ActiveResources, workspace: MagicMock, sql: MagicMock, compute: MagicMock
) -> None:
    unsafe = replace(resources, volume=replace(resources.volume, catalog="bad`name"))
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)
    checkers = ResourceCheckers(resources=unsafe, workspace=workspace, sql=sql, compute=compute, app_sp_id="app-sp-id")

    checkers.check_unity_catalog()

    sql.execute_no_schema.assert_not_called()


def test_storage_accepts_owned_existing_schemas_and_volume(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="app-sp-id")
    workspace.volumes.read.return_value = SimpleNamespace(owner="app-sp-id")
    workspace.grants.get_effective.return_value = _effective_permissions()

    assert checkers.ensure_storage(provision=True).state == StepState.PASSED


def test_dab_storage_is_verified_not_created(checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock) -> None:
    workspace.schemas.get.side_effect = NotFound("missing")

    step = checkers.ensure_storage(provision=False)

    assert step.code == "storage_missing"
    assert "make app-deploy" in "\n".join(step.instructions)
    sql.execute_no_schema.assert_not_called()


def test_dab_schema_with_manage_is_accepted(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="deployer@example.com")
    workspace.volumes.read.return_value = SimpleNamespace(owner="deployer@example.com")
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES, Privilege.MANAGE)

    assert checkers.ensure_storage(provision=False).state == StepState.PASSED


def test_storage_reports_unknown_when_schema_grants_unreadable(
    checkers: ResourceCheckers, workspace: MagicMock, sql: MagicMock
) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="someone-else")
    workspace.grants.get_effective.side_effect = PermissionError("denied")

    step = checkers.ensure_storage(provision=True)

    assert step.code == "storage_permission_check_failed"
    sql.execute_no_schema.assert_not_called()


@pytest.mark.parametrize(
    ("missing_privilege", "grant_privilege"),
    [(Privilege.READ_VOLUME, "READ VOLUME"), (Privilege.WRITE_VOLUME, "WRITE VOLUME")],
)
def test_existing_volume_requires_read_and_write(
    checkers: ResourceCheckers, workspace: MagicMock, missing_privilege: Privilege, grant_privilege: str
) -> None:
    workspace.schemas.get.return_value = SimpleNamespace(owner="app-sp-id")
    workspace.volumes.read.return_value = SimpleNamespace(owner="someone-else")
    granted = {Privilege.READ_VOLUME, Privilege.WRITE_VOLUME} - {missing_privilege}
    workspace.grants.get_effective.return_value = _effective_permissions(*granted)

    step = checkers.ensure_storage(provision=False)

    assert step.code == "volume_permissions_missing"
    assert step.instructions == (f"GRANT {grant_privilege} ON VOLUME `main`.`dqx_studio`.`wheels` TO `app-sp-id`;",)


@pytest.mark.parametrize("app_sp_id", ["", "   ", "app-sp\nid"])
def test_unresolved_app_identity_requires_action_for_storage_checks(
    resources: ActiveResources,
    workspace: MagicMock,
    sql: MagicMock,
    compute: MagicMock,
    app_sp_id: str,
) -> None:
    checkers = ResourceCheckers(resources=resources, workspace=workspace, sql=sql, compute=compute, app_sp_id=app_sp_id)

    for result in (checkers.check_unity_catalog(), checkers.ensure_storage(provision=True)):
        assert result.state == StepState.ACTION_REQUIRED
        assert result.code == "app_identity_unresolved"
    workspace.current_user.me.assert_not_called()
    sql.execute_no_schema.assert_not_called()


def test_storage_checks_use_injected_identity_without_scim_lookup(
    checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)

    assert checkers.check_unity_catalog().state == StepState.PASSED
    workspace.current_user.me.assert_not_called()


def test_warehouse_requires_app_manage(checkers: ResourceCheckers, compute: MagicMock) -> None:
    """CAN_USE alone would let the app run SQL but not manage the warehouse ACL."""
    compute.warehouse_access_status.return_value = "missing"

    step = checkers.check_warehouse()

    assert step.id == SetupStepId.WAREHOUSE
    assert step.state == StepState.ACTION_REQUIRED
    assert step.code == "warehouse_permissions_missing"
    assert "CAN MANAGE" in step.instructions[0]
    compute.reconcile_warehouse_audience.assert_not_called()


def test_warehouse_reconciles_audience_use(checkers: ResourceCheckers, compute: MagicMock) -> None:
    compute.warehouse_access_status.return_value = "granted"
    compute.reconcile_warehouse_audience.return_value = "granted"

    assert checkers.check_warehouse().state == StepState.PASSED
    compute.reconcile_warehouse_audience.assert_called_once_with("warehouse-id", ("data-team",))


def test_warehouse_audience_missing_lists_each_group(
    checkers: ResourceCheckers, compute: MagicMock, resources: ActiveResources
) -> None:
    compute.warehouse_access_status.return_value = "granted"
    compute.reconcile_warehouse_audience.return_value = "missing"

    step = checkers.check_warehouse()

    assert step.code == "warehouse_audience_missing"
    assert step.instructions == ("Grant CAN USE on SQL warehouse `warehouse-id` to group `data-team`.",)


def test_warehouse_audience_unknown_requires_action(checkers: ResourceCheckers, compute: MagicMock) -> None:
    compute.warehouse_access_status.return_value = "granted"
    compute.reconcile_warehouse_audience.return_value = "unknown"

    assert checkers.check_warehouse().code == "warehouse_permission_unknown"


def test_warehouse_unreadable_acl_does_not_fall_back_to_query(
    checkers: ResourceCheckers, compute: MagicMock, sql: MagicMock
) -> None:
    """SELECT 1 proves CAN_USE, not the CAN_MANAGE the app SP needs."""
    compute.warehouse_access_status.return_value = "unknown"

    assert checkers.check_warehouse().code == "warehouse_permission_unknown"
    sql.query.assert_not_called()


def test_warehouse_candidate_uses_supplied_obo_reader(checkers: ResourceCheckers, compute: MagicMock) -> None:
    """Ignoring the caller's OBO reader could report the wrong warehouse access result."""
    obo_workspace = MagicMock(name="obo_workspace")
    compute.reconcile_warehouse_audience.return_value = "granted"

    result = checkers.check_warehouse("candidate-warehouse", reader_ws=obo_workspace)

    assert result.state == StepState.PASSED
    compute.warehouse_access_status.assert_called_once_with("candidate-warehouse", reader_ws=obo_workspace)
    compute.reconcile_warehouse_audience.assert_called_once_with("candidate-warehouse", ("data-team",))


def test_schema_privilege_inspection_requests_manage(
    checkers: ResourceCheckers, workspace: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Omitting MANAGE lets the SHOW GRANTS fallback skip group expansion for a group-held MANAGE."""
    requested: list[frozenset[str]] = []

    def spy(
        _self: GrantInspector, kind: str, full_name: str, principal: str, *, required: frozenset[str] = frozenset()
    ) -> frozenset[str]:
        if kind == "SCHEMA":
            requested.append(required)
        return frozenset({"MANAGE", "USE_SCHEMA", "CREATE_TABLE"})

    monkeypatch.setattr(GrantInspector, "privileges", spy)
    workspace.schemas.get.return_value = SimpleNamespace(owner="deployer@example.com")

    checkers.ensure_storage(provision=False)

    assert requested
    assert all("MANAGE" in required for required in requested)


def test_catalog_grant_instructions_strip_control_characters(resources: ActiveResources) -> None:
    """Interpolating an unsafe principal in instructions would enable administrator-command injection."""
    instructions = required_catalog_grants("app-sp\nGRANT ALL", resources)

    assert instructions
    assert all("\n" not in instruction and "\r" not in instruction for instruction in instructions)
    assert any("CREATE SCHEMA" in instruction for instruction in instructions)


def test_catalog_grant_instructions_strip_c1_control_characters(resources: ActiveResources) -> None:
    """Leaving C1 controls in grant instructions could inject terminal control sequences."""
    instructions = required_catalog_grants("app-sp\u0085GRANT ALL", resources)

    assert all("\u0085" not in instruction for instruction in instructions)
