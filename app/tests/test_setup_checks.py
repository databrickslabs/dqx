"""Behavior tests for deployment-agnostic setup resource capability checks."""

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    EffectivePermissionsList,
    EffectivePrivilege,
    EffectivePrivilegeAssignment,
    Privilege,
)

from databricks_labs_dqx_app.backend.pg_executor import PgExecutor
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.checks import ResourceCheckers, required_catalog_grants
from databricks_labs_dqx_app.backend.setup.models import SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _effective_permissions(*privileges: Privilege) -> EffectivePermissionsList:
    """Build the SDK's complete effective-permissions response shape."""
    return EffectivePermissionsList(
        privilege_assignments=[
            EffectivePrivilegeAssignment(
                principal="app-sp-id",
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
def pg() -> MagicMock:
    executor = create_autospec(PgExecutor, instance=True)
    executor.q.side_effect = lambda identifier: '"' + identifier.replace('"', '""') + '"'
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
    pg: MagicMock,
    compute: MagicMock,
) -> ResourceCheckers:
    return ResourceCheckers(resources=resources, workspace=workspace, sql=sql, pg=pg, compute=compute)


def test_app_identity_resolves_service_principal_once(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    """Removing cached SP resolution would make this identity capability fail."""
    first = checkers.check_app_identity()
    second = checkers.check_app_identity()

    assert first.id == SetupStepId.IDENTITY
    assert first.state == StepState.PASSED
    assert second.state == StepState.PASSED
    workspace.current_user.me.assert_called_once_with()


def test_app_identity_resolution_failure_requires_action(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    """Swallowing an unresolved app identity would falsely pass setup."""
    workspace.current_user.me.side_effect = RuntimeError("platform detail\nthat must not escape")

    result = checkers.check_app_identity()

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "app_identity_unresolved"
    assert "platform detail" not in result.summary
    assert "\n" not in result.summary


def test_app_identity_with_c1_control_character_requires_action(
    checkers: ResourceCheckers, workspace: MagicMock
) -> None:
    """Accepting a C1 control character could inject terminal controls into setup output."""
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp\u0085id", id=None)

    result = checkers.check_app_identity()

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "app_identity_unresolved"


@pytest.mark.parametrize(
    ("missing_privilege", "grant_privilege"),
    [(Privilege.READ_VOLUME, "READ VOLUME"), (Privilege.WRITE_VOLUME, "WRITE VOLUME")],
)
def test_volume_missing_required_privilege_requires_action(
    checkers: ResourceCheckers,
    workspace: MagicMock,
    missing_privilege: Privilege,
    grant_privilege: str,
) -> None:
    """Dropping either required volume permission must make wheel storage unavailable."""
    granted = {Privilege.READ_VOLUME, Privilege.WRITE_VOLUME} - {missing_privilege}
    workspace.grants.get_effective.return_value = _effective_permissions(*granted)

    result = checkers.check_volume()

    assert result.id == SetupStepId.VOLUME
    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "volume_permissions_missing"
    assert grant_privilege in "\n".join(result.instructions)


def test_volume_with_read_and_write_privileges_passes(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    """Changing complete volume access to a failure would block a usable installation."""
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.READ_VOLUME, Privilege.WRITE_VOLUME)

    result = checkers.check_volume()

    assert result.state == StepState.PASSED
    assert result.code == ""


def test_volume_with_all_privileges_passes(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    """Treating ALL_PRIVILEGES literally would reject a fully authorized app identity."""
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)

    result = checkers.check_volume()

    assert result.state == StepState.PASSED
    assert result.code == ""


@pytest.mark.parametrize(
    ("missing_privilege", "grant_privilege"),
    [(Privilege.USE_CATALOG, "USE CATALOG"), (Privilege.CREATE_SCHEMA, "CREATE SCHEMA")],
)
def test_catalog_missing_required_privilege_requires_action(
    checkers: ResourceCheckers,
    workspace: MagicMock,
    missing_privilege: Privilege,
    grant_privilege: str,
) -> None:
    """Omitting a catalog capability must not permit schema reconciliation."""
    granted = {Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA} - {missing_privilege}
    workspace.grants.get_effective.return_value = _effective_permissions(*granted)

    result = checkers.check_unity_catalog()

    assert result.id == SetupStepId.UNITY_CATALOG
    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "catalog_permissions_missing"
    assert grant_privilege in "\n".join(result.instructions)


def test_catalog_and_schema_with_all_privileges_passes(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    """A full effective grant must imply each required catalog and schema capability."""
    workspace.grants.get_effective.return_value = _effective_permissions(Privilege.ALL_PRIVILEGES)

    result = checkers.check_unity_catalog()

    assert result.state == StepState.PASSED
    assert result.code == ""


@pytest.mark.parametrize(
    ("missing_privilege", "grant_privilege"),
    [(Privilege.USE_SCHEMA, "USE SCHEMA"), (Privilege.CREATE_TABLE, "CREATE TABLE")],
)
def test_main_schema_missing_required_privilege_requires_action(
    checkers: ResourceCheckers,
    workspace: MagicMock,
    missing_privilege: Privilege,
    grant_privilege: str,
) -> None:
    """Removing a main-schema capability must keep migrations from starting."""
    granted = {Privilege.USE_SCHEMA, Privilege.CREATE_TABLE} - {missing_privilege}
    workspace.grants.get_effective.side_effect = [
        _effective_permissions(Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA),
        _effective_permissions(*granted),
    ]

    result = checkers.check_unity_catalog()

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "catalog_permissions_missing"
    assert grant_privilege in "\n".join(result.instructions)


def test_catalog_and_main_schema_capabilities_pass(checkers: ResourceCheckers, workspace: MagicMock) -> None:
    """A fully capable app SP must be able to proceed to schema reconciliation."""
    workspace.grants.get_effective.side_effect = [
        _effective_permissions(Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA),
        _effective_permissions(Privilege.USE_SCHEMA, Privilege.CREATE_TABLE),
    ]

    result = checkers.check_unity_catalog()

    assert result.state == StepState.PASSED
    assert result.code == ""


def test_sibling_schema_creation_is_idempotent(checkers: ResourceCheckers, sql: MagicMock) -> None:
    """Removing IF NOT EXISTS would make a second setup reconciliation fail."""
    result = checkers.ensure_sibling_schemas()

    assert result.id == SetupStepId.SCHEMAS
    assert result.state == StepState.PASSED
    assert sql.execute_no_schema.call_count == 2
    assert all("CREATE SCHEMA IF NOT EXISTS" in call.args[0] for call in sql.execute_no_schema.call_args_list)


def test_lakebase_connectivity_failure_is_sanitized(checkers: ResourceCheckers, pg: MagicMock) -> None:
    """Returning database exceptions would disclose secrets and permit log injection."""
    pg.query.side_effect = RuntimeError("password=secret\nforged")

    result = checkers.check_lakebase()

    assert result.id == SetupStepId.LAKEBASE
    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "lakebase_connectivity_failed"
    assert "secret" not in result.summary
    assert "\n" not in result.summary


def test_lakebase_connectivity_passes_after_non_mutating_probe(checkers: ResourceCheckers, pg: MagicMock) -> None:
    """Replacing the health probe with a mutating statement would violate readiness checks."""
    pg.query.return_value = [["1"]]

    result = checkers.check_lakebase()

    assert result.state == StepState.PASSED
    pg.query.assert_called_once_with("SELECT 1")


def test_lakebase_schema_creation_is_idempotent(checkers: ResourceCheckers, pg: MagicMock) -> None:
    """Removing IF NOT EXISTS would make a repeated schema reconciliation fail."""
    result = checkers.ensure_lakebase_schema()

    assert result.state == StepState.PASSED
    pg.execute_no_schema.assert_called_once_with('CREATE SCHEMA IF NOT EXISTS "dqx_studio"')


def test_missing_warehouse_can_use_requires_action(checkers: ResourceCheckers, compute: MagicMock) -> None:
    """Treating a missing warehouse grant as ready would fail later SQL operations."""
    compute.warehouse_access_status.return_value = "missing"

    result = checkers.check_warehouse()

    assert result.id == SetupStepId.WAREHOUSE
    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "warehouse_permissions_missing"
    assert "CAN_USE" in "\n".join(result.instructions)


def test_warehouse_with_can_use_passes(checkers: ResourceCheckers, compute: MagicMock) -> None:
    """Changing a granted warehouse status to failure would block a usable installation."""
    compute.warehouse_access_status.return_value = "granted"

    result = checkers.check_warehouse()

    assert result.state == StepState.PASSED
    assert result.code == ""


def test_bound_warehouse_uses_query_when_acl_cannot_be_inspected(
    checkers: ResourceCheckers, compute: MagicMock, sql: MagicMock
) -> None:
    """An app SP with CAN_USE cannot necessarily inspect its own warehouse ACL."""
    compute.warehouse_access_status.return_value = "unknown"

    result = checkers.check_warehouse()

    assert result.state == StepState.PASSED
    sql.query.assert_called_once_with("SELECT 1")


def test_warehouse_candidate_uses_supplied_obo_reader(checkers: ResourceCheckers, compute: MagicMock) -> None:
    """Ignoring the caller's OBO reader could report the wrong warehouse access result."""
    obo_workspace = MagicMock(name="obo_workspace")

    result = checkers.check_warehouse("candidate-warehouse", reader_ws=obo_workspace)

    assert result.state == StepState.PASSED
    compute.warehouse_access_status.assert_called_once_with("candidate-warehouse", reader_ws=obo_workspace)


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
