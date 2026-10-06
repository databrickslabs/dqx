"""UC privilege inspection with effective-permission and SHOW GRANTS fallback."""

from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    EffectivePermissionsList,
    EffectivePrivilege,
    EffectivePrivilegeAssignment,
    Privilege,
)

from databricks_labs_dqx_app.backend.setup.grants import GrantInspector, has_privilege
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def test_effective_permissions_are_used_when_readable() -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.grants.get_effective.return_value = EffectivePermissionsList(
        privilege_assignments=[
            EffectivePrivilegeAssignment(
                principal="data-team", privileges=[EffectivePrivilege(privilege=Privilege.USE_SCHEMA)]
            )
        ]
    )

    privileges = GrantInspector(workspace).privileges("SCHEMA", "main.studio_tmp", "data-team")

    assert privileges == frozenset({"USE_SCHEMA"})


def test_show_grants_fallback_reads_direct_group_grants() -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.grants.get_effective.side_effect = PermissionError("denied")
    reader = create_autospec(SqlExecutor, instance=True)
    reader.query_dicts.return_value = [{"Principal": "data-team", "ActionType": "USE SCHEMA"}]

    privileges = GrantInspector(workspace, reader).privileges(
        "SCHEMA", "main.studio_tmp", "data-team", required=frozenset({"USE_SCHEMA"})
    )

    assert privileges is not None and "USE_SCHEMA" in privileges


def test_uninspectable_grants_return_none() -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.grants.get_effective.side_effect = PermissionError("denied")

    assert GrantInspector(workspace).privileges("SCHEMA", "main.studio_tmp", "data-team") is None


def test_all_privileges_implies_every_privilege() -> None:
    assert has_privilege(frozenset({"ALL_PRIVILEGES"}), "MODIFY") is True
    assert has_privilege(frozenset({"SELECT"}), "MODIFY") is False
