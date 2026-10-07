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


_SP_APPLICATION_ID = "11111111-2222-3333-4444-555555555555"


def _fallback_inspector(rows: list[dict[str, str]]) -> tuple[GrantInspector, WorkspaceClient]:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.grants.get_effective.side_effect = PermissionError("denied")
    workspace.service_principals.list.return_value = []
    reader = create_autospec(SqlExecutor, instance=True)
    reader.query_dicts.return_value = rows
    return GrantInspector(workspace, reader), workspace


def test_show_grants_group_without_direct_grant_is_inspectable_as_empty() -> None:
    inspector, workspace = _fallback_inspector([{"Principal": "someone-else", "ActionType": "USE CATALOG"}])

    privileges = inspector.privileges("CATALOG", "main", "larry_test", required=frozenset({"USE_CATALOG"}))

    assert privileges == frozenset()
    workspace.service_principals.list.assert_not_called()


def test_show_grants_group_with_direct_grant_returns_it() -> None:
    inspector, _ = _fallback_inspector(
        [
            {"Principal": "larry_test", "ActionType": "USE CATALOG"},
            {"Principal": "someone-else", "ActionType": "ALL PRIVILEGES"},
        ]
    )

    privileges = inspector.privileges("CATALOG", "main", "larry_test", required=frozenset({"USE_CATALOG"}))

    assert privileges == frozenset({"USE_CATALOG"})


def test_show_grants_service_principal_with_no_lookup_match_is_uninspectable() -> None:
    inspector, workspace = _fallback_inspector([{"Principal": "someone-else", "ActionType": "USE CATALOG"}])

    privileges = inspector.privileges("CATALOG", "main", _SP_APPLICATION_ID, required=frozenset({"USE_CATALOG"}))

    assert privileges is None
    workspace.service_principals.list.assert_called_once()
