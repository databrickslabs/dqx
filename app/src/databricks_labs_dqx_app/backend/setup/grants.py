"""Unity Catalog grant inspection with effective-permission and SHOW GRANTS fallback."""

import json
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EffectivePermissionsList

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_identifier


_APPLICATION_ID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def effective_privilege_names(response: EffectivePermissionsList) -> frozenset[str]:
    """Flatten an effective-permissions response into privilege names.

    Args:
        response: Effective permissions returned by the grants API.

    Returns:
        Privilege names across all assignments.
    """
    privileges: set[str] = set()
    for assignment in response.privilege_assignments or []:
        for effective_privilege in assignment.privileges or []:
            privilege = effective_privilege.privilege
            value = getattr(privilege, "value", privilege)
            if isinstance(value, str):
                privileges.add(value)
    return frozenset(privileges)


def has_privilege(privileges: frozenset[str], privilege: str) -> bool:
    """Return whether *privileges* include *privilege*, treating ALL_PRIVILEGES as a superset."""
    return privilege in privileges or "ALL_PRIVILEGES" in privileges


def missing_privileges(response: EffectivePermissionsList, required: frozenset[str]) -> frozenset[str]:
    """Return the *required* privileges absent from an effective-permissions response."""
    privileges = effective_privilege_names(response)
    if "ALL_PRIVILEGES" in privileges:
        return frozenset()
    return required - privileges


class GrantInspector:
    """Inspect a principal's Unity Catalog privileges and securable ownership.

    Caches are per instance, so create one inspector per request-scoped SQL executor.
    """

    def __init__(self, workspace: WorkspaceClient, reader_sql: SqlExecutor | None = None) -> None:
        """Create an inspector.

        Args:
            workspace: Client used for effective permissions, ownership and group lookups.
            reader_sql: Optional executor used for the SHOW GRANTS fallback.
        """
        self._workspace = workspace
        self._reader_sql = reader_sql
        self._grant_rows: dict[tuple[str, str], list[dict[str, str]]] = {}
        self._memberships: dict[str, frozenset[str]] = {}

    def effective_permissions(self, kind: str, full_name: str, principal: str) -> EffectivePermissionsList | None:
        """Return effective permissions for *principal*, or None when unreadable."""
        try:
            return self._workspace.grants.get_effective(kind, full_name, principal=principal)
        except Exception:
            return None

    def owner(self, kind: str, full_name: str, reader_ws: WorkspaceClient | None = None) -> str | None:
        """Return the owner of a securable, or None when unknown or unreadable.

        Args:
            kind: VOLUME, CATALOG, SCHEMA or TABLE.
            full_name: Dotted securable name.
            reader_ws: Optional client to read with instead of the inspector's own.
        """
        workspace = reader_ws or self._workspace
        try:
            if kind == "VOLUME":
                securable = workspace.volumes.read(full_name)
            elif kind == "CATALOG":
                securable = workspace.catalogs.get(full_name)
            elif kind == "SCHEMA":
                securable = workspace.schemas.get(full_name)
            elif kind == "TABLE":
                securable = workspace.tables.get(full_name)
            else:
                return None
        except Exception:
            return None
        owner = getattr(securable, "owner", None)
        return owner if isinstance(owner, str) else None

    def privileges(
        self,
        kind: str,
        full_name: str,
        principal: str,
        *,
        required: frozenset[str] = frozenset(),
    ) -> frozenset[str] | None:
        """Return the principal's privileges on a securable, or None when uninspectable.

        Args:
            kind: Securable type.
            full_name: Dotted securable name.
            principal: Principal to inspect.
            required: Privileges the caller needs; lets the SHOW GRANTS fallback skip group expansion.
        """
        # App credentials support effective grants, unlike the OBO grants API.
        response = self.effective_permissions(kind, full_name, principal)
        if response is not None:
            return effective_privilege_names(response)
        if self._reader_sql is None:
            return None
        try:
            return self._show_grants_privileges(kind, full_name, principal, required, self._reader_sql)
        except Exception:
            return None

    def _show_grants_privileges(
        self,
        kind: str,
        full_name: str,
        principal: str,
        required: frozenset[str],
        reader_sql: SqlExecutor,
    ) -> frozenset[str] | None:
        parts = full_name.split(".")
        for part in parts:
            validate_identifier(part)
        validate_identifier(principal)
        objects = [(kind, full_name)]
        if kind != "CATALOG":
            objects.append(("CATALOG", parts[0]))
        if kind == "VOLUME":
            objects.append(("SCHEMA", ".".join(parts[:2])))
        rows: list[dict[str, str]] = []
        for object_kind, name in objects:
            key = (object_kind, name)
            if key not in self._grant_rows:
                normalized: list[dict[str, str]] = []
                for row in reader_sql.query_dicts(
                    f"SHOW GRANTS ON {object_kind} {quote_fqn(name)}", require_complete=True
                ):
                    values = {column.casefold(): value for column, value in row.items()}
                    granted_principal = values.get("principal")
                    action = values.get("actiontype")
                    if (
                        len(values) != len(row)
                        or not isinstance(granted_principal, str)
                        or not granted_principal.strip()
                        or not isinstance(action, str)
                        or not action.strip()
                    ):
                        return None
                    normalized.append({"principal": granted_principal, "actiontype": action})
                self._grant_rows[key] = normalized
            rows.extend(self._grant_rows[key])
        direct = frozenset(
            row["actiontype"].upper().replace(" ", "_")
            for row in rows
            if row["principal"].strip().casefold() == principal.casefold()
        )
        if required.issubset(direct) or "ALL_PRIVILEGES" in direct:
            return direct
        if not any(row["principal"].strip().casefold() != principal.casefold() for row in rows):
            return direct
        if not _APPLICATION_ID.fullmatch(principal):
            # Groups, users and account users have no service principal group memberships to expand.
            return direct
        if principal not in self._memberships:
            matches = [
                sp
                for sp in self._workspace.service_principals.list(filter=f"applicationId eq {json.dumps(principal)}")
                if (sp.application_id or "").casefold() == principal.casefold()
            ]
            if len(matches) != 1:
                return None
            self._memberships[principal] = frozenset(
                value.strip().casefold()
                for group in matches[0].groups or []
                for value in (group.display, group.value)
                if value
            )
        return direct | frozenset(
            row["actiontype"].upper().replace(" ", "_")
            for row in rows
            if row["principal"].strip().casefold() in self._memberships[principal]
        )
