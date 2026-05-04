"""Role management service for RBAC.

Manages role-to-group mappings stored in a Delta table. Uses the app's
service principal credentials for all operations.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.common.authorization import ROLE_PRIORITY, UserRole
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

_MAPPINGS_CACHE_TTL = 120  # 2 minutes


@dataclass
class RoleMapping:
    """Represents a mapping from a role to a Databricks workspace group."""

    role: str
    group_name: str
    created_by: str | None = None
    created_at: datetime | None = None
    updated_by: str | None = None
    updated_at: datetime | None = None


class RoleService:
    """Manages role-to-group mappings in a Delta table.

    All operations use the app's service principal, not the calling user's
    OBO token.
    """

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        self._table = f"{sql.catalog}.{sql.schema}.dq_role_mappings"
        self._mappings_cache: list[RoleMapping] | None = None
        self._mappings_cache_expires: float = 0.0

    def list_mappings(self, *, use_cache: bool = False) -> list[RoleMapping]:
        """List all role-to-group mappings."""
        if use_cache and self._mappings_cache is not None and time.monotonic() < self._mappings_cache_expires:
            return self._mappings_cache

        sql = (
            f"SELECT role, group_name, created_by, "
            f"CAST(created_at AS STRING), updated_by, CAST(updated_at AS STRING) "
            f"FROM {self._table} ORDER BY role, group_name"
        )
        rows = self._sql.query(sql)
        mappings = [
            RoleMapping(
                role=row[0],
                group_name=row[1],
                created_by=row[2] if row[2] else None,
                created_at=datetime.fromisoformat(row[3]) if row[3] else None,
                updated_by=row[4] if row[4] else None,
                updated_at=datetime.fromisoformat(row[5]) if row[5] else None,
            )
            for row in rows
        ]
        self._mappings_cache = mappings
        self._mappings_cache_expires = time.monotonic() + _MAPPINGS_CACHE_TTL
        return mappings

    def invalidate_mappings_cache(self) -> None:
        """Force the next list_mappings() call to hit the database."""
        self._mappings_cache = None
        self._mappings_cache_expires = 0.0

    def get_mappings_for_role(self, role: str) -> list[RoleMapping]:
        """Get all group mappings for a specific role."""
        escaped_role = escape_sql_string(role)
        sql = (
            f"SELECT role, group_name, created_by, "
            f"CAST(created_at AS STRING), updated_by, CAST(updated_at AS STRING) "
            f"FROM {self._table} WHERE role = '{escaped_role}' ORDER BY group_name"
        )
        rows = self._sql.query(sql)
        return [
            RoleMapping(
                role=row[0],
                group_name=row[1],
                created_by=row[2] if row[2] else None,
                created_at=datetime.fromisoformat(row[3]) if row[3] else None,
                updated_by=row[4] if row[4] else None,
                updated_at=datetime.fromisoformat(row[5]) if row[5] else None,
            )
            for row in rows
        ]

    def create_mapping(self, role: str, group_name: str, user_email: str) -> RoleMapping:
        """Create or update a role-to-group mapping."""
        if role not in [r.value for r in UserRole]:
            raise ValueError(f"Invalid role: {role}. Must be one of {[r.value for r in UserRole]}")

        escaped_role = escape_sql_string(role)
        escaped_group = escape_sql_string(group_name)
        escaped_user = escape_sql_string(user_email)

        sql = (
            f"MERGE INTO {self._table} AS target "
            f"USING (SELECT '{escaped_role}' AS role, '{escaped_group}' AS group_name) AS source "
            "ON target.role = source.role AND target.group_name = source.group_name "
            "WHEN MATCHED THEN UPDATE SET "
            f"  updated_by = '{escaped_user}', "
            "  updated_at = current_timestamp() "
            "WHEN NOT MATCHED THEN INSERT (role, group_name, created_by, created_at, updated_by, updated_at) "
            f"VALUES ('{escaped_role}', '{escaped_group}', '{escaped_user}', current_timestamp(), "
            f"'{escaped_user}', current_timestamp())"
        )
        self._sql.execute(sql)
        self.invalidate_mappings_cache()
        logger.info(f"Created/updated role mapping: {role} -> {group_name}")

        return RoleMapping(
            role=role,
            group_name=group_name,
            created_by=user_email,
            created_at=datetime.now(timezone.utc),
            updated_by=user_email,
            updated_at=datetime.now(timezone.utc),
        )

    def delete_mapping(self, role: str, group_name: str) -> None:
        """Delete a role-to-group mapping."""
        escaped_role = escape_sql_string(role)
        escaped_group = escape_sql_string(group_name)

        sql = f"DELETE FROM {self._table} WHERE role = '{escaped_role}' AND group_name = '{escaped_group}'"
        self._sql.execute(sql)
        self.invalidate_mappings_cache()
        logger.info(f"Deleted role mapping: {role} -> {group_name}")

    def resolve_role(self, user_groups: list[str], admin_group: str | None = None) -> UserRole:
        """Determine user's highest role based on group membership.

        Args:
            user_groups: List of Databricks group names the user belongs to.
            admin_group: Bootstrap admin group from config (always grants Admin).

        Returns:
            The highest priority role the user has, or VIEWER if no mappings match.
        """
        if admin_group and admin_group in user_groups:
            logger.debug(f"User in bootstrap admin group '{admin_group}', granting ADMIN")
            return UserRole.ADMIN

        mappings = self.list_mappings(use_cache=True)
        if not mappings:
            logger.debug("No role mappings configured, defaulting to VIEWER")
            return UserRole.VIEWER

        group_to_roles: dict[str, list[str]] = {}
        for mapping in mappings:
            if mapping.group_name not in group_to_roles:
                group_to_roles[mapping.group_name] = []
            group_to_roles[mapping.group_name].append(mapping.role)

        matched_roles: set[UserRole] = set()
        for group in user_groups:
            if group in group_to_roles:
                for role_str in group_to_roles[group]:
                    try:
                        matched_roles.add(UserRole(role_str))
                    except ValueError:
                        logger.warning(f"Unknown role in mapping: {role_str}")

        if not matched_roles:
            logger.debug("User groups don't match any role mappings, defaulting to VIEWER")
            return UserRole.VIEWER

        for role in reversed(ROLE_PRIORITY):
            if role in matched_roles:
                logger.debug(f"Resolved role: {role.value}")
                return role

        return UserRole.VIEWER
