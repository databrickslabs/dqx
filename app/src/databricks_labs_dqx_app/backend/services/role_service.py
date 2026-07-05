"""Role management service for RBAC.

Manages role-to-group mappings stored in a Delta table. Uses the app's
service principal credentials for all operations.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.common.authorization import ROLE_PRIORITY, UserRole
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql
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


@dataclass
class RoleMappingHistoryEntry:
    """One row from the ``dq_role_mappings_history`` audit table.

    See :meth:`RoleService.list_history` for the read contract and
    :meth:`RoleService._record_history` for the write contract.
    """

    role: str
    group_name: str
    action: str
    changed_by: str | None
    changed_at: datetime | None


class RoleService:
    """Manages role-to-group mappings in ``dq_role_mappings``.

    All operations use the app's service principal, not the calling
    user's OBO token. The table lives on Lakebase Postgres when
    Lakebase is enabled and on Delta otherwise — the injected
    executor decides which.

    Every mutation (``create_mapping`` / ``delete_mapping``) is mirrored
    into the append-only ``dq_role_mappings_history`` audit table so the
    full timeline of who-changed-what-when survives even after a mapping
    is removed. The mutable :attr:`_table` only ever holds the *current*
    set of (role, group) pairs; the history table is the source of truth
    for compliance / "what changed last week" questions. See the docstring
    on :data:`backend.migrations._V7_ROLE_MAPPINGS_HISTORY` for the table
    schema and the rationale for the ``action`` vocabulary.
    """

    # Action vocabulary written into ``dq_role_mappings_history.action``.
    # Kept narrow (only ``create`` and ``delete``) because the live row
    # has no mutable value columns — re-saving an existing (role,
    # group_name) pair is a no-op semantically, so we don't emit an
    # ``update`` row for it. Re-adding a previously-deleted mapping is
    # recorded as a fresh ``create`` (sequence in the audit log
    # disambiguates).
    _ACTION_CREATE = "create"
    _ACTION_DELETE = "delete"

    def __init__(self, sql: OltpExecutorProtocol) -> None:
        self._sql = sql
        self._table = sql.fqn("dq_role_mappings")
        self._history_table = sql.fqn("dq_role_mappings_history")
        self._mappings_cache: list[RoleMapping] | None = None
        self._mappings_cache_expires: float = 0.0

    def list_mappings(self, *, use_cache: bool = False) -> list[RoleMapping]:
        """List all role-to-group mappings."""
        if use_cache and self._mappings_cache is not None and time.monotonic() < self._mappings_cache_expires:
            return self._mappings_cache

        sql = (
            f"SELECT role, group_name, created_by, "
            f"{self._sql.ts_text('created_at')}, updated_by, {self._sql.ts_text('updated_at')} "
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
            f"{self._sql.ts_text('created_at')}, updated_by, {self._sql.ts_text('updated_at')} "
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
        """Create or update a role-to-group mapping.

        Dialect-agnostic — :meth:`OltpExecutorProtocol.upsert_with_audit`
        with ``preserve_created=True`` handles the
        ``MERGE INTO`` (Delta) / ``INSERT ... ON CONFLICT DO UPDATE``
        (Postgres) choice so the original ``created_*`` survive on
        UPDATE.

        Also records a ``create`` row in ``dq_role_mappings_history``
        (best-effort; history failures do not roll back the mapping
        change — see :meth:`_record_history`).
        """
        if role not in [r.value for r in UserRole]:
            raise ValueError(f"Invalid role: {role}. Must be one of {[r.value for r in UserRole]}")

        now = RawSql("now()")
        self._sql.upsert_with_audit(
            table=self._table,
            key_cols={"role": role, "group_name": group_name},
            value_cols={
                "created_by": user_email,
                "created_at": now,
                "updated_by": user_email,
                "updated_at": now,
            },
            preserve_created=True,
        )
        self._record_history(
            role=role,
            group_name=group_name,
            action=self._ACTION_CREATE,
            user_email=user_email,
        )
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

    def delete_mapping(self, role: str, group_name: str, user_email: str | None = None) -> None:
        """Delete a role-to-group mapping.

        ``user_email`` is the actor who performed the deletion (recorded
        in the audit log). Defaulted to ``None`` so existing call sites
        that don't yet pass it keep working — those rows will surface in
        the audit log with ``changed_by = NULL``. New call sites should
        always pass the OBO user email.
        """
        escaped_role = escape_sql_string(role)
        escaped_group = escape_sql_string(group_name)

        sql = f"DELETE FROM {self._table} WHERE role = '{escaped_role}' AND group_name = '{escaped_group}'"
        self._sql.execute(sql)
        self._record_history(
            role=role,
            group_name=group_name,
            action=self._ACTION_DELETE,
            user_email=user_email,
        )
        self.invalidate_mappings_cache()
        logger.info(f"Deleted role mapping: {role} -> {group_name}")

    # ------------------------------------------------------------------
    # Audit history
    # ------------------------------------------------------------------

    def list_history(
        self,
        *,
        role: str | None = None,
        group_name: str | None = None,
        limit: int = 200,
    ) -> list[RoleMappingHistoryEntry]:
        """Return audit-log rows for role mapping changes, newest first.

        Args:
            role: Optional exact-match filter on ``role``.
            group_name: Optional exact-match filter on ``group_name``.
            limit: Hard cap on the number of rows returned (defaults to
                200 — same as the workspace-groups dropdown, plenty for
                the admin Settings page UI).
        """
        # Hard cap so a hostile caller can't OOM the app by passing an
        # absurdly large limit. 1000 matches the cap on the
        # /roles/groups endpoint.
        limit = max(1, min(int(limit), 1000))

        where_clauses: list[str] = []
        if role is not None:
            where_clauses.append(f"role = '{escape_sql_string(role)}'")
        if group_name is not None:
            where_clauses.append(f"group_name = '{escape_sql_string(group_name)}'")
        where = f"WHERE {' AND '.join(where_clauses)} " if where_clauses else ""

        sql = (
            f"SELECT role, group_name, action, changed_by, "
            f"{self._sql.ts_text('changed_at')} "
            f"FROM {self._history_table} "
            f"{where}"
            f"ORDER BY changed_at DESC "
            f"LIMIT {limit}"
        )
        rows = self._sql.query(sql)
        return [
            RoleMappingHistoryEntry(
                role=row[0],
                group_name=row[1],
                action=row[2],
                changed_by=row[3] if row[3] else None,
                changed_at=datetime.fromisoformat(row[4]) if row[4] else None,
            )
            for row in rows
        ]

    def _record_history(
        self,
        *,
        role: str,
        group_name: str,
        action: str,
        user_email: str | None,
    ) -> None:
        """Insert an audit row into ``dq_role_mappings_history`` (best-effort).

        Same contract as :meth:`RulesCatalogService._record_history`:
        a history-write failure must NEVER roll back the primary
        mutation, because losing one audit row is far less harmful
        than refusing a legitimate admin change. Failures are logged at
        WARNING with the stack trace so they're still investigable
        post-hoc.
        """
        try:
            escaped_role = escape_sql_string(role)
            escaped_group = escape_sql_string(group_name)
            escaped_action = escape_sql_string(action)
            user_sql = f"'{escape_sql_string(user_email)}'" if user_email else "NULL"
            sql = (
                f"INSERT INTO {self._history_table} "
                f"(role, group_name, action, changed_by, changed_at) VALUES "
                f"('{escaped_role}', '{escaped_group}', '{escaped_action}', "
                f"{user_sql}, now())"
            )
            self._sql.execute(sql)
        except Exception:
            logger.warning(
                "Failed to record role-mapping history for %s -> %s (non-fatal)",
                role,
                group_name,
                exc_info=True,
            )

    def resolve_role(self, user_groups: list[str], admin_group: str | None = None) -> UserRole:
        """Determine user's highest *primary* role based on group membership.

        Args:
            user_groups: List of Databricks group names the user belongs to.
            admin_group: Bootstrap admin group from config (always grants Admin).

        Returns:
            The highest priority role the user has, or VIEWER if no mappings match.

        Note:
            ``UserRole.RUNNER`` is **not** part of the primary-role hierarchy —
            it's an additive role resolved separately by
            :meth:`has_runner_role`. A user mapped only to RUNNER still has
            ``VIEWER`` as their primary role; the runner privilege is
            applied on top.
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

        # Walk the priority list (which excludes RUNNER) — this is what
        # makes RUNNER orthogonal: it never up-ranks the primary role.
        for role in reversed(ROLE_PRIORITY):
            if role in matched_roles:
                logger.debug(f"Resolved role: {role.value}")
                return role

        # Either there were no matches at all, or the only match was
        # RUNNER (which is fine — primary role stays VIEWER, runner flag
        # is applied separately).
        logger.debug("No primary-role match (or runner-only); defaulting to VIEWER")
        return UserRole.VIEWER

    def has_runner_role(self, user_groups: list[str], admin_group: str | None = None) -> bool:
        """Return True if the user holds the orthogonal RUNNER role.

        Resolution rules:
        - Members of the bootstrap admin group are *always* runners (so
          ADMINs never need an explicit RUNNER mapping).
        - Otherwise, the user is a runner iff at least one of their groups
          is mapped to ``UserRole.RUNNER`` in ``dq_role_mappings``.

        This is intentionally separate from :meth:`resolve_role` so the
        runner flag never bleeds into primary-role hierarchy comparisons.
        """
        if admin_group and admin_group in user_groups:
            return True
        mappings = self.list_mappings(use_cache=True)
        if not mappings:
            return False
        runner_groups = {m.group_name for m in mappings if m.role == UserRole.RUNNER.value}
        if not runner_groups:
            return False
        return any(g in runner_groups for g in user_groups)
