"""Admin "Reset database" — clears DQX Studio-managed data only.

This is a **destructive** operation, so the guardrails are the point:

- The route is hard-gated to :class:`UserRole.ADMIN` (see
  ``routes/v1/admin.py``); a non-admin can never reach this service.
- A confirmation phrase (:data:`RESET_CONFIRMATION_PHRASE`) is required in
  the request body and validated server-side (defense-in-depth on top of
  the role gate).
- Only the app's OWN managed tables are cleared — the ``dq_*`` tables the
  migrations create, enumerated authoritatively from
  :data:`backend.migrations.ANALYTICAL_TABLE_NAMES` /
  :data:`backend.migrations.OLTP_TABLE_NAMES`. The customer data tables the
  app merely *monitors* are never referenced here, so they cannot be
  touched.

Scope decisions:

- Rows are DELETEd, not tables DROPped — the schema (and the
  ``dq_migrations`` version tracker) must survive so the app keeps working
  without a redeploy/re-migrate.
- ``dq_app_settings`` IS cleared (a full reset). Every setting degrades to a
  compiled-in default on read, and the seed rows (run review statuses, label
  definitions) are re-seeded at the next app startup, so a wipe is safe.
- The acting admin is NOT locked out: ``dq_role_mappings`` rows for the
  ``admin`` role are preserved, so every admin (including the caller) keeps
  access. All other role mappings are cleared.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.migrations import (
    ANALYTICAL_TABLE_NAMES,
    OLTP_TABLE_NAMES,
)
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

# The exact phrase the admin must type in the UI and send in the request
# body. Kept lowercase + fixed so the UI can show it verbatim and the check
# is a simple case-sensitive equality. Changing this is a breaking change for
# the UI copy — keep the two in lock-step.
RESET_CONFIRMATION_PHRASE = "reset dqx studio"

# The one OLTP table that gets partial (not full) clearing: admin role
# mappings are preserved so the acting admin is never locked out.
_ROLE_MAPPINGS_TABLE = "dq_role_mappings"


@dataclass(frozen=True)
class DatabaseResetResult:
    """Outcome of a database reset.

    Attributes:
        cleared_tables: App-owned tables whose rows were cleared.
        failed_tables: Tables whose clear raised (name -> error message).
            A table missing on an older deploy shows up here rather than
            aborting the whole reset.
        preserved_note: Human-readable note on what was intentionally kept.
        performed_by: Email of the admin who ran the reset.
        performed_at: UTC timestamp (ISO-8601) of the reset.
    """

    performed_by: str
    performed_at: str
    cleared_tables: list[str] = field(default_factory=list)
    failed_tables: dict[str, str] = field(default_factory=dict)
    preserved_note: str = ""


class DatabaseResetService:
    """Clears DQX Studio-managed data across the Delta + OLTP backends.

    Args:
        delta_sql: SP-scoped Delta executor (owns the analytical tables and,
            when Lakebase is disabled, everything).
        oltp_sql: The executor that owns the OLTP tables — a Postgres
            executor when Lakebase is enabled, otherwise the same Delta
            executor as *delta_sql*.
    """

    def __init__(self, delta_sql: SqlExecutor, oltp_sql: OltpExecutorProtocol) -> None:
        self._delta = delta_sql
        self._oltp = oltp_sql

    def reset_all_data(self, *, performed_by: str) -> DatabaseResetResult:
        """Clear every app-owned table, preserving admin role mappings.

        Analytical tables are cleared through the Delta executor; OLTP
        tables through the injected OLTP executor. The two table sets are
        disjoint, so when Lakebase is disabled (both executors are the same
        Delta executor) nothing is cleared twice.

        Table clears are best-effort and independent: a failure on one table
        (e.g. it does not exist on an older deploy) is recorded and the reset
        continues, rather than leaving the database half-cleared on the first
        error.

        Args:
            performed_by: Email of the acting admin, for the audit log.

        Returns:
            A :class:`DatabaseResetResult` describing what was cleared,
            what failed, and what was preserved.
        """
        cleared: list[str] = []
        failed: dict[str, str] = {}

        for name in ANALYTICAL_TABLE_NAMES:
            self._clear_table(self._delta, name, cleared, failed)

        for name in OLTP_TABLE_NAMES:
            if name == _ROLE_MAPPINGS_TABLE:
                self._clear_role_mappings_preserving_admins(cleared, failed)
            else:
                self._clear_table(self._oltp, name, cleared, failed)

        # Audit log — who/when/what, no data payloads. ``performed_by`` is
        # newline-stripped to prevent log-forging (CWE-117); it originates
        # from the platform-verified identity but we sanitise defensively.
        safe_actor = performed_by.replace("\n", " ").replace("\r", " ")
        logger.warning(
            f"DATABASE RESET performed by={safe_actor} cleared_count={len(cleared)} "
            f"failed_count={len(failed)} (admin role mappings preserved; dq_migrations untouched)"
        )
        if failed:
            logger.warning(f"DATABASE RESET tables that failed to clear: {sorted(failed)}")

        return DatabaseResetResult(
            performed_by=performed_by,
            performed_at=datetime.now(timezone.utc).isoformat(),
            cleared_tables=cleared,
            failed_tables=failed,
            preserved_note=(
                "Cleared all DQX Studio-managed data. Preserved: the schema itself, "
                "the dq_migrations version tracker, and admin role mappings (so admins "
                "keep access). Customer/monitored data tables are never touched."
            ),
        )

    def _clear_table(
        self,
        executor: SqlExecutor | OltpExecutorProtocol,
        table: str,
        cleared: list[str],
        failed: dict[str, str],
    ) -> None:
        """DELETE all rows from a single app-owned table (best-effort)."""
        fqn = executor.fqn(table)
        try:
            executor.execute(f"DELETE FROM {fqn}")
            cleared.append(table)
        except Exception as exc:
            # Best-effort per table: a missing table on an older deploy is
            # recorded, not swallowed, and never aborts the whole reset.
            failed[table] = str(exc)
            logger.warning("Failed to clear table %s: %s", table, exc, exc_info=True)

    def _clear_role_mappings_preserving_admins(
        self,
        cleared: list[str],
        failed: dict[str, str],
    ) -> None:
        """Clear role mappings EXCEPT the ``admin`` role.

        Preserving admin rows guarantees the acting admin — and every other
        admin — keeps access after the reset. (Admins granted via the
        bootstrap ``DQX_ADMIN_GROUP`` env var are unaffected regardless, but
        an admin whose access comes only from a stored mapping would be
        locked out if we cleared it.)
        """
        fqn = self._oltp.fqn(_ROLE_MAPPINGS_TABLE)
        admin_role = escape_sql_string(UserRole.ADMIN.value)
        try:
            self._oltp.execute(f"DELETE FROM {fqn} WHERE role <> '{admin_role}'")
            cleared.append(_ROLE_MAPPINGS_TABLE)
        except Exception as exc:
            # Best-effort: recorded, not swallowed, and never aborts the reset.
            failed[_ROLE_MAPPINGS_TABLE] = str(exc)
            logger.warning("Failed to clear role mappings: %s", exc, exc_info=True)
