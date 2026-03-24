from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

logger = logging.getLogger(__name__)


class RuleCatalogEntry:
    """Represents a single rule set in the rules catalog."""

    def __init__(
        self,
        table_fqn: str,
        checks: list[dict[str, Any]],
        version: int = 1,
        status: str = "draft",
        created_by: str | None = None,
        created_at: str | None = None,
        updated_by: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        self.table_fqn = table_fqn
        self.checks = checks
        self.version = version
        self.status = status
        self.created_by = created_by
        self.created_at = created_at
        self.updated_by = updated_by
        self.updated_at = updated_at


class RulesCatalogService:
    """Manages the rules catalog in a Delta table using app (SP) credentials.

    Each table has at most one rule set. Updates increment the version and
    reset the status to 'draft'. All operations use the app's service
    principal, not the calling user's OBO token.
    """

    VALID_STATUSES = {"draft", "pending_approval", "approved", "rejected"}

    VALID_TRANSITIONS: dict[str, set[str]] = {
        "draft": {"pending_approval"},
        "pending_approval": {"approved", "rejected"},
        "approved": {"draft"},
        "rejected": {"draft"},
    }

    def __init__(self, ws: WorkspaceClient, warehouse_id: str, catalog: str, schema: str) -> None:
        self._ws = ws
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._table = f"{catalog}.{schema}.dq_quality_rules"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_table(self) -> None:
        """Create the rules catalog table if it doesn't exist."""
        sql = (
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  table_fqn STRING NOT NULL,"
            "  checks STRING NOT NULL,"
            "  version INT DEFAULT 1,"
            "  status STRING DEFAULT 'draft',"
            "  created_by STRING,"
            "  created_at TIMESTAMP,"
            "  updated_by STRING,"
            "  updated_at TIMESTAMP"
            ")"
        )
        self._execute(sql)
        logger.info(f"Ensured rules catalog table exists: {self._table}")

    def save(self, table_fqn: str, checks: list[dict[str, Any]], user_email: str) -> RuleCatalogEntry:
        """Upsert a rule set for a table. Inserts if new, increments version if existing."""
        checks_json = json.dumps(checks).replace("'", "\\'")
        now = datetime.utcnow().isoformat()
        escaped_email = user_email.replace("'", "\\'")
        escaped_fqn = table_fqn.replace("'", "\\'")

        sql = (
            f"MERGE INTO {self._table} AS target "
            f"USING (SELECT '{escaped_fqn}' AS table_fqn) AS source "
            "ON target.table_fqn = source.table_fqn "
            "WHEN MATCHED THEN UPDATE SET "
            f"  checks = '{checks_json}', "
            "  version = target.version + 1, "
            "  status = 'draft', "
            f"  updated_by = '{escaped_email}', "
            f"  updated_at = '{now}' "
            "WHEN NOT MATCHED THEN INSERT (table_fqn, checks, version, status, created_by, created_at, updated_by, updated_at) "
            f"VALUES ('{escaped_fqn}', '{checks_json}', 1, 'draft', '{escaped_email}', '{now}', '{escaped_email}', '{now}')"
        )
        self._execute(sql)
        logger.info(f"Saved rules for table: {table_fqn}")

        entry = self.get(table_fqn)
        if entry is None:
            # Return a constructed entry if immediate read-after-write doesn't work
            return RuleCatalogEntry(
                table_fqn=table_fqn,
                checks=checks,
                version=1,
                status="draft",
                created_by=user_email,
                created_at=now,
                updated_by=user_email,
                updated_at=now,
            )
        return entry

    def get(self, table_fqn: str) -> RuleCatalogEntry | None:
        """Get the rule set for a specific table."""
        escaped_fqn = table_fqn.replace("'", "\\'")
        sql = (
            f"SELECT table_fqn, checks, version, status, created_by, "
            f"CAST(created_at AS STRING), updated_by, CAST(updated_at AS STRING) "
            f"FROM {self._table} WHERE table_fqn = '{escaped_fqn}'"
        )
        rows = self._query(sql)
        if not rows:
            return None
        return self.row_to_entry(rows[0])

    def list_rules(self, status: str | None = None) -> list[RuleCatalogEntry]:
        """List all rule sets, optionally filtered by status."""
        sql = (
            f"SELECT table_fqn, checks, version, status, created_by, "
            f"CAST(created_at AS STRING), updated_by, CAST(updated_at AS STRING) "
            f"FROM {self._table}"
        )
        if status:
            escaped_status = status.replace("'", "\\'")
            sql += f" WHERE status = '{escaped_status}'"
        sql += " ORDER BY updated_at DESC"

        rows = self._query(sql)
        return [self.row_to_entry(row) for row in rows]

    def delete(self, table_fqn: str, user_email: str) -> None:
        """Delete the rule set for a table."""
        escaped_fqn = table_fqn.replace("'", "\\'")
        sql = f"DELETE FROM {self._table} WHERE table_fqn = '{escaped_fqn}'"
        self._execute(sql)
        logger.info(f"Deleted rules for table: {table_fqn} (by {user_email})")

    def set_status(
        self,
        table_fqn: str,
        status: str,
        user_email: str,
        expected_version: int | None = None,
    ) -> RuleCatalogEntry:
        """Update the approval status of a rule set.

        Enforces valid state transitions and optional optimistic concurrency
        via ``expected_version``. If the row's current version does not match,
        the UPDATE is a no-op and a ``RuntimeError`` is raised.
        """
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}. Must be one of {self.VALID_STATUSES}")

        entry = self.get(table_fqn)
        if entry is None:
            raise RuntimeError(f"Rule set not found: {table_fqn}")

        allowed = self.VALID_TRANSITIONS.get(entry.status, set())
        if status not in allowed:
            raise ValueError(
                f"Cannot transition from '{entry.status}' to '{status}'. " f"Allowed transitions: {allowed or 'none'}"
            )

        if expected_version is not None and entry.version != expected_version:
            raise RuntimeError(
                f"Version conflict for {table_fqn}: expected v{expected_version}, "
                f"but current is v{entry.version}. Another user may have modified the rules."
            )

        escaped_fqn = table_fqn.replace("'", "\\'")
        escaped_status = status.replace("'", "\\'")
        escaped_email = user_email.replace("'", "\\'")
        now = datetime.utcnow().isoformat()

        version_guard = f" AND version = {entry.version}"
        sql = (
            f"UPDATE {self._table} SET "
            f"  status = '{escaped_status}', "
            f"  updated_by = '{escaped_email}', "
            f"  updated_at = '{now}' "
            f"WHERE table_fqn = '{escaped_fqn}'{version_guard}"
        )
        self._execute(sql)
        logger.info(f"Updated status for {table_fqn} to {status} (by {user_email})")

        updated = self.get(table_fqn)
        if updated is None:
            raise RuntimeError(f"Rule set not found after status update: {table_fqn}")
        if updated.status != status:
            raise RuntimeError(
                f"Status update for {table_fqn} did not take effect — " "the row may have been modified concurrently."
            )
        return updated

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def row_to_entry(self, row: list[str]) -> RuleCatalogEntry:
        """Convert a query result row to a RuleCatalogEntry."""
        checks = json.loads(row[1]) if row[1] else []
        return RuleCatalogEntry(
            table_fqn=row[0],
            checks=checks,
            version=int(row[2]) if row[2] else 1,
            status=row[3] or "draft",
            created_by=row[4],
            created_at=row[5],
            updated_by=row[6],
            updated_at=row[7],
        )

    def _execute(self, sql: str) -> None:
        """Execute a SQL statement that doesn't return rows."""
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}")

    def _query(self, sql: str) -> list[list[str]]:
        """Execute a SQL query and return rows as lists of strings."""
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}")

        if resp.result and resp.result.data_array:
            return resp.result.data_array
        return []
