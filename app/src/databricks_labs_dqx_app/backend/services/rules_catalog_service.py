from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

logger = logging.getLogger(__name__)


class RuleCatalogEntry:
    """Represents a single rule (individual check) in the rules catalog."""

    def __init__(
        self,
        table_fqn: str,
        checks: list[dict[str, Any]],
        version: int = 1,
        status: str = "draft",
        source: str = "ui",
        created_by: str | None = None,
        created_at: str | None = None,
        updated_by: str | None = None,
        updated_at: str | None = None,
        rule_id: str | None = None,
    ) -> None:
        self.table_fqn = table_fqn
        self.checks = checks
        self.version = version
        self.status = status
        self.source = source
        self.created_by = created_by
        self.created_at = created_at
        self.updated_by = updated_by
        self.updated_at = updated_at
        self.rule_id = rule_id


class RulesCatalogService:
    """Manages the rules catalog in a Delta table using app (SP) credentials.

    Each row represents a single rule (one check) identified by ``rule_id``.
    Multiple rules can target the same ``table_fqn``.  For execution the caller
    aggregates all approved rules for a table into a single checks array.
    """

    VALID_STATUSES = {"draft", "pending_approval", "approved", "rejected"}

    VALID_TRANSITIONS: dict[str, set[str]] = {
        "draft": {"pending_approval"},
        "pending_approval": {"approved", "rejected", "draft"},
        "approved": {"draft"},
        "rejected": {"draft"},
    }

    _SELECT_COLS = (
        "table_fqn, checks, version, status, created_by, "
        "CAST(created_at AS STRING), updated_by, CAST(updated_at AS STRING), "
        "COALESCE(source, 'ui'), COALESCE(rule_id, '')"
    )

    def __init__(self, ws: WorkspaceClient, warehouse_id: str, catalog: str, schema: str) -> None:
        self._ws = ws
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._table = f"{catalog}.{schema}.dq_quality_rules"
        self._history_table = f"{catalog}.{schema}.dq_quality_rules_history"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(
        self,
        table_fqn: str,
        checks: list[dict[str, Any]],
        user_email: str,
        source: str = "ui",
    ) -> list[RuleCatalogEntry]:
        """Save one or more checks as individual rule rows.

        Each element in *checks* becomes its own row with a unique ``rule_id``.
        Duplicate checks (same function + arguments for the same table) are
        silently skipped.  Returns the list of newly created entries.
        """
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

        validate_fqn(table_fqn)
        self._validate_sql_checks(checks)

        duplicates = self.find_duplicates(table_fqn, checks)
        dup_sigs = {self._check_signature(d) for d in duplicates}
        non_dup_checks = [c for c in checks if self._check_signature(c) not in dup_sigs]
        if not non_dup_checks:
            raise ValueError("All submitted checks already exist for this table. " "Duplicate rules cannot be created.")
        if duplicates:
            logger.info("Skipped %d duplicate check(s) for table %s", len(duplicates), table_fqn)

        now = datetime.now(timezone.utc).isoformat()
        e_email = escape_sql_string(user_email)
        e_fqn = escape_sql_string(table_fqn)
        e_source = escape_sql_string(source)

        created: list[RuleCatalogEntry] = []
        for check in non_dup_checks:
            rule_id = uuid4().hex[:16]
            single_check_json = escape_sql_string(json.dumps([check]))
            sql = (
                f"INSERT INTO {self._table} "
                "(table_fqn, checks, version, status, source, created_by, created_at, updated_by, updated_at, rule_id) "
                f"VALUES ('{e_fqn}', '{single_check_json}', 1, 'draft', '{e_source}', "
                f"'{e_email}', '{now}', '{e_email}', '{now}', '{rule_id}')"
            )
            self._execute(sql)
            self._record_history(table_fqn, single_check_json, 1, source, user_email, now, "save", rule_id)
            created.append(
                RuleCatalogEntry(
                    table_fqn=table_fqn,
                    checks=[check],
                    version=1,
                    status="draft",
                    source=source,
                    created_by=user_email,
                    created_at=now,
                    updated_by=user_email,
                    updated_at=now,
                    rule_id=rule_id,
                )
            )
        logger.info("Saved %d rule(s) for table %s (source=%s)", len(created), table_fqn, source)
        return created

    def update_rule(
        self,
        rule_id: str,
        checks: list[dict[str, Any]],
        user_email: str,
    ) -> RuleCatalogEntry:
        """Update the check definition for an existing rule, incrementing version and resetting to draft."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        self._validate_sql_checks(checks)
        entry = self.get_by_rule_id(rule_id)
        if entry is None:
            raise RuntimeError(f"Rule not found: {rule_id}")

        checks_json = escape_sql_string(json.dumps(checks))
        e_email = escape_sql_string(user_email)
        e_rule_id = escape_sql_string(rule_id)
        now = datetime.now(timezone.utc).isoformat()

        sql = (
            f"UPDATE {self._table} SET "
            f"  checks = '{checks_json}', "
            "  version = version + 1, "
            "  status = 'draft', "
            f"  updated_by = '{e_email}', "
            f"  updated_at = '{now}' "
            f"WHERE rule_id = '{e_rule_id}'"
        )
        self._execute(sql)
        self._record_history(
            entry.table_fqn, checks_json, entry.version + 1, entry.source, user_email, now, "update", rule_id
        )
        logger.info("Updated rule %s (table %s)", rule_id, entry.table_fqn)
        return self.get_by_rule_id(rule_id) or entry

    def get(self, table_fqn: str) -> RuleCatalogEntry | None:
        """Get a combined rule entry for a table (merges all rules into one checks array).

        Kept for backward compatibility with dryrun/scheduler which expect a
        single entry per table with all checks.  Returns ``None`` when no
        approved rules exist.
        """
        rules = self.list_rules_for_table(table_fqn, status="approved")
        if not rules:
            rules = self.list_rules_for_table(table_fqn)
        if not rules:
            return None
        merged_checks: list[dict[str, Any]] = []
        for r in rules:
            merged_checks.extend(r.checks)
        first = rules[0]
        return RuleCatalogEntry(
            table_fqn=table_fqn,
            checks=merged_checks,
            version=max(r.version for r in rules),
            status=first.status,
            source=first.source,
            created_by=first.created_by,
            created_at=first.created_at,
            updated_by=first.updated_by,
            updated_at=first.updated_at,
            rule_id=first.rule_id,
        )

    def get_by_rule_id(self, rule_id: str) -> RuleCatalogEntry | None:
        """Get a single rule by its rule_id."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        e_id = escape_sql_string(rule_id)
        sql = f"SELECT {self._SELECT_COLS} FROM {self._table} WHERE rule_id = '{e_id}'"
        rows = self._query(sql)
        if not rows:
            return None
        return self._row_to_entry(rows[0])

    def list_rules_for_table(self, table_fqn: str, status: str | None = None) -> list[RuleCatalogEntry]:
        """List all individual rules for a given table, optionally filtered by status."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        e_fqn = escape_sql_string(table_fqn)
        sql = f"SELECT {self._SELECT_COLS} FROM {self._table} WHERE table_fqn = '{e_fqn}'"
        if status:
            e_status = escape_sql_string(status)
            sql += f" AND status = '{e_status}'"
        sql += " ORDER BY updated_at DESC"
        rows = self._query(sql)
        return [self._row_to_entry(row) for row in rows]

    def list_rules(self, status: str | None = None) -> list[RuleCatalogEntry]:
        """List all individual rules, optionally filtered by status."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        sql = f"SELECT {self._SELECT_COLS} FROM {self._table}"
        if status:
            e_status = escape_sql_string(status)
            sql += f" WHERE status = '{e_status}'"
        sql += " ORDER BY updated_at DESC LIMIT 2000"
        rows = self._query(sql)
        return [self._row_to_entry(row) for row in rows]

    _IDENTITY_ARGS = frozenset(
        {
            "column",
            "columns",
            "col_name",
            "expression",
            "msg",
            "query",
            "allowed",
            "not_allowed",
            "limit",
            "min_limit",
            "max_limit",
            "regex",
            "date_format",
            "timestamp_format",
        }
    )

    @classmethod
    def _check_signature(cls, check: dict[str, Any]) -> str:
        """Produce a normalised, case-insensitive identity for a check.

        Only the function name and semantically meaningful arguments are
        included.  Options like ``trim_strings``, ``name``, ``weight``, and
        ``criticality`` are excluded so that profiler-generated rules and
        manually-created rules with the same function + column are treated as
        duplicates.
        """
        inner = check.get("check", check)
        fn = inner.get("function", "")
        raw_args = inner.get("arguments", {})
        identity_args = {k: v for k, v in raw_args.items() if k in cls._IDENTITY_ARGS}
        return json.dumps({"function": fn, "arguments": identity_args}, sort_keys=True).lower()

    def find_duplicates(
        self,
        table_fqn: str,
        checks: list[dict[str, Any]],
        exclude_rule_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return checks from *checks* that already exist as non-rejected rules for *table_fqn*.

        A duplicate is defined as same ``check.function`` and ``check.arguments``
        (case-insensitive) for the same table.  Rejected rules are excluded from
        duplicate detection.  If *exclude_rule_id* is given, that rule's checks
        are not considered existing (useful when updating an existing rule).
        """
        existing = self.list_rules_for_table(table_fqn)
        existing_sigs: set[str] = set()
        for e in existing:
            if exclude_rule_id and e.rule_id == exclude_rule_id:
                continue
            if e.status == "rejected":
                continue
            for c in e.checks:
                existing_sigs.add(self._check_signature(c))

        duplicates: list[dict[str, Any]] = []
        for check in checks:
            if self._check_signature(check) in existing_sigs:
                duplicates.append(check)
        return duplicates

    def delete(self, rule_id: str, user_email: str) -> None:
        """Delete a single rule by rule_id."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        entry = self.get_by_rule_id(rule_id)
        e_id = escape_sql_string(rule_id)
        sql = f"DELETE FROM {self._table} WHERE rule_id = '{e_id}'"
        self._execute(sql)
        now = datetime.now(timezone.utc).isoformat()
        table_fqn = entry.table_fqn if entry else "unknown"
        version = entry.version if entry else 0
        source = entry.source if entry else "ui"
        self._record_history(table_fqn, None, version, source, user_email, now, "delete", rule_id)
        logger.info("Deleted rule %s (table %s, by %s)", rule_id, table_fqn, user_email)

    def delete_by_table(self, table_fqn: str, user_email: str) -> None:
        """Delete all rules for a table."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        e_fqn = escape_sql_string(table_fqn)
        sql = f"DELETE FROM {self._table} WHERE table_fqn = '{e_fqn}'"
        self._execute(sql)
        now = datetime.now(timezone.utc).isoformat()
        self._record_history(table_fqn, None, 0, "ui", user_email, now, "delete_all")
        logger.info("Deleted all rules for table %s (by %s)", table_fqn, user_email)

    def set_status(
        self,
        rule_id: str,
        status: str,
        user_email: str,
        expected_version: int | None = None,
    ) -> RuleCatalogEntry:
        """Update the approval status of an individual rule."""
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}. Must be one of {self.VALID_STATUSES}")

        entry = self.get_by_rule_id(rule_id)
        if entry is None:
            raise RuntimeError(f"Rule not found: {rule_id}")

        allowed = self.VALID_TRANSITIONS.get(entry.status, set())
        if status not in allowed:
            raise ValueError(
                f"Cannot transition from '{entry.status}' to '{status}'. Allowed transitions: {allowed or 'none'}"
            )

        if status == "pending_approval":
            self._check_no_duplicate_pending_or_approved(entry)

        if expected_version is not None and entry.version != expected_version:
            raise RuntimeError(
                f"Version conflict for rule {rule_id}: expected v{expected_version}, "
                f"but current is v{entry.version}. Another user may have modified the rule."
            )

        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        e_id = escape_sql_string(rule_id)
        e_status = escape_sql_string(status)
        e_email = escape_sql_string(user_email)
        now = datetime.now(timezone.utc).isoformat()

        version_guard = f" AND version = {int(entry.version)}"
        sql = (
            f"UPDATE {self._table} SET "
            f"  status = '{e_status}', "
            f"  updated_by = '{e_email}', "
            f"  updated_at = '{now}' "
            f"WHERE rule_id = '{e_id}'{version_guard}"
        )
        self._execute(sql)
        self._record_history(
            entry.table_fqn, None, entry.version, entry.source, user_email, now, f"status:{status}", rule_id
        )
        logger.info("Updated status for rule %s to %s (by %s)", rule_id, status, user_email)

        updated = self.get_by_rule_id(rule_id)
        if updated is None:
            raise RuntimeError(f"Rule not found after status update: {rule_id}")
        if updated.status != status:
            raise RuntimeError(
                f"Status update for rule {rule_id} did not take effect — the row may have been modified concurrently."
            )
        return updated

    def set_status_by_table(
        self,
        table_fqn: str,
        status: str,
        user_email: str,
    ) -> list[RuleCatalogEntry]:
        """Batch-update status for all rules of a table that can transition to *status*."""
        rules = self.list_rules_for_table(table_fqn)
        updated: list[RuleCatalogEntry] = []
        for rule in rules:
            allowed = self.VALID_TRANSITIONS.get(rule.status, set())
            if status in allowed and rule.rule_id:
                try:
                    u = self.set_status(rule.rule_id, status, user_email)
                    updated.append(u)
                except Exception:
                    logger.warning("Failed to set status for rule %s", rule.rule_id, exc_info=True)
        return updated

    def get_approved_checks_for_table(self, table_fqn: str) -> list[dict[str, Any]]:
        """Return the merged checks array of all approved rules for a table."""
        rules = self.list_rules_for_table(table_fqn, status="approved")
        merged: list[dict[str, Any]] = []
        for r in rules:
            merged.extend(r.checks)
        return merged

    def backfill_rule_ids(self) -> int:
        """Assign a rule_id to every row that currently has NULL or empty rule_id.

        Returns the number of rows updated.
        """
        count_sql = f"SELECT COUNT(*) FROM {self._table} WHERE rule_id IS NULL OR rule_id = ''"
        rows = self._query(count_sql)
        total = int(rows[0][0]) if rows and rows[0] else 0
        if total == 0:
            return 0

        sql = (
            f"UPDATE {self._table} "
            "SET rule_id = SUBSTRING(MD5(CONCAT(table_fqn, checks, CAST(RAND() AS STRING))), 1, 16) "
            "WHERE rule_id IS NULL OR rule_id = ''"
        )
        self._execute(sql)
        logger.info("Backfilled rule_id for %d rule(s)", total)
        return total

    def read_external_rules_table(self, source_table_fqn: str) -> list[dict[str, str]]:
        """Read rows from an external Delta table with the same schema as dq_quality_rules."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

        validate_fqn(source_table_fqn)
        sql = f"SELECT table_fqn, checks FROM {quote_fqn(source_table_fqn)}"
        rows = self._query(sql)
        return [{"table_fqn": row[0] or "", "checks": row[1] or "[]"} for row in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_sql_checks(checks: list[dict[str, Any]]) -> None:
        """Reject any sql_query check whose query fails is_sql_query_safe()."""
        from databricks.labs.dqx.utils import is_sql_query_safe
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        for check in checks:
            inner = check.get("check") or {}
            if inner.get("function") != "sql_query":
                continue
            query = (inner.get("arguments") or {}).get("query", "")
            if query and not is_sql_query_safe(query):
                raise UnsafeSqlQueryError(
                    "The SQL query in this check contains prohibited statements "
                    "(e.g. DROP, INSERT, UPDATE) and cannot be saved."
                )

    def _check_no_duplicate_pending_or_approved(self, entry: RuleCatalogEntry) -> None:
        """Raise ValueError if another pending/approved rule with the same check exists."""
        check_sig = json.dumps(entry.checks[0].get("check", {}) if entry.checks else {}, sort_keys=True)
        siblings = self.list_rules_for_table(entry.table_fqn)
        for sibling in siblings:
            if sibling.rule_id == entry.rule_id:
                continue
            if sibling.status not in ("pending_approval", "approved"):
                continue
            sib_sig = json.dumps(sibling.checks[0].get("check", {}) if sibling.checks else {}, sort_keys=True)
            if sib_sig == check_sig:
                raise ValueError(
                    f"A duplicate rule with the same check already exists "
                    f"(rule_id={sibling.rule_id}, status={sibling.status}). "
                    f"Cannot submit a duplicate for approval."
                )

    def _row_to_entry(self, row: list[str]) -> RuleCatalogEntry:
        """Convert a query result row to a RuleCatalogEntry."""
        checks = json.loads(row[1], strict=False) if row[1] else []
        return RuleCatalogEntry(
            table_fqn=row[0],
            checks=checks,
            version=int(row[2]) if row[2] else 1,
            status=row[3] or "draft",
            created_by=row[4],
            created_at=row[5],
            updated_by=row[6],
            updated_at=row[7],
            source=row[8] if len(row) > 8 and row[8] else "ui",
            rule_id=row[9] if len(row) > 9 and row[9] else None,
        )

    def _record_history(
        self,
        table_fqn: str,
        checks_json: str | None,
        version: int,
        source: str,
        user_email: str,
        timestamp: str,
        action: str,
        rule_id: str | None = None,
    ) -> None:
        """Insert an audit row into the history table (best-effort)."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        try:
            e_fqn = escape_sql_string(table_fqn)
            e_checks = escape_sql_string(checks_json) if checks_json else ""
            e_email = escape_sql_string(user_email)
            e_source = escape_sql_string(source)
            e_action = escape_sql_string(action)
            e_rule_id = escape_sql_string(rule_id) if rule_id else ""
            sql = (
                f"INSERT INTO {self._history_table} "
                "(table_fqn, checks, version, source, action, changed_by, changed_at, rule_id) VALUES "
                f"('{e_fqn}', '{e_checks}', {int(version)}, '{e_source}', '{e_action}', "
                f"'{e_email}', '{timestamp}', '{e_rule_id}')"
            )
            self._execute(sql)
        except Exception:
            logger.warning("Failed to record history for %s (non-fatal)", table_fqn, exc_info=True)

    def _execute(self, sql: str) -> None:
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
