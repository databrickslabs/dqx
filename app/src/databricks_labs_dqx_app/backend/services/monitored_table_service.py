"""Monitored Table service (Phase 3B — CRUD + profiling READ path).

Manages the LIVE ``dq_monitored_tables`` bindings and their
``dq_applied_rules`` links, per
``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §3.1 and §7.

This is Layer 2 of the Rules Registry: a thin binding recording that a
table is under active governance, plus the live link between a published
registry rule (``dq_rules``) and that table's column mapping. Applying new
rules, mapping columns, and materializing into ``dq_quality_rules`` (Phase
3C) are explicitly out of scope here — this module only covers register/
list/get/delete of the binding + applied rules, and a READ-ONLY path over
the existing ``dq_profiling_results`` Delta table (never written here; the
profiler job owns writes to that table).

Mirrors :class:`~databricks_labs_dqx_app.backend.services.registry_service.RegistryService`'s
shape (dialect-portable SQL via the executor helpers, Python-side
filtering over JSON metadata) but operates on the monitored-table tables.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, cast, get_args
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    ColumnMappingGroup,
    MonitoredTable,
    MonitoredTableStatus,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
)
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)


class DuplicateMonitoredTableError(ValueError):
    """Raised by :meth:`MonitoredTableService.register` for an already-bound ``table_fqn``."""


@dataclass
class BulkRegisterResult:
    """Summary returned by :meth:`MonitoredTableService.bulk_register`."""

    registered: list[str] = field(default_factory=list)
    skipped_existing: list[str] = field(default_factory=list)
    invalid: list[str] = field(default_factory=list)


@dataclass
class AppliedRuleSummary:
    """An ``AppliedRule`` joined (in Python, over the JSON ``user_metadata`` blob) with its
    registry rule's descriptive tags — used by :meth:`MonitoredTableService.get`.
    """

    applied_rule: AppliedRule
    rule_name: str | None = None
    rule_dimension: str | None = None
    rule_severity: str | None = None


@dataclass
class MonitoredTableDetail:
    """A monitored table binding plus its applied rules — :meth:`MonitoredTableService.get`."""

    table: MonitoredTable
    applied_rules: list[AppliedRuleSummary] = field(default_factory=list)


@dataclass
class MonitoredTableSummary:
    """A monitored table binding plus lightweight list-view counters."""

    table: MonitoredTable
    applied_rule_count: int = 0


@dataclass
class LatestProfile:
    """A read-only projection of the most recent ``dq_profiling_results`` row for a table."""

    run_id: str
    source_table_fqn: str
    status: str | None = None
    rows_profiled: int | None = None
    columns_profiled: int | None = None
    duration_seconds: float | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    generated_rules: list[dict[str, Any]] = field(default_factory=list)
    profiled_at: str | None = None


class MonitoredTableService:
    """Manages Monitored Tables (``dq_monitored_tables`` / ``dq_applied_rules``) in the OLTP store.

    ``profiling_sql`` is a separate executor because ``dq_profiling_results``
    is always a Delta analytical table (written by the profiler job),
    independent of whether the OLTP tables live in Lakebase Postgres or the
    Delta OLTP-fallback baseline.
    """

    VALID_STATUSES: frozenset[str] = frozenset(get_args(MonitoredTableStatus))

    def __init__(self, sql: OltpExecutorProtocol, profiling_sql: SqlExecutor) -> None:
        self._sql = sql
        self._profiling_sql = profiling_sql
        self._table = sql.fqn("dq_monitored_tables")
        self._applied_table = sql.fqn("dq_applied_rules")
        self._rules_table = sql.fqn("dq_rules")
        self._profiling_table = profiling_sql.fqn("dq_profiling_results")
        self._select_cols = self._build_select_cols()
        self._applied_select_cols = self._build_applied_select_cols()

    def _build_select_cols(self) -> str:
        created_at = self._sql.ts_text("created_at")
        updated_at = self._sql.ts_text("updated_at")
        last_profiled_at = self._sql.ts_text("last_profiled_at")
        return (
            f"binding_id, table_fqn, steward, status, {last_profiled_at} AS last_profiled_at, "
            f"created_by, {created_at} AS created_at, updated_by, {updated_at} AS updated_at"
        )

    def _build_applied_select_cols(self) -> str:
        column_mapping = self._sql.select_json_text("column_mapping")
        user_metadata = self._sql.select_json_text("user_metadata")
        created_at = self._sql.ts_text("created_at")
        return (
            "id, binding_id, rule_id, pinned_version, severity_override, "
            f"{column_mapping} AS column_mapping_json, {user_metadata} AS user_metadata_json, "
            f"mapping_hash, created_by, {created_at} AS created_at"
        )

    # ------------------------------------------------------------------
    # Register
    # ------------------------------------------------------------------

    def register(self, table_fqn: str, user_email: str, steward: str | None = None) -> MonitoredTable:
        """Register *table_fqn* under Rules Registry governance (status ``draft``).

        Raises :class:`DuplicateMonitoredTableError` if the table is already
        monitored (``table_fqn`` is unique).
        """
        validate_fqn(table_fqn)
        existing = self._get_by_table_fqn(table_fqn)
        if existing is not None:
            raise DuplicateMonitoredTableError(
                f"Table '{table_fqn}' is already monitored (binding_id={existing.binding_id})."
            )
        now = datetime.now(timezone.utc)
        binding = MonitoredTable(
            binding_id=uuid4().hex[:16],
            table_fqn=table_fqn,
            steward=steward,
            status="draft",
            last_profiled_at=None,
            created_by=user_email,
            created_at=now,
            updated_by=user_email,
            updated_at=now,
        )
        self._insert(binding)
        logger.info("Registered monitored table %s (binding_id=%s)", table_fqn, binding.binding_id)
        return binding

    def bulk_register(
        self, table_fqns: list[str], user_email: str, steward: str | None = None
    ) -> BulkRegisterResult:
        """Register many *table_fqns* under Rules Registry governance in one pass.

        Unlike :meth:`register`, already-monitored tables are skipped
        gracefully (reported in ``skipped_existing``) rather than raising
        :class:`DuplicateMonitoredTableError`, and syntactically invalid FQNs
        are reported in ``invalid`` rather than aborting the whole batch.
        Input order is preserved and duplicates are deduped. Existence is
        checked with a single ``IN (...)`` query rather than one round-trip
        per FQN.
        """
        deduped = list(dict.fromkeys(table_fqns))
        valid: list[str] = []
        invalid: list[str] = []
        for fqn in deduped:
            try:
                validate_fqn(fqn)
                valid.append(fqn)
            except ValueError:
                invalid.append(fqn)
        if not valid:
            return BulkRegisterResult(registered=[], skipped_existing=[], invalid=invalid)

        existing = self._get_existing_table_fqns(valid)
        skipped_existing = [fqn for fqn in valid if fqn in existing]
        to_register = [fqn for fqn in valid if fqn not in existing]

        registered: list[str] = []
        for fqn in to_register:
            now = datetime.now(timezone.utc)
            binding = MonitoredTable(
                binding_id=uuid4().hex[:16],
                table_fqn=fqn,
                steward=steward,
                status="draft",
                last_profiled_at=None,
                created_by=user_email,
                created_at=now,
                updated_by=user_email,
                updated_at=now,
            )
            self._insert(binding)
            registered.append(fqn)

        logger.info(
            "Bulk-registered %d monitored table(s), skipped %d existing, rejected %d invalid",
            len(registered),
            len(skipped_existing),
            len(invalid),
        )
        return BulkRegisterResult(registered=registered, skipped_existing=skipped_existing, invalid=invalid)

    def _get_existing_table_fqns(self, table_fqns: list[str]) -> set[str]:
        in_list = ", ".join(f"'{escape_sql_string(fqn)}'" for fqn in table_fqns)
        sql = f"SELECT table_fqn FROM {self._table} WHERE table_fqn IN ({in_list})"  # noqa: S608
        rows = self._sql.query(sql)
        return {row[0] for row in rows if row and row[0] is not None}

    def _insert(self, binding: MonitoredTable) -> None:
        sql = (
            f"INSERT INTO {self._table} "
            "(binding_id, table_fqn, steward, status, created_by, created_at, updated_by, updated_at) VALUES "
            f"('{escape_sql_string(binding.binding_id)}', '{escape_sql_string(binding.table_fqn)}', "
            f"{self._opt_str(binding.steward)}, '{escape_sql_string(binding.status)}', "
            f"{self._opt_str(binding.created_by)}, now(), {self._opt_str(binding.updated_by)}, now())"
        )
        self._sql.execute(sql)

    # ------------------------------------------------------------------
    # List / Get
    # ------------------------------------------------------------------

    def list_monitored_tables(
        self,
        *,
        status: str | None = None,
        steward: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        name: str | None = None,
    ) -> list[MonitoredTableSummary]:
        """List monitored tables, optionally filtered.

        ``status`` and ``steward`` are pushed down into SQL; ``catalog``,
        ``schema``, and ``name`` filter over ``table_fqn`` in Python
        (matching how :class:`RegistryService.list_rules` handles
        JSON-blob metadata filters).
        """
        clauses: list[str] = []
        if status:
            clauses.append(f"status = '{escape_sql_string(status)}'")
        if steward:
            clauses.append(f"steward = '{escape_sql_string(steward)}'")
        sql = f"SELECT {self._select_cols} FROM {self._table}"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC LIMIT 2000"
        rows = self._sql.query(sql)
        tables = [self._row_to_table(row) for row in rows]
        if catalog:
            tables = [t for t in tables if self._fqn_part(t.table_fqn, 0) == catalog]
        if schema:
            tables = [t for t in tables if self._fqn_part(t.table_fqn, 1) == schema]
        if name:
            needle = name.lower()
            tables = [t for t in tables if needle in t.table_fqn.lower()]
        return [
            MonitoredTableSummary(table=t, applied_rule_count=self._count_applied_rules(t.binding_id))
            for t in tables
        ]

    @staticmethod
    def _fqn_part(table_fqn: str, index: int) -> str | None:
        parts = table_fqn.split(".")
        return parts[index] if len(parts) > index else None

    def _count_applied_rules(self, binding_id: str) -> int:
        e = escape_sql_string(binding_id)
        sql = f"SELECT COUNT(*) FROM {self._applied_table} WHERE binding_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        return int(rows[0][0]) if rows and rows[0] and rows[0][0] is not None else 0

    def get(self, binding_id: str) -> MonitoredTableDetail | None:
        """Get a monitored table binding plus its applied rules (with joined rule tags)."""
        table = self._get(binding_id)
        if table is None:
            return None
        applied_rules = self._list_applied_rules(binding_id)
        return MonitoredTableDetail(table=table, applied_rules=applied_rules)

    def _get(self, binding_id: str) -> MonitoredTable | None:
        e = escape_sql_string(binding_id)
        sql = f"SELECT {self._select_cols} FROM {self._table} WHERE binding_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_table(rows[0])

    def _get_by_table_fqn(self, table_fqn: str) -> MonitoredTable | None:
        e = escape_sql_string(table_fqn)
        sql = f"SELECT {self._select_cols} FROM {self._table} WHERE table_fqn = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_table(rows[0])

    def _list_applied_rules(self, binding_id: str) -> list[AppliedRuleSummary]:
        e = escape_sql_string(binding_id)
        sql = (
            f"SELECT {self._applied_select_cols} FROM {self._applied_table} "  # noqa: S608
            f"WHERE binding_id = '{e}' ORDER BY created_at"
        )
        rows = self._sql.query(sql)
        applied_rules = [self._row_to_applied_rule(row) for row in rows]
        summaries: list[AppliedRuleSummary] = []
        for applied_rule in applied_rules:
            name, dimension, severity = self._rule_tags(applied_rule.rule_id)
            summaries.append(
                AppliedRuleSummary(
                    applied_rule=applied_rule,
                    rule_name=name,
                    rule_dimension=dimension,
                    rule_severity=severity,
                )
            )
        return summaries

    def _rule_tags(self, rule_id: str) -> tuple[str | None, str | None, str | None]:
        """Look up name/dimension/severity tags for *rule_id* from ``dq_rules``.

        Returns ``(None, None, None)`` if the rule row is missing (e.g. a
        registry rule was hard-deleted out from under an application) —
        callers display a graceful blank rather than failing the whole
        monitored-table detail view.
        """
        e = escape_sql_string(rule_id)
        user_metadata = self._sql.select_json_text("user_metadata")
        sql = f"SELECT {user_metadata} FROM {self._rules_table} WHERE rule_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows or not rows[0]:
            return None, None, None
        metadata = self._parse_json_dict(rows[0][0])
        return get_rule_name(metadata), get_rule_dimension(metadata), get_rule_severity(metadata)

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    def publish(self, binding_id: str, user_email: str) -> MonitoredTable:
        """Flip a monitored table binding to ``published``.

        This only flips the binding's own status flag — it does NOT
        materialize applied rules into ``dq_quality_rules`` itself; the
        route layer calls
        :meth:`~databricks_labs_dqx_app.backend.services.materializer.Materializer.materialize_binding`
        right after this (see ``routes/v1/monitored_tables.py:publish_monitored_table``).
        Publishing is idempotent — publishing an already-published table is
        a no-op status-wise but still worth re-running the materializer
        (e.g. to pick up new applied rules or version upgrades).
        """
        table = self._get(binding_id)
        if table is None:
            raise RuntimeError(f"Monitored table not found: {binding_id}")
        e = escape_sql_string(binding_id)
        self._sql.execute(
            f"UPDATE {self._table} SET status = 'published', "
            f"updated_by = {self._opt_str(user_email)}, updated_at = now() "
            f"WHERE binding_id = '{e}'"
        )
        table.status = "published"
        table.updated_by = user_email
        logger.info("Published monitored table %s (binding_id=%s, by %s)", table.table_fqn, binding_id, user_email)
        return table

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, binding_id: str, user_email: str) -> None:
        """Delete a monitored table binding and its applied rules.

        TODO(Phase 3C): once the materializer exists, de-materialize (or at
        minimum orphan-flag) any ``dq_quality_rules`` rows whose
        ``applied_rule_id`` references an application under this binding
        before deleting the rows here — otherwise materialized runner rows
        are left pointing at applications that no longer exist. Not
        implemented yet because the materializer itself doesn't exist.
        """
        binding = self._get(binding_id)
        if binding is None:
            raise RuntimeError(f"Monitored table not found: {binding_id}")
        e = escape_sql_string(binding_id)
        self._sql.execute(f"DELETE FROM {self._applied_table} WHERE binding_id = '{e}'")
        self._sql.execute(f"DELETE FROM {self._table} WHERE binding_id = '{e}'")
        logger.info("Deleted monitored table %s (binding_id=%s, by %s)", binding.table_fqn, binding_id, user_email)

    # ------------------------------------------------------------------
    # Profiling READ (reuses dq_profiling_results — never written here)
    # ------------------------------------------------------------------

    def get_latest_profile(self, table_fqn: str) -> LatestProfile | None:
        """Read the most recent successful ``dq_profiling_results`` row for *table_fqn*.

        Read-only: this service never writes to ``dq_profiling_results``
        (owned by the profiler job — see ``routes/v1/profiler.py``). Mirrors
        the row shape read by ``get_profile_run_results``.
        """
        e = escape_sql_string(table_fqn)
        cols = (
            "run_id, source_table_fqn, rows_profiled, columns_profiled, duration_seconds, "
            "summary_json, generated_rules_json, status, "
            "CAST(created_at AS STRING) AS created_at"
        )
        sql = (
            f"SELECT {cols} FROM {self._profiling_table} "  # noqa: S608
            f"WHERE source_table_fqn = '{e}' AND status = 'SUCCESS' "
            f"ORDER BY created_at DESC LIMIT 1"
        )
        rows = self._profiling_sql.query_dicts(sql)
        if not rows:
            return None
        row = rows[0]
        summary_json = row.get("summary_json") or "{}"
        rules_json = row.get("generated_rules_json") or "[]"
        try:
            summary = json.loads(summary_json)
        except json.JSONDecodeError:
            summary = {}
        try:
            generated_rules = json.loads(rules_json)
        except json.JSONDecodeError:
            generated_rules = []
        return LatestProfile(
            run_id=row.get("run_id") or "",
            source_table_fqn=row.get("source_table_fqn") or table_fqn,
            status=row.get("status"),
            rows_profiled=int(v) if (v := row.get("rows_profiled")) else None,
            columns_profiled=int(v) if (v := row.get("columns_profiled")) else None,
            duration_seconds=float(v) if (v := row.get("duration_seconds")) else None,
            summary=summary if isinstance(summary, dict) else {},
            generated_rules=generated_rules if isinstance(generated_rules, list) else [],
            profiled_at=row.get("created_at"),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _opt_str(value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"

    @classmethod
    def _parse_status(cls, value: str | None, *, binding_id: str) -> MonitoredTableStatus:
        """Validate *value* against :data:`MonitoredTableStatus`'s allowed members and narrow it."""
        if value not in cls.VALID_STATUSES:
            raise ValueError(
                f"Monitored table {binding_id!r} has invalid status {value!r}; "
                f"expected one of {sorted(cls.VALID_STATUSES)}"
            )
        return cast(MonitoredTableStatus, value)

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            logger.warning("Unparsable timestamp %r; treating as None", value)
            return None

    @staticmethod
    def _parse_json_dict(raw: str | None) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw, strict=False)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _parse_column_mapping(raw: str | None) -> list[ColumnMappingGroup]:
        if not raw:
            return []
        try:
            parsed = json.loads(raw, strict=False)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        groups: list[ColumnMappingGroup] = []
        for item in parsed:
            if isinstance(item, dict):
                groups.append({str(k): str(v) for k, v in item.items()})
        return groups

    def _row_to_table(self, row: list[str]) -> MonitoredTable:
        binding_id = row[0]
        return MonitoredTable(
            binding_id=binding_id,
            table_fqn=row[1],
            steward=row[2],
            status=self._parse_status(row[3], binding_id=binding_id),
            last_profiled_at=self._parse_timestamp(row[4]),
            created_by=row[5],
            created_at=self._parse_timestamp(row[6]),
            updated_by=row[7],
            updated_at=self._parse_timestamp(row[8]),
        )

    def _row_to_applied_rule(self, row: list[str]) -> AppliedRule:
        return AppliedRule(
            id=row[0],
            binding_id=row[1],
            rule_id=row[2],
            pinned_version=int(row[3]) if row[3] not in (None, "") else None,
            severity_override=row[4],
            column_mapping=self._parse_column_mapping(row[5]),
            user_metadata=self._parse_json_dict(row[6]),
            mapping_hash=row[7],
            created_by=row[8],
            created_at=self._parse_timestamp(row[9]),
        )
