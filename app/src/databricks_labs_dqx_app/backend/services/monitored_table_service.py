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
from databricks_labs_dqx_app.backend.services.score_cache_service import parse_cached_score
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
    """A monitored table binding plus lightweight list-view counters.

    The ``score*`` fields carry the cached DQ score LEFT-JOINed from
    ``dq_score_cache`` (P3.4) — all ``None`` when the table has never been
    scored. ``score_computed_at`` is the executor's ``ts_text`` string.
    """

    table: MonitoredTable
    applied_rule_count: int = 0
    check_count: int = 0
    score: float | None = None
    failed_tests: int | None = None
    total_tests: int | None = None
    score_computed_at: str | None = None


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
        self._quality_rules_table = sql.fqn("dq_quality_rules")
        self._score_cache_table = sql.fqn("dq_score_cache")
        self._profiling_table = profiling_sql.fqn("dq_profiling_results")
        self._select_cols = self._build_select_cols()
        self._applied_select_cols = self._build_applied_select_cols()

    def _build_select_cols(self, prefix: str = "") -> str:
        created_at = self._sql.ts_text(f"{prefix}created_at")
        updated_at = self._sql.ts_text(f"{prefix}updated_at")
        last_profiled_at = self._sql.ts_text(f"{prefix}last_profiled_at")
        return (
            f"{prefix}binding_id, {prefix}table_fqn, {prefix}steward, {prefix}status, "
            f"{prefix}version, {prefix}schedule_cron, {prefix}schedule_tz, "
            f"{last_profiled_at} AS last_profiled_at, "
            f"{prefix}created_by, {created_at} AS created_at, "
            f"{prefix}updated_by, {updated_at} AS updated_at"
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
            "(binding_id, table_fqn, steward, status, version, created_by, created_at, updated_by, updated_at) "
            "VALUES "
            f"('{escape_sql_string(binding.binding_id)}', '{escape_sql_string(binding.table_fqn)}', "
            f"{self._opt_str(binding.steward)}, '{escape_sql_string(binding.status)}', 0, "
            f"{self._opt_str(binding.created_by)}, now(), {self._opt_str(binding.updated_by)}, now())"
        )
        self._sql.execute(sql)

    # ------------------------------------------------------------------
    # List / Get
    # ------------------------------------------------------------------

    def count(self) -> int:
        """Total monitored table bindings, any status (homepage stat card)."""
        rows = self._sql.query(f"SELECT COUNT(*) FROM {self._table}")  # noqa: S608
        return int(rows[0][0]) if rows and rows[0] and rows[0][0] is not None else 0

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

        The cached DQ score columns are LEFT-JOINed from ``dq_score_cache``
        in the same round-trip (P3.4) — never recomputed here; a page load
        must not touch the warehouse. NULLs (no cache row yet) surface as
        ``None`` score fields on the summary.
        """
        clauses: list[str] = []
        if status:
            clauses.append(f"mt.status = '{escape_sql_string(status)}'")
        if steward:
            clauses.append(f"mt.steward = '{escape_sql_string(steward)}'")
        score_computed_at = self._sql.ts_text("sc.computed_at")
        sql = (
            f"SELECT {self._build_select_cols('mt.')}, "
            f"sc.score, sc.failed_tests, sc.total_tests, {score_computed_at} AS score_computed_at "
            f"FROM {self._table} mt "
            f"LEFT JOIN {self._score_cache_table} sc "
            f"ON sc.scope_type = 'table' AND sc.scope_key = mt.table_fqn"
        )
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY mt.updated_at DESC LIMIT 2000"
        rows = self._sql.query(sql)
        tables = [(self._row_to_table(row), parse_cached_score(row[12], row[13], row[14], row[15])) for row in rows]
        if catalog:
            tables = [(t, s) for t, s in tables if self._fqn_part(t.table_fqn, 0) == catalog]
        if schema:
            tables = [(t, s) for t, s in tables if self._fqn_part(t.table_fqn, 1) == schema]
        if name:
            needle = name.lower()
            tables = [(t, s) for t, s in tables if needle in t.table_fqn.lower()]
        applied_counts = self._applied_rule_counts([t.binding_id for t, _ in tables])
        check_counts = self._materialized_check_counts([t.table_fqn for t, _ in tables])
        # Derive last_profiled_at from dq_profiling_results (the stored column is
        # never written) so the overview "last profiled" column is real.
        profiled_at = self._latest_profiled_at_map([t.table_fqn for t, _ in tables])
        for t, _ in tables:
            t.last_profiled_at = profiled_at.get(t.table_fqn)
        return [
            MonitoredTableSummary(
                table=t,
                applied_rule_count=applied_counts.get(t.binding_id, 0),
                check_count=check_counts.get(t.table_fqn, 0),
                score=cached.score,
                failed_tests=cached.failed_tests,
                total_tests=cached.total_tests,
                score_computed_at=cached.computed_at,
            )
            for t, cached in tables
        ]

    @staticmethod
    def _fqn_part(table_fqn: str, index: int) -> str | None:
        parts = table_fqn.split(".")
        return parts[index] if len(parts) > index else None

    def _applied_rule_counts(self, binding_ids: list[str]) -> dict[str, int]:
        """Applied-rule counts for all *binding_ids* in ONE grouped query (no per-binding round-trip)."""
        if not binding_ids:
            return {}
        in_list = ", ".join(f"'{escape_sql_string(b)}'" for b in binding_ids)
        sql = (
            f"SELECT binding_id, COUNT(*) FROM {self._applied_table} "  # noqa: S608
            f"WHERE binding_id IN ({in_list}) GROUP BY binding_id"
        )
        rows = self._sql.query(sql)
        return {row[0]: int(row[1]) for row in rows if row and row[0] is not None and row[1] is not None}

    def _materialized_check_counts(self, table_fqns: list[str]) -> dict[str, int]:
        """Count active ``dq_quality_rules`` rows per *table_fqns* entry, regardless of authoring source.

        One grouped query for all listed tables (no per-table round-trip); a
        table with zero active rows is simply absent from the result.

        ``dq_quality_rules`` holds every check for a table — authored
        directly (``source`` in ``ui``/``sql``/``profiler``/``import``/``ai``)
        as well as materialized from a Rules Registry application
        (``source = 'registry'``). This must count all of them, not just
        registry-sourced rows, matching dqlake's `BindingOutBrief.check_count`
        semantics (which counts every check on a binding with no provenance
        filter). ``rejected`` rows are excluded since they're no longer
        active, mirroring :data:`RulesCatalogService.VALID_STATUSES`'s
        terminal "dead" state.
        """
        if not table_fqns:
            return {}
        in_list = ", ".join(f"'{escape_sql_string(f)}'" for f in table_fqns)
        sql = (
            f"SELECT table_fqn, COUNT(*) FROM {self._quality_rules_table} "  # noqa: S608
            f"WHERE table_fqn IN ({in_list}) AND status != 'rejected' GROUP BY table_fqn"
        )
        rows = self._sql.query(sql)
        return {row[0]: int(row[1]) for row in rows if row and row[0] is not None and row[1] is not None}

    def _latest_profiled_at_map(self, table_fqns: list[str]) -> dict[str, datetime]:
        """Newest SUCCESS profiling timestamp per table, from ``dq_profiling_results``.

        Derive-on-read for ``last_profiled_at``: the ``dq_monitored_tables``
        column is never written (always NULL), so the real "last profiled"
        value is the most recent successful profiler run for the table — the
        same row :meth:`get_latest_profile` trusts. One grouped query covers
        every listed table (no per-table round-trip); tables never profiled are
        simply absent from the result. Self-healing — no backfill needed.
        """
        if not table_fqns:
            return {}
        in_list = ", ".join(f"'{escape_sql_string(f)}'" for f in table_fqns)
        last_profiled_at = self._profiling_sql.ts_text("MAX(created_at)")
        sql = (
            f"SELECT source_table_fqn, {last_profiled_at} AS last_profiled_at "  # noqa: S608
            f"FROM {self._profiling_table} "
            f"WHERE source_table_fqn IN ({in_list}) AND status = 'SUCCESS' "
            f"GROUP BY source_table_fqn"
        )
        rows = self._profiling_sql.query(sql)
        result: dict[str, datetime] = {}
        for row in rows:
            ts = self._parse_timestamp(row[1])
            if row[0] and ts is not None:
                result[row[0]] = ts
        return result

    def get(self, binding_id: str) -> MonitoredTableDetail | None:
        """Get a monitored table binding plus its applied rules (with joined rule tags)."""
        table = self._get(binding_id)
        if table is None:
            return None
        # Derive last_profiled_at from dq_profiling_results (the stored column is
        # never written) so the About tab shows a real "last profiled".
        table.last_profiled_at = self._latest_profiled_at_map([table.table_fqn]).get(table.table_fqn)
        applied_rules = self._list_applied_rules(binding_id)
        return MonitoredTableDetail(table=table, applied_rules=applied_rules)

    def get_by_table_fqn(self, table_fqn: str) -> MonitoredTableDetail | None:
        """Like :meth:`get`, but keyed by the bound table's FQN.

        Used by the dq-results endpoints to attribute a table's check
        results (keyed by ``input_location``) back to the binding's
        applied-rule metadata. None when the table is not monitored.
        """
        table = self._get_by_table_fqn(table_fqn)
        if table is None:
            return None
        return MonitoredTableDetail(table=table, applied_rules=self._list_applied_rules(table.binding_id))

    def get_binding_ids_by_table_fqn(self, table_fqns: list[str]) -> dict[str, str]:
        """Batched ``table_fqn -> binding_id`` lookup in ONE ``IN (...)`` query.

        Used by the dq-results global endpoint to enrich its ``by_table``
        rows with a link target without a per-table round-trip (the
        table_fqn column is unique, so at most one binding per FQN).
        Tables that are not monitored are simply absent from the result.

        Inputs may be warehouse-sourced (``dq_metrics.input_location``), so
        anything failing :func:`validate_fqn` is silently dropped before
        interpolation — an unmonitorable name can never match anyway.
        """
        candidates: list[str] = []
        for fqn in table_fqns:
            try:
                validate_fqn(fqn)
            except ValueError:
                logger.warning("Dropping invalid table FQN from binding-id lookup")
                continue
            candidates.append(fqn)
        if not candidates:
            return {}
        in_list = ", ".join(f"'{escape_sql_string(fqn)}'" for fqn in candidates)
        sql = f"SELECT table_fqn, binding_id FROM {self._table} WHERE table_fqn IN ({in_list})"  # noqa: S608
        rows = self._sql.query(sql)
        return {row[0]: row[1] for row in rows if row and row[0] and row[1]}

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
    # Submit-for-review lifecycle (draft -> pending_approval -> approved/rejected)
    # ------------------------------------------------------------------

    def set_status(self, binding_id: str, status: str, user_email: str) -> MonitoredTable:
        """Set a monitored table binding's own review-lifecycle status flag.

        Only flips the binding row's ``status`` column — it never touches
        ``dq_applied_rules`` or the materialized ``dq_quality_rules`` rows.
        The route layer (``routes/v1/monitored_tables.py``) orchestrates the
        binding status alongside the materializer and the per-rule
        submit/approve/reject transitions so the binding's status stays a
        faithful roll-up of its materialized checks.

        Raises:
            ValueError: *status* is not a member of :data:`MonitoredTableStatus`.
            RuntimeError: *binding_id* does not exist.
        """
        if status not in self.VALID_STATUSES:
            raise ValueError(
                f"Invalid monitored table status {status!r}; expected one of {sorted(self.VALID_STATUSES)}"
            )
        table = self._get(binding_id)
        if table is None:
            raise RuntimeError(f"Monitored table not found: {binding_id}")
        e = escape_sql_string(binding_id)
        self._sql.execute(
            f"UPDATE {self._table} SET status = '{escape_sql_string(status)}', "
            f"updated_by = {self._opt_str(user_email)}, updated_at = now() "
            f"WHERE binding_id = '{e}'"
        )
        table.status = cast(MonitoredTableStatus, status)
        table.updated_by = user_email
        logger.info(
            "Set monitored table %s (binding_id=%s) status to %s (by %s)",
            table.table_fqn,
            binding_id,
            status,
            user_email,
        )
        return table

    def update_schedule(
        self,
        binding_id: str,
        schedule_cron: str | None,
        schedule_tz: str | None,
        user_email: str,
    ) -> MonitoredTable:
        """Set (or clear) the binding's run schedule (P21 item 14).

        Schedule is operational config that is orthogonal to the rule-review
        lifecycle, so — unlike a Table Space edit — this deliberately does NOT
        flip the binding's ``status`` back to ``draft``: an approved table stays
        approved (and thus schedulable) after its cadence changes. Only the
        ``schedule_*`` columns and the ``updated_*`` audit fields move.

        Pass ``schedule_cron=None`` to remove the schedule; ``schedule_tz`` is
        forced to NULL alongside it so a cleared schedule never leaves a dangling
        timezone.

        Raises:
            RuntimeError: *binding_id* does not exist.
        """
        table = self._get(binding_id)
        if table is None:
            raise RuntimeError(f"Monitored table not found: {binding_id}")
        cron = schedule_cron or None
        tz = schedule_tz if cron is not None else None
        e = escape_sql_string(binding_id)
        self._sql.execute(
            f"UPDATE {self._table} SET schedule_cron = {self._opt_str(cron)}, "
            f"schedule_tz = {self._opt_str(tz)}, "
            f"updated_by = {self._opt_str(user_email)}, updated_at = now() "
            f"WHERE binding_id = '{e}'"
        )
        table.schedule_cron = cron
        table.schedule_tz = tz
        table.updated_by = user_email
        logger.info(
            "Updated monitored table %s (binding_id=%s) schedule (cron=%s, tz=%s, by %s)",
            table.table_fqn,
            binding_id,
            cron,
            tz,
            user_email,
        )
        return table

    def list_materialized_rule_statuses(self, binding_id: str) -> list[tuple[str, str]]:
        """Return ``(rule_id, status)`` for every ``dq_quality_rules`` row this binding materialized.

        Resolves the binding's materialized rows through the SAME
        ``dq_quality_rules.applied_rule_id`` -> ``dq_applied_rules.id`` ->
        ``dq_applied_rules.binding_id`` linkage the materializer maintains
        (:meth:`Materializer.materialize_binding` /
        :meth:`Materializer._cleanup_orphans`), rather than matching on
        ``table_fqn`` — two bindings can never share this precise link. Used
        by the submit/approve/reject route orchestration to drive the
        per-rule status transitions and to roll the binding status up from
        its checks.
        """
        applied_ids = self._applied_rule_ids(binding_id)
        if not applied_ids:
            return []
        placeholders = ", ".join(f"'{escape_sql_string(i)}'" for i in applied_ids)
        sql = (
            f"SELECT rule_id, status FROM {self._quality_rules_table} "  # noqa: S608
            f"WHERE applied_rule_id IN ({placeholders})"
        )
        rows = self._sql.query(sql)
        return [(row[0], row[1]) for row in rows if row and row[0]]

    def rollup_status(self, binding_id: str, user_email: str) -> MonitoredTable | None:
        """Roll a binding's own status up from its materialized checks' statuses.

        Mirrors the submit/approve/reject route orchestration's
        ``_rollup_binding_status`` (``routes/v1/monitored_tables.py``): any
        check still ``pending_approval`` keeps the binding
        ``pending_approval``; otherwise if any check is ``approved`` the
        binding is ``approved``; with neither it falls back to ``draft``.

        Exposed as a service method (not just a route helper) because a
        registry-rule REPUBLISH re-materializes a follower binding's checks
        out-of-band of any binding-level transition: with auto-upgrade OFF a
        changed follower check silently drops to ``pending_approval``
        (materializer Behaviour B), and without this roll-up the binding would
        keep claiming ``approved`` while its frozen snapshot serves the stale
        version and the table-level approve path (which requires
        ``pending_approval``) stays blocked — the exact "the rule updated but
        the run still uses the old checks" gap (P23 item 1).

        No-op (returns the binding unchanged) when the binding has no
        materialized checks or the rolled-up status already matches, so it is
        safe to call unconditionally after a re-materialization. Returns
        ``None`` when *binding_id* does not exist.
        """
        table = self._get(binding_id)
        if table is None:
            return None
        statuses = {status for _, status in self.list_materialized_rule_statuses(binding_id)}
        if not statuses:
            return table
        if "pending_approval" in statuses:
            target = "pending_approval"
        elif "approved" in statuses:
            target = "approved"
        else:
            target = "draft"
        if target == table.status:
            return table
        return self.set_status(binding_id, target, user_email)

    def _applied_rule_ids(self, binding_id: str) -> list[str]:
        e = escape_sql_string(binding_id)
        sql = f"SELECT id FROM {self._applied_table} WHERE binding_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        return [row[0] for row in rows if row and row[0]]

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
            version=int(row[4]) if row[4] not in (None, "") else 0,
            schedule_cron=row[5] or None,
            schedule_tz=row[6] or None,
            last_profiled_at=self._parse_timestamp(row[7]),
            created_by=row[8],
            created_at=self._parse_timestamp(row[9]),
            updated_by=row[10],
            updated_at=self._parse_timestamp(row[11]),
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
