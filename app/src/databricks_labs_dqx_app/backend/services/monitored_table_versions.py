"""Monitored-table version freeze service (Data Products Task 2).

A monitored table binding carries a monotonically increasing ``version``
(``dq_monitored_tables.version``, 0 = never approved). Each version is
backed by a FROZEN snapshot row in ``dq_monitored_table_versions`` whose
``checks_json`` is exactly the shape the runner consumes — the same shape
``RulesCatalogService.get_approved_checks_for_table`` returns, i.e. what
``routes/v1/dryrun.py:batch_run_from_catalog`` feeds to
``JobService.submit_run`` today.

Freeze rule (design spec §3.2 / §4.1): snapshot vN ALWAYS mirrors the
binding's CURRENT approved rule set. The version integer bumps ONLY on
table approval (:meth:`freeze_new_version`); any event that changes the
approved rule set WITHOUT a table re-approval rewrites vN in place and
stamps ``refrozen_at`` (:meth:`refreeze_current`) — auto-upgrade, a
per-rule approval, or a per-rule rejection/deprecation.

Scoping (SAFETY-CRITICAL): a binding's approved rows are identified via
the ``dq_quality_rules.applied_rule_id`` -> ``dq_applied_rules.id`` ->
``dq_applied_rules.binding_id`` linkage the materializer maintains — NEVER
by ``table_fqn`` alone (the P16-H pattern in
``routes/v1/monitored_tables.py``). Directly-authored (non-registry)
approved rules on the same ``table_fqn`` — rows with a NULL
``applied_rule_id`` that never flowed through this binding's apply +
submit + approve lifecycle — are DELIBERATELY EXCLUDED from the snapshot:
they are not part of the binding's governed rule set (the version bump on
table approval only ever transitions this binding's own materialized
rows), and a binding never surfaces them in its detail view. Freezing
them would make vN reflect state the table-approval never governed.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import MonitoredTableVersion
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


class MonitoredTableVersionService:
    """Freezes/reads per-version approved-rule snapshots for monitored tables.

    Reads the binding's current approved ``dq_quality_rules`` rows (through
    :class:`RulesCatalogService` + the applied-rule linkage on
    :class:`MonitoredTableService`) and persists them as immutable-per-bump
    snapshots in ``dq_monitored_table_versions``.
    """

    def __init__(
        self,
        sql: OltpExecutorProtocol,
        monitored_tables: MonitoredTableService,
        rules_catalog: RulesCatalogService,
    ) -> None:
        self._sql = sql
        self._monitored_tables = monitored_tables
        self._rules_catalog = rules_catalog
        self._versions_table = sql.fqn("dq_monitored_table_versions")
        self._tables = sql.fqn("dq_monitored_tables")
        self._applied_table = sql.fqn("dq_applied_rules")
        self._quality_rules_table = sql.fqn("dq_quality_rules")

    # ------------------------------------------------------------------
    # Freeze / re-freeze
    # ------------------------------------------------------------------

    def freeze_new_version(self, binding_id: str, user_email: str) -> int:
        """Bump the binding's version and freeze the current approved rule set as vN.

        Reads the binding's CURRENT approved ``dq_quality_rules`` rows
        (scoped to this binding's ``applied_rule_id``s), increments
        ``dq_monitored_tables.version``, and inserts a new
        ``dq_monitored_table_versions`` snapshot at the new version.

        Args:
            binding_id: The monitored-table binding to freeze.
            user_email: The approver, recorded as ``created_by``.

        Returns:
            The new (bumped) version integer.

        Raises:
            LookupError: *binding_id* does not exist.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise LookupError(f"Monitored table not found: {binding_id}")

        new_version = self._current_version(binding_id) + 1
        checks, state = self._current_approved_snapshot(binding_id, detail)

        e = escape_sql_string(binding_id)
        self._sql.execute(f"UPDATE {self._tables} SET version = {new_version} WHERE binding_id = '{e}'")

        row_id = uuid4().hex
        checks_expr = self._sql.json_literal_expr(json.dumps(checks))
        state_expr = self._sql.json_literal_expr(json.dumps(state))
        self._sql.execute(
            f"INSERT INTO {self._versions_table} "
            "(id, binding_id, version, checks_json, state_json, created_by, created_at, refrozen_at) "
            f"VALUES ('{escape_sql_string(row_id)}', '{e}', {new_version}, {checks_expr}, {state_expr}, "
            f"{self._opt_str(user_email)}, now(), NULL)"
        )
        logger.info("Froze monitored-table %s version %d (%d checks)", binding_id, new_version, len(checks))
        return new_version

    def refreeze_current(self, binding_id: str) -> None:
        """Rewrite the binding's CURRENT version snapshot in place, stamping ``refrozen_at``.

        No-op when the binding has never been approved (version 0): there is
        no snapshot to rewrite, and the table is "draft-run only" until a
        first approval mints v1. Otherwise re-reads the current approved rule
        set and overwrites vN's ``checks_json``/``state_json`` — the version
        integer is unchanged (design spec §3.2 re-freeze-without-bump).

        Raises:
            LookupError: *binding_id* does not exist.
        """
        current = self._current_version(binding_id)
        if current == 0:
            return
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            return
        checks, state = self._current_approved_snapshot(binding_id, detail)
        e = escape_sql_string(binding_id)
        checks_expr = self._sql.json_literal_expr(json.dumps(checks))
        state_expr = self._sql.json_literal_expr(json.dumps(state))
        self._sql.execute(
            f"UPDATE {self._versions_table} SET "
            f"checks_json = {checks_expr}, state_json = {state_expr}, refrozen_at = now() "
            f"WHERE binding_id = '{e}' AND version = {current}"
        )
        logger.info("Re-froze monitored-table %s version %d in place (%d checks)", binding_id, current, len(checks))

    def refreeze_for_quality_rule(self, rule_id: str) -> str | None:
        """Re-freeze the binding owning materialized ``dq_quality_rules`` *rule_id*.

        Hook entry point for per-rule approve/reject in
        ``routes/v1/rules.py``: resolves the row's ``applied_rule_id`` ->
        ``dq_applied_rules.binding_id`` linkage and re-freezes that binding's
        current snapshot. Returns the binding_id re-frozen, or ``None`` when
        the row is not a materialized registry check (NULL ``applied_rule_id``
        — a directly-authored rule, which carries no binding) or its
        application/binding can't be resolved.
        """
        applied_rule_id = self._resolve_applied_rule_id(rule_id)
        if not applied_rule_id:
            return None
        binding_id = self._resolve_binding_id(applied_rule_id)
        if not binding_id:
            return None
        self.refreeze_current(binding_id)
        return binding_id

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_versions(self, binding_id: str) -> list[MonitoredTableVersion]:
        """Return the binding's version snapshots (newest first), ``checks_json`` omitted.

        The heavy ``checks_json`` payload is left empty; callers that need
        the frozen checks resolve a single version via :meth:`get_checks`.
        """
        e = escape_sql_string(binding_id)
        state_text = self._sql.select_json_text("state_json")
        created_at = self._sql.ts_text("created_at")
        refrozen_at = self._sql.ts_text("refrozen_at")
        sql = (
            f"SELECT id, binding_id, version, {state_text} AS state_json, created_by, "  # noqa: S608
            f"{created_at} AS created_at, {refrozen_at} AS refrozen_at "
            f"FROM {self._versions_table} WHERE binding_id = '{e}' ORDER BY version DESC"
        )
        rows = self._sql.query(sql)
        return [self._row_to_version(row) for row in rows]

    def get_checks(self, binding_id: str, version: int) -> list[dict[str, Any]]:
        """Return the frozen ``checks_json`` for a specific version.

        Args:
            binding_id: The monitored-table binding.
            version: The frozen version number.

        Returns:
            The exact list of DQX check dicts the runner consumes.

        Raises:
            LookupError: no snapshot exists for *(binding_id, version)*.
        """
        e = escape_sql_string(binding_id)
        checks_text = self._sql.select_json_text("checks_json")
        sql = (
            f"SELECT {checks_text} FROM {self._versions_table} "  # noqa: S608
            f"WHERE binding_id = '{e}' AND version = {int(version)}"
        )
        rows = self._sql.query(sql)
        if not rows:
            raise LookupError(f"No frozen snapshot for binding {binding_id} version {version}")
        return self._parse_checks(rows[0][0])

    def snapshot_counts(self, binding_id: str, version: int) -> tuple[int, int] | None:
        """Return ``(applied_rule_count, check_count)`` for a frozen snapshot, or ``None``.

        ``check_count`` is the length of the frozen ``checks_json`` (the exact
        checks the runner would execute for that pin); ``applied_rule_count``
        is the number of applied rules recorded in ``state_json``. Returns
        ``None`` when no snapshot exists for *(binding_id, version)*, so callers
        can fall back to the live binding counts.

        Used by :class:`~.data_product_service.DataProductService` so a
        version-pinned product member reports the counts of the PINNED
        snapshot rather than the binding's current (possibly newer) live state.
        """
        e = escape_sql_string(binding_id)
        checks_text = self._sql.select_json_text("checks_json")
        state_text = self._sql.select_json_text("state_json")
        sql = (
            f"SELECT {checks_text}, {state_text} FROM {self._versions_table} "  # noqa: S608
            f"WHERE binding_id = '{e}' AND version = {int(version)}"
        )
        rows = self._sql.query(sql)
        if not rows:
            return None
        checks = self._parse_checks(rows[0][0])
        state = self._parse_state(rows[0][1])
        applied = state.get("applied_rules")
        rules_count = len(applied) if isinstance(applied, list) else 0
        return rules_count, len(checks)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _current_version(self, binding_id: str) -> int:
        e = escape_sql_string(binding_id)
        rows = self._sql.query(f"SELECT version FROM {self._tables} WHERE binding_id = '{e}'")  # noqa: S608
        if not rows:
            raise LookupError(f"Monitored table not found: {binding_id}")
        value = rows[0][0]
        return int(value) if value not in (None, "") else 0

    def _current_approved_snapshot(
        self, binding_id: str, detail: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Build ``(checks_json, state_json)`` from the binding's current approved rows."""
        applied_ids = {s.applied_rule.id for s in detail.applied_rules if s.applied_rule.id}
        all_checks = self._rules_catalog.get_approved_checks_for_table(detail.table.table_fqn)
        checks: list[dict[str, Any]] = []
        for check in all_checks:
            if not isinstance(check, dict):
                continue
            metadata = check.get("user_metadata") or {}
            if metadata.get("applied_rule_id") in applied_ids:
                checks.append(check)
        frozen_ids = {(c.get("user_metadata") or {}).get("applied_rule_id") for c in checks}
        state = self._build_state(detail, frozen_ids)
        return checks, state

    @staticmethod
    def _build_state(detail: Any, frozen_applied_ids: set[str | None]) -> dict[str, Any]:
        """Assemble display-only metadata for the applied rules present in the frozen set."""
        applied: list[dict[str, Any]] = []
        for summary in detail.applied_rules:
            ar = summary.applied_rule
            if ar.id not in frozen_applied_ids:
                continue
            applied.append(
                {
                    "applied_rule_id": ar.id,
                    "rule_id": ar.rule_id,
                    "pinned_version": ar.pinned_version,
                    "severity_override": ar.severity_override,
                    "column_mapping": ar.column_mapping,
                    "rule_name": summary.rule_name,
                    "rule_dimension": summary.rule_dimension,
                    "rule_severity": summary.rule_severity,
                }
            )
        return {"applied_rules": applied}

    def _resolve_applied_rule_id(self, rule_id: str) -> str | None:
        e = escape_sql_string(rule_id)
        rows = self._sql.query(
            f"SELECT applied_rule_id FROM {self._quality_rules_table} WHERE rule_id = '{e}'"  # noqa: S608
        )
        if not rows or not rows[0] or not rows[0][0]:
            return None
        return rows[0][0]

    def _resolve_binding_id(self, applied_rule_id: str) -> str | None:
        e = escape_sql_string(applied_rule_id)
        rows = self._sql.query(f"SELECT binding_id FROM {self._applied_table} WHERE id = '{e}'")  # noqa: S608
        if not rows or not rows[0] or not rows[0][0]:
            return None
        return rows[0][0]

    def _row_to_version(self, row: list[str]) -> MonitoredTableVersion:
        return MonitoredTableVersion(
            id=row[0],
            binding_id=row[1],
            version=int(row[2]) if row[2] not in (None, "") else 0,
            checks_json=[],
            state_json=self._parse_state(row[3]),
            created_by=row[4],
            created_at=self._parse_timestamp(row[5]),
            refrozen_at=self._parse_timestamp(row[6]),
        )

    @staticmethod
    def _parse_checks(raw: str | None) -> list[dict[str, Any]]:
        if not raw:
            return []
        try:
            parsed = json.loads(raw, strict=False)
        except (json.JSONDecodeError, TypeError):
            return []
        return [c for c in parsed if isinstance(c, dict)] if isinstance(parsed, list) else []

    @staticmethod
    def _parse_state(raw: str | None) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw, strict=False)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _parse_timestamp(value: str | None) -> Any:
        if not value:
            return None
        from datetime import datetime

        try:
            return datetime.fromisoformat(str(value).replace(" ", "T"))
        except ValueError:
            return None

    @staticmethod
    def _opt_str(value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"
