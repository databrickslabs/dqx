"""Monitored-table version freeze service (Data Products Task 2).

A monitored table binding carries a monotonically increasing ``version``
(``dq_monitored_tables.version``, 0 = never approved). Each version is
backed by a snapshot row in ``dq_monitored_table_versions`` that stores
lightweight **references** to the versioned registry rules that make up
the approved rule set — NOT a copy of the rendered rule set. The runner
payload is reconstructed ON DEMAND (:meth:`get_checks`) by resolving each
reference against the IMMUTABLE ``dq_rule_versions`` publish snapshot via
:meth:`Materializer.render_applied_checks`, so the result is byte-identical
to what materialization persisted (see design note below) — the same shape
``RulesCatalogService.get_approved_checks_for_table`` returns and
``routes/v1/dryrun.py:batch_run_from_catalog`` feeds to
``JobService.submit_run``.

Why references, not a frozen rule-set copy (reviewer feedback): the
registry already versions every rule (``dq_rule_versions``), and each
applied rule already refers to a specific version (``dq_applied_rules
.pinned_version``). Logging a second full copy of every rule set applied
per binding version duplicates that versioned state. Instead we freeze,
per applied rule in the approved set, the RESOLVED registry version that
was in effect at freeze time (``registry_version`` below) plus its column
mapping / severity override / per-application tags, and re-render from the
registry when the runner payload is needed. ``dq_rule_versions`` rows are
immutable once published, so a pinned reference always reconstructs the
same check. The one value not reproduced from immutable state is the
rendered ``criticality`` (resolved through the admin-editable
severity->criticality mapping at render time) — see
:meth:`Materializer.render_applied_checks`.

Freeze rule (design spec §3.2 / §4.1): snapshot vN ALWAYS mirrors the
binding's CURRENT approved rule set. The version integer bumps ONLY on
table approval (:meth:`freeze_new_version`); any event that changes the
approved rule set WITHOUT a table re-approval rewrites vN's references in
place and stamps ``refrozen_at`` (:meth:`refreeze_current`) —
auto-upgrade, a per-rule approval, or a per-rule rejection/deprecation.

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

from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTableVersion
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


class MonitoredTableVersionService:
    """Freezes/reads per-version approved-rule REFERENCE snapshots for monitored tables.

    Reads the binding's current approved ``dq_quality_rules`` rows (through
    :class:`RulesCatalogService` + the applied-rule linkage on
    :class:`MonitoredTableService`) to determine which applied rules — and at
    which resolved registry version — belong to the approved set, and
    persists those references in ``dq_monitored_table_versions.state_json``.
    The runner payload is reconstructed on demand from the registry via
    :class:`Materializer`.
    """

    def __init__(
        self,
        sql: OltpExecutorProtocol,
        monitored_tables: MonitoredTableService,
        rules_catalog: RulesCatalogService,
        materializer: Materializer,
    ) -> None:
        self._sql = sql
        self._monitored_tables = monitored_tables
        self._rules_catalog = rules_catalog
        self._materializer = materializer
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
        state = self._current_approved_snapshot(detail)

        e = escape_sql_string(binding_id)
        self._sql.execute(f"UPDATE {self._tables} SET version = {new_version} WHERE binding_id = '{e}'")

        row_id = uuid4().hex
        state_expr = self._sql.json_literal_expr(json.dumps(state))
        self._sql.execute(
            f"INSERT INTO {self._versions_table} "
            "(id, binding_id, version, state_json, created_by, created_at, refrozen_at) "
            f"VALUES ('{escape_sql_string(row_id)}', '{e}', {new_version}, {state_expr}, "
            f"{self._opt_str(user_email)}, now(), NULL)"
        )
        logger.info(
            "Froze monitored-table %s version %d (%d rule refs)",
            binding_id,
            new_version,
            len(state.get("rule_refs", [])),
        )
        return new_version

    def refreeze_current(self, binding_id: str) -> None:
        """Rewrite the binding's CURRENT version snapshot in place, stamping ``refrozen_at``.

        No-op when the binding has never been approved (version 0): there is
        no snapshot to rewrite, and the table is "draft-run only" until a
        first approval mints v1. Otherwise re-reads the current approved rule
        set and overwrites vN's ``state_json`` references — the version integer
        is unchanged (design spec §3.2 re-freeze-without-bump).

        NEVER destructive: just as version 0 is left untouched, a re-freeze
        that would replace a NON-EMPTY reference snapshot with an EMPTY one is
        refused. An empty re-computed set here means the binding's approved
        rows momentarily resolved to nothing — e.g. an upstream
        re-materialization couldn't re-render a rule against a new version — and
        blindly writing it would leave the pinned version (and every
        ``source == "approved"`` run) with zero checks. The previous snapshot is
        kept instead; the mismatch surfaces for re-review.

        Raises:
            LookupError: *binding_id* does not exist.
        """
        current = self._current_version(binding_id)
        if current == 0:
            return
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            return
        state = self._current_approved_snapshot(detail)
        if not state.get("rule_refs") and self._has_nonempty_snapshot(binding_id, current):
            logger.warning(
                "Refusing to overwrite non-empty frozen snapshot for binding %s version %d with an "
                "empty reference set; keeping the previous snapshot (re-review required)",
                binding_id,
                current,
            )
            return
        e = escape_sql_string(binding_id)
        state_expr = self._sql.json_literal_expr(json.dumps(state))
        self._sql.execute(
            f"UPDATE {self._versions_table} SET "
            f"state_json = {state_expr}, refrozen_at = now() "
            f"WHERE binding_id = '{e}' AND version = {current}"
        )
        logger.info(
            "Re-froze monitored-table %s version %d in place (%d rule refs)",
            binding_id,
            current,
            len(state.get("rule_refs", [])),
        )

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
        """Return the binding's version snapshots (newest first), resolved checks omitted.

        Only the audit + display metadata (``state_json``) is read; the
        model's ``checks_json`` is left empty. Callers that need the runner
        payload resolve a single version's references via :meth:`get_checks`.
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
        """Reconstruct the runner-shaped check dicts for a specific frozen version.

        Loads the version's frozen references (``state_json.rule_refs``) and
        re-renders each against the immutable ``dq_rule_versions`` snapshot via
        :meth:`Materializer.render_applied_checks`, reproducing the exact rule
        set that was approved at freeze time. The returned list is the same
        shape the runner consumes (identical to
        ``RulesCatalogService.get_approved_checks_for_table`` output).

        Args:
            binding_id: The monitored-table binding.
            version: The frozen version number.

        Returns:
            The list of DQX check dicts the runner consumes.

        Raises:
            LookupError: no snapshot exists for *(binding_id, version)*.
        """
        e = escape_sql_string(binding_id)
        state_text = self._sql.select_json_text("state_json")
        sql = (
            f"SELECT {state_text} FROM {self._versions_table} "  # noqa: S608
            f"WHERE binding_id = '{e}' AND version = {int(version)}"
        )
        rows = self._sql.query(sql)
        if not rows:
            raise LookupError(f"No frozen snapshot for binding {binding_id} version {version}")
        state = self._parse_state(rows[0][0])
        refs = state.get("rule_refs")
        if not isinstance(refs, list) or not refs:
            return []
        table_fqn = self._binding_table_fqn(binding_id)
        if table_fqn is None:
            return []
        applied = [self._ref_to_applied(binding_id, ref) for ref in refs if isinstance(ref, dict)]
        return self._materializer.render_applied_checks(table_fqn, applied)

    def snapshot_counts_many(self, pins: list[tuple[str, int]]) -> dict[tuple[str, int], tuple[int, int]]:
        """Return ``(applied_rule_count, check_count)`` per frozen *(binding_id, version)* pin.

        ``check_count`` is the number of rendered checks the pin would execute
        (cached in ``state_json.check_count`` at freeze time so this stays a
        single metadata query with no per-pin re-render); ``applied_rule_count``
        is the number of frozen applied-rule references. All pins are resolved
        in ONE query (no per-pin round-trip); a pin with no snapshot row is
        simply absent from the result, so callers can fall back to the live
        binding counts.

        Used by :class:`~.data_product_service.DataProductService` so a
        version-pinned product member reports the counts of the PINNED
        snapshot rather than the binding's current (possibly newer) live state.
        """
        if not pins:
            return {}
        state_text = self._sql.select_json_text("state_json")
        predicates = " OR ".join(
            f"(binding_id = '{escape_sql_string(binding_id)}' AND version = {int(version)})"
            for binding_id, version in sorted(set(pins))
        )
        sql = (
            f"SELECT binding_id, version, {state_text} AS state_json "  # noqa: S608
            f"FROM {self._versions_table} WHERE {predicates}"
        )
        rows = self._sql.query(sql)
        result: dict[tuple[str, int], tuple[int, int]] = {}
        for row in rows:
            state = self._parse_state(row[2])
            refs = state.get("rule_refs")
            rules_count = len(refs) if isinstance(refs, list) else 0
            check_count = state.get("check_count")
            checks_total = int(check_count) if isinstance(check_count, int) else rules_count
            version = int(row[1]) if row[1] not in (None, "") else 0
            result[(row[0], version)] = (rules_count, checks_total)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _has_nonempty_snapshot(self, binding_id: str, version: int) -> bool:
        """True when a frozen snapshot exists for *(binding_id, version)* and carries >=1 reference.

        Backs the non-destructive guard in :meth:`refreeze_current`: a missing
        snapshot (or one already empty) is safe to (re)write, but a non-empty
        one must never be replaced with an empty reference set. Reads the
        stored references directly (no re-render) so the guard doesn't depend
        on the registry being resolvable at the moment of re-freeze.
        """
        e = escape_sql_string(binding_id)
        state_text = self._sql.select_json_text("state_json")
        rows = self._sql.query(
            f"SELECT {state_text} FROM {self._versions_table} "  # noqa: S608
            f"WHERE binding_id = '{e}' AND version = {int(version)}"
        )
        if not rows:
            return False
        refs = self._parse_state(rows[0][0]).get("rule_refs")
        return isinstance(refs, list) and len(refs) > 0

    def _binding_table_fqn(self, binding_id: str) -> str | None:
        e = escape_sql_string(binding_id)
        rows = self._sql.query(f"SELECT table_fqn FROM {self._tables} WHERE binding_id = '{e}'")  # noqa: S608
        if not rows or not rows[0] or not rows[0][0]:
            return None
        return rows[0][0]

    @staticmethod
    def _ref_to_applied(binding_id: str, ref: dict[str, Any]) -> AppliedRule:
        """Reconstruct an :class:`AppliedRule` from a frozen reference.

        ``pinned_version`` is set to the RESOLVED ``registry_version`` frozen at
        freeze time (never the live pin), so
        :meth:`Materializer.render_applied_checks` renders exactly that version
        regardless of any later republish/auto-upgrade of the live application.
        """
        column_mapping = ref.get("column_mapping")
        registry_version = ref.get("registry_version")
        user_metadata = ref.get("user_metadata")
        return AppliedRule(
            id=ref.get("applied_rule_id"),
            binding_id=binding_id,
            rule_id=str(ref.get("rule_id", "")),
            pinned_version=int(registry_version) if isinstance(registry_version, int) else None,
            severity_override=ref.get("severity_override"),
            column_mapping=column_mapping if isinstance(column_mapping, list) else [],
            user_metadata=user_metadata if isinstance(user_metadata, dict) else {},
        )

    def _current_version(self, binding_id: str) -> int:
        e = escape_sql_string(binding_id)
        rows = self._sql.query(f"SELECT version FROM {self._tables} WHERE binding_id = '{e}'")  # noqa: S608
        if not rows:
            raise LookupError(f"Monitored table not found: {binding_id}")
        value = rows[0][0]
        return int(value) if value not in (None, "") else 0

    def _current_approved_snapshot(self, detail: Any) -> dict[str, Any]:
        """Build the reference ``state_json`` from the binding's current approved rows.

        Determines which applied rules belong to the binding's approved set
        (scoped by ``applied_rule_id``, excluding directly-authored rules) and
        the RESOLVED registry version each was rendered at, by reading the
        approved ``dq_quality_rules`` rows via
        ``RulesCatalogService.get_approved_checks_for_table``. Returns
        ``state_json`` carrying, per approved applied rule, the reference the
        runner payload is reconstructed from (``rule_refs``), the display
        metadata (``applied_rules``), and the rendered ``check_count``.
        """
        applied_ids = {s.applied_rule.id for s in detail.applied_rules if s.applied_rule.id}
        all_checks = self._rules_catalog.get_approved_checks_for_table(detail.table.table_fqn)
        # Resolved registry version per approved applied rule, plus how many
        # rendered checks it contributes — read straight off the frozen
        # ``dq_quality_rules`` checks (their ``user_metadata`` carries both).
        resolved_version: dict[str, int] = {}
        check_count = 0
        for check in all_checks:
            if not isinstance(check, dict):
                continue
            metadata = check.get("user_metadata") or {}
            applied_rule_id = metadata.get("applied_rule_id")
            if not isinstance(applied_rule_id, str) or applied_rule_id not in applied_ids:
                continue
            check_count += 1
            raw_version = metadata.get("registry_version")
            if raw_version is None or applied_rule_id in resolved_version:
                continue
            try:
                resolved_version[applied_rule_id] = int(raw_version)
            except (TypeError, ValueError):
                continue
        return self._build_state(detail, resolved_version, check_count)

    @staticmethod
    def _build_state(detail: Any, resolved_version: dict[str, int], check_count: int) -> dict[str, Any]:
        """Assemble the frozen references + display metadata for the approved set.

        ``rule_refs`` is the render source (:meth:`get_checks` /
        :meth:`Materializer.render_applied_checks`); ``applied_rules`` mirrors
        it with extra display-only fields the version picker shows;
        ``check_count`` caches the rendered check total for
        :meth:`snapshot_counts_many`.
        """
        rule_refs: list[dict[str, Any]] = []
        applied: list[dict[str, Any]] = []
        for summary in detail.applied_rules:
            ar = summary.applied_rule
            if ar.id not in resolved_version:
                continue
            registry_version = resolved_version[ar.id]
            rule_refs.append(
                {
                    "applied_rule_id": ar.id,
                    "rule_id": ar.rule_id,
                    "registry_version": registry_version,
                    "severity_override": ar.severity_override,
                    "column_mapping": ar.column_mapping,
                    "user_metadata": ar.user_metadata,
                }
            )
            applied.append(
                {
                    "applied_rule_id": ar.id,
                    "rule_id": ar.rule_id,
                    "registry_version": registry_version,
                    "pinned_version": ar.pinned_version,
                    "severity_override": ar.severity_override,
                    "column_mapping": ar.column_mapping,
                    "rule_name": summary.rule_name,
                    "rule_dimension": summary.rule_dimension,
                    "rule_severity": summary.rule_severity,
                }
            )
        return {"applied_rules": applied, "rule_refs": rule_refs, "check_count": check_count}

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
