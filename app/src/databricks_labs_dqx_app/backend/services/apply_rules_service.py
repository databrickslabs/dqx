"""Apply Rules service (Phase 3C — tier-2 apply/map layer).

Manages the LIVE ``dq_applied_rules`` link between a *published* registry
rule (``dq_rules``) and a monitored table's column mapping, per
``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §5 and §7.

This is deliberately separate from :class:`~databricks_labs_dqx_app.backend.services.monitored_table_service.MonitoredTableService`
(which owns the ``dq_monitored_tables`` binding and read-only joins for
display) — ``ApplyRulesService`` owns the CRUD lifecycle of an application:
create/update via :meth:`apply_rule`, remove (with materialized-row
cleanup), and the two narrow mutations a table owner can make without
re-applying (:meth:`set_pin`, :meth:`set_severity_override`).

Applying a rule does NOT materialize it — that's
:class:`~databricks_labs_dqx_app.backend.services.materializer.Materializer`'s
job, called separately (typically right after `apply_rule`/`set_pin`/
`set_severity_override`/`remove_applied` from the routes layer).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    ColumnMappingGroup,
    compute_mapping_hash,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


class RuleNotPublishedError(ValueError):
    """Raised by :meth:`ApplyRulesService.apply_rule` when *rule_id* is not currently published."""


class MappingIncompleteError(ValueError):
    """Raised by :meth:`ApplyRulesService.apply_rule` when *column_mapping* doesn't cover every slot."""


@dataclass
class DesiredAppliedRule:
    """One entry in the FULL desired set passed to :meth:`ApplyRulesService.save_applied_rules`.

    Mirrors the mutable fields of :class:`~databricks_labs_dqx_app.backend.registry_models.AppliedRule`
    minus the persistence-only fields (``id``/``binding_id``/``mapping_hash``/``created_by``/``created_at``)
    that the reconcile loop derives or fills in itself.
    """

    rule_id: str
    column_mapping: list[ColumnMappingGroup] = field(default_factory=list)
    pinned_version: int | None = None
    severity_override: str | None = None
    tags: dict[str, Any] = field(default_factory=dict)


class ApplyRulesService:
    """Manages ``dq_applied_rules`` (the tier-2 apply/map link) in the OLTP store."""

    def __init__(self, sql: OltpExecutorProtocol, registry: RegistryService, app_settings: AppSettingsService) -> None:
        self._sql = sql
        self._registry = registry
        self._app_settings = app_settings
        self._table = sql.fqn("dq_applied_rules")
        self._monitored_table = sql.fqn("dq_monitored_tables")
        self._quality_rules_table = sql.fqn("dq_quality_rules")
        self._select_cols = self._build_select_cols()

    def _build_select_cols(self) -> str:
        column_mapping = self._sql.select_json_text("column_mapping")
        user_metadata = self._sql.select_json_text("user_metadata")
        created_at = self._sql.ts_text("created_at")
        return (
            "id, binding_id, rule_id, pinned_version, severity_override, "
            f"{column_mapping} AS column_mapping_json, {user_metadata} AS user_metadata_json, "
            f"mapping_hash, created_by, {created_at} AS created_at"
        )

    # ------------------------------------------------------------------
    # Apply
    # ------------------------------------------------------------------

    def apply_rule(
        self,
        binding_id: str,
        rule_id: str,
        column_mapping: list[ColumnMappingGroup],
        user_email: str,
        pinned_version: int | None = None,
        severity_override: str | None = None,
        tags: dict[str, Any] | None = None,
    ) -> AppliedRule:
        """Apply a published registry rule to a monitored table's column mapping.

        Insert/update semantics: re-applying the same rule with an
        identical *column_mapping* (same normalized ``mapping_hash``) is
        treated as an UPDATE of the mutable fields (``pinned_version``,
        ``severity_override``, ``tags``) on the existing row rather than a
        duplicate — enforced by the ``UNIQUE(binding_id, rule_id,
        mapping_hash)`` constraint on the table, mirrored here so the
        behaviour is identical on the Delta OLTP-fallback baseline (which
        can't declare that constraint natively).

        Args:
            binding_id: The monitored table binding this application belongs to.
            rule_id: The registry rule being applied. Must be ``approved`` (published).
            column_mapping: One mapping GROUP per materialized check — every
                group's keys must exactly match the rule's slot names.
            user_email: Attributed as ``created_by`` on a new row.
            pinned_version: ``None`` to follow the latest published version;
                a concrete version number to freeze to that snapshot. On a
                brand-new application, an explicit ``None`` here is resolved
                against the ``default_auto_upgrade`` app-setting (see
                :meth:`~databricks_labs_dqx_app.backend.services.app_settings_service.AppSettingsService.resolve_pinned_version_for_new_attachment`) —
                it only means "follow latest" when that setting is on;
                otherwise the rule's current version is pinned instead. On
                an existing application (identical-mapping re-apply), the
                setting does NOT apply — ``None`` is honoured as-is.
            severity_override: Overrides the rule's tagged severity for this
                application only.
            tags: Per-application free-text tags (merged with rule tags at
                materialization time).

        Returns:
            The created or updated :class:`AppliedRule`.

        Raises:
            RuntimeError: *binding_id* or *rule_id* does not exist.
            RuleNotPublishedError: *rule_id* is not currently ``approved``.
            MappingIncompleteError: a provided group's keys don't exactly
                match the rule's slot names. An empty *column_mapping* is
                allowed — it stages the application with no mapping yet.
        """
        self._require_binding_exists(binding_id)
        rule = self._registry.get_rule(rule_id)
        if rule is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        if rule.status != "approved":
            raise RuleNotPublishedError(
                f"Registry rule '{rule_id}' is not published (status='{rule.status}'); "
                "only published rules can be applied to a monitored table."
            )
        self._validate_mapping_complete(column_mapping, rule.definition.slots)

        mapping_hash = compute_mapping_hash(column_mapping)
        existing = self._get_by_natural_key(binding_id, rule_id, mapping_hash)
        if existing is not None:
            return self._update_mutable_fields(
                existing, pinned_version=pinned_version, severity_override=severity_override, tags=tags
            )

        # Attach-time-only default_auto_upgrade resolution: this is the
        # INSERT branch (no existing row for this natural key), i.e. a
        # genuinely new application. An update (see _update_mutable_fields
        # above) never goes through this resolution — an explicit
        # pinned_version=None there already means "steward chose to follow
        # latest", not "caller left it unspecified".
        resolved_pinned_version = self._app_settings.resolve_pinned_version_for_new_attachment(
            pinned_version, rule.version
        )

        now = datetime.now(timezone.utc)
        applied = AppliedRule(
            id=uuid4().hex[:16],
            binding_id=binding_id,
            rule_id=rule_id,
            pinned_version=resolved_pinned_version,
            severity_override=severity_override,
            column_mapping=column_mapping,
            user_metadata=dict(tags or {}),
            mapping_hash=mapping_hash,
            created_by=user_email,
            created_at=now,
        )
        self._insert(applied)
        logger.info(
            "Applied registry rule %s to binding %s (applied_rule_id=%s)", rule_id, binding_id, applied.id
        )
        return applied

    @staticmethod
    def _validate_mapping_complete(column_mapping: list[ColumnMappingGroup], slots: list[Any]) -> None:
        # An empty column_mapping is allowed: it stages the rule application
        # (materializer.py skips a row with zero mapping groups, so nothing
        # runs until the caller submits a follow-up apply_rule() call with a
        # fully-covering group). This lets the UI apply a rule immediately
        # on selection and defer column mapping to the by-rule card, instead
        # of forcing mapping to complete before the rule can be staged at
        # all. Any group that IS provided must still cover the rule's slots
        # exactly — partial groups are never accepted.
        if not column_mapping:
            return
        expected = {slot.name for slot in slots}
        for group in column_mapping:
            actual = set(group.keys())
            if actual != expected:
                missing = expected - actual
                extra = actual - expected
                detail_parts = []
                if missing:
                    detail_parts.append(f"missing slot(s) {sorted(missing)}")
                if extra:
                    detail_parts.append(f"unknown slot(s) {sorted(extra)}")
                raise MappingIncompleteError(
                    f"column_mapping group {group} does not cover the rule's slots exactly: "
                    + "; ".join(detail_parts)
                )

    def _update_mutable_fields(
        self,
        existing: AppliedRule,
        *,
        pinned_version: int | None,
        severity_override: str | None,
        tags: dict[str, Any] | None,
    ) -> AppliedRule:
        existing.pinned_version = pinned_version
        existing.severity_override = severity_override
        if tags is not None:
            existing.user_metadata = dict(tags)
        e_id = escape_sql_string(existing.id or "")
        metadata_expr = self._sql.json_literal_expr(json.dumps(existing.user_metadata))
        sql = (
            f"UPDATE {self._table} SET "
            f"  pinned_version = {existing.pinned_version if existing.pinned_version is not None else 'NULL'}, "
            f"  severity_override = {self._opt_str(existing.severity_override)}, "
            f"  user_metadata = {metadata_expr} "
            f"WHERE id = '{e_id}'"
        )
        self._sql.execute(sql)
        logger.info("Updated applied rule %s (re-applied with identical mapping)", existing.id)
        return existing

    def _insert(self, applied: AppliedRule) -> None:
        column_mapping_expr = self._sql.json_literal_expr(json.dumps(applied.column_mapping))
        metadata_expr = self._sql.json_literal_expr(json.dumps(applied.user_metadata))
        sql = (
            f"INSERT INTO {self._table} "
            "(id, binding_id, rule_id, pinned_version, severity_override, column_mapping, user_metadata, "
            "mapping_hash, created_by, created_at) VALUES "
            f"('{escape_sql_string(applied.id or '')}', '{escape_sql_string(applied.binding_id)}', "
            f"'{escape_sql_string(applied.rule_id)}', "
            f"{applied.pinned_version if applied.pinned_version is not None else 'NULL'}, "
            f"{self._opt_str(applied.severity_override)}, {column_mapping_expr}, {metadata_expr}, "
            f"'{escape_sql_string(applied.mapping_hash or '')}', {self._opt_str(applied.created_by)}, now())"
        )
        self._sql.execute(sql)

    def _require_binding_exists(self, binding_id: str) -> None:
        e = escape_sql_string(binding_id)
        sql = f"SELECT binding_id FROM {self._monitored_table} WHERE binding_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            raise RuntimeError(f"Monitored table not found: {binding_id}")

    def _get_by_natural_key(self, binding_id: str, rule_id: str, mapping_hash: str) -> AppliedRule | None:
        e_binding = escape_sql_string(binding_id)
        e_rule = escape_sql_string(rule_id)
        e_hash = escape_sql_string(mapping_hash)
        sql = (
            f"SELECT {self._select_cols} FROM {self._table} "  # noqa: S608
            f"WHERE binding_id = '{e_binding}' AND rule_id = '{e_rule}' AND mapping_hash = '{e_hash}'"
        )
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_applied_rule(rows[0])

    # ------------------------------------------------------------------
    # Batch reconcile (staged editor — save/publish in one action)
    # ------------------------------------------------------------------

    def save_applied_rules(
        self,
        binding_id: str,
        desired: list[DesiredAppliedRule],
        user_email: str,
    ) -> list[AppliedRule]:
        """Reconcile the FULL desired set of applied rules for *binding_id* in one batch.

        Backs the staged Apply Rules editor: the UI stages every add/remove/
        mapping-edit/severity-override/pin change locally and calls this once
        on Save-as-draft or Publish, instead of firing an immediate API call
        per edit. *desired* must be the complete set of applications the UI
        wants to end up with for this binding — anything currently applied
        that isn't (re)supplied here is removed.

        Reconciliation, per entry (keyed by ``rule_id`` — the UI enforces at
        most one entry per rule; if duplicates arrive anyway, the last one
        in *desired* wins):

        - Upserts via :meth:`apply_rule`, which already handles the
          identical-mapping-hash update-in-place case.
        - A mapping change (new ``mapping_hash``) inserts the new row via
          :meth:`apply_rule` and removes the old row (via :meth:`remove_applied`,
          which also cleans up any materialized ``dq_quality_rules`` rows)
          since it no longer matches any desired entry's hash.
        - Any existing row whose ``rule_id`` isn't in *desired* at all is
          removed the same way.

        All entries are validated (published-rule + slot-coverage per group)
        BEFORE any mutation happens, so a single bad entry never leaves the
        binding half-reconciled.

        Args:
            binding_id: The monitored table binding to reconcile.
            desired: The full desired set of applications for this binding.
            user_email: Attributed as ``created_by`` on newly inserted rows.

        Returns:
            The resulting list of :class:`AppliedRule` rows for *binding_id*
            (insertion order of *desired*, deduplicated by ``rule_id``).

        Raises:
            RuntimeError: *binding_id* or a desired entry's *rule_id* does not exist.
            RuleNotPublishedError: a desired entry's rule is not currently ``approved``.
            MappingIncompleteError: a desired entry's mapping group doesn't
                exactly cover its rule's slots.
        """
        self._require_binding_exists(binding_id)

        deduped: dict[str, DesiredAppliedRule] = {}
        for entry in desired:
            deduped[entry.rule_id] = entry
        deduped_entries = list(deduped.values())

        # Validate every entry up front so a bad one never leaves a
        # half-applied set (no removals or upserts have happened yet).
        desired_hashes: dict[str, str] = {}
        for entry in deduped_entries:
            rule = self._registry.get_rule(entry.rule_id)
            if rule is None:
                raise RuntimeError(f"Registry rule not found: {entry.rule_id}")
            if rule.status != "approved":
                raise RuleNotPublishedError(
                    f"Registry rule '{entry.rule_id}' is not published (status='{rule.status}'); "
                    "only published rules can be applied to a monitored table."
                )
            self._validate_mapping_complete(entry.column_mapping, rule.definition.slots)
            desired_hashes[entry.rule_id] = compute_mapping_hash(entry.column_mapping)

        # Remove anything not present in the desired set (by rule_id) or
        # superseded by a mapping change (hash mismatch for that rule_id).
        for existing in self.list_applied(binding_id):
            if existing.id is not None and desired_hashes.get(existing.rule_id) != existing.mapping_hash:
                self.remove_applied(existing.id)

        results = [
            self.apply_rule(
                binding_id,
                entry.rule_id,
                entry.column_mapping,
                user_email,
                pinned_version=entry.pinned_version,
                severity_override=entry.severity_override,
                tags=entry.tags,
            )
            for entry in deduped_entries
        ]
        logger.info(
            "Reconciled applied rules for binding %s: %d desired, %d resulting",
            binding_id,
            len(deduped_entries),
            len(results),
        )
        return results

    # ------------------------------------------------------------------
    # List / Get
    # ------------------------------------------------------------------

    def list_applied(self, binding_id: str) -> list[AppliedRule]:
        """List every applied rule for *binding_id*."""
        e = escape_sql_string(binding_id)
        sql = (
            f"SELECT {self._select_cols} FROM {self._table} "  # noqa: S608
            f"WHERE binding_id = '{e}' ORDER BY created_at"
        )
        rows = self._sql.query(sql)
        return [self._row_to_applied_rule(row) for row in rows]

    def count_applications_for_rule(self, rule_id: str) -> int:
        """Count how many monitored tables currently have *rule_id* applied.

        Used by the registry delete gate — a rule that's live on one or more
        tables cannot be deleted until every application is removed first.
        """
        e = escape_sql_string(rule_id)
        sql = f"SELECT COUNT(*) FROM {self._table} WHERE rule_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        return int(rows[0][0]) if rows and rows[0] and rows[0][0] is not None else 0

    def get_applied(self, applied_rule_id: str) -> AppliedRule | None:
        """Get a single applied rule by id."""
        e = escape_sql_string(applied_rule_id)
        sql = f"SELECT {self._select_cols} FROM {self._table} WHERE id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        return self._row_to_applied_rule(rows[0])

    # ------------------------------------------------------------------
    # Remove
    # ------------------------------------------------------------------

    def remove_applied(self, applied_rule_id: str) -> None:
        """Remove an applied rule and every ``dq_quality_rules`` row it materialized.

        Raises:
            RuntimeError: *applied_rule_id* does not exist.
        """
        existing = self.get_applied(applied_rule_id)
        if existing is None:
            raise RuntimeError(f"Applied rule not found: {applied_rule_id}")
        e = escape_sql_string(applied_rule_id)
        self._sql.execute(f"DELETE FROM {self._quality_rules_table} WHERE applied_rule_id = '{e}'")
        self._sql.execute(f"DELETE FROM {self._table} WHERE id = '{e}'")
        logger.info("Removed applied rule %s (binding=%s, rule=%s)", applied_rule_id, existing.binding_id, existing.rule_id)

    # ------------------------------------------------------------------
    # Pin / severity override
    # ------------------------------------------------------------------

    def set_pin(self, applied_rule_id: str, pinned_version: int | None) -> AppliedRule:
        """Set (or clear, with ``None``) the version pin for an applied rule."""
        existing = self.get_applied(applied_rule_id)
        if existing is None:
            raise RuntimeError(f"Applied rule not found: {applied_rule_id}")
        e = escape_sql_string(applied_rule_id)
        value = pinned_version if pinned_version is not None else "NULL"
        self._sql.execute(f"UPDATE {self._table} SET pinned_version = {value} WHERE id = '{e}'")
        existing.pinned_version = pinned_version
        logger.info("Set pin for applied rule %s to %s", applied_rule_id, pinned_version)
        return existing

    def set_severity_override(self, applied_rule_id: str, severity: str | None) -> AppliedRule:
        """Set (or clear, with ``None``) the severity override for an applied rule."""
        existing = self.get_applied(applied_rule_id)
        if existing is None:
            raise RuntimeError(f"Applied rule not found: {applied_rule_id}")
        e = escape_sql_string(applied_rule_id)
        value = self._opt_str(severity)
        self._sql.execute(f"UPDATE {self._table} SET severity_override = {value} WHERE id = '{e}'")
        existing.severity_override = severity
        logger.info("Set severity override for applied rule %s to %s", applied_rule_id, severity)
        return existing

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _opt_str(value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"

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

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            logger.warning("Unparsable timestamp %r; treating as None", value)
            return None

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
