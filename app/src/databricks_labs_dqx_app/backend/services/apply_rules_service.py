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

import json
import logging
from collections.abc import Collection
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from databricks_labs_dqx_app.backend.registry_models import (
    AUTO_ORIGINS,
    ORIGIN_KEY,
    ORIGIN_POLICY,
    ORIGIN_TAG_AUTO,
    POLICY_ID_KEY,
    AppliedRule,
    ColumnMappingGroup,
    RegistryRule,
    compute_mapping_hash,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


class RuleNotPublishedError(ValueError):
    """Raised by :meth:`ApplyRulesService.apply_rule` when *rule_id* is not currently published."""


class MappingIncompleteError(ValueError):
    """Raised by :meth:`ApplyRulesService.apply_rule` when *column_mapping* doesn't cover every slot."""


class UnsafeRowFilterError(ValueError):
    """Raised when a per-rule ``row_filter`` predicate contains prohibited SQL."""


def _clean_row_filter(raw: object) -> str | None:
    """Normalize a per-rule ``row_filter`` to a non-empty stripped str, or None."""
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _clamp_pass_threshold(raw: object) -> int | None:
    """Coerce a per-rule ``pass_threshold`` to an int in [0, 100], or None."""
    if raw is None or raw == "":
        return None
    if not isinstance(raw, (int, float, str, bytes, bytearray)):
        return None
    try:
        return max(0, min(100, int(raw)))
    except (TypeError, ValueError):
        return None


def validate_row_filter(row_filter: str | None) -> None:
    """Reject an unsafe per-rule row filter before it is persisted.

    ``None``/blank is always allowed. A concrete predicate is validated by
    wrapping it in a throwaway ``SELECT`` and running DQX's ``is_sql_query_safe``
    (the same guard the view layer uses), so statement terminators, DDL/DML and
    other injection vectors are rejected. Raises :class:`UnsafeRowFilterError`.
    """
    cleaned = _clean_row_filter(row_filter)
    if cleaned is None:
        return
    if len(cleaned) > _ROW_FILTER_MAX_LEN:
        raise UnsafeRowFilterError(f"Row filter is too long (max {_ROW_FILTER_MAX_LEN} characters).")
    from databricks.labs.dqx.utils import is_sql_query_safe

    if not is_sql_query_safe(f"SELECT * FROM _t WHERE ({cleaned})"):
        raise UnsafeRowFilterError("Row filter contains prohibited SQL and cannot be used.")


# Cap the free-text per-rule filter so it can't bloat the row or the rendered check.
_ROW_FILTER_MAX_LEN = 4000


_ORIGIN_KEYS = (ORIGIN_KEY, POLICY_ID_KEY)


def _without_origin(tags: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in tags.items() if key not in _ORIGIN_KEYS}


def _origin_of(metadata: dict[str, Any]) -> dict[str, Any]:
    return {key: metadata[key] for key in _ORIGIN_KEYS if key in metadata}


def _group_hashes(column_mapping: list[ColumnMappingGroup]) -> list[str]:
    return [compute_mapping_hash([group]) for group in column_mapping]


def _dedupe_groups(column_mapping: list[ColumnMappingGroup]) -> list[ColumnMappingGroup]:
    seen: set[str] = set()
    out: list[ColumnMappingGroup] = []
    for group in column_mapping:
        key = compute_mapping_hash([group])
        if key not in seen:
            seen.add(key)
            out.append(group)
    return out


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
    row_filter: str | None = None
    pass_threshold: int | None = None
    tags: dict[str, Any] = field(default_factory=dict)


class ApplyRulesService:
    """Manages ``dq_applied_rules`` (the tier-2 apply/map link) in the OLTP store."""

    def __init__(self, sql: OltpExecutorProtocol, registry: RegistryService, app_settings: AppSettingsService) -> None:
        self._sql = sql
        self._registry = registry
        self._app_settings = app_settings
        self._table = sql.fqn("dq_applied_rules")
        self._monitored_table = sql.fqn("dq_monitored_tables")
        self._quality_rules_table = sql.fqn("dq_resolved_rules")
        self._suppressions_table = sql.fqn("dq_tag_auto_suppressions")
        self._select_cols = self._build_select_cols()

    def _build_select_cols(self) -> str:
        column_mapping = self._sql.select_json_text("column_mapping")
        user_metadata = self._sql.select_json_text("user_metadata")
        created_at = self._sql.ts_text("created_at")
        return (
            "id, binding_id, rule_id, pinned_version, severity_override, "
            f"{column_mapping} AS column_mapping_json, {user_metadata} AS user_metadata_json, "
            f"mapping_hash, created_by, {created_at} AS created_at, "
            # row_filter (10) + pass_threshold (11) appended last so existing
            # positional indices in ``_row_to_applied_rule`` stay stable.
            "row_filter, pass_threshold"
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
        row_filter: str | None = None,
        pass_threshold: int | None = None,
        tags: dict[str, Any] | None = None,
    ) -> AppliedRule:
        """Apply a published registry rule to a monitored table's column mapping.

        Insert/update semantics: re-applying the same rule with an
        identical *column_mapping* (same normalized ``mapping_hash``) is
        treated as an UPDATE of the mutable fields (``pinned_version``,
        ``severity_override``, ``tags``) on the existing row rather than a
        duplicate — enforced by the ``UNIQUE(binding_id, rule_id,
        mapping_hash)`` constraint on the table, mirrored here so the
        behaviour is defined by the Lakebase OLTP baseline (which
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
        rule = self._validate_applicable_rule(binding_id, rule_id, column_mapping)
        validate_row_filter(row_filter)

        mapping_hash = compute_mapping_hash(column_mapping)
        existing = self._get_by_natural_key(binding_id, rule_id, mapping_hash)
        if existing is not None:
            return self._update_mutable_fields(
                existing,
                pinned_version=pinned_version,
                severity_override=severity_override,
                row_filter=row_filter,
                pass_threshold=pass_threshold,
                tags=tags,
            )

        applied = self.build_applied_rule(
            binding_id=binding_id,
            rule_id=rule_id,
            column_mapping=column_mapping,
            user_email=user_email,
            pinned_version=pinned_version,
            severity_override=severity_override,
            row_filter=row_filter,
            pass_threshold=pass_threshold,
            tags=tags,
            _rule=rule,
            _mapping_hash=mapping_hash,
        )
        self._insert(applied)
        logger.info("Applied registry rule %s to binding %s (applied_rule_id=%s)", rule_id, binding_id, applied.id)
        return applied

    def build_applied_rule(
        self,
        binding_id: str,
        rule_id: str,
        column_mapping: list[ColumnMappingGroup],
        user_email: str,
        pinned_version: int | None = None,
        severity_override: str | None = None,
        row_filter: str | None = None,
        pass_threshold: int | None = None,
        tags: dict[str, Any] | None = None,
        *,
        _rule: RegistryRule | None = None,
        _mapping_hash: str | None = None,
    ) -> AppliedRule:
        """Build an :class:`AppliedRule` in memory WITHOUT persisting it.

        Performs the same validation and construction as the INSERT branch of
        :meth:`apply_rule` — verifies the binding exists, the rule is published,
        the column mapping covers every slot, and resolves *pinned_version*
        through ``default_auto_upgrade`` — but does NOT call :meth:`_insert`.

        This is the staging path for the profiler-suggestion flow: the
        resolved-or-created registry rule template is bound in memory so the
        frontend can drop the row into the Apply Rules tab's unsaved selection
        (as if the user hand-picked the rule). Callers that need persistence
        should use :meth:`apply_rule` instead.

        Args:
            binding_id: The monitored table binding the rule would be applied to.
            rule_id: The registry rule to apply. Must be ``approved``.
            column_mapping: Column mapping groups — same constraints as
                :meth:`apply_rule`.
            user_email: Attributed as ``created_by`` on the transient row.
            pinned_version: Resolved via ``default_auto_upgrade`` when *None*,
                same as the INSERT branch of :meth:`apply_rule`.
            severity_override: Optional per-application severity override.
            row_filter: Optional SQL predicate (validated).
            pass_threshold: Optional per-rule pass threshold.
            tags: Per-application free-text tags.
            _rule: Pre-resolved :class:`RegistryRule` (avoids a redundant DB
                lookup when called from :meth:`apply_rule`).
            _mapping_hash: Pre-computed mapping hash (avoids redundant hashing).

        Returns:
            An un-persisted :class:`AppliedRule` with a fresh ``id`` and
            ``created_at`` set to the current UTC time.

        Raises:
            RuntimeError: *binding_id* or *rule_id* does not exist.
            RuleNotPublishedError: *rule_id* is not currently ``approved``.
            MappingIncompleteError: a group's keys don't match the rule's slots.
            UnsafeRowFilterError: *row_filter* contains prohibited SQL.
        """
        rule = _rule or self._validate_applicable_rule(binding_id, rule_id, column_mapping)
        if _rule is None:
            validate_row_filter(row_filter)
        mapping_hash = _mapping_hash if _mapping_hash is not None else compute_mapping_hash(column_mapping)

        # Attach-time-only default_auto_upgrade resolution: this is the
        # INSERT branch (no existing row for this natural key), i.e. a
        # genuinely new application. An update (see _update_mutable_fields
        # above) never goes through this resolution — an explicit
        # pinned_version=None there already means "owner chose to follow
        # latest", not "caller left it unspecified".
        resolved_pinned_version = self._app_settings.resolve_pinned_version_for_new_attachment(
            pinned_version, rule.version
        )

        now = datetime.now(timezone.utc)
        return AppliedRule(
            id=uuid4().hex[:16],
            binding_id=binding_id,
            rule_id=rule_id,
            pinned_version=resolved_pinned_version,
            severity_override=severity_override,
            row_filter=_clean_row_filter(row_filter),
            pass_threshold=_clamp_pass_threshold(pass_threshold),
            column_mapping=column_mapping,
            user_metadata=_without_origin(tags or {}),  # origin markers are only set by auto-attach
            mapping_hash=mapping_hash,
            created_by=user_email,
            created_at=now,
        )

    def attach_policy_mapping(
        self,
        binding_id: str,
        rule_id: str,
        column_mapping: list[ColumnMappingGroup],
        user_email: str,
        policy_id: str,
        *,
        suppression_checked: bool = False,
    ) -> AppliedRule | None:
        """Idempotently attach a mapping produced by a rule policy.

        Add-only and origin-stamped: inserts a new row stamped
        ``user_metadata = {ORIGIN_KEY: ORIGIN_POLICY, POLICY_ID_KEY: policy_id}`` when absent; when a row
        already exists for the natural key ``(binding_id, rule_id, mapping_hash)``
        it is LEFT UNTOUCHED and returned as-is — whether it is a prior auto row
        (idempotent no-op, preserves its pin/severity) or a hand-applied row
        (never clobbered).

        Suppression skip: if the natural key carries a suppression tombstone in
        ``dq_tag_auto_suppressions`` — recorded by :meth:`remove_applied` when a
        owner DELIBERATELY removed an auto-made row — this returns ``None`` and
        inserts nothing. No evaluation may resurrect an auto mapping the user
        removed on purpose.

        This is deliberately NOT :meth:`apply_rule`: the reconcile loop must
        never mutate an existing row. ``apply_rule`` treats an identical
        ``mapping_hash`` as an UPDATE of the mutable fields (pin / severity /
        tags), which would silently destroy an owner's hand-applied pin or
        severity override and re-stamp their row as auto-made — breaking the
        spec's "hand-applied rows are never touched" invariant (§2/§3.3). It
        would also make reconcile non-idempotent for its own rows (a pin set at
        insert time would be reset to ``None`` on the next sweep).

        Args:
            binding_id: The monitored table binding this application belongs to.
            rule_id: The registry rule being attached. Must be ``approved``.
            column_mapping: One fully-covering mapping group (the reconcile loop
                passes a single-element list); every group's keys must exactly
                match the rule's slot names.
            user_email: Attributed as ``created_by`` on a newly inserted row.
            policy_id: The rule policy that produced the mapping.
            suppression_checked: The caller already read this binding's tombstones (in one
                :meth:`list_suppressed_keys` call) and found none for this mapping, so the
                per-mapping tombstone query is skipped. A caller whose tombstone read failed
                must not attach at all (fail closed).

        Returns:
            The existing :class:`AppliedRule` (unchanged) when one already
            matches the natural key, the newly inserted origin-stamped
            :class:`AppliedRule` when the mapping is attached, or ``None`` when
            the natural key is suppressed (deliberate prior removal).

        Raises:
            RuntimeError: *binding_id* or *rule_id* does not exist.
            RuleNotPublishedError: *rule_id* is not currently ``approved``.
            MappingIncompleteError: a provided group's keys don't exactly match
                the rule's slot names.
        """
        return self._attach_origin_mapping(
            binding_id,
            rule_id,
            column_mapping,
            user_email,
            {ORIGIN_KEY: ORIGIN_POLICY, POLICY_ID_KEY: policy_id},
            suppression_checked=suppression_checked,
        )

    def _attach_origin_mapping(
        self,
        binding_id: str,
        rule_id: str,
        column_mapping: list[ColumnMappingGroup],
        user_email: str,
        origin_metadata: dict[str, str],
        *,
        suppression_checked: bool = False,
    ) -> AppliedRule | None:
        rule = self._validate_applicable_rule(binding_id, rule_id, column_mapping)

        mapping_hash = compute_mapping_hash(column_mapping)
        if not suppression_checked and self.is_suppressed(binding_id, rule_id, mapping_hash):
            # An owner deliberately removed this auto mapping; the sweep must
            # not re-add it. See remove_applied's tombstone write.
            logger.info("Skipped auto-attach of rule %s to binding %s: mapping is suppressed", rule_id, binding_id)
            return None
        existing = self._get_by_natural_key(binding_id, rule_id, mapping_hash)
        if existing is not None:
            # Add-only: an existing row (auto or hand-applied) is never mutated.
            return existing

        resolved_pinned_version = self._app_settings.resolve_pinned_version_for_new_attachment(None, rule.version)
        now = datetime.now(timezone.utc)
        applied = AppliedRule(
            id=uuid4().hex[:16],
            binding_id=binding_id,
            rule_id=rule_id,
            pinned_version=resolved_pinned_version,
            severity_override=None,
            column_mapping=column_mapping,
            user_metadata=dict(origin_metadata),
            mapping_hash=mapping_hash,
            created_by=user_email,
            created_at=now,
        )
        self._insert(applied)
        logger.info(
            "Auto-attached registry rule %s to binding %s (applied_rule_id=%s)", rule_id, binding_id, applied.id
        )
        return applied

    def restamp_tag_auto_to_policy(self, rule_id: str, policy_id: str) -> int:
        """Re-stamp *rule_id*'s legacy ``origin=tag_auto`` rows as made by rule policy *policy_id*.

        One-off data migration helper for retiring tag auto-apply: only rows whose
        ``user_metadata`` origin is ``tag_auto`` change (origin becomes ``policy``,
        ``policy_id`` is added, other keys are kept). Hand-applied and policy rows
        are untouched.

        Returns:
            The number of rows re-stamped.
        """
        count = 0
        for applied in self.list_bindings_for_rule(rule_id):
            if applied.user_metadata.get(ORIGIN_KEY) != ORIGIN_TAG_AUTO:
                continue
            metadata = {**applied.user_metadata, ORIGIN_KEY: ORIGIN_POLICY, POLICY_ID_KEY: policy_id}
            metadata_expr = self._sql.json_literal_expr(json.dumps(metadata))
            self._sql.execute(
                f"UPDATE {self._table} SET user_metadata = {metadata_expr} "
                f"WHERE id = '{escape_sql_string(applied.id or '')}'"
            )
            count += 1
        return count

    def _validate_applicable_rule(
        self, binding_id: str, rule_id: str, column_mapping: list[ColumnMappingGroup]
    ) -> RegistryRule:
        """Shared apply/attach validation: binding exists, rule published, mapping covers slots.

        Returns the resolved :class:`RegistryRule` so the caller can read its
        ``version``. Raises the same errors documented on :meth:`apply_rule`.
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
        return rule

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
                    f"column_mapping group {group} does not cover the rule's slots exactly: " + "; ".join(detail_parts)
                )

    def _update_mutable_fields(
        self,
        existing: AppliedRule,
        *,
        pinned_version: int | None,
        severity_override: str | None,
        row_filter: str | None,
        pass_threshold: int | None,
        tags: dict[str, Any] | None,
    ) -> AppliedRule:
        existing.pinned_version = pinned_version
        existing.severity_override = severity_override
        existing.row_filter = _clean_row_filter(row_filter)
        existing.pass_threshold = _clamp_pass_threshold(pass_threshold)
        if tags is not None:
            # origin / policy_id are server-owned: the row keeps its own, whatever the caller sent.
            existing.user_metadata = {**_without_origin(tags), **_origin_of(existing.user_metadata)}
        e_id = escape_sql_string(existing.id or "")
        metadata_expr = self._sql.json_literal_expr(json.dumps(existing.user_metadata))
        sql = (
            f"UPDATE {self._table} SET "
            f"  pinned_version = {existing.pinned_version if existing.pinned_version is not None else 'NULL'}, "
            f"  severity_override = {self._opt_str(existing.severity_override)}, "
            f"  row_filter = {self._opt_str(existing.row_filter)}, "
            f"  pass_threshold = {existing.pass_threshold if existing.pass_threshold is not None else 'NULL'}, "
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
            "(id, binding_id, rule_id, pinned_version, severity_override, row_filter, pass_threshold, "
            "column_mapping, user_metadata, mapping_hash, created_by, created_at) VALUES "
            f"('{escape_sql_string(applied.id or '')}', '{escape_sql_string(applied.binding_id)}', "
            f"'{escape_sql_string(applied.rule_id)}', "
            f"{applied.pinned_version if applied.pinned_version is not None else 'NULL'}, "
            f"{self._opt_str(applied.severity_override)}, {self._opt_str(applied.row_filter)}, "
            f"{applied.pass_threshold if applied.pass_threshold is not None else 'NULL'}, "
            f"{column_mapping_expr}, {metadata_expr}, "
            f"'{escape_sql_string(applied.mapping_hash or '')}', {self._opt_str(applied.created_by)}, now())"
        )
        self._sql.execute(sql)

    def _touch_binding(self, binding_id: str, user_email: str) -> None:
        """Bump the monitored table's ``updated_at`` / ``updated_by`` after an edit.

        Applied-rule writes land in ``dq_applied_rules``, not the binding row,
        so without this an edit would leave ``dq_monitored_tables.updated_at``
        stale — and the B2-118 draft-run gate reads that column as the binding's
        last-change instant. ``now()`` rewrites to each backend's native syntax.
        """
        e = escape_sql_string(binding_id)
        self._sql.execute(
            f"UPDATE {self._monitored_table} SET updated_at = now(), "  # noqa: S608
            f"updated_by = {self._opt_str(user_email)} WHERE binding_id = '{e}'"
        )

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
    # Suppression tombstones (apply-on-tag: deliberate auto-removal record)
    # ------------------------------------------------------------------

    def is_suppressed(self, binding_id: str, rule_id: str, mapping_hash: str) -> bool:
        """Return whether ``(binding_id, rule_id, mapping_hash)`` has a suppression tombstone.

        Fails closed: a read failure propagates, so a caller can never attach a
        mapping the owner removed just because the tombstone was unreadable.
        """
        e_binding = escape_sql_string(binding_id)
        e_rule = escape_sql_string(rule_id)
        e_hash = escape_sql_string(mapping_hash)
        sql = (
            f"SELECT 1 FROM {self._suppressions_table} "
            f"WHERE binding_id = '{e_binding}' AND rule_id = '{e_rule}' AND mapping_hash = '{e_hash}'"
        )
        return bool(self._sql.query(sql))

    def list_suppressed_keys(self, binding_id: str) -> set[tuple[str, str]]:
        """Return every suppressed ``(rule_id, mapping_hash)`` on *binding_id* in one query.

        Fails closed: a read failure propagates (callers must then attach nothing).
        """
        e_binding = escape_sql_string(binding_id)
        sql = f"SELECT rule_id, mapping_hash FROM {self._suppressions_table} " f"WHERE binding_id = '{e_binding}'"
        return {(str(row[0]), str(row[1])) for row in self._sql.query(sql) if row and row[0] and row[1]}

    def _record_suppression(self, binding_id: str, rule_id: str, mapping_hash: str, user_email: str | None) -> None:
        """Upsert a suppression tombstone for ``(binding_id, rule_id, mapping_hash)``.

        Idempotent: re-removing an already-suppressed key just refreshes
        ``suppressed_by`` / ``suppressed_at``.
        """
        self._sql.upsert(
            self._suppressions_table,
            key_cols={"binding_id": binding_id, "rule_id": rule_id, "mapping_hash": mapping_hash},
            value_cols={"suppressed_by": user_email, "suppressed_at": RawSql("current_timestamp()")},
        )
        logger.info("Recorded tag-auto suppression for rule %s on binding %s", rule_id, binding_id)

    # ------------------------------------------------------------------
    # Batch reconcile (staged editor — save/publish in one action)
    # ------------------------------------------------------------------

    def save_applied_rules(
        self,
        binding_id: str,
        desired: list[DesiredAppliedRule],
        user_email: str,
        *,
        known_applied_ids: Collection[str] | None = None,
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
        in *desired* wins), matched per mapping GROUP so a rule applied as
        several rows (one per column, as rule policies attach it) is not
        collapsed into one:

        - An existing row of the rule whose mapping equals the entry's, or whose
          groups are all among the entry's groups, is kept and updated in place
          (pin / severity / filter / threshold / tags) via :meth:`apply_rule`.
        - The entry's groups no kept row covers are inserted as one new row.
        - Any other existing row (rule not desired, or a group no longer
          desired) is removed via :meth:`remove_applied` (which tombstones
          auto-made rows) — but only when it is in *known_applied_ids*.

        *known_applied_ids* is the baseline the caller edited: the applied-row ids it
        had loaded. A row created after that (for example attached by a rule policy
        while the editor was open) is never removed or tombstoned by this save; it is
        kept and returned. ``None`` (internal callers that replace the whole set)
        treats every row as known.

        The ``origin`` / ``policy_id`` markers in *tags* are server-owned: they are
        never copied onto a saved row, and an updated row keeps its own.

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
            validate_row_filter(entry.row_filter)
            desired_hashes[entry.rule_id] = compute_mapping_hash(entry.column_mapping)

        known = None if known_applied_ids is None else set(known_applied_ids)
        keep, unknown, remove = self._plan_reconcile(self.list_applied(binding_id), deduped_entries, known)
        for existing in remove:
            if existing.id is not None:
                self.remove_applied(existing.id, user_email)

        results: list[AppliedRule] = []
        covered: dict[str, set[str]] = {}
        for row in [*keep, *unknown]:
            covered.setdefault(row.rule_id, set()).update(_group_hashes(row.column_mapping))
        for entry in deduped_entries:
            kept = [row for row in keep if row.rule_id == entry.rule_id]
            for row in kept:
                results.append(self._reapply(binding_id, entry, row.column_mapping, user_email))
            done = covered.get(entry.rule_id, set())
            remaining = [g for g in entry.column_mapping if compute_mapping_hash([g]) not in done]
            if remaining or (not kept and not entry.column_mapping):
                results.append(self._reapply(binding_id, entry, _dedupe_groups(remaining), user_email))
        results.extend(unknown)
        # B2-118: stamp the binding's ``updated_at`` so this edit is recorded as
        # the monitored table's most-recent-change instant. The draft-run gate
        # (``DraftRunGateService.enforce``) compares a run's ``created_at``
        # against this to require a FRESH draft run after any rule edit — adds,
        # removals, mapping changes, and in-place pin / severity overrides alike,
        # none of which touch ``dq_monitored_tables`` otherwise.
        self._touch_binding(binding_id, user_email)
        logger.info(
            "Reconciled applied rules for binding %s: %d desired, %d resulting",
            binding_id,
            len(deduped_entries),
            len(results),
        )
        return results

    @staticmethod
    def _plan_reconcile(
        existing: list[AppliedRule], entries: list[DesiredAppliedRule], known: set[str] | None
    ) -> tuple[list[AppliedRule], list[AppliedRule], list[AppliedRule]]:
        """Split *existing* rows into ``(keep, unknown, remove)`` for :meth:`save_applied_rules`."""
        by_rule = {entry.rule_id: entry for entry in entries}
        exact = {entry.rule_id: compute_mapping_hash(entry.column_mapping) for entry in entries}
        groups = {entry.rule_id: set(_group_hashes(entry.column_mapping)) for entry in entries}
        covered: dict[str, set[str]] = {}
        exact_kept: set[str] = set()
        keep: list[AppliedRule] = []
        unknown: list[AppliedRule] = []
        remove: list[AppliedRule] = []
        # Rows whose whole mapping equals the entry's are matched first.
        for row in sorted(existing, key=lambda r: r.mapping_hash != exact.get(r.rule_id)):
            row_groups = set(_group_hashes(row.column_mapping))
            taken = covered.setdefault(row.rule_id, set())
            if row.rule_id in by_rule:
                is_exact = row.mapping_hash == exact[row.rule_id] and row.rule_id not in exact_kept
                is_subset = bool(row_groups) and row_groups <= groups[row.rule_id]
                if (is_exact or is_subset) and not row_groups & taken:
                    keep.append(row)
                    taken.update(row_groups)
                    if is_exact:
                        exact_kept.add(row.rule_id)
                    continue
            if known is not None and row.id not in known:
                unknown.append(row)  # created after the caller loaded the binding: never removed here
                taken.update(row_groups)
                continue
            remove.append(row)
        return keep, unknown, remove

    def _reapply(
        self, binding_id: str, entry: DesiredAppliedRule, column_mapping: list[ColumnMappingGroup], user_email: str
    ) -> AppliedRule:
        return self.apply_rule(
            binding_id,
            entry.rule_id,
            column_mapping,
            user_email,
            pinned_version=entry.pinned_version,
            severity_override=entry.severity_override,
            row_filter=entry.row_filter,
            pass_threshold=entry.pass_threshold,
            tags=entry.tags,
        )

    def rule_display_tags(self, rule_id: str) -> tuple[str | None, str | None, str | None, str | None]:
        """Return the ``(name, dimension, severity, source)`` for *rule_id*.

        Read from the LIVE registry rule — the same source
        ``MonitoredTableService`` joins when it builds the detail view's
        applied-rule summaries. Lets callers (e.g. the ``saveAppliedRules``
        route) return the ENRICHED :class:`AppliedRuleOut` shape without a
        second round-trip through ``MonitoredTableService``. Returns
        ``(None, None, None, None)`` when the rule row is missing so the caller
        degrades to a blank display rather than failing the whole save.
        """
        rule = self._registry.get_rule(rule_id)
        if rule is None:
            return None, None, None, None
        metadata = rule.user_metadata
        return (
            get_rule_name(metadata),
            get_rule_dimension(metadata),
            get_rule_severity(metadata),
            rule.source,
        )

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

    def list_all(self) -> list[AppliedRule]:
        """List every applied rule across all monitored-table bindings."""
        sql = f"SELECT {self._select_cols} FROM {self._table} ORDER BY created_at DESC"  # noqa: S608
        rows = self._sql.query(sql)
        return [self._row_to_applied_rule(row) for row in rows]

    def list_bindings_for_rule(self, rule_id: str) -> list[AppliedRule]:
        """List every application of *rule_id*, across all monitored-table bindings.

        Reverse lookup of :meth:`list_applied` — same row shape, filtered on
        ``rule_id`` instead of ``binding_id``. Used by the rule-level DQ score
        aggregate to fan out to each binding's source table.
        """
        e = escape_sql_string(rule_id)
        sql = (
            f"SELECT {self._select_cols} FROM {self._table} "  # noqa: S608
            f"WHERE rule_id = '{e}' ORDER BY created_at"
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

    def count_bindings_by_policy(self, policy_ids: list[str]) -> dict[str, int]:
        """Count, per rule policy, the distinct bindings holding an applied row stamped with it.

        One grouped query over ``dq_applied_rules`` filtering ``user_metadata.policy_id``.
        Every requested id is present in the result; ids with no applied rows map to 0.
        """
        unique = list(dict.fromkeys(policy_ids))
        counts = {policy_id: 0 for policy_id in unique}
        if not unique:
            return counts
        policy_expr = self._policy_id_expr()
        in_list = ", ".join(f"'{escape_sql_string(policy_id)}'" for policy_id in unique)
        sql = (
            f"SELECT {policy_expr} AS policy_id, COUNT(DISTINCT binding_id) FROM {self._table} "
            f"WHERE {policy_expr} IN ({in_list}) GROUP BY {policy_expr}"
        )
        for row in self._sql.query(sql):
            if row and row[0] in counts:
                counts[row[0]] = int(row[1]) if row[1] is not None else 0
        return counts

    def _policy_id_expr(self) -> str:
        """SQL text of ``user_metadata.policy_id`` on this executor's dialect."""
        if self._sql.dialect == "postgres":
            return f"(user_metadata ->> '{POLICY_ID_KEY}')"
        return f"get_json_object({self._sql.select_json_text('user_metadata')}, '$.{POLICY_ID_KEY}')"

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

    def remove_applied(self, applied_rule_id: str, user_email: str | None = None) -> None:
        """Remove an applied rule and every ``dq_resolved_rules`` row it materialized.

        Suppression tombstone: when the removed row was auto-applied
        (``user_metadata[ORIGIN_KEY]`` is ``policy`` or legacy ``tag_auto``),
        this also records a tombstone in ``dq_tag_auto_suppressions`` for its
        natural key so no later evaluation re-adds it
        (:meth:`attach_policy_mapping` skips suppressed keys). *user_email* is attributed as ``suppressed_by``
        when known. A HAND-applied row (no auto origin) is removed WITHOUT a
        tombstone — for v1 only auto-origin removals are suppressed, so a
        hand-applied removal of a mapping a tag-match would also create is left
        free to be re-suggested.

        Args:
            applied_rule_id: The applied-rule row to remove.
            user_email: The acting remover, recorded as ``suppressed_by`` on a
                tombstone when the removed row was auto-applied.

        Raises:
            RuntimeError: *applied_rule_id* does not exist.
        """
        existing = self.get_applied(applied_rule_id)
        if existing is None:
            raise RuntimeError(f"Applied rule not found: {applied_rule_id}")
        self._sql.delete(self._quality_rules_table, where={"applied_rule_id": applied_rule_id})
        self._sql.delete(self._table, where={"id": applied_rule_id})
        if existing.user_metadata.get(ORIGIN_KEY) in AUTO_ORIGINS and existing.mapping_hash:
            self._record_suppression(existing.binding_id, existing.rule_id, existing.mapping_hash, user_email)
        logger.info(
            "Removed applied rule %s (binding=%s, rule=%s)", applied_rule_id, existing.binding_id, existing.rule_id
        )

    # ------------------------------------------------------------------
    # Pin / severity override
    # ------------------------------------------------------------------

    def set_pin(self, applied_rule_id: str, pinned_version: int | None) -> AppliedRule:
        """Set (or clear, with ``None``) the version pin for an applied rule."""
        existing = self.get_applied(applied_rule_id)
        if existing is None:
            raise RuntimeError(f"Applied rule not found: {applied_rule_id}")
        self._sql.update(
            self._table,
            updates={"pinned_version": pinned_version},
            where={"id": applied_rule_id},
        )
        existing.pinned_version = pinned_version
        logger.info("Set pin for applied rule %s to %s", applied_rule_id, pinned_version)
        return existing

    def set_severity_override(self, applied_rule_id: str, severity: str | None) -> AppliedRule:
        """Set (or clear, with ``None``) the severity override for an applied rule."""
        existing = self.get_applied(applied_rule_id)
        if existing is None:
            raise RuntimeError(f"Applied rule not found: {applied_rule_id}")
        self._sql.update(
            self._table,
            updates={"severity_override": severity},
            where={"id": applied_rule_id},
        )
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
            row_filter=_clean_row_filter(row[10] if len(row) > 10 else None),
            pass_threshold=_clamp_pass_threshold(row[11] if len(row) > 11 else None),
        )
