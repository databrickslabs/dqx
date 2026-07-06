"""Materializer (Phase 3C) — renders applied registry rules into ``dq_quality_rules``.

This is the SAFETY-CRITICAL boundary between the Rules Registry (authoring/
governance layer) and the existing, UNCHANGED runner: every
``dq_quality_rules`` row this module writes must be shaped exactly like a
row a human would have hand-authored through the single-table editor
(``RulesCatalogService``), because the wheel-task runner that executes
checks was not touched by the Rules Registry work and only understands
that shape — see
``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §7 and §9.

Materialization is **publish-gated, not live-synced**: nothing in this
module runs automatically as a side effect of authoring actions on
``dq_applied_rules`` (apply a rule, pin/unpin a version, set a severity
override). A binding's applied rules only get (re-)materialized when:

* a steward submits the monitored table for review
  (``POST /monitored-tables/{binding_id}/submit``, ``submitMonitoredTable``
  -> :meth:`Materializer.materialize_binding`), or
* a registry rule is approved/published and that propagates to its
  FOLLOWING (unpinned) applications (``POST /registry-rules/{rule_id}/approve``
  -> :meth:`Materializer.rematerialize_for_rule`).

Applying, pinning, or overriding a rule only ever writes to
``dq_applied_rules``; it never touches ``dq_quality_rules`` until one of
the two publish-triggered calls above runs.

For each ``dq_applied_rules`` row under a monitored table binding, this
renders ONE ``dq_quality_rules`` row per mapping GROUP in
``column_mapping`` (slots substituted with real columns, non-``None``
parameter values filled in), stamps dimension/severity/polarity/
provenance into ``user_metadata`` (§9 — the runner already aggregates
check ``user_metadata`` into ``dq_metrics.user_metadata``, so this alone
is what makes Runs History/Insights light up, no runner change needed),
and sets ``registry_rule_id``/``registry_version``/``applied_rule_id`` so
the materialized row can always be traced back to its source application.

Idempotency: each materialized row gets a deterministic
``rule_id = f"{applied_rule_id}-{group_index}"`` so re-materializing the
same application upserts in place rather than duplicating. Rows whose
owning application (or a specific mapping group within it) no longer
exists are deleted at the end of :meth:`Materializer.materialize_binding`.

Per-table approval + auto-upgrade (design spec §5): a newly materialized
row always starts at ``draft`` — publishing a registry rule never
auto-approves a table's copy of it. Re-materializing an *existing*,
previously-``approved`` row whose content changed pushes it back to
``pending_approval`` unless the ``auto_upgrade_without_approval`` admin
setting is enabled, in which case a FOLLOWING (``pinned_version IS NULL``)
application's ``approved`` row is silently kept ``approved``. A PINNED
application's content only ever changes because of a direct edit
(``set_severity_override``), which always requires re-review regardless
of the auto-upgrade setting — pins only exempt an application from
*version* upgrades, not from re-approval after an intentional edit.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from databricks.labs.dqx.utils import is_sql_query_safe

from databricks_labs_dqx_app.backend.registry_models import (
    RESERVED_DIMENSION_KEY,
    RESERVED_SEVERITY_KEY,
    AppliedRule,
    ColumnMappingGroup,
    RegistryRule,
    RuleMode,
    RuleParameter,
    RuleSlot,
    RuleVersion,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
    resolve_criticality,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

_DEFAULT_SEVERITY = "Medium"
_SYNTHETIC_TABLE_PREFIX = "__sql_check__"


class MaterializationError(RuntimeError):
    """Raised by :meth:`Materializer.materialize_binding` for an unknown *binding_id*."""


def _substitute_value(value: Any, group: ColumnMappingGroup, slots: list[RuleSlot]) -> Any:
    """Substitute ``{{slot}}`` placeholder(s) inside a single ``body.arguments`` value.

    A native rule's frozen argument value is always either a ``{{slotName}}``
    placeholder string (the KEY stays the DQX function's real parameter
    name, e.g. ``column`` — only the VALUE is templated), a list of such
    strings, or a plain value with no placeholder at all (left unchanged).
    For a ``"many"`` cardinality slot whose placeholder is the *entire*
    string value, the substitution expands into a LIST of columns (mirrors
    how a "many" slot's mapped value is a comma-separated string); a
    ``"one"`` cardinality slot substitutes in place, matching
    :func:`_substitute_text`.
    """
    if isinstance(value, list):
        return [_substitute_value(item, group, slots) for item in value]
    if not isinstance(value, str):
        return value
    for slot in slots:
        placeholder = f"{{{{{slot.name}}}}}"
        if placeholder not in value:
            continue
        if slot.name not in group:
            raise ValueError(f"Mapping group is missing slot '{slot.name}'")
        mapped = group[slot.name]
        if slot.cardinality == "many":
            if value == placeholder:
                return [c.strip() for c in mapped.split(",") if c.strip()]
            replacement = ", ".join(c.strip() for c in mapped.split(",") if c.strip())
        else:
            replacement = mapped
        value = value.replace(placeholder, replacement)
    return value


def _substitute_arguments(
    body_arguments: dict[str, Any],
    group: ColumnMappingGroup,
    slots: list[RuleSlot],
    parameters: list[RuleParameter],
) -> dict[str, Any]:
    """Render a ``dqx_native`` rule's frozen ``arguments`` template against one mapping group.

    The dict KEY is always preserved from *body_arguments* — it is the DQX
    check function's real parameter name (e.g. ``column``) and is
    independent of the author-chosen slot ``name``. Only the VALUE, which
    holds a ``{{slotName}}`` placeholder, is substituted with the mapped
    column(s). Every declared slot must be used inside some argument value
    and mapped in *group* — a slot with no entry in *group* always raises,
    even if (in a malformed definition) its placeholder isn't actually
    referenced anywhere in *body_arguments*.
    """
    arguments = {key: _substitute_value(value, group, slots) for key, value in body_arguments.items()}
    for slot in slots:
        if slot.name not in group:
            raise ValueError(f"Mapping group is missing slot '{slot.name}'")
    for param in parameters:
        if param.value is not None:
            arguments[param.name] = param.value
    return arguments


def _substitute_text(text: str, group: ColumnMappingGroup, slots: list[RuleSlot]) -> str:
    """Replace every ``{{slot}}`` placeholder inside a SQL/lowcode predicate or query string."""
    result = text
    for slot in slots:
        if slot.name not in group:
            raise ValueError(f"Mapping group is missing slot '{slot.name}'")
        value = group[slot.name]
        if slot.cardinality == "many":
            replacement = ", ".join(c.strip() for c in value.split(",") if c.strip())
        else:
            replacement = value
        result = result.replace(f"{{{{{slot.name}}}}}", replacement)
    return result


def render_check(
    *,
    mode: RuleMode,
    version: RuleVersion,
    group: ColumnMappingGroup,
    effective_severity: str,
    per_application_tags: dict[str, Any],
    registry_rule_id: str,
    registry_version: int,
    applied_rule_id: str,
) -> tuple[dict[str, Any], bool]:
    """Render one materialized ``dq_quality_rules.check`` dict for one mapping group.

    Returns ``(check_dict, is_tableless)`` — *is_tableless* is ``True`` only
    for a dataset-level ``sql`` rule with no column slots at all (a genuine
    cross-table aggregate query with nothing to bind to the monitored
    table's own columns), matching the existing
    ``__sql_check__/<name>`` synthetic-FQN convention documented in
    ``backend/CLAUDE.md``.

    Raises:
        ValueError: *mode* is not a supported rule mode, or *group* is
            missing a slot the rule's definition declares.
        UnsafeSqlQueryError: a rendered sql/lowcode predicate or query
            fails :func:`is_sql_query_safe`.
    """
    definition = version.definition
    body = definition.body
    is_tableless = False

    if mode == "dqx_native":
        function = str(body.get("function", ""))
        arguments = _substitute_arguments(
            dict(body.get("arguments", {})), group, definition.slots, definition.parameters
        )
        check_inner: dict[str, Any] = {"function": function, "arguments": arguments}
    elif mode in ("sql", "lowcode"):
        negate = version.polarity == "fail"
        if "sql_query" in body:
            query = _substitute_text(str(body.get("sql_query", "")), group, definition.slots)
            if not is_sql_query_safe(query):
                raise UnsafeSqlQueryError(
                    "The registry rule's SQL query contains prohibited statements and cannot be materialized."
                )
            arguments = {"query": query, "negate": negate}
            for param in definition.parameters:
                if param.value is not None:
                    arguments[param.name] = param.value
            check_inner = {"function": "sql_query", "arguments": arguments}
            is_tableless = not definition.slots
        else:
            expression = _substitute_text(str(body.get("predicate", "")), group, definition.slots)
            if not is_sql_query_safe(expression):
                raise UnsafeSqlQueryError(
                    "The registry rule's SQL predicate contains prohibited statements and cannot be materialized."
                )
            arguments = {"expression": expression, "negate": negate}
            for param in definition.parameters:
                if param.value is not None:
                    arguments[param.name] = param.value
            check_inner = {"function": "sql_expression", "arguments": arguments}
    else:
        raise ValueError(f"Unsupported rule mode: {mode}")

    user_metadata = _build_user_metadata(
        rule_tags=version.user_metadata,
        per_application_tags=per_application_tags,
        effective_severity=effective_severity,
        polarity=version.polarity,
        registry_rule_id=registry_rule_id,
        registry_version=registry_version,
        applied_rule_id=applied_rule_id,
    )

    check_dict: dict[str, Any] = {
        "criticality": resolve_criticality(effective_severity),
        "check": check_inner,
        "user_metadata": user_metadata,
    }
    name = get_rule_name(version.user_metadata)
    if name:
        check_dict["name"] = name
    error_message = definition.error_message
    if error_message:
        check_dict["message_expr"] = error_message
    return check_dict, is_tableless


def _build_user_metadata(
    *,
    rule_tags: dict[str, Any],
    per_application_tags: dict[str, Any],
    effective_severity: str,
    polarity: str | None,
    registry_rule_id: str,
    registry_version: int,
    applied_rule_id: str,
) -> dict[str, str]:
    merged: dict[str, str] = {}
    for k, v in rule_tags.items():
        if isinstance(k, str) and isinstance(v, str):
            merged[k] = v
    for k, v in (per_application_tags or {}).items():
        if isinstance(k, str) and isinstance(v, str):
            merged[k] = v
    dimension = get_rule_dimension(rule_tags)
    if dimension:
        merged[RESERVED_DIMENSION_KEY] = dimension
    merged[RESERVED_SEVERITY_KEY] = effective_severity
    if polarity:
        merged["polarity"] = polarity
    merged["registry_rule_id"] = registry_rule_id
    merged["registry_version"] = str(registry_version)
    merged["applied_rule_id"] = applied_rule_id
    return merged


def _slugify(value: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in value.strip().lower()).strip("_") or "check"


class Materializer:
    """Renders applied registry rules into ``dq_quality_rules`` rows (Phase 3C).

    Reads live application state (``dq_applied_rules`` via
    :class:`MonitoredTableService`) and the registry's frozen publish
    snapshots (``dq_rule_versions`` via :class:`RegistryService`), and
    writes/upserts/cleans-up ``dq_quality_rules`` rows accordingly.
    """

    def __init__(
        self,
        sql: OltpExecutorProtocol,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
        app_settings: AppSettingsService,
    ) -> None:
        self._sql = sql
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._app_settings = app_settings
        self._quality_rules_table = sql.fqn("dq_quality_rules")
        self._check_col = sql.q("check")

    def materialize_binding(self, binding_id: str) -> list[str]:
        """Materialize every applied rule under *binding_id*.

        Returns the sorted list of materialized ``dq_quality_rules.rule_id``
        values written (for diagnostics/tests).

        Raises:
            MaterializationError: *binding_id* does not exist.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise MaterializationError(f"Monitored table not found: {binding_id}")

        auto_upgrade = self._app_settings.get_auto_upgrade_without_approval()
        written_ids: set[str] = set()
        applied_ids: set[str] = set()

        for summary in detail.applied_rules:
            applied = summary.applied_rule
            if not applied.id:
                continue
            applied_ids.add(applied.id)
            expected_ids = self._materialize_applied_rule(detail.table.table_fqn, applied, auto_upgrade)
            written_ids.update(expected_ids)

        self._cleanup_orphans(applied_ids=applied_ids, written_ids=written_ids)
        return sorted(written_ids)

    def _materialize_applied_rule(
        self, table_fqn: str, applied: AppliedRule, auto_upgrade: bool
    ) -> set[str]:
        registry_rule = self._registry.get_rule(applied.rule_id)
        if registry_rule is None or not applied.id:
            logger.warning("Skipping applied rule %s: registry rule %s not found", applied.id, applied.rule_id)
            return set()

        version_number = applied.pinned_version or registry_rule.version
        if version_number <= 0:
            logger.warning("Skipping applied rule %s: rule %s has no published version", applied.id, applied.rule_id)
            return set()

        version_snapshot = self._registry.get_version(applied.rule_id, version_number)
        if version_snapshot is None:
            logger.warning(
                "Skipping applied rule %s: version %d of rule %s not found", applied.id, version_number, applied.rule_id
            )
            return set()

        effective_severity = (
            applied.severity_override
            or get_rule_severity(version_snapshot.user_metadata)
            or _DEFAULT_SEVERITY
        )
        pinned = applied.pinned_version is not None
        expected_ids: set[str] = set()

        for idx, group in enumerate(applied.column_mapping):
            row_id = f"{applied.id}-{idx}"
            try:
                check, is_tableless = render_check(
                    mode=registry_rule.mode,
                    version=version_snapshot,
                    group=group,
                    effective_severity=effective_severity,
                    per_application_tags=applied.user_metadata,
                    registry_rule_id=applied.rule_id,
                    registry_version=version_number,
                    applied_rule_id=applied.id,
                )
            except (ValueError, UnsafeSqlQueryError):
                logger.warning("Failed to render applied rule %s group %d", applied.id, idx, exc_info=True)
                continue
            row_table_fqn = self._resolve_table_fqn(table_fqn, is_tableless, registry_rule, version_snapshot)
            self._upsert_materialized_row(
                row_id=row_id,
                table_fqn=row_table_fqn,
                check=check,
                version_number=version_number,
                applied=applied,
                pinned=pinned,
                auto_upgrade=auto_upgrade,
            )
            expected_ids.add(row_id)

        self._delete_stale_groups(applied.id, expected_ids)
        return expected_ids

    def _resolve_table_fqn(
        self, table_fqn: str, is_tableless: bool, registry_rule: RegistryRule, version: RuleVersion
    ) -> str:
        if not is_tableless:
            return table_fqn
        name = get_rule_name(version.user_metadata) or registry_rule.rule_id
        return f"{_SYNTHETIC_TABLE_PREFIX}/{_slugify(name)}"

    def _upsert_materialized_row(
        self,
        *,
        row_id: str,
        table_fqn: str,
        check: dict[str, Any],
        version_number: int,
        applied: AppliedRule,
        pinned: bool,
        auto_upgrade: bool,
    ) -> None:
        existing = self._get_materialized_row(row_id)
        check_json = json.dumps(check, sort_keys=True)
        check_expr = self._sql.json_literal_expr(json.dumps(check))

        if existing is None:
            self._sql.execute(
                f"INSERT INTO {self._quality_rules_table} "
                f"(rule_id, table_fqn, {self._check_col}, version, status, source, "
                "registry_rule_id, registry_version, applied_rule_id, created_by, created_at, updated_by, updated_at) "
                f"VALUES ('{escape_sql_string(row_id)}', '{escape_sql_string(table_fqn)}', {check_expr}, "
                f"{version_number}, 'draft', 'registry', '{escape_sql_string(applied.rule_id)}', "
                f"{version_number}, '{escape_sql_string(applied.id or '')}', "
                f"{self._opt_str(applied.created_by)}, now(), {self._opt_str(applied.created_by)}, now())"
            )
            return

        existing_status, existing_check_json = existing
        content_changed = existing_check_json != check_json
        new_status = self._decide_status(existing_status, content_changed, pinned, auto_upgrade)
        self._sql.execute(
            f"UPDATE {self._quality_rules_table} SET "
            f"  table_fqn = '{escape_sql_string(table_fqn)}', "
            f"  {self._check_col} = {check_expr}, "
            f"  version = {version_number}, "
            f"  status = '{escape_sql_string(new_status)}', "
            "  source = 'registry', "
            f"  registry_rule_id = '{escape_sql_string(applied.rule_id)}', "
            f"  registry_version = {version_number}, "
            f"  applied_rule_id = '{escape_sql_string(applied.id or '')}', "
            "  updated_at = now() "
            f"WHERE rule_id = '{escape_sql_string(row_id)}'"
        )

    @staticmethod
    def _decide_status(existing_status: str, content_changed: bool, pinned: bool, auto_upgrade: bool) -> str:
        if not content_changed:
            return existing_status
        if existing_status == "approved":
            if pinned:
                return "pending_approval"
            return "approved" if auto_upgrade else "pending_approval"
        if existing_status == "rejected":
            return "draft"
        return existing_status

    def _get_materialized_row(self, row_id: str) -> tuple[str, str] | None:
        check_text = self._sql.select_json_text(self._check_col)
        e = escape_sql_string(row_id)
        sql = f"SELECT status, {check_text} FROM {self._quality_rules_table} WHERE rule_id = '{e}'"  # noqa: S608
        rows = self._sql.query(sql)
        if not rows:
            return None
        status, check_json_raw = rows[0][0], rows[0][1]
        try:
            normalized = json.dumps(json.loads(check_json_raw), sort_keys=True) if check_json_raw else ""
        except json.JSONDecodeError:
            normalized = check_json_raw or ""
        return status, normalized

    def _delete_stale_groups(self, applied_rule_id: str | None, expected_ids: set[str]) -> None:
        if not applied_rule_id:
            return
        e = escape_sql_string(applied_rule_id)
        sql = (
            f"SELECT rule_id FROM {self._quality_rules_table} "  # noqa: S608
            f"WHERE applied_rule_id = '{e}'"
        )
        rows = self._sql.query(sql)
        for row in rows:
            existing_id = row[0]
            if existing_id not in expected_ids:
                e_id = escape_sql_string(existing_id)
                self._sql.execute(f"DELETE FROM {self._quality_rules_table} WHERE rule_id = '{e_id}'")

    def _cleanup_orphans(self, *, applied_ids: set[str], written_ids: set[str]) -> None:
        """Delete materialized rows whose owning applied rule no longer exists under this binding."""
        if not applied_ids:
            return
        placeholders = ", ".join(f"'{escape_sql_string(i)}'" for i in applied_ids)
        sql = (
            f"SELECT rule_id, applied_rule_id FROM {self._quality_rules_table} "  # noqa: S608
            f"WHERE applied_rule_id IS NOT NULL AND applied_rule_id NOT IN ({placeholders})"
        )
        rows = self._sql.query(sql)
        for row in rows:
            existing_id = row[0]
            if existing_id not in written_ids:
                e_id = escape_sql_string(existing_id)
                self._sql.execute(f"DELETE FROM {self._quality_rules_table} WHERE rule_id = '{e_id}'")

    @staticmethod
    def _opt_str(value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"

    def rematerialize_for_rule(self, rule_id: str) -> list[str]:
        """Re-materialize every binding with a FOLLOWING (unpinned) application of *rule_id*.

        Wired as the "publish a registry rule -> propagate to followers"
        entry point (design spec §5): called from
        ``routes/v1/registry_rules.py:approve_registry_rule`` right after
        ``RegistryService.approve()`` — kept as a separate explicit call
        (injected via its own ``Depends(get_materializer)``) rather than an
        automatic side effect of ``RegistryService.approve()`` itself, so
        ``RegistryService`` doesn't need a circular dependency on
        ``MonitoredTableService``/``Materializer``.

        Returns:
            The sorted list of ``binding_id``s that were re-materialized.
        """
        e_rule = escape_sql_string(rule_id)
        applied_table = self._sql.fqn("dq_applied_rules")
        sql = (
            f"SELECT DISTINCT binding_id FROM {applied_table} "  # noqa: S608
            f"WHERE rule_id = '{e_rule}' AND pinned_version IS NULL"
        )
        rows = self._sql.query(sql)
        binding_ids = sorted({row[0] for row in rows if row and row[0]})
        for binding_id in binding_ids:
            try:
                self.materialize_binding(binding_id)
            except MaterializationError:
                logger.warning("Skipping re-materialization for missing binding %s", binding_id, exc_info=True)
        return binding_ids
