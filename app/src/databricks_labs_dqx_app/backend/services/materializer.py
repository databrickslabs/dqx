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
from collections.abc import Mapping
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
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, strip_sql_line_comments

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
    app_settings: AppSettingsService,
    row_filter: str | None = None,
    pass_threshold: int | None = None,
) -> tuple[dict[str, Any], bool]:
    """Render one materialized ``dq_quality_rules.check`` dict for one mapping group.

    *app_settings* is only read to resolve the rendered ``criticality``: the
    severity -> criticality mapping is admin-editable via the reserved
    ``severity`` label definition (see ``registry_models.resolve_criticality``).

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
        # A native check that accepts a ``negate`` argument surfaces it in the
        # authoring UI as the PASS/FAIL polarity switcher rather than a raw
        # boolean parameter (item 11). Polarity is therefore the single source
        # of truth for negation: a non-null ``polarity`` on a native rule means
        # "this function supports negate" and we inject the boolean here.
        # ``polarity is None`` for native checks WITHOUT a ``negate`` argument,
        # so this leaves those untouched — no spurious ``negate`` key.
        if version.polarity is not None:
            arguments["negate"] = version.polarity == "fail"
        check_inner: dict[str, Any] = {"function": function, "arguments": arguments}
    elif mode in ("sql", "lowcode"):
        negate = version.polarity == "fail"
        if "sql_query" in body:
            query = _substitute_text(str(body.get("sql_query", "")), group, definition.slots)
            # Comments (e.g. a leading `-- explanation` block, item 6) are inert
            # at runtime; strip them before the keyword scan so their prose can't
            # trip it. The stored `query` keeps its comments for round-trip.
            if not is_sql_query_safe(strip_sql_line_comments(query)):
                raise UnsafeSqlQueryError(
                    "The registry rule's SQL query contains prohibited statements and cannot be materialized."
                )
            arguments = {"query": query, "negate": negate}
            # A low-code advanced (group-by) rule folds its group-by columns
            # into ``body.merge_columns`` so the ``sql_query`` check joins the
            # per-group violation result back onto the source rows (row-level
            # semantics). Each entry is a ``{{slot}}`` reference substituted
            # against the mapping group exactly like the query itself.
            merge_columns = body.get("merge_columns")
            if isinstance(merge_columns, list) and merge_columns:
                arguments["merge_columns"] = [
                    _substitute_text(str(col), group, definition.slots) for col in merge_columns
                ]
            for param in definition.parameters:
                if param.value is not None:
                    arguments[param.name] = param.value
            check_inner = {"function": "sql_query", "arguments": arguments}
            is_tableless = not definition.slots
        else:
            expression = _substitute_text(str(body.get("predicate", "")), group, definition.slots)
            # Comments (e.g. a leading `-- explanation` block, item 6) are inert
            # at runtime — Spark's F.expr lexer skips `--` lines — so strip them
            # before the keyword scan. The stored `expression` keeps its comments
            # so the explanation survives to the applied rule and Spark ignores it.
            if not is_sql_query_safe(strip_sql_line_comments(expression)):
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

    # Per-applied-rule ``pass_threshold`` is carried on the check's user_metadata
    # (stored/surfaced now; run-time enforcement wired later — store_display).
    if pass_threshold is not None:
        user_metadata = {**user_metadata, "pass_threshold": str(pass_threshold)}

    check_dict: dict[str, Any] = {
        "criticality": resolve_criticality(effective_severity, app_settings),
        "check": check_inner,
        "user_metadata": user_metadata,
    }
    name = get_rule_name(version.user_metadata)
    if name:
        check_dict["name"] = name
    error_message = definition.error_message
    if error_message:
        check_dict["message_expr"] = error_message
    # Per-applied-rule ``row_filter`` scopes which rows THIS check validates —
    # rendered straight into DQX's native per-check ``filter`` (a SQL WHERE
    # predicate). Blank/None = validate every row. Safety was validated before
    # the filter was persisted (ApplyRulesService.validate_row_filter).
    row_filter_clean = row_filter.strip() if row_filter and row_filter.strip() else None
    if row_filter_clean:
        check_dict["filter"] = row_filter_clean
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

    def _resolve_registry(
        self,
        applied: AppliedRule,
        *,
        rules: Mapping[str, RegistryRule] | None = None,
        versions: Mapping[tuple[str, int], RuleVersion] | None = None,
    ) -> tuple[RegistryRule, int, RuleVersion] | None:
        """Resolve an applied rule to its ``(registry_rule, version_number, snapshot)``.

        This is the ONLY place the ``get_rule`` + ``get_version`` OLTP lookups
        happen for a render. When *rules* / *versions* preloaded maps are
        supplied (batch path), they are consulted instead of the per-id
        ``RegistryService`` calls — so a batch caller pays two grouped queries
        for N applications rather than ``2N`` sequential round-trips. The
        resolution logic (missing rule, unpublished version, missing snapshot)
        is identical either way, so single- and batch-path renders stay
        byte-identical.

        Returns ``None`` (with the same warning logs as before) when the applied
        rule can't be resolved at all.
        """
        registry_rule = rules.get(applied.rule_id) if rules is not None else self._registry.get_rule(applied.rule_id)
        if registry_rule is None or not applied.id:
            logger.warning("Skipping applied rule %s: registry rule %s not found", applied.id, applied.rule_id)
            return None

        version_number = applied.pinned_version or registry_rule.version
        if version_number <= 0:
            logger.warning("Skipping applied rule %s: rule %s has no published version", applied.id, applied.rule_id)
            return None

        if versions is not None:
            version_snapshot = versions.get((applied.rule_id, version_number))
        else:
            version_snapshot = self._registry.get_version(applied.rule_id, version_number)
        if version_snapshot is None:
            logger.warning(
                "Skipping applied rule %s: version %d of rule %s not found", applied.id, version_number, applied.rule_id
            )
            return None
        return registry_rule, version_number, version_snapshot

    def _iter_rendered_checks(
        self,
        table_fqn: str,
        applied: AppliedRule,
        *,
        rules: Mapping[str, RegistryRule] | None = None,
        versions: Mapping[tuple[str, int], RuleVersion] | None = None,
    ) -> list[tuple[str, str, dict[str, Any]]] | None:
        """Resolve + render an applied rule's mapping groups WITHOUT writing anything.

        The single shared rendering path used by both
        :meth:`_materialize_applied_rule` (which then upserts the rows) and
        :meth:`render_binding_checks` (which only collects the check dicts) —
        so the draft-run render is byte-identical to what materialization
        would persist.

        Returns ``None`` when the applied rule can't be resolved at all
        (missing registry rule / no ``applied.id`` / unpublished version /
        missing version snapshot) — the caller must then leave any existing
        materialized rows untouched, matching the pre-refactor behaviour where
        these early returns skipped ``_delete_stale_groups``. Otherwise returns
        the ``(row_id, row_table_fqn, check_dict)`` tuples in mapping-group
        order — possibly EMPTY when every group failed to render, which the
        materializer treats as "this application now renders no rows" and
        cleans up accordingly.

        Registry resolution (the ``get_rule`` + ``get_version`` round-trips) is
        delegated to :meth:`_resolve_registry`; passing a preloaded
        *rules*/*versions* map lets a batch caller resolve every application's
        registry rows in two grouped queries and share them here without any
        per-application round-trip (see :meth:`render_binding_checks_many`).
        """
        resolved = self._resolve_registry(applied, rules=rules, versions=versions)
        if resolved is None:
            return None
        registry_rule, version_number, version_snapshot = resolved
        # ``_resolve_registry`` already rejected a falsy ``applied.id`` (returning
        # None), so it is a real str here — narrow it for the type checker.
        applied_id = applied.id or ""
        effective_severity = (
            applied.severity_override or get_rule_severity(version_snapshot.user_metadata) or _DEFAULT_SEVERITY
        )
        # Render with the mode FROZEN into the version snapshot, not the live
        # rule's mode: an approved rule is editable in place (its mode can be
        # switched, e.g. native -> sql, before the revision is re-approved as
        # vN+1), so a follower still serving vN must render vN's frozen mode or
        # the snapshot's body would be interpreted under the wrong mode. Legacy
        # snapshots written before mode was frozen carry ``None`` — fall back to
        # the live rule's mode for those.
        rendered_mode = version_snapshot.mode or registry_rule.mode
        rendered: list[tuple[str, str, dict[str, Any]]] = []
        for idx, group in enumerate(applied.column_mapping):
            row_id = f"{applied_id}-{idx}"
            try:
                check, is_tableless = render_check(
                    mode=rendered_mode,
                    version=version_snapshot,
                    group=group,
                    effective_severity=effective_severity,
                    per_application_tags=applied.user_metadata,
                    registry_rule_id=applied.rule_id,
                    registry_version=version_number,
                    applied_rule_id=applied_id,
                    app_settings=self._app_settings,
                    row_filter=applied.row_filter,
                    pass_threshold=applied.pass_threshold,
                )
            except (ValueError, UnsafeSqlQueryError):
                logger.warning("Failed to render applied rule %s group %d", applied.id, idx, exc_info=True)
                continue
            row_table_fqn = self._resolve_table_fqn(table_fqn, is_tableless, registry_rule, version_snapshot)
            rendered.append((row_id, row_table_fqn, check))
        return rendered

    def _materialize_applied_rule(self, table_fqn: str, applied: AppliedRule, auto_upgrade: bool) -> set[str]:
        rendered = self._iter_rendered_checks(table_fqn, applied)
        if rendered is None:
            return set()

        # DATA-LOSS GUARD: an EMPTY render is only a legitimate "this
        # application materializes no rows" signal when the application had no
        # mapping groups to begin with. When it DID have groups but every one
        # failed to render (e.g. an auto-upgraded rule's new version no longer
        # exposes the slots this follower's stored column_mapping binds), an
        # empty result must NOT be treated as delete-all: doing so would
        # ``_delete_stale_groups(expected=∅)`` and wipe every approved
        # ``dq_quality_rules`` row for this application, then the downstream
        # re-freeze would empty the frozen snapshot and leave the binding
        # unrunnable. Instead leave the existing rows untouched (mirroring the
        # ``rendered is None`` early-return) so the previous approved checks
        # keep serving; the mismatch surfaces for re-review rather than as
        # silent loss.
        if not rendered and applied.column_mapping:
            existing_ids = self._existing_group_ids(applied.id)
            logger.warning(
                "All %d mapping group(s) failed to render for applied rule %s (rule %s); "
                "keeping %d existing materialized row(s) intact for re-review",
                len(applied.column_mapping),
                applied.id,
                applied.rule_id,
                len(existing_ids),
            )
            return existing_ids

        pinned = applied.pinned_version is not None
        expected_ids: set[str] = set()
        for row_id, row_table_fqn, check in rendered:
            # Every rendered row of one applied rule shares the resolved
            # version; recover it from the provenance stamp render_check writes
            # so the upsert keeps its original numeric ``version`` column.
            version_number = int(check["user_metadata"]["registry_version"])
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

        existing_status, existing_registry_version, existing_check_json = existing
        content_changed = existing_check_json != check_json
        # A DIRECT edit (severity override, unpin, or any change that leaves the
        # resolved registry version untouched) must always return an approved
        # row to review — only a genuine VERSION move of an unpinned follower is
        # eligible for the auto-upgrade "keep approved" shortcut. Legacy rows
        # with no recorded registry_version can't be proven to be a version
        # move, so they take the safe (re-review) branch.
        version_changed = existing_registry_version is not None and existing_registry_version != version_number
        new_status = self._decide_status(existing_status, content_changed, pinned, auto_upgrade, version_changed)
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
    def _decide_status(
        existing_status: str,
        content_changed: bool,
        pinned: bool,
        auto_upgrade: bool,
        version_changed: bool,
    ) -> str:
        if not content_changed:
            return existing_status
        if existing_status == "approved":
            # An approved row whose content changed falls into three cases:
            #   * Same resolved version (version_changed=False) — e.g. a
            #     severity override — always returns to review.
            #   * A pinned row always returns to review: a pin's content only
            #     changes through a deliberate edit (severity override, or a pin
            #     bump to a different version), which is exactly what re-approval
            #     exists to gate.
            #   * A genuine VERSION move of an unpinned follower is eligible to
            #     stay approved, but only when the admin enabled auto-upgrade;
            #     otherwise it returns to review. Unpinning FROM a stale pinned
            #     version also moves the resolved version, so it lands in this
            #     branch and is (correctly) treated as a version move rather
            #     than a same-version edit.
            if pinned or not version_changed:
                return "pending_approval"
            return "approved" if auto_upgrade else "pending_approval"
        if existing_status == "rejected":
            return "draft"
        return existing_status

    def _get_materialized_row(self, row_id: str) -> tuple[str, int | None, str] | None:
        check_text = self._sql.select_json_text(self._check_col)
        e = escape_sql_string(row_id)
        sql = (
            f"SELECT status, registry_version, {check_text} "  # noqa: S608
            f"FROM {self._quality_rules_table} WHERE rule_id = '{e}'"
        )
        rows = self._sql.query(sql)
        if not rows:
            return None
        status, registry_version_raw, check_json_raw = rows[0][0], rows[0][1], rows[0][2]
        try:
            normalized = json.dumps(json.loads(check_json_raw), sort_keys=True) if check_json_raw else ""
        except json.JSONDecodeError:
            normalized = check_json_raw or ""
        registry_version = int(registry_version_raw) if registry_version_raw not in (None, "") else None
        return status, registry_version, normalized

    def _existing_group_ids(self, applied_rule_id: str | None) -> set[str]:
        """Return the ``dq_quality_rules.rule_id``s currently materialized for *applied_rule_id*.

        Used by :meth:`_materialize_applied_rule` to preserve (and keep
        counting as "expected") the rows of an application whose every mapping
        group failed to render against the resolved version — so orphan
        cleanup never treats them as stale.
        """
        if not applied_rule_id:
            return set()
        e = escape_sql_string(applied_rule_id)
        rows = self._sql.query(
            f"SELECT rule_id FROM {self._quality_rules_table} WHERE applied_rule_id = '{e}'"  # noqa: S608
        )
        return {row[0] for row in rows if row and row[0]}

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

    def render_binding_checks(self, binding_id: str) -> list[dict[str, Any]]:
        """Render the binding's CURRENT persisted applied-rules state to check dicts.

        Read-only draft-run source (design spec §4.1, ``source == draft``):
        renders every applied rule under *binding_id* through the SAME
        :meth:`_iter_rendered_checks` path materialization uses, but writes
        NOTHING to ``dq_quality_rules``. The returned list is exactly the
        shape the runner consumes (same as
        ``RulesCatalogService.get_approved_checks_for_table`` output), so a
        draft run of a monitored table executes its live authored state
        without waiting for approval/materialization.

        Reflects the PERSISTED applied-rule state only — staged-but-unsaved
        editor edits are not included (the UI must save first). For an
        approved binding with no pending edits this equals the frozen
        snapshot / materialized output.

        Raises:
            MaterializationError: *binding_id* does not exist.
        """
        detail = self._monitored_tables.get(binding_id)
        if detail is None:
            raise MaterializationError(f"Monitored table not found: {binding_id}")

        checks: list[dict[str, Any]] = []
        for summary in detail.applied_rules:
            applied = summary.applied_rule
            if not applied.id:
                continue
            rendered = self._iter_rendered_checks(detail.table.table_fqn, applied)
            if rendered is None:
                continue
            checks.extend(check for _row_id, _row_fqn, check in rendered)
        return checks

    def render_applied_checks(self, table_fqn: str, applied_rules: list[AppliedRule]) -> list[dict[str, Any]]:
        """Render a GIVEN list of applied-rule references to runner-shaped check dicts.

        The reference-resolution counterpart of :meth:`render_binding_checks`:
        instead of reading a binding's LIVE ``dq_applied_rules`` state, it
        renders the exact *applied_rules* handed in — each expected to pin an
        explicit registry version (``pinned_version`` set to the version that
        was resolved when the reference was frozen). Resolving every reference
        against the IMMUTABLE ``dq_rule_versions`` snapshot reproduces the same
        check dicts materialization persisted, so a frozen monitored-table
        version can store lightweight references
        (:class:`~.monitored_table_versions.MonitoredTableVersionService`)
        rather than a full copy of the rendered rule set, and reconstruct the
        runner payload on demand.

        The only value that is NOT reproduced from immutable state is the
        rendered ``criticality`` — :func:`render_check` resolves it through the
        admin-editable severity->criticality mapping (``app_settings``), so a
        later change to that global policy is reflected on the next resolve.
        The frozen ``severity`` tag itself (from the pinned version snapshot,
        plus any per-application ``severity_override``) is fully reproducible.

        Registry rows for every reference are resolved in two grouped queries
        (``get_rules_many`` + ``get_versions_many``) and shared across the
        per-reference renders — no per-reference round-trip.
        """
        resolvable = [a for a in applied_rules if a.id]
        if not resolvable:
            return []
        rules = self._registry.get_rules_many({a.rule_id for a in resolvable})
        version_pairs: set[tuple[str, int]] = set()
        for applied in resolvable:
            registry_rule = rules.get(applied.rule_id)
            version_number = applied.pinned_version or (registry_rule.version if registry_rule else 0)
            if version_number > 0:
                version_pairs.add((applied.rule_id, version_number))
        versions = self._registry.get_versions_many(version_pairs)

        checks: list[dict[str, Any]] = []
        for applied in resolvable:
            rendered = self._iter_rendered_checks(table_fqn, applied, rules=rules, versions=versions)
            if rendered is None:
                continue
            checks.extend(check for _row_id, _row_fqn, check in rendered)
        return checks

    def render_binding_checks_counts_many(self, bindings: list[tuple[str, str]]) -> dict[str, int]:
        """Batched draft-render CHECK COUNT for many *(binding_id, table_fqn)* pairs.

        The bounded-query counterpart of calling :meth:`render_binding_checks`
        in a loop: it produces the SAME per-binding count (the number of checks
        a draft run would render) but resolves the registry rows for EVERY
        binding's applications in a constant handful of grouped queries instead
        of ``~3N + 2·ΣR`` sequential round-trips (N bindings, R applied rules
        each):

        1. ONE grouped ``dq_applied_rules`` query for all bindings' applications
           (:meth:`MonitoredTableService.list_applied_rules_many`);
        2. ONE ``dq_rules`` ``IN (...)`` query for the distinct rule ids
           (:meth:`RegistryService.get_rules_many`);
        3. ONE ``dq_rule_versions`` predicate-OR query for the distinct
           ``(rule_id, version)`` pairs (:meth:`RegistryService.get_versions_many`).

        Rendering then runs purely in-memory per binding through the SAME
        :meth:`_iter_rendered_checks` path — so each count is byte-identical to
        what the per-binding :meth:`render_binding_checks` would return. A
        binding whose applications all fail to resolve counts 0. Bindings not
        present in *bindings* are absent from the result; an empty input issues
        no query and returns ``{{}}``.
        """
        if not bindings:
            return {}
        binding_fqns = {binding_id: table_fqn for binding_id, table_fqn in bindings}
        applied_by_binding = self._monitored_tables.list_applied_rules_many(list(binding_fqns))

        rule_ids: set[str] = set()
        for applied_rules in applied_by_binding.values():
            for applied in applied_rules:
                if applied.id:
                    rule_ids.add(applied.rule_id)
        rules = self._registry.get_rules_many(rule_ids)

        version_pairs: set[tuple[str, int]] = set()
        for applied_rules in applied_by_binding.values():
            for applied in applied_rules:
                if not applied.id:
                    continue
                registry_rule = rules.get(applied.rule_id)
                if registry_rule is None:
                    continue
                version_number = applied.pinned_version or registry_rule.version
                if version_number > 0:
                    version_pairs.add((applied.rule_id, version_number))
        versions = self._registry.get_versions_many(version_pairs)

        counts: dict[str, int] = {}
        for binding_id, table_fqn in binding_fqns.items():
            count = 0
            for applied in applied_by_binding.get(binding_id, []):
                if not applied.id:
                    continue
                rendered = self._iter_rendered_checks(table_fqn, applied, rules=rules, versions=versions)
                if rendered is None:
                    continue
                count += len(rendered)
            counts[binding_id] = count
        return counts

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
