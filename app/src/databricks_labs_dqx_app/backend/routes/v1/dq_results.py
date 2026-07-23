"""DQ results query API — dqlake response shapes over the score views.

Serves the breakdowns/trends/runs the ported dqlake results UI consumes
(see ``routes/dq_results.py`` in dqlake; shapes recorded in the Phase 2
port manifest). Aggregate axes are computed from ``v_dq_check_results``
(one row per run x table x check), whose rows already carry the
AS-OF-THE-RUN attribution (severity tag, quality dimension, mapped
columns, registry rule id) baked in from the run's frozen
``dq_validation_runs.checks_json`` rendered rule set — no live join to
the binding's current applied-rule metadata anywhere on these paths, so
editing or renaming a tag today never rewrites historical results. See
``services.dq_results_service`` for the aggregation semantics and
``services.score_view_service`` for the attribution DDL.

Permission model (unchanged from Phase 1):

- Aggregates are catalog-gated via *get_user_catalog_names*: single-table
  endpoints 403 on an inaccessible catalog (dq_score convention); the
  multi-table endpoints (global/product/rule) silently FILTER inaccessible
  tables, never 403.
- The filtered failed-rows endpoint returns actual row values, so it runs
  the Task 7 security gates in the load-bearing order enforced by
  ``services/quarantine_sample_service.py``: FQN validation (400) -> live OBO SELECT
  self-check as the caller (empty 200 on denial, never 403/404) ->
  fine-grained-control suppression -> SP fetch last.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_apply_rules_service,
    get_conf,
    get_data_product_service,
    get_entitlement_service,
    get_monitored_table_service,
    get_obo_ws,
    get_preview_sql_executor,
    get_registry_service,
    get_run_set_service,
    get_score_cache_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.metrics_utils import catalog_of, safe_float, safe_int
from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    get_applied_column_pass_thresholds,
    get_rule_pass_threshold,
    resolve_pass_threshold,
)
from databricks_labs_dqx_app.backend.models import (
    DimensionOut,
    EntityResultsOut,
    FailedRowOut,
    FailedRowsOut,
    RefreshScoresIn,
    RefreshScoresOut,
    RunRowOut,
    RunsOut,
    SeverityOut,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.dq_results_service import (
    CheckResultRow,
    ResultFacets,
    ThresholdResolver,
    annotate_trend_versions,
    breach_criticality_by_run,
    compute_entity_results,
    parse_check_rows,
)
from databricks_labs_dqx_app.backend.services.entitlement_service import EntitlementService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
from databricks_labs_dqx_app.backend.services.quarantine_sample_service import (
    QuarantineSampleService,
    enrich_failures,
    parse_failures,
    to_failing_record,
)
from databricks_labs_dqx_app.backend.services.score_view_service import (
    ASOF_VIEW_NAME,
    RUN_MODE_PUBLISHED,
    SHAPING_VIEW_NAME,
    metric_view_fqn,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, quote_object_fqn, validate_fqn

logger = logging.getLogger(__name__)
router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]

_RUNS_LIMIT = 50
# Upper bound on failing-row scans (matches the failed-rows endpoint's
# ``limit`` ceiling and the UI download cap). Bounds both the preview
# window and the true-filtered-count pass so neither can pull an unbounded
# result set on a pathologically large run.
_FAILED_ROWS_MAX = 100000
# Fallback swatch for label values missing a configured colour (matches
# the UI's muted gray).
_DEFAULT_LABEL_COLOR = "#6B7280"

# Conservative allowlist for the user-supplied run_id filter. Observer run
# ids are uuid4 strings (metrics_observer.DQMetricsObserver — hex plus
# hyphens); the slightly wider charset tolerates prefixed/timestamped
# overrides without admitting quotes, backslashes, whitespace, or control
# characters. This validation is LOAD-BEARING: *escape_sql_string*
# deliberately does not escape backslashes (it relies on upstream
# validation, normally *validate_fqn* — which run_id never passes
# through), so run_id must be charset-validated before it is interpolated
# into any SQL string literal.
_RUN_ID_SAFE = re.compile(r"^[A-Za-z0-9_\-.:]+$")


def _validate_run_id(run_id: str | None) -> None:
    """Reject a run_id unsafe to embed in a SQL string literal (400)."""
    if run_id is not None and not _RUN_ID_SAFE.fullmatch(run_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid run_id: only letters, digits, '_', '-', '.' and ':' are allowed",
        )


# ---------------------------------------------------------------------------
# Shared query / attribution helpers
# ---------------------------------------------------------------------------


def _app_object_fqn(app_conf: AppConfig, name: str) -> str:
    """Backtick-quoted FQN of a main-schema object (*name* is a trusted constant).

    Use this for base tables only (``dq_metrics``, ``dq_quarantine_records``, …).
    For the seven derived Genie objects that live in the genie schema, use
    :func:`_genie_object_fqn` instead.
    """
    return quote_object_fqn(app_conf.catalog, app_conf.schema_name, name)


def _genie_object_fqn(app_conf: AppConfig, name: str) -> str:
    """Backtick-quoted FQN of a genie-schema derived object (*name* is a trusted constant)."""
    return quote_object_fqn(app_conf.catalog, app_conf.genie_schema_name, name)


def _shaping_view_fqn(app_conf: AppConfig) -> str:
    return _genie_object_fqn(app_conf, SHAPING_VIEW_NAME)


def _is_valid_fqn(table_fqn: str, source: str) -> bool:
    """Defense-in-depth re-validation of an app-DB-sourced table FQN.

    Binding/member FQNs were validated on write, but they round-trip
    through the app database before being interpolated into SQL string
    literals here — and *escape_sql_string* deliberately relies on
    *validate_fqn* having rejected backslashes. Re-validate at the read
    boundary and skip (never 500) anything that no longer passes.
    """
    try:
        validate_fqn(table_fqn)
    except ValueError:
        logger.warning(f"Skipping invalid table FQN from {source}")
        return False
    return True


def _in_list(values: list[str]) -> str:
    return ", ".join(f"'{escape_sql_string(v)}'" for v in values)


def _fetch_check_rows(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_fqns: list[str] | None,
    run_id: str | None = None,
    include_drafts: bool = False,
) -> list[CheckResultRow]:
    """Read per-check result rows from ``v_dq_check_results``.

    *table_fqns* None means "every table" (the global endpoint filters
    by catalog app-side afterwards); an empty list short-circuits.
    Draft runs are excluded unless *include_drafts* — the view's
    ``run_mode`` column already resolves the stamped run-level tag
    (untagged legacy runs classify as published).
    """
    if table_fqns is not None and not table_fqns:
        return []
    view = _shaping_view_fqn(app_conf)
    conds: list[str] = []
    if table_fqns is not None:
        conds.append(f"input_location IN ({_in_list(table_fqns)})")
    if run_id:
        conds.append(f"run_id = '{escape_sql_string(run_id)}'")
    if not include_drafts:
        conds.append(f"run_mode = '{RUN_MODE_PUBLISHED}'")
    where = f"WHERE {' AND '.join(conds)} " if conds else ""
    stmt = (
        f"SELECT input_location, run_id, CAST(run_time AS STRING) AS run_date, "
        f"check_name, error_count, warning_count, input_row_count, run_mode, "
        # As-of-run attribution baked into the view rows (frozen
        # checks_json payload — see score_view_service).
        f"severity, dimension, criticality, registry_rule_id, rule_name, pass_threshold, "
        f"to_json(columns) AS columns_json "
        f"FROM {view} "  # noqa: S608
        f"{where}"
        f"ORDER BY run_time"
    )
    return parse_check_rows(sql.query_dicts(stmt))


def _fetch_asof_check_rows(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_fqns: list[str] | None,
    run_id: str | None = None,
    include_drafts: bool = False,
) -> list[CheckResultRow]:
    """Read the scope's slice of the AS-OF expansion ``v_dq_check_results_asof``.

    The view pre-computes the carry-forward consolidation (at every run
    instant, each table's latest run at-or-before it — see
    ``score_view_service.asof_view_ddl``), so this is a plain filter:
    the *include_drafts* partition selector (the FALSE partition is
    built over published runs only; TRUE over all runs — exactly one is
    ever read), the per-table-list filter, and optionally a pinned
    *run_id* (restricting the expansion to instants where that run is
    the carried one). ``run_date`` is aliased to the expansion's
    ``as_of_time`` so ``parse_check_rows`` yields rows keyed by the
    consolidated instant. The instant set is further restricted to the
    scope's own run instants app-side (``compute_entity_results``) —
    the view is table-agnostic, so its instants span every table.
    """
    if table_fqns is not None and not table_fqns:
        return []
    view = _genie_object_fqn(app_conf, ASOF_VIEW_NAME)
    conds = [f"include_drafts = {'true' if include_drafts else 'false'}"]
    if table_fqns is not None:
        conds.append(f"input_location IN ({_in_list(table_fqns)})")
    if run_id:
        conds.append(f"run_id = '{escape_sql_string(run_id)}'")
    stmt = (
        f"SELECT input_location, run_id, CAST(as_of_time AS STRING) AS run_date, "
        f"check_name, error_count, warning_count, input_row_count, run_mode, "
        f"severity, dimension, criticality, registry_rule_id, rule_name, pass_threshold, "
        f"to_json(columns) AS columns_json "
        f"FROM {view} "  # noqa: S608
        f"WHERE {' AND '.join(conds)} "
        f"ORDER BY as_of_time"
    )
    return parse_check_rows(sql.query_dicts(stmt))


def _wants_trends(axes: str) -> bool:
    """Whether the *axes* selection computes the over-time series (anything
    but the explicit ``"breakdown"`` slice — unknown values select all,
    mirroring ``compute_entity_results``)."""
    return axes != "breakdown"


def _fetch_failed_records_by_run(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_fqns: list[str] | None,
) -> dict[tuple[str, str], int | None]:
    """Distinct failing-row count per (table, run) from ``dq_metrics``.

    The observer emits table-wide ``input_row_count`` and
    ``valid_row_count`` per run; their difference is the number of rows
    carrying any error or warning — the analogue of dqlake's persisted
    ``failed_records``. None when either metric is missing/unparseable.

    Deliberately NOT run_mode-filtered: this is a lookup map consulted
    only for the (table, run) keys present in the already-filtered check
    rows (``_trend_failures``), so draft-run entries are simply never
    read when the caller excluded drafts.
    """
    if table_fqns is not None and not table_fqns:
        return {}
    metrics_table = _app_object_fqn(app_conf, "dq_metrics")
    where = f"WHERE input_location IN ({_in_list(table_fqns)}) " if table_fqns is not None else ""
    stmt = (
        f"SELECT input_location, run_id, "
        f"MAX(CASE WHEN metric_name = 'input_row_count' THEN metric_value END) AS input_rows, "
        f"MAX(CASE WHEN metric_name = 'valid_row_count' THEN metric_value END) AS valid_rows "
        f"FROM {metrics_table} "  # noqa: S608
        f"{where}"
        f"GROUP BY input_location, run_id"
    )
    out: dict[tuple[str, str], int | None] = {}
    for row in sql.query_dicts(stmt):
        fqn, run_id = row.get("input_location"), row.get("run_id")
        if not fqn or not run_id:
            continue
        input_rows = safe_int(row.get("input_rows"))
        valid_rows = safe_int(row.get("valid_rows"))
        failed = input_rows - valid_rows if input_rows is not None and valid_rows is not None else None
        out[(fqn, run_id)] = failed if failed is None or failed >= 0 else None
    return out


def _count_filtered_failed_rows(
    sql: SqlExecutor,
    quarantine_table: str,
    escaped_fqn: str,
    run_cond: str,
    facets: ResultFacets,
) -> int | None:
    """True count of a run's failing rows matching the active facets.

    The facet predicates (dimension/severity/rule/column) are evaluated
    app-side over each row's parsed failure structs — they are not SQL
    predicates — so the filtered *total* cannot come from the mode-wide
    ``dq_metrics`` number (that counts every failing row of the run,
    whatever check failed). This scans the WHOLE run's failure structs
    (``errors``/``warnings`` only — never the wide ``row_data`` payload)
    and counts matches, so a capped preview window can't undercount a
    selective filter. Returns None on any error (the caller then keeps the
    scan-window count rather than surfacing a wrong headline).
    """
    # ORDER BY created_at DESC to MATCH the preview scan's window: both are
    # capped, so aligning their order guarantees the count covers the same
    # top-N rows the preview shows — otherwise, on a run larger than the cap,
    # the two scans could pull disjoint windows and report total < len(rows)
    # (breaking the "N of M" display).
    stmt = (
        f"SELECT to_json(errors) AS errors, to_json(warnings) AS warnings "
        f"FROM {quarantine_table} WHERE source_table_fqn = '{escaped_fqn}' "  # noqa: S608
        f"{run_cond}"
        f"ORDER BY created_at DESC LIMIT {_FAILED_ROWS_MAX}"
    )
    try:
        rows = sql.query_dicts(stmt)
    except Exception:
        logger.warning("Filtered failing-row count failed; keeping scan-window total", exc_info=True)
        return None
    count = 0
    for raw in rows:
        parsed_failures = parse_failures(raw)
        failed_columns = sorted({c for f in parsed_failures for c in f.columns})
        if QuarantineSampleService.row_matches_filters(
            enrich_failures(parsed_failures),
            failed_columns,
            dimensions=facets.dimensions,
            severities=facets.severities,
            rules=facets.rules,
            columns=facets.columns,
        ):
            count += 1
    return count


def _facets(
    dimension: list[str] | None,
    severity: list[str] | None,
    rule: list[str] | None,
    column: list[str] | None,
    table: list[str] | None = None,
    catalog: list[str] | None = None,
    schema: list[str] | None = None,
) -> ResultFacets:
    return ResultFacets(
        dimensions=tuple(dimension or ()),
        severities=tuple(severity or ()),
        rules=tuple(rule or ()),
        columns=tuple(column or ()),
        tables=tuple(table or ()),
        catalogs=tuple(catalog or ()),
        schemas=tuple(schema or ()),
    )


def _validate_table_facet(table: list[str] | None) -> None:
    """Reject a table-facet value that is not a valid three-part FQN (400).

    Run before any warehouse call — the values are user-supplied and, like
    every other facet, only ever compared app-side against already-fetched
    rows (never interpolated into SQL); the validation keeps the surface
    consistent with every other FQN-accepting parameter.
    """
    for fqn in table or ():
        try:
            validate_fqn(fqn)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc


def _scope_table_facet(table: list[str] | None, member_fqns: list[str]) -> list[str]:
    """Constrain the table facet to the scope's (accessible) member set.

    Values outside the scope are SILENTLY DROPPED — never a 403 (matching
    the multi-table endpoints' inaccessible-member convention), and never
    an impossible facet that would blank every box: dropping every value
    deactivates the facet, leaving the scope unfiltered.
    """
    members = set(member_fqns)
    return [fqn for fqn in table or () if fqn in members]


def _run_set_map(run_sets: RunSetService, run_ids: list[str]) -> dict[str, str]:
    """Best-effort run_id -> run_set_id join (the batch key for consolidation).

    Degrades to an empty map on any OLTP hiccup — a missing join simply
    leaves runs unconsolidated (each its own batch via COALESCE), never a
    500. The run-set tables are OLTP (Lakebase/Delta-fallback) while the
    results views are Delta/UC, so this is a query-time Python join rather
    than a view-side one.
    """
    if not run_ids:
        return {}
    try:
        return run_sets.run_set_ids_by_run_id(sorted(set(run_ids)))
    except Exception:
        logger.warning("Failed to resolve run-set membership; leaving runs unconsolidated", exc_info=True)
        return {}


def _rule_ids_of(*row_lists: list[CheckResultRow] | None) -> set[str]:
    """Distinct non-null registry rule ids across the given row lists."""
    ids: set[str] = set()
    for rows in row_lists:
        for row in rows or ():
            if row.rule_id is not None:
                ids.add(row.rule_id)
    return ids


def _registry_defaults(registry: RegistryService, rule_ids: set[str]) -> dict[str, int | None]:
    """rule_id -> registry-rule default pass threshold (%), best-effort.

    A registry-lookup hiccup degrades to no registry defaults (the resolver
    falls back to the admin default), never a 500 on the results path.
    """
    if not rule_ids:
        return {}
    try:
        rules = registry.get_rules_many(rule_ids)
    except Exception:
        logger.warning("Failed to load registry rules for threshold resolution", exc_info=True)
        return {}
    return {rid: get_rule_pass_threshold(rule.user_metadata) for rid, rule in rules.items()}


def _build_threshold_resolver(
    *,
    admin_default: int,
    registry_defaults: dict[str, int | None],
    rule_overrides: dict[str, int] | None = None,
    column_overrides: dict[str, dict[str, int]] | None = None,
) -> ThresholdResolver:
    """Compose the per-check pass-threshold resolver for one results request.

    The precedence chain (per-column -> per-rule -> registry -> admin) is
    fixed in :func:`resolve_pass_threshold`. For scoped results the caller
    supplies the applied-rule overrides via
    :func:`_applied_threshold_overrides`; global (org-wide) results pass none,
    so the chain degenerates to registry-default -> admin-default (there is
    no single per-binding value org-wide — a deliberate controller choice).

    When a check spans several mapped columns, the STRICTEST (max) column
    override among its columns is used so one lax column can't hide a breach.

    Breach evaluation always uses the **live** precedence chain so threshold
    edits on applied rules take effect on historical runs' pass rates
    immediately after save — without requiring a re-run. The per-run
    ``pass_threshold`` stamped into ``checks_json`` at materialization time is
    retained for audit/export but does not gate the Results UI verdict.
    """
    rule_overrides = rule_overrides or {}
    column_overrides = column_overrides or {}

    def resolve(row: CheckResultRow) -> int:
        rid = row.rule_id or ""
        col_map = column_overrides.get(rid, {})
        col_candidates = [col_map[col] for col in row.columns if col in col_map]
        column_override = max(col_candidates) if col_candidates else None
        return resolve_pass_threshold(
            column_override=column_override,
            rule_override=rule_overrides.get(rid),
            registry_default=registry_defaults.get(rid),
            admin_default=admin_default,
        )

    return resolve


def _applied_threshold_overrides(
    applied_rules: Iterable[AppliedRule],
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Fold applied rules into (rule_id -> per-rule threshold, rule_id -> per-column map).

    Keyed by rule id (not binding) per the plan's scoped-resolution design.
    When the same rule is applied on several bindings with different values,
    the STRICTEST (max) per-rule threshold and the max per-column override
    win — a single lax binding can't mask a breach.
    """
    rule_overrides: dict[str, int] = {}
    column_overrides: dict[str, dict[str, int]] = {}
    for applied in applied_rules:
        if applied.pass_threshold is not None:
            prev = rule_overrides.get(applied.rule_id)
            rule_overrides[applied.rule_id] = (
                applied.pass_threshold if prev is None else max(prev, applied.pass_threshold)
            )
        col_map = get_applied_column_pass_thresholds(applied.user_metadata)
        if col_map:
            merged = column_overrides.setdefault(applied.rule_id, {})
            for col, pct in col_map.items():
                merged[col] = pct if col not in merged else max(merged[col], pct)
    return rule_overrides, column_overrides


def _table_breach_by_run(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_fqns: list[str],
    include_drafts: bool,
    monitored_tables: MonitoredTableService,
    apply_rules: ApplyRulesService,
    app_settings: AppSettingsService,
    registry: RegistryService,
) -> dict[str | None, str | None]:
    """run_id -> worst breach criticality for *table_fqns*' runs (run picker badge).

    Fetches the tables' per-check rows and builds the scope's threshold
    resolver from the tables' applied rules (per-rule + per-column overrides),
    then evaluates each run's breach. Best-effort: any lookup failure yields
    an empty map (no badges), never a 500 on the runs path.
    """
    try:
        rows = _fetch_check_rows(sql, app_conf, table_fqns, None, include_drafts)
        binding_by_fqn = monitored_tables.get_binding_ids_by_table_fqn(table_fqns)
        applied: list[AppliedRule] = []
        for binding_id in dict.fromkeys(binding_by_fqn.values()):
            if binding_id:
                applied.extend(apply_rules.list_applied(binding_id))
        if not app_settings.get_pass_threshold_enabled():
            return {}
        rule_overrides, column_overrides = _applied_threshold_overrides(applied)
        resolver = _build_threshold_resolver(
            admin_default=app_settings.get_default_pass_threshold(),
            registry_defaults=_registry_defaults(registry, _rule_ids_of(rows)),
            rule_overrides=rule_overrides,
            column_overrides=column_overrides,
        )
        return breach_criticality_by_run(rows, resolver)
    except Exception:
        logger.warning("Failed to compute per-run breach badges; leaving runs unbadged", exc_info=True)
        return {}


def _consolidate_runs(rows: list[RunRowOut], run_set_by_run_id: dict[str, str]) -> list[RunRowOut]:
    """Roll up per-run rows into one row per RUN BATCH, newest first.

    A batch = ``COALESCE(run_set_id, run_id)``: all concurrent member runs
    of one Table-Space "Run now" collapse to a single picker entry at the
    batch instant (the batch's last ``run_ts``), with the equal-weight
    mean member score and summed test counts (dqlake ``product_runs``
    parity). The batch's REPRESENTATIVE run (the latest member run) is
    surfaced as ``run_id`` so (a) the picker's selection resolves to that
    run's batch via ``as_of_batch`` and (b) the review-status card still
    gets a real run id. Un-setted runs stay one-per-batch (COALESCE), so a
    single-table runs list is unaffected.
    """
    batches: dict[str, list[RunRowOut]] = {}
    for row in rows:
        key = (run_set_by_run_id.get(row.run_id or "") or row.run_id) if row.run_id else None
        batches.setdefault(key or "", []).append(row)
    out: list[RunRowOut] = []
    for members in batches.values():
        # Representative = latest member run (max run_ts; the mv query
        # already returned rows newest-first, so the first is the latest).
        latest = max(members, key=lambda m: m.run_ts or "")
        rates = [m.pass_rate for m in members if m.pass_rate is not None]
        failed = [m.failed_tests for m in members if m.failed_tests is not None]
        totals = [m.total_tests for m in members if m.total_tests is not None]
        # A batch breaches if any member run breached; carries the worst.
        batch_crit: str | None = None
        for m in members:
            if m.breached:
                batch_crit = "error" if batch_crit == "error" or m.breach_criticality == "error" else "warn"
        out.append(
            RunRowOut(
                run_id=latest.run_id,
                run_ts=latest.run_ts,
                pass_rate=sum(rates) / len(rates) if rates else None,
                failed_tests=sum(failed) if failed else None,
                total_tests=sum(totals) if totals else None,
                # A batch is 'draft' only if every member is a draft run;
                # any published member makes it a published batch.
                run_mode=RUN_MODE_PUBLISHED
                if any(m.run_mode == RUN_MODE_PUBLISHED for m in members)
                else next((m.run_mode for m in members if m.run_mode), None),
                breached=batch_crit is not None,
                breach_criticality=batch_crit,
            )
        )
    out.sort(key=lambda r: r.run_ts or "", reverse=True)
    return out


def _runs_from_metric_view(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_fqns: list[str],
    include_drafts: bool = False,
    run_sets: RunSetService | None = None,
    breach_by_run: dict[str | None, str | None] | None = None,
) -> RunsOut:
    """Per-run rollup from ``mv_dq_scores``, newest first (dqlake RunsOut).

    Draft runs are excluded unless *include_drafts*; every row carries its
    ``run_mode`` so the picker can badge drafts when they are included
    (grouping by run_mode is lossless — a run has exactly one mode).

    *run_sets*, when supplied, rolls the per-run rows up into one row per
    RUN BATCH (the Table-Space runs picker) via a best-effort
    ``dq_run_set_members`` join over the rows just fetched. None keeps the
    raw per-run rollup (the single-table / binding runs picker).

    *breach_by_run* (run_id -> worst breach criticality), when supplied,
    stamps each run's ``breached``/``breach_criticality`` so the picker can
    badge threshold breaches; batches inherit the worst member's breach.
    """
    if not table_fqns:
        return RunsOut()
    breaches = breach_by_run or {}
    mv = metric_view_fqn(app_conf.catalog, app_conf.genie_schema_name)
    conds = [f"input_location IN ({_in_list(table_fqns)})"]
    if not include_drafts:
        conds.append(f"run_mode = '{RUN_MODE_PUBLISHED}'")
    stmt = (
        f"SELECT run_id, CAST(run_time AS STRING) AS run_ts, run_mode, "
        f"MEASURE(score) AS pass_rate, MEASURE(failed_tests) AS failed_tests, "
        f"MEASURE(total_tests) AS total_tests "
        f"FROM {mv} "  # noqa: S608
        f"WHERE {' AND '.join(conds)} "
        f"GROUP BY run_id, run_time, run_mode "
        f"ORDER BY run_time DESC LIMIT {_RUNS_LIMIT}"
    )
    rows = sql.query_dicts(stmt)
    run_rows = [
        RunRowOut(
            run_id=row.get("run_id"),
            run_ts=row.get("run_ts"),
            pass_rate=safe_float(row.get("pass_rate")),
            failed_tests=safe_int(row.get("failed_tests")),
            total_tests=safe_int(row.get("total_tests")),
            run_mode=row.get("run_mode"),
            breached=breaches.get(row.get("run_id")) is not None,
            breach_criticality=breaches.get(row.get("run_id")),
        )
        for row in rows
    ]
    if run_sets is not None:
        run_set_by_run_id = _run_set_map(run_sets, [r.run_id for r in run_rows if r.run_id])
        run_rows = _consolidate_runs(run_rows, run_set_by_run_id)
    return RunsOut(rows=run_rows)


def _label_registry(app_settings: AppSettingsService, key: str) -> list[tuple[str, str, int]]:
    """(name, color, rank) entries for one reserved label definition.

    Rank = 1-based position in the definition's values array (dqlake's
    ascending rank convention: Low=1 .. Critical=4). Colours come from
    ``value_colors`` with a neutral fallback.
    """
    for definition in app_settings.get_label_definitions():
        if definition.get("key") != key:
            continue
        values = definition.get("values")
        if not isinstance(values, list):
            return []
        colors = definition.get("value_colors")
        color_map = colors if isinstance(colors, dict) else {}
        return [
            (str(value), str(color_map.get(value) or _DEFAULT_LABEL_COLOR), idx + 1) for idx, value in enumerate(values)
        ]
    return []


# ---------------------------------------------------------------------------
# Registries (fixed paths first — FastAPI matches in declaration order)
# ---------------------------------------------------------------------------


@router.get(
    "/registries/severities",
    operation_id="listResultSeverities",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_result_severities(
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> list[SeverityOut]:
    """Severity registry derived from the reserved ``severity`` label definition."""
    try:
        return [
            SeverityOut(name=name, color=color, rank=rank)
            for name, color, rank in _label_registry(app_settings, "severity")
        ]
    except Exception as exc:
        logger.exception("Failed to read severity label definition")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/registries/dimensions",
    operation_id="listResultDimensions",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_result_dimensions(
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> list[DimensionOut]:
    """Dimension registry derived from the reserved ``dimension`` label definition."""
    try:
        return [
            DimensionOut(name=name, color=color, rank=rank)
            for name, color, rank in _label_registry(app_settings, "dimension")
        ]
    except Exception as exc:
        logger.exception("Failed to read dimension label definition")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Score-cache refresh (P3.4 — run-completion trigger, no polling/cron)
# ---------------------------------------------------------------------------


@router.post(
    "/refresh-scores",
    operation_id="refreshDqScores",
    dependencies=[require_role(*_ALL_ROLES)],
)
def refresh_dq_scores(
    body: RefreshScoresIn,
    score_cache: Annotated[ScoreCacheService, Depends(get_score_cache_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
) -> RefreshScoresOut:
    """Recompute the cached DQ scores for the just-finished tables.

    Called (fire-and-forget) by the frontend at the exact run-completion
    moments that already fire the results invalidation — see
    ``ui/lib/results-invalidation.ts``. Recomputes the given tables (ONE
    batched warehouse query over the metric view, published runs only),
    every table space containing any of them, and the global rollup —
    all upserted into ``dq_score_cache`` so the list pages never touch
    the warehouse on load.

    The same run-completion moment also refreshes each table's denormalized
    ``last_run_at`` / ``last_profiled_at`` (T-perf / B2-15) so the overview
    "Last run" column and table-space last-run stay current without the
    list path ever touching the warehouse. Best-effort: a timestamp-refresh
    failure only leaves those columns stale until the next completion or the
    scheduler's reconcile, so it never fails the score refresh.

    SP-side by design: the cache is shared/global and viewer-independent;
    the existing catalog filtering on the list endpoints scopes what each
    viewer sees. Viewer+ RBAC like the other dq-results routes. The list
    length is capped (see ``RefreshScoresIn``) and every FQN is validated
    before it can reach a SQL string literal (400 on the first invalid).
    """
    for fqn in body.table_fqns:
        try:
            validate_fqn(fqn)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        refreshed_tables, refreshed_products = score_cache.refresh_all_for_tables(body.table_fqns)
    except Exception as exc:
        logger.exception("Failed to refresh DQ score cache")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    try:
        monitored_tables.refresh_run_timestamps(body.table_fqns)
    except Exception:
        logger.exception("Failed to refresh monitored-table run timestamps; leaving them stale until next completion")
    return RefreshScoresOut(
        refreshed_tables=refreshed_tables,
        refreshed_products=refreshed_products,
        global_refreshed=True,
    )


# ---------------------------------------------------------------------------
# Global results (adaptation #1: full results UI over ALL accessible tables)
# ---------------------------------------------------------------------------


@router.get(
    "/global",
    operation_id="getGlobalResults",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_global_results(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    run_sets: Annotated[RunSetService, Depends(get_run_set_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    table: Annotated[list[str] | None, Query()] = None,
    catalog: Annotated[list[str] | None, Query()] = None,
    schema: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    axes: str = Query("all"),
    include_drafts: bool = Query(False),
    as_of_batch: str | None = Query(None),
) -> EntityResultsOut:
    """Results over every table tracked in dq_metrics that the caller can access.

    Tables in catalogs the caller cannot access are silently filtered
    (never 403) — the same gate as the dq-score global endpoint.
    Draft runs are excluded unless *include_drafts*. *table* (P7.2) is
    the By-table cross-filter: a repeatable list of member FQNs, applied
    app-side like the other four facets (the rows it filters are already
    catalog-gated, so an inaccessible value simply matches nothing).

    Concurrent member runs of one run set are consolidated onto their
    RUN-BATCH instant (``dq_run_set_members`` join) so the per-table trend
    markers align; *as_of_batch* caps to a chosen batch's instant.
    """
    _validate_run_id(run_id)
    _validate_run_id(as_of_batch)
    _validate_table_facet(table)
    try:
        rows = [
            row
            for row in _fetch_check_rows(sql, app_conf, None, run_id, include_drafts)
            if catalog_of(row.table_fqn) in user_catalogs
        ]
        accessible_fqns = sorted({row.table_fqn for row in rows})
        failed_records = _fetch_failed_records_by_run(sql, app_conf, accessible_fqns)
        run_set_by_run_id = _run_set_map(run_sets, [row.run_id for row in rows if row.run_id])
        # The as-of expansion feeds the carry-forward trend series; the
        # global scope's table filter is app-side (catalog gate), same as
        # the raw-row fetch above.
        asof_rows = None
        if _wants_trends(axes):
            asof_rows = [
                row
                for row in _fetch_asof_check_rows(sql, app_conf, None, run_id, include_drafts)
                if catalog_of(row.table_fqn) in user_catalogs
            ]
    except Exception as exc:
        logger.exception("Failed to compute global results")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    # by_table -> binding-id enrichment so the UI can link rows to their
    # monitored-table pages. ONE batched lookup (never per-table), and
    # best-effort: an OLTP hiccup degrades to unlinked rows, never a 500.
    try:
        binding_ids = monitored_tables.get_binding_ids_by_table_fqn(accessible_fqns)
    except Exception:
        logger.warning("Failed to resolve binding ids for global by_table rows", exc_info=True)
        binding_ids = {}
    # Org-wide scope: no single per-binding threshold applies, so the chain
    # degenerates to registry-default -> admin-default (see plan Task 4 — a
    # deliberate controller decision, not a per-binding join).
    resolver = (
        _build_threshold_resolver(
            admin_default=app_settings.get_default_pass_threshold(),
            registry_defaults=_registry_defaults(registry, _rule_ids_of(rows, asof_rows)),
        )
        if app_settings.get_pass_threshold_enabled()
        else None
    )
    return compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column, table, catalog, schema),
        axes=axes,
        table_axis="by_table",
        failed_records_by_run=failed_records,
        binding_ids_by_table=binding_ids,
        asof_rows=asof_rows,
        run_set_by_run_id=run_set_by_run_id,
        as_of_batch=as_of_batch,
        resolve_threshold=resolver,
    )


# ---------------------------------------------------------------------------
# Rule results (adaptation #2: results UI locked to one registry rule)
# ---------------------------------------------------------------------------


@router.get(
    "/rule/{rule_id}",
    operation_id="getRuleResults",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_rule_results(
    rule_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    run_sets: Annotated[RunSetService, Depends(get_run_set_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    table: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    axes: str = Query("all"),
    include_drafts: bool = Query(False),
    as_of_batch: str | None = Query(None),
) -> EntityResultsOut:
    """Results across the rule's applied tables, restricted to that rule's checks.

    The rule's current applications only SCOPE which tables to query;
    which check rows belong to the rule is decided by each run's own
    frozen ``registry_rule_id`` provenance tag (version-accurate: a check
    renamed since the run still attributes to the rule, and a run
    predating checks_json simply carries no provenance).

    Tables in inaccessible catalogs are silently filtered (never 403).
    *table* (P7.2) is the By-table cross-filter, constrained to the
    rule's scoped tables (out-of-scope values are silently dropped).
    ``failed_records`` is intentionally absent from *trend_failures*: the
    per-run failing-row count is table-wide and cannot be scoped to one
    rule's failures.
    """
    _validate_run_id(run_id)
    _validate_run_id(as_of_batch)
    _validate_table_facet(table)
    try:
        applications = apply_rules.list_bindings_for_rule(rule_id)
        binding_ids = list(dict.fromkeys(a.binding_id for a in applications))
        table_fqns: list[str] = []
        binding_id_by_fqn: dict[str, str] = {}
        for binding_id in binding_ids:
            try:
                detail = monitored_tables.get(binding_id)
            except Exception:
                logger.warning(f"Skipping binding {binding_id} in rule results: lookup failed", exc_info=True)
                continue
            if detail is None:
                continue
            fqn = detail.table.table_fqn
            # Defense-in-depth: the binding's FQN round-trips through the app
            # DB before being interpolated into a SQL string literal below.
            if not _is_valid_fqn(fqn, f"binding {binding_id} in rule results"):
                continue
            if catalog_of(fqn) not in user_catalogs or fqn in table_fqns:
                continue
            table_fqns.append(fqn)
            binding_id_by_fqn[fqn] = binding_id
        all_rows = _fetch_check_rows(sql, app_conf, table_fqns, run_id, include_drafts)
        rows: list[CheckResultRow] = [row for row in all_rows if row.rule_id == rule_id]
        run_set_by_run_id = _run_set_map(run_sets, [row.run_id for row in rows if row.run_id])
        # Rule scoping applies to the as-of expansion the same way: the
        # carried run stays each table's latest run (the expansion's
        # choice), and only its rows attributed to this rule count.
        asof_rows = None
        if _wants_trends(axes):
            asof_rows = [
                row
                for row in _fetch_asof_check_rows(sql, app_conf, table_fqns, run_id, include_drafts)
                if row.rule_id == rule_id
            ]
    except Exception as exc:
        logger.exception(f"Failed to compute results for rule {rule_id}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    # Scoped resolver: the rule's applications carry the per-rule and
    # per-column overrides (folded across every binding, strictest wins).
    rule_overrides, column_overrides = _applied_threshold_overrides(applications)
    resolver = (
        _build_threshold_resolver(
            admin_default=app_settings.get_default_pass_threshold(),
            registry_defaults=_registry_defaults(registry, {rule_id} | _rule_ids_of(rows, asof_rows)),
            rule_overrides=rule_overrides,
            column_overrides=column_overrides,
        )
        if app_settings.get_pass_threshold_enabled()
        else None
    )
    return compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column, _scope_table_facet(table, table_fqns)),
        axes=axes,
        table_axis="by_table",
        binding_ids_by_table=binding_id_by_fqn,
        asof_rows=asof_rows,
        run_set_by_run_id=run_set_by_run_id,
        as_of_batch=as_of_batch,
        resolve_threshold=resolver,
    )


# ---------------------------------------------------------------------------
# Product results
# ---------------------------------------------------------------------------


def _accessible_member_fqns(
    data_products: DataProductService,
    product_id: str,
    user_catalogs: frozenset[str],
) -> tuple[list[str], list[str]]:
    """(accessible member fqns, accessible member binding_ids); 404 when the
    product does not exist. Inaccessible members are silently filtered."""
    detail = data_products.get(product_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Data product not found: {product_id}")
    fqns: list[str] = []
    binding_ids: list[str] = []
    for member in detail.members:
        # Defense-in-depth: member FQNs round-trip through the app DB before
        # being interpolated into SQL string literals on the query paths.
        if not _is_valid_fqn(member.table_fqn, f"product {product_id} member {member.binding_id}"):
            continue
        if catalog_of(member.table_fqn) not in user_catalogs:
            continue
        if member.table_fqn in fqns:
            continue
        fqns.append(member.table_fqn)
        binding_ids.append(member.binding_id)
    return fqns, binding_ids


@router.get(
    "/product/{product_id}/runs",
    operation_id="getProductResultsRuns",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_product_results_runs(
    product_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    data_products: Annotated[DataProductService, Depends(get_data_product_service)],
    run_sets: Annotated[RunSetService, Depends(get_run_set_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    include_drafts: bool = Query(False),
) -> RunsOut:
    """Run rollups across the product's accessible member tables, newest first.

    Rolled up per RUN BATCH (``dq_run_set_members`` join): concurrent
    member runs of one Table-Space "Run now" collapse to a single picker
    entry, so the picker offers coherent product-level batches rather than
    per-member-table runs. Each batch is stamped with a threshold-breach
    badge (worst member run's breach).
    """
    try:
        fqns, _ = _accessible_member_fqns(data_products, product_id, user_catalogs)
        breach_by_run = _table_breach_by_run(
            sql, app_conf, fqns, include_drafts, monitored_tables, apply_rules, app_settings, registry
        )
        return _runs_from_metric_view(
            sql, app_conf, fqns, include_drafts, run_sets=run_sets, breach_by_run=breach_by_run
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(f"Failed to list runs for product {product_id}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/product/{product_id}",
    operation_id="getProductResults",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_product_results(
    product_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    data_products: Annotated[DataProductService, Depends(get_data_product_service)],
    run_sets: Annotated[RunSetService, Depends(get_run_set_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    table: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    axes: str = Query("all"),
    include_drafts: bool = Query(False),
    as_of_batch: str | None = Query(None),
) -> EntityResultsOut:
    """Results aggregated over the product's member tables (by_table filled).

    Members in inaccessible catalogs are silently filtered (never 403).
    Draft runs are excluded unless *include_drafts*. *table* (P7.2) is
    the By-table cross-filter, constrained to the product's accessible
    member set (out-of-scope values are silently dropped).

    Concurrent member runs of one Table-Space "Run now" are consolidated
    onto a single RUN-BATCH instant (``dq_run_set_members`` join), so the
    per-table trend markers share the Average point's x and the trend
    tooltip lists every member. *as_of_batch* (a run_id from the batch-
    keyed runs picker) caps the series/snapshot to that batch's instant.
    """
    _validate_run_id(run_id)
    _validate_run_id(as_of_batch)
    _validate_table_facet(table)
    try:
        fqns, binding_ids = _accessible_member_fqns(data_products, product_id, user_catalogs)
        rows = _fetch_check_rows(sql, app_conf, fqns, run_id, include_drafts)
        failed_records = _fetch_failed_records_by_run(sql, app_conf, fqns)
        asof_rows = _fetch_asof_check_rows(sql, app_conf, fqns, run_id, include_drafts) if _wants_trends(axes) else None
        run_set_by_run_id = _run_set_map(run_sets, [row.run_id for row in rows if row.run_id])
        # Applied rules across every accessible member binding carry the
        # per-rule + per-column overrides for this scope's resolver.
        member_applied: list[AppliedRule] = []
        for binding_id in binding_ids:
            try:
                member_applied.extend(apply_rules.list_applied(binding_id))
            except Exception:
                logger.warning(
                    f"Skipping applied-rule thresholds for binding {binding_id} in product results", exc_info=True
                )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(f"Failed to compute results for product {product_id}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    rule_overrides, column_overrides = _applied_threshold_overrides(member_applied)
    resolver = (
        _build_threshold_resolver(
            admin_default=app_settings.get_default_pass_threshold(),
            registry_defaults=_registry_defaults(registry, _rule_ids_of(rows, asof_rows)),
            rule_overrides=rule_overrides,
            column_overrides=column_overrides,
        )
        if app_settings.get_pass_threshold_enabled()
        else None
    )
    return compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column, _scope_table_facet(table, fqns)),
        axes=axes,
        table_axis="by_table",
        failed_records_by_run=failed_records,
        # Member binding ids are already loaded — no extra lookup needed.
        binding_ids_by_table=dict(zip(fqns, binding_ids)),
        asof_rows=asof_rows,
        run_set_by_run_id=run_set_by_run_id,
        as_of_batch=as_of_batch,
        resolve_threshold=resolver,
    )


# ---------------------------------------------------------------------------
# Filtered failed rows (Task 7 path + server-side failure filters)
# ---------------------------------------------------------------------------


@router.get(
    "/failed-rows/{table_fqn:path}",
    operation_id="getDqResultsFailedRows",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_dq_results_failed_rows(
    table_fqn: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    obo_sql: Annotated[SqlExecutor, Depends(get_preview_sql_executor)],
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    email: Annotated[str, Depends(get_user_email)],
    entitlements: Annotated[EntitlementService, Depends(get_entitlement_service)],
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    limit: int = Query(200, ge=1, le=100000),
    include_drafts: bool = Query(False),
) -> FailedRowsOut:
    """One run's failing rows for *table_fqn*, filtered server-side (OBO-gated).

    Failing records are PER-RUN — the response never stacks rows across
    runs. An explicit *run_id* pins exactly that run; otherwise the
    default is the table's LATEST run, resolved the way the dq-score
    endpoints resolve it (``ORDER BY run_time DESC LIMIT 1`` under
    run_mode filtering). ``dq_quarantine_records`` carries no run_mode of
    its own, so the resolve is a subselect against ``v_dq_check_results``
    (the one place run_mode is resolved: stamped tag first, untagged
    legacy runs classify as published). ``include_drafts=true`` widens
    which runs QUALIFY as "latest" — never how many runs are returned.

    SECURITY MODEL — the checks from ``services/quarantine_sample_service.py``,
    in the same load-bearing order:

    1. Validate the FQN (400 before any backend call).
    2. Live OBO SELECT self-check on the SOURCE table, as the CALLER; on
       failure return HTTP 200 with an empty list — never 403/404.
    3. Fine-grained-control check via the caller's OBO metadata read; if
       present — or unknowable — suppress the sample entirely.
    4. Only then does the app's service principal read the quarantine
       table. Filters are applied AFTER the gates, over the parsed rows.
    """
    _validate_run_id(run_id)
    try:
        validate_fqn(table_fqn)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # (2) Cheap denial, as the caller. Empty 200 — not 403/404 — so an
    # unauthorized caller cannot confirm or deny the table's existence.
    if not QuarantineSampleService.user_can_select(obo_sql, table_fqn):
        return FailedRowsOut(rows=[], total=0, suppressed=False)

    # (3) Fine-grained controls (or an unverifiable state) suppress the
    # sample entirely — copied quarantine rows can't replicate the policy.
    if QuarantineSampleService.has_fine_grained_access_control(obo_ws, table_fqn):
        return FailedRowsOut(rows=[], total=0, suppressed=True)

    # Piggyback (P4.1): the caller just passed the exact gates the Genie
    # failing-rows view relies on, so cache the entitlement now — the tables
    # a user actually opens are pre-verified without a separate round-trip.
    # After BOTH gates deliberately: a fine-grained-controlled table must not
    # open in v_dq_failing_rows when this endpoint itself suppresses it.
    # Best-effort (never raises) and never affects this response.
    entitlements.record_entitlement(email, table_fqn)

    facets = _facets(dimension, severity, rule, column)
    # When filters are active, scan a wider window than the page size so a
    # selective filter can still fill the page. *total* is corrected to the
    # TRUE filtered count below (a dedicated count pass), so this window only
    # governs how many rows the preview itself renders.
    scan_limit = limit if not facets.any_active() else min(max(limit * 5, 1000), _FAILED_ROWS_MAX)

    # (4) SP-side fetch of the precomputed failing rows, with created_at
    # surfaced as the run_ts.
    quarantine_table = _app_object_fqn(app_conf, "dq_quarantine_records")
    e_fqn = escape_sql_string(table_fqn)
    if run_id:
        # A pinned run: exactly that run's rows (run_id is charset-validated
        # above — the _RUN_ID_SAFE precondition escape_sql_string relies on).
        run_cond = f"AND run_id = '{escape_sql_string(run_id)}' "
    else:
        # Default: exactly the table's LATEST run, resolved the way the
        # dq-score endpoints resolve it. Quarantine rows have no run_mode
        # column, so the resolve subselect goes through the shaping view;
        # include_drafts widens which runs qualify as "latest" — the
        # response is always a single run's rows.
        mode_cond = "" if include_drafts else f"AND run_mode = '{RUN_MODE_PUBLISHED}' "
        run_cond = (
            f"AND run_id = (SELECT run_id FROM {_shaping_view_fqn(app_conf)} "
            f"WHERE input_location = '{e_fqn}' {mode_cond}"
            f"ORDER BY run_time DESC LIMIT 1) "
        )
    stmt = (
        f"SELECT quarantine_id, run_id, to_json(row_data) AS row_data, "
        f"to_json(errors) AS errors, to_json(warnings) AS warnings, "
        f"CAST(created_at AS STRING) AS created_at "
        f"FROM {quarantine_table} WHERE source_table_fqn = '{e_fqn}' "  # noqa: S608
        f"{run_cond}"
        f"ORDER BY created_at DESC LIMIT {int(scan_limit)}"
    )
    try:
        raw_rows = sp_sql.query_dicts(stmt)
    except Exception as exc:
        # Only reachable after both OBO checks passed, so this 500 leaks
        # nothing to unauthorized callers.
        logger.exception(f"Failed to load failed rows for {table_fqn}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    matched: list[FailedRowOut] = []
    for raw in raw_rows:
        # Severity/dimension/rule_id come from each failure struct's OWN
        # frozen user_metadata (as-of-run payload) — no live rule join.
        parsed_failures = parse_failures(raw)
        record = to_failing_record(raw, parsed_failures)
        failures = enrich_failures(parsed_failures)
        if not QuarantineSampleService.row_matches_filters(
            failures,
            record.failed_columns,
            dimensions=facets.dimensions,
            severities=facets.severities,
            rules=facets.rules,
            columns=facets.columns,
        ):
            continue
        matched.append(
            FailedRowOut(
                record_key=record.record_key,
                row_values=record.row_values,
                failed_columns=record.failed_columns,
                failures=failures,
                run_ts=raw.get("created_at"),
            )
        )

    # *total* must reflect the TRUE number of failing records for the
    # resolved run — never the size of the (capped) preview scan.
    #  - No active facets: read the run's authoritative distinct
    #    failing-row count from dq_metrics (input_row_count -
    #    valid_row_count) so the "download to view all N" headline is
    #    correct even when *rows* is capped at *limit*.
    #  - Active facets: the filters are applied app-side over each row's
    #    parsed failure structs (not a SQL predicate), so the metrics
    #    number — which counts EVERY failing row of the run regardless of
    #    which check failed — would over-report. Instead count the true
    #    matches across the WHOLE run (a dedicated errors/warnings-only
    #    scan), not just the capped preview window. Without this the
    #    filtered headline reported the scan-window match count, a lower
    #    bound that badly undershot a selective filter on a large run
    #    (item 63: a single-check filter showed 307 of its real 1183).
    total = len(matched)
    if not facets.any_active() and raw_rows:
        # Every returned row belongs to exactly ONE run (the WHERE clause
        # pins it), so the effective run is the pin or the run the sample
        # carries.
        resolved_run_id = run_id or raw_rows[0].get("run_id")
        if resolved_run_id:
            failed_by_run = _fetch_failed_records_by_run(sp_sql, app_conf, [table_fqn])
            true_total = failed_by_run.get((table_fqn, resolved_run_id))
            if true_total is not None:
                total = true_total
    elif facets.any_active() and raw_rows:
        # len(matched) counts only the capped preview window; count the true
        # filtered total across the FULL run so a selective filter's headline
        # isn't a scan-window lower bound.
        exact = _count_filtered_failed_rows(sp_sql, quarantine_table, e_fqn, run_cond, facets)
        if exact is not None:
            total = exact
    return FailedRowsOut(rows=matched[:limit], total=total, suppressed=False)


# ---------------------------------------------------------------------------
# Runs + table results (path-param catch-alls — declared LAST)
# ---------------------------------------------------------------------------


@router.get(
    "/runs/{binding_or_table:path}",
    operation_id="getDqResultsRuns",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_dq_results_runs(
    binding_or_table: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    include_drafts: bool = Query(False),
) -> RunsOut:
    """Per-run rollup for one table, newest first (backs the run picker).

    Accepts either a three-part table FQN or a monitored-table binding id
    (resolved to its bound table). Draft runs are excluded unless
    *include_drafts*. Each run is stamped with a threshold-breach badge
    computed from its per-check rows.
    """
    table_fqn = binding_or_table
    try:
        validate_fqn(binding_or_table)
    except ValueError:
        try:
            detail = monitored_tables.get(binding_or_table)
        except Exception as exc:
            logger.exception(f"Failed to resolve binding {binding_or_table}")
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if detail is None:
            raise HTTPException(
                status_code=400,
                detail="Expected a three-part table FQN or a known binding id",
            )
        table_fqn = detail.table.table_fqn
        # Defense-in-depth: the binding-resolved FQN comes from the app DB
        # and is interpolated into a SQL string literal below.
        if not _is_valid_fqn(table_fqn, f"binding {binding_or_table} in runs"):
            raise HTTPException(status_code=400, detail="Binding resolves to an invalid table FQN")

    if catalog_of(table_fqn) not in user_catalogs:
        raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")

    try:
        breach_by_run = _table_breach_by_run(
            sql, app_conf, [table_fqn], include_drafts, monitored_tables, apply_rules, app_settings, registry
        )
        return _runs_from_metric_view(sql, app_conf, [table_fqn], include_drafts, breach_by_run=breach_by_run)
    except Exception as exc:
        logger.exception(f"Failed to list runs for {table_fqn}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/table/{table_fqn:path}",
    operation_id="getTableResults",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_table_results(
    table_fqn: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    axes: str = Query("all"),
    include_drafts: bool = Query(False),
) -> EntityResultsOut:
    """Breakdowns + trends for one table (dqlake's table Results tab shapes).

    ``trend_failures`` honours the run filter but not the drilldown chips
    (dqlake parity: its table reader filters that series on binding/run
    only). Draft runs are excluded unless *include_drafts*. No as-of
    expansion fetch here: a single table's per-run rows ARE its as-of
    degeneration (``compute_entity_results`` falls back to them).
    """
    _validate_run_id(run_id)
    try:
        validate_fqn(table_fqn)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if catalog_of(table_fqn) not in user_catalogs:
        raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")

    try:
        rows = _fetch_check_rows(sql, app_conf, [table_fqn], run_id, include_drafts)
        failed_records = _fetch_failed_records_by_run(sql, app_conf, [table_fqn])
        # Resolve the table's binding once — reused for both the threshold
        # resolver (applied-rule overrides) and the trend version markers.
        binding_id = monitored_tables.get_binding_ids_by_table_fqn([table_fqn]).get(table_fqn)
        applied: list[AppliedRule] = []
        if binding_id:
            try:
                applied = apply_rules.list_applied(binding_id)
            except Exception:
                logger.warning(f"Skipping applied-rule thresholds for {table_fqn}: lookup failed", exc_info=True)
    except Exception as exc:
        logger.exception(f"Failed to compute results for {table_fqn}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    rule_overrides, column_overrides = _applied_threshold_overrides(applied)
    resolver = (
        _build_threshold_resolver(
            admin_default=app_settings.get_default_pass_threshold(),
            registry_defaults=_registry_defaults(registry, _rule_ids_of(rows)),
            rule_overrides=rule_overrides,
            column_overrides=column_overrides,
        )
        if app_settings.get_pass_threshold_enabled()
        else None
    )
    result = compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column),
        axes=axes,
        table_axis="tables",
        failed_records_by_run=failed_records,
        failures_ignore_facets=True,
        resolve_threshold=resolver,
    )
    # Stamp the overall-score trend with the binding version active at each
    # run (#65) so the UI can mark version increments. Best-effort: a lookup
    # failure or an unmonitored table just leaves version=None.
    if _wants_trends(axes) and result.trend and binding_id:
        try:
            annotate_trend_versions(result.trend, monitored_tables.get_version_freezes(binding_id))
        except Exception:
            logger.warning(f"Skipping trend version markers for {table_fqn}: lookup failed", exc_info=True)
    return result
