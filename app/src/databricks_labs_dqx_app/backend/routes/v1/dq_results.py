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
from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_apply_rules_service,
    get_conf,
    get_data_product_service,
    get_monitored_table_service,
    get_obo_ws,
    get_preview_sql_executor,
    get_score_cache_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.metrics_utils import catalog_of, safe_float, safe_int
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
    compute_entity_results,
    parse_check_rows,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
from databricks_labs_dqx_app.backend.services.quarantine_sample_service import (
    QuarantineSampleService,
    enrich_failures,
    parse_failures,
    to_failing_record,
)
from databricks_labs_dqx_app.backend.services.score_view_service import (
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
    """Backtick-quoted FQN of an app-schema object (*name* is a trusted constant)."""
    return quote_object_fqn(app_conf.catalog, app_conf.schema_name, name)


def _shaping_view_fqn(app_conf: AppConfig) -> str:
    return _app_object_fqn(app_conf, SHAPING_VIEW_NAME)


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
        f"check_name, error_count, warning_count, input_row_count, "
        # As-of-run attribution baked into the view rows (frozen
        # checks_json payload — see score_view_service).
        f"severity, dimension, registry_rule_id, to_json(columns) AS columns_json "
        f"FROM {view} "  # noqa: S608
        f"{where}"
        f"ORDER BY run_time"
    )
    return parse_check_rows(sql.query_dicts(stmt))


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


def _facets(
    dimension: list[str] | None,
    severity: list[str] | None,
    rule: list[str] | None,
    column: list[str] | None,
) -> ResultFacets:
    return ResultFacets(
        dimensions=tuple(dimension or ()),
        severities=tuple(severity or ()),
        rules=tuple(rule or ()),
        columns=tuple(column or ()),
    )


def _runs_from_metric_view(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_fqns: list[str],
    include_drafts: bool = False,
) -> RunsOut:
    """Per-run rollup from ``mv_dq_scores``, newest first (dqlake RunsOut).

    Draft runs are excluded unless *include_drafts*; every row carries its
    ``run_mode`` so the picker can badge drafts when they are included
    (grouping by run_mode is lossless — a run has exactly one mode).
    """
    if not table_fqns:
        return RunsOut()
    mv = metric_view_fqn(app_conf.catalog, app_conf.schema_name)
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
    return RunsOut(
        rows=[
            RunRowOut(
                run_id=row.get("run_id"),
                run_ts=row.get("run_ts"),
                pass_rate=safe_float(row.get("pass_rate")),
                failed_tests=safe_int(row.get("failed_tests")),
                total_tests=safe_int(row.get("total_tests")),
                run_mode=row.get("run_mode"),
            )
            for row in rows
        ]
    )


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
            (str(value), str(color_map.get(value) or _DEFAULT_LABEL_COLOR), idx + 1)
            for idx, value in enumerate(values)
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
) -> RefreshScoresOut:
    """Recompute the cached DQ scores for the just-finished tables.

    Called (fire-and-forget) by the frontend at the exact run-completion
    moments that already fire the results invalidation — see
    ``ui/lib/results-invalidation.ts``. Recomputes the given tables (ONE
    batched warehouse query over the metric view, published runs only),
    every table space containing any of them, and the global rollup —
    all upserted into ``dq_score_cache`` so the list pages never touch
    the warehouse on load.

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
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    axes: str = Query("all"),
    include_drafts: bool = Query(False),
) -> EntityResultsOut:
    """Results over every table tracked in dq_metrics that the caller can access.

    Tables in catalogs the caller cannot access are silently filtered
    (never 403) — the same gate as the dq-score global endpoint.
    Draft runs are excluded unless *include_drafts*.
    """
    _validate_run_id(run_id)
    try:
        rows = [
            row
            for row in _fetch_check_rows(sql, app_conf, None, run_id, include_drafts)
            if catalog_of(row.table_fqn) in user_catalogs
        ]
        accessible_fqns = sorted({row.table_fqn for row in rows})
        failed_records = _fetch_failed_records_by_run(sql, app_conf, accessible_fqns)
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
    return compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column),
        axes=axes,
        table_axis="by_table",
        failed_records_by_run=failed_records,
        binding_ids_by_table=binding_ids,
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
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    axes: str = Query("all"),
    include_drafts: bool = Query(False),
) -> EntityResultsOut:
    """Results across the rule's applied tables, restricted to that rule's checks.

    The rule's current applications only SCOPE which tables to query;
    which check rows belong to the rule is decided by each run's own
    frozen ``registry_rule_id`` provenance tag (version-accurate: a check
    renamed since the run still attributes to the rule, and a run
    predating checks_json simply carries no provenance).

    Tables in inaccessible catalogs are silently filtered (never 403).
    ``failed_records`` is intentionally absent from *trend_failures*: the
    per-run failing-row count is table-wide and cannot be scoped to one
    rule's failures.
    """
    _validate_run_id(run_id)
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
    except Exception as exc:
        logger.exception(f"Failed to compute results for rule {rule_id}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column),
        axes=axes,
        table_axis="by_table",
        binding_ids_by_table=binding_id_by_fqn,
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
    include_drafts: bool = Query(False),
) -> RunsOut:
    """Run rollups across the product's accessible member tables, newest first."""
    try:
        fqns, _ = _accessible_member_fqns(data_products, product_id, user_catalogs)
        return _runs_from_metric_view(sql, app_conf, fqns, include_drafts)
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
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    run_id: str | None = Query(None),
    axes: str = Query("all"),
    include_drafts: bool = Query(False),
) -> EntityResultsOut:
    """Results aggregated over the product's member tables (by_table filled).

    Members in inaccessible catalogs are silently filtered (never 403).
    Draft runs are excluded unless *include_drafts*.
    """
    _validate_run_id(run_id)
    try:
        fqns, binding_ids = _accessible_member_fqns(data_products, product_id, user_catalogs)
        rows = _fetch_check_rows(sql, app_conf, fqns, run_id, include_drafts)
        failed_records = _fetch_failed_records_by_run(sql, app_conf, fqns)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(f"Failed to compute results for product {product_id}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column),
        axes=axes,
        table_axis="by_table",
        failed_records_by_run=failed_records,
        # Member binding ids are already loaded — no extra lookup needed.
        binding_ids_by_table=dict(zip(fqns, binding_ids)),
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
    dimension: Annotated[list[str] | None, Query()] = None,
    severity: Annotated[list[str] | None, Query()] = None,
    rule: Annotated[list[str] | None, Query()] = None,
    column: Annotated[list[str] | None, Query()] = None,
    limit: int = Query(200, ge=1, le=100000),
    include_drafts: bool = Query(False),
) -> FailedRowsOut:
    """Latest failing rows for *table_fqn*, filtered server-side (OBO-gated).

    ``dq_quarantine_records`` carries no run_mode of its own, but it does
    carry ``run_id`` — so the default published-only filter is a subselect
    of the table's published run ids from ``v_dq_check_results`` (the one
    place run_mode is resolved: stamped tag first, untagged legacy runs
    classify as published). ``include_drafts=true`` drops the subselect.

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

    facets = _facets(dimension, severity, rule, column)
    # When filters are active, scan a wider window than the page size so a
    # selective filter can still fill the page; *total* counts the matches
    # within the scanned window (a lower bound when the window is capped).
    scan_limit = limit if not facets.any_active() else min(max(limit * 5, 1000), 100000)

    # (4) SP-side fetch of the precomputed failing rows, with created_at
    # surfaced as the run_ts.
    quarantine_table = _app_object_fqn(app_conf, "dq_quarantine_records")
    e_fqn = escape_sql_string(table_fqn)
    run_mode_cond = ""
    if not include_drafts:
        # Quarantine rows have no run_mode column; scope them to the
        # table's published run ids via the shaping view instead.
        run_mode_cond = (
            f"AND run_id IN (SELECT run_id FROM {_shaping_view_fqn(app_conf)} "
            f"WHERE input_location = '{e_fqn}' AND run_mode = '{RUN_MODE_PUBLISHED}') "
        )
    stmt = (
        f"SELECT quarantine_id, run_id, to_json(row_data) AS row_data, "
        f"to_json(errors) AS errors, to_json(warnings) AS warnings, "
        f"CAST(created_at AS STRING) AS created_at "
        f"FROM {quarantine_table} WHERE source_table_fqn = '{e_fqn}' "  # noqa: S608
        f"{run_mode_cond}"
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
    return FailedRowsOut(rows=matched[:limit], total=len(matched), suppressed=False)


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
    include_drafts: bool = Query(False),
) -> RunsOut:
    """Per-run rollup for one table, newest first (backs the run picker).

    Accepts either a three-part table FQN or a monitored-table binding id
    (resolved to its bound table). Draft runs are excluded unless
    *include_drafts*.
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
        return _runs_from_metric_view(sql, app_conf, [table_fqn], include_drafts)
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
    only). Draft runs are excluded unless *include_drafts*.
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
    except Exception as exc:
        logger.exception(f"Failed to compute results for {table_fqn}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return compute_entity_results(
        rows,
        _facets(dimension, severity, rule, column),
        axes=axes,
        table_axis="tables",
        failed_records_by_run=failed_records,
        failures_ignore_facets=True,
    )
