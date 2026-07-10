"""DQ score read API.

Reads table-level DQ scores from the ``mv_dq_scores`` UC metric view
via MEASURE() queries (see ``services.score_view_service`` for the view
DDL) — the view derives everything from the existing ``dq_metrics``
table, so the frozen metrics-emission pipeline is unchanged. The score
formula is specified (and unit-tested) by ``ScoreService``; the metric
view is its SQL translation.

The metric view is SP-owned and executes with definer's rights, so it
is NOT the permission boundary: aggregate scores are low-sensitivity
(counts only, no row values) and access is gated at catalog granularity
via the same *get_user_catalog_names* OBO pattern already used by
``metrics.py``, not a full per-table live check (that stricter check is
reserved for the row-level sample endpoint, since that returns actual
row values).
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_apply_rules_service,
    get_conf,
    get_monitored_table_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.metrics_utils import (
    catalog_of,
    safe_float,
    safe_int,
)
from databricks_labs_dqx_app.backend.models import (
    RuleScoreOut,
    TableScoreOut,
)
from databricks_labs_dqx_app.backend.registry_models import AppliedRule
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.score_service import ScoreService
from databricks_labs_dqx_app.backend.services.score_view_service import (
    RUN_MODE_PUBLISHED,
    metric_view_fqn,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)
router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


def _row_to_table_score(table_fqn: str, row: dict[str, str | None]) -> TableScoreOut:
    """Map one mv_dq_scores MEASURE() result row onto TableScoreOut.

    The Statement Execution API returns every value as a string (or
    None for SQL NULL): *score* is NULL when the run has no rows or no
    per-check breakdown — the metric view's TRY_DIVIDE analogue of
    ScoreService returning None.
    """
    score = safe_float(row.get("score"))
    return TableScoreOut(
        source_table_fqn=table_fqn,
        score=round(score, 4) if score is not None else None,
        latest_run_id=row.get("run_id"),
        total_tests=safe_int(row.get("total_tests")) or 0,
        failed_tests=safe_int(row.get("failed_tests")) or 0,
    )


def _compute_score_for_table(
    table_fqn: str, sql: SqlExecutor, app_conf: AppConfig, include_drafts: bool = False
) -> TableScoreOut:
    """Read the row-weighted DQ score for *table_fqn*'s latest run.

    By default the latest PUBLISHED run is scored (``run_mode`` filter on
    the metric view — stamped run-level tag, legacy run_type heuristic as
    fallback), so a newer draft run never displaces the published score.
    The latest run within the selected mode set is picked via
    ``ORDER BY run_time DESC LIMIT 1`` rather than the view's
    ``is_latest_run`` flag, which is computed over ALL runs regardless of
    mode. Raises the underlying exception on SQL failure — the caller
    maps it to an HTTP status.
    """
    mv = metric_view_fqn(app_conf.catalog, app_conf.schema_name)
    e_fqn = escape_sql_string(table_fqn)
    conds = [f"input_location = '{e_fqn}'"]
    if not include_drafts:
        conds.append(f"run_mode = '{RUN_MODE_PUBLISHED}'")
    stmt = (
        f"SELECT run_id, MEASURE(score) AS score, "
        f"MEASURE(failed_tests) AS failed_tests, MEASURE(total_tests) AS total_tests "
        f"FROM {mv} "  # noqa: S608
        f"WHERE {' AND '.join(conds)} "
        f"GROUP BY run_id, run_time "
        f"ORDER BY run_time DESC LIMIT 1"
    )
    rows = sql.query_dicts(stmt)
    if not rows:
        return TableScoreOut(source_table_fqn=table_fqn)
    return _row_to_table_score(table_fqn, rows[0])


def _resolve_binding_fqns(applications: list[AppliedRule], monitored_tables: MonitoredTableService) -> list[str]:
    """Map each application's binding to its source table FQN, deduplicated.

    A binding that no longer resolves (deleted concurrently, or its lookup
    errors transiently) is skipped rather than failing the whole aggregate —
    the remaining tables still yield a useful score.
    """
    fqns: list[str] = []
    seen: set[str] = set()
    for application in applications:
        try:
            detail = monitored_tables.get(application.binding_id)
        except Exception:
            logger.warning("Skipping binding %s in rule score: lookup failed", application.binding_id, exc_info=True)
            continue
        if detail is None:
            logger.warning("Skipping binding %s in rule score: binding not found", application.binding_id)
            continue
        fqn = detail.table.table_fqn
        # Defense-in-depth: the binding's FQN round-trips through the app DB
        # before being interpolated into a SQL string literal
        # (_compute_score_for_table), and *escape_sql_string* relies on
        # *validate_fqn* having rejected backslashes. Skip, never 500.
        try:
            validate_fqn(fqn)
        except ValueError:
            logger.warning("Skipping binding %s in rule score: invalid table FQN", application.binding_id)
            continue
        if fqn not in seen:
            seen.add(fqn)
            fqns.append(fqn)
    return fqns


@router.get(
    "/rule/{rule_id}",
    operation_id="getRuleScore",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_rule_score(
    rule_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    include_drafts: bool = Query(False),
) -> RuleScoreOut:
    """Return the aggregate DQ score for a registry rule across its applied tables.

    *applied_to_count* is the TOTAL number of applications across all
    bindings — deliberately NOT restricted to the viewer's accessible
    catalogs, since the frontend uses ``applied_to_count == 0`` to mean
    "not applied anywhere". *per_table* applies the same silent catalog
    filter as the product endpoint and is deduplicated by table (a rule
    applied twice to one table is scored once). Per-table scores read the
    latest PUBLISHED run unless *include_drafts*.
    """
    try:
        applications = apply_rules.list_bindings_for_rule(rule_id)
        table_fqns = _resolve_binding_fqns(applications, monitored_tables)
        accessible = [fqn for fqn in table_fqns if catalog_of(fqn) in user_catalogs]
        per_table = [_compute_score_for_table(fqn, sql, app_conf, include_drafts) for fqn in accessible]
    except Exception as exc:
        logger.exception("Failed to compute score for rule %s", rule_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    scored = [s.score for s in per_table if s.score is not None]
    overall = ScoreService.compute_product_score(scored)
    return RuleScoreOut(
        rule_id=rule_id,
        applied_to_count=len(applications),
        overall_score=round(overall, 4) if overall is not None else None,
        per_table=per_table,
    )
