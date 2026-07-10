"""DQ score read API.

Computes table-level DQ scores from the existing ``dq_metrics`` table —
no changes to the frozen metrics-emission pipeline. Aggregate scores
are low-sensitivity (counts only, no row values), so access is gated
at catalog granularity via the same *get_user_catalog_names* OBO
pattern already used by ``metrics.py``, not a full per-table live check
(that stricter check is reserved for the row-level sample endpoint,
since that returns actual row values).
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_apply_rules_service,
    get_conf,
    get_data_product_service,
    get_monitored_table_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.metrics_utils import (
    catalog_of,
    parse_check_metrics,
    safe_int,
)
from databricks_labs_dqx_app.backend.models import (
    GlobalScoreOut,
    ProductScoreOut,
    RuleScoreOut,
    TableScoreOut,
)
from databricks_labs_dqx_app.backend.registry_models import AppliedRule
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.score_service import ScoreService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)
router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


def _compute_score_for_table(table_fqn: str, sql: SqlExecutor, app_conf: AppConfig) -> TableScoreOut:
    """Compute the row-weighted DQ score for *table_fqn*'s latest run.

    Shared by the table and product endpoints. Raises the underlying
    exception on SQL failure — callers map it to an HTTP status.
    """
    metrics_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    e_fqn = escape_sql_string(table_fqn)
    stmt = (
        f"WITH latest_run AS ("
        f"  SELECT run_id FROM {metrics_table} WHERE input_location = '{e_fqn}' "  # noqa: S608
        f"  ORDER BY run_time DESC LIMIT 1"
        f") "
        f"SELECT m.run_id, m.metric_name, m.metric_value FROM {metrics_table} m "
        f"JOIN latest_run lr ON lr.run_id = m.run_id"
    )
    rows = sql.query_dicts(stmt)
    if not rows:
        return TableScoreOut(source_table_fqn=table_fqn)

    metrics = {r["metric_name"]: r["metric_value"] for r in rows}
    input_row_count = safe_int(metrics.get("input_row_count")) or 0
    check_metrics = parse_check_metrics(metrics.get("check_metrics"))
    score = ScoreService.compute_table_score(check_metrics, input_row_count)
    failed = sum(m.error_count + m.warning_count for m in check_metrics)

    return TableScoreOut(
        source_table_fqn=table_fqn,
        score=round(score, 4) if score is not None else None,
        latest_run_id=rows[0]["run_id"],
        total_tests=input_row_count * len(check_metrics),
        failed_tests=failed,
    )


# Registered BEFORE the parameterized routes: FastAPI matches in
# declaration order, so a fixed path declared first can never be
# swallowed by a catch-all like /table/{table_fqn:path}.
@router.get(
    "/global",
    operation_id="getGlobalScore",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_global_score(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> GlobalScoreOut:
    """Return the cross-table DQ score over every table tracked in dq_metrics.

    Takes the latest run per *input_location*, silently filters out
    tables in catalogs the requesting user cannot access (same gate as
    the product endpoint — filtered, never 403), and averages the
    scored tables with an unweighted mean.
    """
    metrics_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    stmt = (
        f"WITH latest AS ("
        f"  SELECT input_location, run_id, "
        f"         ROW_NUMBER() OVER (PARTITION BY input_location ORDER BY run_time DESC) AS rn "
        f"  FROM {metrics_table}"  # noqa: S608
        f") SELECT DISTINCT input_location, run_id FROM latest WHERE rn = 1"
    )
    try:
        latest_runs = sql.query_dicts(stmt)
        fqns = [fqn for r in latest_runs if (fqn := r.get("input_location"))]
        accessible = [fqn for fqn in fqns if catalog_of(fqn) in user_catalogs]
        tables = [_compute_score_for_table(fqn, sql, app_conf) for fqn in accessible]
    except Exception as exc:
        logger.exception("Failed to compute global score")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    scored = [t.score for t in tables if t.score is not None]
    overall = ScoreService.compute_product_score(scored)
    return GlobalScoreOut(
        overall_score=round(overall, 4) if overall is not None else None,
        table_count=len(tables),
        tables=tables,
    )


@router.get(
    "/table/{table_fqn:path}",
    operation_id="getTableScore",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_table_score(
    table_fqn: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> TableScoreOut:
    """Return the row-weighted DQ score for a table's latest run."""
    try:
        validate_fqn(table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if catalog_of(table_fqn) not in user_catalogs:
        raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")

    try:
        return _compute_score_for_table(table_fqn, sql, app_conf)
    except Exception as exc:
        logger.exception("Failed to compute score for %s", table_fqn)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/product/{product_id}",
    operation_id="getProductScore",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_product_score(
    product_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    data_products: Annotated[DataProductService, Depends(get_data_product_service)],
) -> ProductScoreOut:
    """Return the unweighted mean DQ score over a data product's member tables.

    Members in catalogs the requesting user cannot access are silently
    excluded (filtered, not 403'd) — the same catalog gate as the table
    endpoint, applied per member. Members with no computable score are
    listed with a null score but excluded from the mean.
    """
    try:
        detail = data_products.get(product_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Data product not found: {product_id}")
        accessible = [m.table_fqn for m in detail.members if catalog_of(m.table_fqn) in user_catalogs]
        table_scores = [_compute_score_for_table(fqn, sql, app_conf) for fqn in accessible]
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to compute score for product %s", product_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    scored = [s.score for s in table_scores if s.score is not None]
    product_score = ScoreService.compute_product_score(scored)
    return ProductScoreOut(
        product_id=product_id,
        score=round(product_score, 4) if product_score is not None else None,
        member_table_scores=table_scores,
    )


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
            logger.warning(
                "Skipping binding %s in rule score: lookup failed", application.binding_id, exc_info=True
            )
            continue
        if detail is None:
            logger.warning("Skipping binding %s in rule score: binding not found", application.binding_id)
            continue
        fqn = detail.table.table_fqn
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
) -> RuleScoreOut:
    """Return the aggregate DQ score for a registry rule across its applied tables.

    *applied_to_count* is the TOTAL number of applications across all
    bindings — deliberately NOT restricted to the viewer's accessible
    catalogs, since the frontend uses ``applied_to_count == 0`` to mean
    "not applied anywhere". *per_table* applies the same silent catalog
    filter as the product endpoint and is deduplicated by table (a rule
    applied twice to one table is scored once).
    """
    try:
        applications = apply_rules.list_bindings_for_rule(rule_id)
        table_fqns = _resolve_binding_fqns(applications, monitored_tables)
        accessible = [fqn for fqn in table_fqns if catalog_of(fqn) in user_catalogs]
        per_table = [_compute_score_for_table(fqn, sql, app_conf) for fqn in accessible]
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
