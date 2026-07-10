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
    get_conf,
    get_data_product_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.metrics_utils import (
    catalog_of,
    parse_check_metrics,
    safe_int,
)
from databricks_labs_dqx_app.backend.models import ProductScoreOut, TableScoreOut
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
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
