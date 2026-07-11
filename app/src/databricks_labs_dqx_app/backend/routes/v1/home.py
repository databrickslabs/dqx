"""Homepage stats — counts + cached overall score, composed server-side.

One endpoint backs the homepage "At a Glance" stat cards (the port of
dqlake's ``/home/stats``). The three counts are cheap app-DB COUNT(*)
queries (registry rules, monitored tables, table spaces — the same
Lakebase/Delta-OLTP round-trips the list pages already make), and the
overall DQ score card reads the ``dq_score_cache`` 'global' row that the
run-completion refresh maintains (P3.4). Nothing here ever touches the
warehouse, so the landing page stays milliseconds-fast. dqlake's extra
in-process TTL cache (``home_stats_cache.py``) existed to hide a ~12s
inline warehouse read; with Postgres-only reads it is not needed — layer
it later only if these counts ever show up hot.

Scope caveat (explicit, reviewable): the cached global aggregate is NOT
catalog-scoped — it spans ALL monitored tables, so a viewer whose catalog
access is narrower still sees the org-wide number (deliberate for the
homepage "overall health" card; the per-table surfaces stay catalog-
filtered). The counts are likewise org-wide, matching dqlake's
control-plane counts.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_data_product_service,
    get_monitored_table_service,
    get_registry_service,
    get_score_cache_service,
    require_role,
)
from databricks_labs_dqx_app.backend.models import HomeStatsOut, ScoreTrendPointOut
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.score_cache_service import (
    GLOBAL_SCOPE_KEY,
    SCOPE_GLOBAL,
    ScoreCacheService,
)

logger = logging.getLogger(__name__)
router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]

# How many global ``dq_score_history`` points feed the homepage trend
# chart — dqlake's home trend showed the recent run history, not the
# full archive (the history table keeps HISTORY_KEEP_ROWS per scope).
_TREND_POINTS = 30


def _trend_from_history(score_cache: ScoreCacheService) -> list[ScoreTrendPointOut]:
    """Map the global score-history points onto the response trend.

    Points arrive oldest-first from *get_history*; rows missing a score
    or timestamp are dropped defensively (the append path never writes
    them, but the trend must not 500 over a hand-edited row).
    """
    return [
        ScoreTrendPointOut(ts=p.computed_at, score=p.score)
        for p in score_cache.get_history(SCOPE_GLOBAL, GLOBAL_SCOPE_KEY, limit=_TREND_POINTS)
        if p.score is not None and p.computed_at is not None
    ]


@router.get(
    "/stats",
    operation_id="getHomeStats",
    response_model=HomeStatsOut,
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_home_stats(
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    products: Annotated[DataProductService, Depends(get_data_product_service)],
    score_cache: Annotated[ScoreCacheService, Depends(get_score_cache_service)],
) -> HomeStatsOut:
    """Return the homepage stat-card numbers + score trend in one response.

    A never-populated score cache serves ``score=None`` (the homepage
    renders an em dash); a populated row whose score is NULL ("computed,
    nothing found") still carries *computed_at* so the two are
    distinguishable. *score_trend*/*score_delta* come from the
    ``dq_score_history`` append rows (P3.5) — still zero warehouse.
    """
    try:
        rule_count = registry.count()
        monitored_table_count = monitored_tables.count()
        table_space_count = products.count()
        cached = score_cache.get_many(SCOPE_GLOBAL, [GLOBAL_SCOPE_KEY]).get(GLOBAL_SCOPE_KEY)
        trend = _trend_from_history(score_cache)
    except Exception as exc:
        logger.exception("Failed to compose homepage stats")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # Change since the previous recompute — the last two points of the same
    # trend the homepage chart plots (dqlake's delta semantics). Rounded to
    # the cache's own 4-decimal score precision so float noise never flips
    # the flat/up/down badge.
    score_delta = round(trend[-1].score - trend[-2].score, 4) if len(trend) >= 2 else None

    return HomeStatsOut(
        rule_count=rule_count,
        monitored_table_count=monitored_table_count,
        table_space_count=table_space_count,
        score=cached.score if cached else None,
        failed_tests=cached.failed_tests if cached else None,
        total_tests=cached.total_tests if cached else None,
        computed_at=cached.computed_at if cached else None,
        score_trend=trend,
        score_delta=score_delta,
    )
