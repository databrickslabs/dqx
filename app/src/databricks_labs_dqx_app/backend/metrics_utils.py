"""Shared parsing helpers for the long-format ``dq_metrics`` table.

Used by the metrics and dq-score routes (and, later, the product/rule/
global score endpoints), so they live here rather than as private
helpers inside one route module.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown


def catalog_of(fqn: str) -> str:
    """Extract the catalog part from a fully qualified table name."""
    parts = fqn.split(".", 1)
    return parts[0] if parts else ""


def safe_int(value: Any) -> int | None:
    """Best-effort string→int that tolerates ``None`` and decimal strings."""
    if value in (None, ""):
        return None
    try:
        # Accept '123', '123.0', 123, 123.0 — counts can be promoted to
        # bigint by Spark and arrive as strings.
        return int(float(value))
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    """Best-effort string→float that tolerates ``None`` and empty strings.

    The Statement Execution API returns every value as a string, so
    MEASURE() results arrive as e.g. ``'0.9'`` — parse them without
    letting a malformed value crash the response mapping.
    """
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_check_metrics(raw: Any) -> list[CheckMetricBreakdown]:
    """Parse the ``check_metrics`` JSON-string emitted by the observer."""
    # Local import breaks the module-level cycle this helper module would
    # otherwise create: ``models`` imports the OLTP services, and those
    # services import the ``safe_int``/``safe_float`` coercers above (via
    # ``score_cache_service``). Same deferred-import convention as
    # ``_scheduler_registry.notify_scheduler``'s call sites.
    from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown

    if not raw:
        return []
    try:
        items = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(items, list):
        return []
    out: list[CheckMetricBreakdown] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append(
            CheckMetricBreakdown(
                check_name=str(item.get("check_name") or "unknown"),
                error_count=int(item.get("error_count") or 0),
                warning_count=int(item.get("warning_count") or 0),
            )
        )
    return out
