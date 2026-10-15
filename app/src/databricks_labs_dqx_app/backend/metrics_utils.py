"""Shared parsing helpers for the long-format ``dq_metrics`` table.

Used by the metrics and dq-score routes (and, later, the product/rule/
global score endpoints), so they live here rather than as private
helpers inside one route module.
"""

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown


def catalog_of(fqn: str) -> str:
    """Extract the catalog part from a fully qualified table name.

    A backtick-quoted catalog (```my.catalog`.schema.table``) is returned
    unquoted, with doubled backticks collapsed, so it matches the plain
    catalog name.
    """
    if fqn.startswith("`"):
        i = 1
        while i < len(fqn):
            if fqn[i] == "`":
                if fqn[i + 1 : i + 2] == "`":
                    i += 2
                    continue
                return fqn[1:i].replace("``", "`")
            i += 1
    parts = fqn.split(".", 1)
    return parts[0] if parts else ""


def schema_of(fqn: str) -> str:
    """Extract the ``catalog.schema`` prefix from a fully qualified table name.

    Returns the two-part schema identity (not the bare schema name) so it is
    unambiguous across catalogs — two catalogs can both hold a ``sales``
    schema. Empty string when the FQN has fewer than two parts.
    """
    parts = fqn.split(".")
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else ""


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


def _parse_evaluated_counts(raw: Any) -> dict[str, int]:
    """check_name -> evaluated rows from the *check_evaluated_rows* payload.

    Tolerant of an absent or corrupt payload (empty map) and of malformed
    entries (skipped), so a bad value only drops the evaluated count — the
    check then falls back to the table's input rows.
    """
    if not raw:
        return {}
    try:
        items = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(items, list):
        return {}
    out: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict) or not item.get("check_name"):
            continue
        count = safe_int(item.get("evaluated_count"))
        if count is not None:
            out[str(item["check_name"])] = count
    return out


def parse_check_metrics(raw: Any, evaluated_raw: Any = None) -> "list[CheckMetricBreakdown]":
    """Parse the ``check_metrics`` JSON-string emitted by the observer.

    Args:
        raw: The run's *check_metrics* metric value.
        evaluated_raw: Optional *check_evaluated_rows* metric value; each
            listed check gets its *evaluated_count*. Absent or corrupt
            payloads leave every *evaluated_count* None.
    """
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
    evaluated = _parse_evaluated_counts(evaluated_raw)
    out: list[CheckMetricBreakdown] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        check_name = str(item.get("check_name") or "unknown")
        out.append(
            CheckMetricBreakdown(
                check_name=check_name,
                error_count=int(item.get("error_count") or 0),
                warning_count=int(item.get("warning_count") or 0),
                evaluated_count=evaluated.get(check_name),
            )
        )
    return out
