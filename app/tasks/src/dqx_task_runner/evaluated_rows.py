"""Per-check evaluated-row counts for filtered row-level checks.

DQX skips rows where a check's *filter* is not TRUE, but the observer only
reports a table-wide *input_row_count*. This module builds one extra observer
metric so a filtered check's pass rate can use the rows it actually evaluated.
"""

import json
import logging

from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.utils import is_sql_query_safe

logger = logging.getLogger(__name__)

EVALUATED_ROWS_METRIC = "check_evaluated_rows"


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _strip_line_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def build_evaluated_rows_metric(checks: list[dict[str, object]]) -> str | None:
    """Build the *check_evaluated_rows* observer metric, or None.

    Args:
        checks: The run's DQX check dicts (as passed to *apply_checks_by_metadata*).

    Returns:
        A Spark SQL aggregate expression aliased *check_evaluated_rows* that yields a
        JSON array of check_name / evaluated_count objects, or None when no row-level
        check carries a safe filter.
    """
    entries: list[str] = []
    for rule in deserialize_checks(checks):
        if not isinstance(rule, DQRowRule) or not rule.filter or not rule.filter.strip():
            continue
        row_filter = _strip_line_comments(rule.filter)
        if not is_sql_query_safe(f"SELECT * FROM _t WHERE ({row_filter})"):
            logger.warning("Skipping evaluated-row count for a check whose filter failed the SQL safety check")
            continue
        # JSON-encode the name (quotes, backslashes, control chars), then escape as a SQL literal.
        json_name_literal = _sql_string_literal(json.dumps(rule.name))
        entries.append(
            f"concat('{{\"check_name\":', {json_name_literal}, ',\"evaluated_count\":', "
            f"cast(count(case when ({row_filter}) then 1 end) as string), '}}')"
        )
    if not entries:
        return None
    return f"concat('[', concat_ws(',', {', '.join(entries)}), ']') as {EVALUATED_ROWS_METRIC}"
