"""Per-check evaluated-row counts for filtered row-level checks.

DQX skips rows where a check's *filter* is not TRUE, but the observer only
reports a table-wide *input_row_count*. This module builds one extra observer
metric so a filtered check's pass rate can use the rows it actually evaluated.
"""

import json
import logging
import re

import pyspark.sql.functions as F
from pyspark.errors import AnalysisException, ParseException
from pyspark.sql import DataFrame

from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.utils import is_sql_query_safe

logger = logging.getLogger(__name__)

EVALUATED_ROWS_METRIC = "check_evaluated_rows"

# String literals, quoted identifiers and comments, so keywords inside them are ignored.
_SQL_LITERAL_OR_COMMENT = re.compile(
    r"--[^\n]*|/\*.*?\*/|'(?:[^'\\]|''|\\.)*'|\"(?:[^\"\\]|\"\"|\\.)*\"|`(?:[^`]|``)*`",
    re.DOTALL,
)
_SELECT_KEYWORD = re.compile(r"\bselect\b", re.IGNORECASE)


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _has_subquery(sql: str) -> bool:
    return bool(_SELECT_KEYWORD.search(_SQL_LITERAL_OR_COMMENT.sub(" ", sql)))


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
        if not isinstance(rule, DQRowRule) or not rule.filter:
            continue
        row_filter = rule.filter
        if not row_filter.strip():
            continue
        # The raw filter is embedded as-is (DQX evaluates it unmodified); the trailing newline
        # terminates a final line comment so it cannot swallow the closing parenthesis.
        if not is_sql_query_safe(f"SELECT * FROM _t WHERE ({row_filter}\n)"):
            logger.warning("Skipping evaluated-row count for a check whose filter failed the SQL safety check")
            continue
        # Spark rejects subqueries inside the observed aggregate, which would fail the whole run.
        if _has_subquery(row_filter):
            logger.warning("Skipping evaluated-row count for a check whose filter uses a subquery")
            continue
        # JSON-encode the name (quotes, backslashes, control chars), then escape as a SQL literal.
        json_name_literal = _sql_string_literal(json.dumps(rule.name))
        entries.append(
            f"concat('{{\"check_name\":', {json_name_literal}, ',\"evaluated_count\":', "
            f"cast(count(case when ({row_filter}\n) then 1 end) as string), '}}')"
        )
    if not entries:
        return None
    return f"concat('[', concat_ws(',', {', '.join(entries)}), ']') as {EVALUATED_ROWS_METRIC}"


def with_evaluated_rows_metric(df: DataFrame, checks: list[dict[str, object]], custom_metrics: list[str]) -> list[str]:
    """Append the *check_evaluated_rows* metric to the observer's custom metrics when it is usable.

    The expression is analysed lazily against *df* (no Spark job runs); if Spark cannot
    analyse it, the metric is dropped so the run still completes and scores fall back to
    the table row count.

    Args:
        df: The DataFrame the checks will run on.
        checks: The run's DQX check dicts.
        custom_metrics: The validated user-defined custom metrics.

    Returns:
        *custom_metrics*, plus the evaluated-row metric when one was built and analysed.
    """
    metric = build_evaluated_rows_metric(checks)
    if metric is None:
        return custom_metrics
    try:
        _ = df.agg(F.expr(metric)).schema  # analysis only; raises on an unanalysable expression
    # The Spark Connect exception classes subclass these, so one clause covers classic and Connect.
    except (AnalysisException, ParseException):
        logger.warning("Skipping evaluated-row counts: the metric could not be analysed against the input data")
        return custom_metrics
    return [*custom_metrics, metric]
