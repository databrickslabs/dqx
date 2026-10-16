"""Per-check evaluated-row counts for filtered row-level checks.

DQX skips rows where a check's *filter* is not TRUE, but the observer only
reports a table-wide *input_row_count*. This module builds one extra observer
metric so a filtered check's pass rate can use the rows it actually evaluated.
Filters with a subquery can't run inside an observed metric, so they are
counted with one separate aggregate after the run's main action.

A filter that can't be counted fails the run: a silent fallback would score the
check against rows it never evaluated.
"""

import json
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.errors import AnalysisException, ParseException, PySparkException
from pyspark.sql import Column, DataFrame

from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.errors import ComputationError, InvalidCheckError, UnsafeSqlQueryError
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
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
_MAX_NAME_LENGTH = 200


@dataclass(frozen=True)
class EvaluatedRowsPlan:
    """How each filtered row check's evaluated rows are counted.

    Attributes:
        filters: Every filtered row check as (check name, filter), in check order.
        observed_metric: The observer metric counting the filters without a subquery, or None.
        separate_filters: The (check name, filter) pairs with a subquery, counted by a separate aggregate.
    """

    filters: list[tuple[str, str]]
    observed_metric: str | None
    separate_filters: list[tuple[str, str]]


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _has_subquery(sql: str) -> bool:
    return bool(_SELECT_KEYWORD.search(_SQL_LITERAL_OR_COMMENT.sub(" ", sql)))


def _quoted_names(names: Iterable[str]) -> str:
    """Check names for an error message: control characters replaced, length capped, quoted."""
    return ", ".join(f"'{_CONTROL_CHARS.sub(' ', name)[:_MAX_NAME_LENGTH]}'" for name in names)


def _count_where(row_filter: str, alias: str) -> Column:
    # F.expr parses the filter on its own, so parentheses can't break out into the aggregate.
    return F.count(F.when(F.expr(row_filter), 1)).alias(alias)


def plan_evaluated_rows(checks: list[dict[str, object]]) -> EvaluatedRowsPlan:
    """Plan how to count the rows each filtered row-level check evaluates.

    Args:
        checks: The run's DQX check dicts (as passed to *apply_checks_by_metadata*).

    Returns:
        The plan: an observer metric aliased *check_evaluated_rows* (yielding a JSON array of
        check_name / evaluated_count objects) for plain filters, and the subquery filters to count
        separately.

    Raises:
        UnsafeSqlQueryError: A filter failed the SQL safety check.
    """
    filters: list[tuple[str, str]] = []
    for rule in deserialize_checks(checks):
        if isinstance(rule, DQRowRule) and rule.filter and rule.filter.strip():
            filters.append((rule.name, rule.filter))

    # The raw filter is embedded as-is (DQX evaluates it unmodified); the trailing newline
    # terminates a final line comment so it cannot swallow the closing parenthesis.
    unsafe = [name for name, row_filter in filters if not is_sql_query_safe(f"SELECT * FROM _t WHERE ({row_filter}\n)")]
    if unsafe:
        raise UnsafeSqlQueryError(f"The filter of check(s) {_quoted_names(unsafe)} failed the SQL safety check")

    entries: list[str] = []
    separate: list[tuple[str, str]] = []
    for name, row_filter in filters:
        # Spark rejects subqueries inside an observed metric, so those are counted separately.
        if _has_subquery(row_filter):
            separate.append((name, row_filter))
            continue
        # JSON-encode the name (quotes, backslashes, control chars), then escape as a SQL literal.
        json_name_literal = _sql_string_literal(json.dumps(name))
        entries.append(
            f"concat('{{\"check_name\":', {json_name_literal}, ',\"evaluated_count\":', "
            f"cast(count(case when ({row_filter}\n) then 1 end) as string), '}}')"
        )
    metric = f"concat('[', concat_ws(',', {', '.join(entries)}), ']') as {EVALUATED_ROWS_METRIC}" if entries else None
    return EvaluatedRowsPlan(filters=filters, observed_metric=metric, separate_filters=separate)


def build_evaluated_rows_metric(checks: list[dict[str, object]]) -> str | None:
    """Build the *check_evaluated_rows* observer metric, or None.

    Args:
        checks: The run's DQX check dicts (as passed to *apply_checks_by_metadata*).

    Returns:
        The observer metric for the filters without a subquery, or None when there are none.

    Raises:
        UnsafeSqlQueryError: A filter failed the SQL safety check.
    """
    return plan_evaluated_rows(checks).observed_metric


def _analyses(df: DataFrame, *cols: Column) -> bool:
    try:
        _ = df.agg(*cols).schema  # analysis only; no Spark job runs
    # The Spark Connect exception classes subclass these, so one clause covers classic and Connect.
    except (AnalysisException, ParseException):
        return False
    return True


def prepare_evaluated_rows(df: DataFrame, checks: list[dict[str, object]]) -> EvaluatedRowsPlan:
    """Plan the evaluated-row counts and check, lazily, that Spark can compute them on *df*.

    Each filter is analysed as its own expression, then the assembled observer metric is
    analysed; neither runs a Spark job.

    Args:
        df: The DataFrame the checks will run on.
        checks: The run's DQX check dicts.

    Returns:
        The analysed plan.

    Raises:
        UnsafeSqlQueryError: A filter failed the SQL safety check.
        InvalidCheckError: A filter, or the observer metric, could not be analysed against *df*.
    """
    plan = plan_evaluated_rows(checks)
    if not plan.filters:
        return plan
    counts = [_count_where(row_filter, f"_f{i}") for i, (_, row_filter) in enumerate(plan.filters)]
    if not _analyses(df, *counts):
        # Narrow down to the failing check(s) so the error names them; still analysis only.
        failing = [name for (name, _), count in zip(plan.filters, counts) if not _analyses(df, count)]
        raise InvalidCheckError(
            f"Can't count the rows evaluated by filtered check(s) {_quoted_names(failing or [n for n, _ in plan.filters])}: "
            "the filter could not be analysed against the input data"
        )
    if plan.observed_metric is not None and not _analyses(df, F.expr(plan.observed_metric)):
        observed_names = [name for name, f in plan.filters if (name, f) not in plan.separate_filters]
        raise InvalidCheckError(
            f"Can't count the rows evaluated by filtered check(s) {_quoted_names(observed_names)}: "
            "the evaluated-rows metric could not be analysed against the input data"
        )
    return plan


def with_separate_evaluated_rows(
    df: DataFrame, plan: EvaluatedRowsPlan, observed: dict[str, object]
) -> dict[str, object]:
    """Count the subquery filters with one aggregate over *df* and merge them into the observed metrics.

    Call after the run's main action has materialised the observation. On a sample-limited run
    (*df.limit(n)* without ordering) this aggregate re-evaluates the limit, so its sample can differ
    from the checked one; scheduled and full runs are exact.

    Args:
        df: The DataFrame the checks ran on.
        plan: The plan from *prepare_evaluated_rows*.
        observed: The run's observed-metrics snapshot; not mutated.

    Returns:
        A copy of *observed* whose *check_evaluated_rows* payload also holds the separate counts,
        or *observed* itself when there is nothing to count separately.

    Raises:
        ComputationError: The separate aggregate failed.
    """
    if not plan.separate_filters:
        return observed
    names = [name for name, _ in plan.separate_filters]
    try:
        row = df.agg(*[_count_where(f, f"_f{i}") for i, (_, f) in enumerate(plan.separate_filters)]).collect()[0]
    except PySparkException as exc:
        raise ComputationError(f"Can't count the rows evaluated by filtered check(s) {_quoted_names(names)}") from exc
    raw_payload = observed.get(EVALUATED_ROWS_METRIC)
    entries: list[object] = json.loads(raw_payload) if isinstance(raw_payload, str) else []
    entries.extend({"check_name": name, "evaluated_count": int(row[i])} for i, name in enumerate(names))
    return {**observed, EVALUATED_ROWS_METRIC: json.dumps(entries, separators=(",", ":"))}
