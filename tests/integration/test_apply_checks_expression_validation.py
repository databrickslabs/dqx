"""Integration tests for validating check SQL expressions (filters, custom messages, *sql_expression*) against
the input DataFrame before the checks are applied.

Split out of test_apply_checks.py to keep that module under pylint's max-module-lines limit.
"""

import uuid
from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule, register_rule, row_preserving
from databricks.labs.dqx import check_funcs
from tests.integration.conftest import EXTRA_PARAMS


def _drop_column_check(column: str) -> tuple[Column, Callable]:
    """Dataset-level check that drops *column* from its result and never fails."""
    condition_col = f"condition_{uuid.uuid4().hex}"

    def apply(df: DataFrame) -> DataFrame:
        return df.drop(column).withColumn(condition_col, F.lit(False))

    return make_condition(F.col(condition_col), f"'{column}' dropped", f"{column}_dropped"), apply


@register_rule("dataset")
def drops_column(column: str) -> tuple[Column, Callable]:
    """Custom dataset-level check, not marked as row-preserving, that drops *column*."""
    return _drop_column_check(column)


@row_preserving
@register_rule("dataset")
def drops_column_row_preserving(column: str) -> tuple[Column, Callable]:
    """Custom dataset-level check, marked as row-preserving, that still drops *column*."""
    return _drop_column_check(column)


@row_preserving
@register_rule("dataset")
def retypes_column_row_preserving(column: str) -> tuple[Column, Callable]:
    """Custom dataset-level check, marked as row-preserving, that replaces *column* with a struct of the same name."""
    condition_col = f"condition_{uuid.uuid4().hex}"

    def apply(df: DataFrame) -> DataFrame:
        return df.withColumn(column, F.struct(F.col(column).alias("v"))).withColumn(condition_col, F.lit(False))

    return make_condition(F.col(condition_col), f"'{column}' retyped", f"{column}_retyped"), apply


def _skip_messages(checked: DataFrame, result_col: str, check_name: str) -> list[str]:
    """Returns the skip message of *check_name* in *result_col* for each row (asserting it is skipped)."""
    messages = []
    for row in checked.select(result_col).collect():
        entries = [entry for entry in (row[result_col] or []) if entry["name"] == check_name]
        assert len(entries) == 1, f"{check_name}: {row[result_col]}"
        assert entries[0]["skipped"] is True
        messages.append(entries[0]["message"])
    return messages


def test_apply_checks_filter_cannot_resolve_against_another_check_expression(ws, spark):
    """Check expressions are validated together in a single schema analysis. A filter referring to a missing
    column must still be skipped as invalid, even when that column name could resolve against another expression
    of the same analysis (lateral column alias)."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None], [2, "ok"]], "id int, b string")

    checks = [
        DQRowRule(
            name="b_is_null_valid_filter",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="b",
            filter="id > 0",
        ),
        DQRowRule(
            name="b_is_null_missing_column_filter",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="b",
            filter="expr_0 IS NOT NULL",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)
    errors_by_row = {row["id"]: row["_errors"] for row in checked.select("id", "_errors").collect()}

    for row_id in (1, 2):
        skipped = [e for e in (errors_by_row[row_id] or []) if e["name"] == "b_is_null_missing_column_filter"]
        assert len(skipped) == 1, f"row {row_id}: {errors_by_row[row_id]}"
        assert skipped[0]["skipped"] is True
        assert skipped[0]["message"] == "Check evaluation skipped due to invalid check filter: 'expr_0 IS NOT NULL'"

    valid_row1 = [e for e in (errors_by_row[1] or []) if e["name"] == "b_is_null_valid_filter"]
    assert len(valid_row1) == 1
    assert valid_row1[0]["skipped"] is None
    assert not [e for e in (errors_by_row[2] or []) if e["name"] == "b_is_null_valid_filter"]


def test_apply_checks_column_dropped_by_row_changing_check_is_skipped(ws, spark):
    """A check that may change the rows (an unmarked custom dataset-level check) ends the reuse of the input schema:
    a later check on a column it dropped is skipped as invalid instead of failing the run."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None], [2, "ok"]], "id int, b string")

    checks = [
        DQDatasetRule(criticality="error", check_func=drops_column, check_func_kwargs={"column": "b"}),
        DQRowRule(name="b_is_null", criticality="error", check_func=check_funcs.is_not_null, column="b"),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    assert (
        _skip_messages(checked, "_errors", "b_is_null")
        == ["Check evaluation skipped due to invalid check columns: ['b']"] * 2
    )


def test_apply_checks_warning_on_column_dropped_by_row_changing_error_check_is_skipped(ws, spark):
    """The warning checks are applied after the error checks: once an error check may have changed the rows, the
    warning checks no longer reuse the input schema, so a warning on a column it dropped is skipped as invalid."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None], [2, "ok"]], "id int, b string")

    checks = [
        DQDatasetRule(criticality="error", check_func=drops_column, check_func_kwargs={"column": "b"}),
        DQRowRule(name="b_is_null", criticality="warn", check_func=check_funcs.is_not_null, column="b"),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    assert (
        _skip_messages(checked, "_warnings", "b_is_null")
        == ["Check evaluation skipped due to invalid check columns: ['b']"] * 2
    )


def test_apply_checks_filter_on_column_dropped_by_row_preserving_check_is_skipped(ws, spark):
    """If a row-preserving check drops an input column anyway, the filter of a later check on that column resolved
    against the input when the expressions were validated up front, but must be skipped as invalid instead of
    failing the run."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None], [2, "ok"]], "id int, b string")

    checks = [
        DQDatasetRule(criticality="error", check_func=drops_column_row_preserving, check_func_kwargs={"column": "b"}),
        DQRowRule(
            name="id_is_null_filtered_on_b",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="id",
            filter="b IS NOT NULL",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    assert (
        _skip_messages(checked, "_errors", "id_is_null_filtered_on_b")
        == ["Check evaluation skipped due to invalid check filter: 'b IS NOT NULL'"] * 2
    )


def test_apply_checks_filter_on_column_retyped_by_row_preserving_check_is_skipped(ws, spark):
    """If a row-preserving check replaces an input column under the same name with a different type anyway, the
    filter of a later check resolved against the input type when the expressions were validated up front, but no
    longer resolves against the new type, so it must be skipped as invalid instead of failing the run."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None], [2, "ok"]], "id int, b string")

    checks = [
        DQDatasetRule(criticality="error", check_func=retypes_column_row_preserving, check_func_kwargs={"column": "b"}),
        DQRowRule(
            name="id_is_null_filtered_on_b",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="id",
            filter="b LIKE 'o%'",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    assert (
        _skip_messages(checked, "_errors", "id_is_null_filtered_on_b")
        == ["Check evaluation skipped due to invalid check filter: 'b LIKE 'o%''"] * 2
    )
