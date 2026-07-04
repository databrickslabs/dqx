import pytest
from unittest.mock import create_autospec, PropertyMock

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.executor import DQCheckResult
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.labs.dqx.errors import UnsafeSqlQueryError


def _make_manager_with_missing_column(df: DataFrame, spark: SparkSession, *, suppress_skipped: bool) -> DQRuleManager:
    return DQRuleManager(
        check=DQRowRule(check_func=check_funcs.is_not_null, column="missing_col"),
        df=df,
        spark=spark,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=suppress_skipped,
    )


def test_rule_manager_suppress_skipped_returns_null_condition_for_invalid_column():
    """When suppress_skipped=True and a column cannot be resolved, process() returns a null-cast Column
    so the check produces no entry in _errors/_warnings."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(df_mock.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'missing_col' not found")
    )

    manager = _make_manager_with_missing_column(df_mock, spark_mock, suppress_skipped=True)
    result = manager.process()

    assert isinstance(result, DQCheckResult)
    assert result.condition is not None
    assert "CAST(NULL AS STRUCT" in str(result.condition)
    assert result.check_df is df_mock


def test_rule_manager_suppress_skipped_false_returns_struct_for_invalid_column():
    """When suppress_skipped=False and a column cannot be resolved, process() returns a result struct
    (not null) so the skipped check is recorded in _errors/_warnings."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(df_mock.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'missing_col' not found")
    )

    manager = _make_manager_with_missing_column(df_mock, spark_mock, suppress_skipped=False)
    result = manager.process()

    assert isinstance(result, DQCheckResult)
    assert result.condition is not None
    assert "skipped" in str(result.condition).lower()
    assert result.check_df is df_mock


def test_rule_manager_raises_on_unsafe_filter():
    """A check whose filter contains a forbidden statement (e.g. SELECT) is rejected
    end-to-end: process() raises UnsafeSqlQueryError via the wired safe_filter_expr guard."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(
            check_func=check_funcs.is_not_null,
            column="col1",
            filter="id IN (SELECT id FROM users)",
        ),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )
    with pytest.raises(UnsafeSqlQueryError):
        manager.process()


def test_rule_manager_allows_safe_filter():
    """A normal predicate filter passes the guard (does not raise)."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(
            check_func=check_funcs.is_not_null,
            column="col1",
            filter="country = 'US'",
        ),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )
    # accessing filter_condition compiles the filter; a safe one must not raise
    assert manager.filter_condition is not None   


def test_rule_manager_raises_on_unsafe_filter_for_dataset_check():
    """A dataset check's filter is pushed down to row_filter; an unsafe one is rejected
    end-to-end through the check_funcs safe_filter_expr wiring."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQDatasetRule(
            check_func=check_funcs.is_unique,
            columns=["col1"],
            filter="id IN (SELECT id FROM users)",
        ),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )
    with pytest.raises(UnsafeSqlQueryError):
        manager.process()
