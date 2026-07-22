from unittest.mock import create_autospec, PropertyMock

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.executor import DQCheckResult
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule


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


def _make_manager_with_filter(df: DataFrame, spark: SparkSession, check: DQRowRule | DQDatasetRule) -> DQRuleManager:
    return DQRuleManager(
        check=check,
        df=df,
        spark=spark,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )


def test_rule_manager_skips_check_with_destructive_filter():
    """A check whose filter contains a destructive statement (e.g. DROP) is skipped (not evaluated) rather
    than aborting the whole rule set; the skip message identifies the unsafe filter."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQRowRule(check_func=check_funcs.is_not_null, column="col1", filter="id = 1 OR DROP TABLE users"),
    )

    assert manager.has_unsafe_filter is True
    # process() returns a skipped result struct instead of raising or running the executor
    result = manager.process()
    assert isinstance(result, DQCheckResult)
    condition_str = str(result.condition)
    assert "skipped" in condition_str.lower()
    assert "unsafe check filter" in condition_str


def test_rule_manager_suppress_skipped_removes_check_with_destructive_filter():
    """With suppress_skipped=True, a check with an unsafe filter produces no _errors/_warnings entry."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(check_func=check_funcs.is_not_null, column="col1", filter="id = 1 OR DROP TABLE users"),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=True,
    )
    result = manager.process()
    assert "CAST(NULL AS STRUCT" in str(result.condition)


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


def test_rule_manager_allows_select_filter():
    """A filter with a SELECT subquery is allowed (referential filters are a valid feature)."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(
            check_func=check_funcs.is_not_null,
            column="email",
            filter="customer_id IN (SELECT customer_id FROM main.ref.active_customers)",
        ),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )
    # a SELECT/subquery filter must not raise — compiling filter_condition is enough to prove it
    assert manager.filter_condition is not None


def test_rule_manager_skips_dataset_check_with_destructive_filter():
    """A dataset check's filter is pushed down to row_filter; a destructive one causes the check to be
    skipped (not evaluated) rather than aborting the whole rule set."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQDatasetRule(check_func=check_funcs.is_unique, columns=["col1"], filter="id = 1 OR DROP TABLE users"),
    )

    assert manager.has_unsafe_filter is True
    result = manager.process()
    assert "skipped" in str(result.condition).lower()


def test_rule_manager_skips_check_with_destructive_row_filter_in_kwargs():
    """An unsafe filter supplied directly as a row_filter kwarg (not the check-level filter) is also skipped."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQDatasetRule(
            check_func=check_funcs.is_unique,
            columns=["col1"],
            check_func_kwargs={"row_filter": "id = 1 OR DROP TABLE users"},
        ),
    )

    assert manager.has_unsafe_filter is True
    result = manager.process()
    assert "skipped" in str(result.condition).lower()
