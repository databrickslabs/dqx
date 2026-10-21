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


def test_rule_manager_validates_columns_against_original_input_schema():
    current_df = create_autospec(DataFrame)
    input_df = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(input_df.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'derived_col' not found")
    )
    manager = DQRuleManager(
        check=DQRowRule(check_func=check_funcs.is_not_null, column="derived_col"),
        df=current_df,
        input_df=input_df,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
    )

    result = manager.process()

    assert "skipped" in str(result.condition).lower()
    input_df.select.assert_called()
    current_df.select.assert_called_once()


def test_rule_manager_validates_filter_against_current_dataframe():
    current_df = create_autospec(DataFrame)
    input_df = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(current_df.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'derived_col' not found")
    )
    manager = DQRuleManager(
        check=DQRowRule(check_func=check_funcs.is_not_null, column="base_col", filter="derived_col > 0"),
        df=current_df,
        input_df=input_df,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
    )
    assert manager.has_invalid_filter is True
    current_df.select.assert_called()
    input_df.select.assert_not_called()


def test_rule_manager_is_skipped_for_invalid_column():
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(df_mock.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'missing_col' not found")
    )

    manager = _make_manager_with_missing_column(df_mock, spark_mock, suppress_skipped=False)

    assert manager.is_skipped is True


def test_rule_manager_is_not_skipped_for_valid_check():
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(check_func=check_funcs.is_not_null, column="col1"),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
    )
    assert manager.is_skipped is False


def test_rule_manager_validates_message_expression_against_current_dataframe():
    current_df = create_autospec(DataFrame)
    input_df = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(current_df.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'derived_message' not found")
    )
    manager = DQRuleManager(
        check=DQRowRule(
            check_func=check_funcs.is_not_null,
            column="base_col",
            message_expr="derived_message",
        ),
        df=current_df,
        input_df=input_df,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
    )
    assert manager.has_invalid_custom_message is True
    current_df.select.assert_called()
    input_df.select.assert_not_called()


def test_rule_manager_is_skipped_for_unresolvable_row_filter_kwarg(caplog):
    df_mock = create_autospec(DataFrame)

    def _select(column):
        if "missing_col" in str(column):
            raise AnalysisException("Column 'missing_col' not found")
        return df_mock

    df_mock.select.side_effect = _select
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQDatasetRule(
            check_func=check_funcs.is_unique, columns=["col1"], check_func_kwargs={"row_filter": "missing_col > 1"}
        ),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
    )
    assert manager.has_invalid_columns is False
    assert manager.has_invalid_filter is True
    assert manager.is_skipped is True
    manager.process()
    assert "due to invalid check filter: 'missing_col > 1'" in caplog.text


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


def test_rule_manager_skips_check_with_destructive_row_filter_positional():
    """An unsafe row_filter supplied positionally via check_func_args is also detected and skipped, so it
    cannot slip past the manager gate and hard-raise in the executor."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    # has_no_outliers(column, row_filter=None): pass row_filter positionally through check_func_args
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQDatasetRule(
            check_func=check_funcs.has_no_outliers,
            column="col1",
            check_func_args=["id = 1 OR DROP TABLE users"],
        ),
    )

    assert manager.has_unsafe_filter is True
    result = manager.process()
    assert "skipped" in str(result.condition).lower()
