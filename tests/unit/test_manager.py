from unittest.mock import create_autospec, PropertyMock

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.executor import DQCheckResult
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import DQRowRule


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
