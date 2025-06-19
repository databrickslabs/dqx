from collections.abc import Callable

from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import lit
from databricks.labs.dqx.executor import (
    DQRowRuleExecutor,
    DQDatasetRuleExecutor,
    DQRowRule,
    DQDatasetRule,
)


def dummy_row_check_func(*args, **kwargs) -> Column:
    return lit(f"row check args: {args}, kwargs: {kwargs}")


def dummy_dataset_check_func(*args, **kwargs) -> tuple[Column, Callable]:
    def apply(df: DataFrame) -> DataFrame:
        return df.limit(1)

    condition = lit(f"dataset check args: {args}, kwargs: {kwargs}")
    return condition, apply


def test_row_rule_executor_apply(spark):
    df = spark.createDataFrame([(1,)], ["id: int"])
    rule = DQRowRule(
        check_func=dummy_row_check_func,
        check_func_args=["arg1"],
        check_func_kwargs={"kwarg1": "value1"},
    )
    executor = DQRowRuleExecutor(rule)
    result = executor.apply(df)

    result_df = result.check_df.select(result.condition.alias("check"))
    df_condition = spark.createDataFrame(["row check args: ('arg1',), kwargs: {'kwarg1': 'value1'}"], "check: string")
    assert_df_equality(result_df, df_condition, ignore_nullable=True)


def test_dataset_rule_executor_apply(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    rule = DQDatasetRule(
        check_func=dummy_dataset_check_func,
        check_func_args=["arg1"],
        check_func_kwargs={"kwarg1": "value1"},
    )
    executor = DQDatasetRuleExecutor(rule)
    result = executor.apply(df)

    assert_df_equality(df.limit(1), result.check_df, ignore_nullable=True)

    result_df = result.check_df.select(result.condition.alias("check"))
    df_condition = spark.createDataFrame(
        ["dataset check args: ('arg1',), kwargs: {'kwarg1': 'value1'}"], "check: string"
    )
    assert_df_equality(result_df, df_condition, ignore_nullable=True)
