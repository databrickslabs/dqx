from datetime import datetime
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column, SparkSession

from databricks.labs.dqx.rule import (
    DQRule,
)
from databricks.labs.dqx.schema.dq_result_schema import df_result_item_schema
from databricks.labs.dqx.utils import get_column_as_string


@dataclass(frozen=True)
class DQRuleProcessor:

    check: DQRule
    spark: SparkSession
    df: DataFrame
    engine_user_metadata: dict[str, str]
    run_time: datetime

    @property
    def user_metadata(self) -> dict[str, str]:
        """
        Returns user metadata as a dictionary.
        """
        if self.check.user_metadata is not None:
            # Checks defined in the user metadata override checks defined in the engine
            return (self.engine_user_metadata or {}) | self.check.user_metadata
        return self.engine_user_metadata or {}

    @property
    def filter_condition(self):
        """
        Returns the filter condition for the check.
        """
        return F.expr(self.check.filter) if self.check.filter else F.lit(True)

    def process(self) -> tuple[Column, DataFrame]:
        """
        Process the data quality rule and return a tuple containing:
        - Column with the check result
        - DataFrame with the results of the check
        """
        check_df, condition = self.check.apply(self.spark, self.df)
        name = self.check.name if self.check.name else get_column_as_string(condition, normalize=True)

        result = F.struct(
            F.lit(name).alias("name"),
            condition.alias("message"),
            self.check.columns_as_string_expr.alias("columns"),
            F.lit(self.check.filter or None).cast("string").alias("filter"),
            F.lit(self.check.check_func.__name__).alias("function"),
            F.lit(self.run_time).alias("run_time"),
            F.create_map(*[item for kv in self.user_metadata.items() for item in (F.lit(kv[0]), F.lit(kv[1]))]).alias(
                "user_metadata"
            ),
        ).cast(df_result_item_schema)

        check_result = F.when(self.filter_condition & condition.isNotNull(), result)
        return check_result, check_df
