from datetime import datetime
from dataclasses import dataclass
from functools import cached_property

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column, SparkSession

from databricks.labs.dqx.executor import DQCheckResult, DQRuleExecutorFactory
from databricks.labs.dqx.rule import (
    DQRule,
)
from databricks.labs.dqx.schema.dq_result_schema import dq_result_item_schema


@dataclass(frozen=True)
class DQRuleManager:
    """
    Orchestrates the application of a data quality rule to a DataFrame and builds the final check result.

    The manager is responsible for:
    - Executing the rule using the appropriate row or dataset executor.
    - Applying any filter condition specified in the rule to the check result.
    - Combining user-defined and engine-provided metadata into the result.
    - Constructing the final structured output (including check name, function, columns, metadata, etc.)
      as a DQCheckResult.

    The manager does not implement the logic of individual checks. Instead, it delegates
    rule application to the appropriate DQRuleExecutor based on the rule type (row-level or dataset-level).

    Attributes:
        check: The DQRule instance that defines the check to apply.
        df: The DataFrame on which to apply the check.
        engine_user_metadata: Metadata provided by the engine (overridden by check.user_metadata if present).
        run_time: The timestamp when the check is executed.
        ref_dfs: Optional reference DataFrames for dataset-level checks.
    """

    check: DQRule
    df: DataFrame
    spark: SparkSession
    engine_user_metadata: dict[str, str]
    run_time: datetime
    ref_dfs: dict[str, DataFrame] | None = None

    @cached_property
    def user_metadata(self) -> dict[str, str]:
        """
        Returns user metadata as a dictionary.
        """
        if self.check.user_metadata is not None:
            # Checks defined in the user metadata override checks defined in the engine
            return (self.engine_user_metadata or {}) | self.check.user_metadata
        return self.engine_user_metadata or {}

    @cached_property
    def filter_condition(self):
        """
        Returns the filter condition for the check.
        """
        return F.expr(self.check.filter) if self.check.filter else F.lit(True)

    def process(self) -> DQCheckResult:
        """
        Process the data quality rule and return results as DQCheckResult containing:
        - Column with the check result
        - optional DataFrame with the results of the check
        """
        executor = DQRuleExecutorFactory.create(self.check)
        raw_result = executor.apply(self.df, self.spark, self.ref_dfs)
        return self._wrap_result(raw_result)

    def _wrap_result(self, raw_result: DQCheckResult) -> DQCheckResult:
        result_struct = self._build_result_struct(raw_result.condition)
        check_result = F.when(self.filter_condition & raw_result.condition.isNotNull(), result_struct)

        return DQCheckResult(condition=check_result, check_df=raw_result.check_df)

    def _build_result_struct(self, condition: Column) -> Column:
        return F.struct(
            F.lit(self.check.name).alias("name"),
            condition.alias("message"),
            self.check.columns_as_string_expr.alias("columns"),
            F.lit(self.check.filter or None).cast("string").alias("filter"),
            F.lit(self.check.check_func.__name__).alias("function"),
            F.lit(self.run_time).alias("run_time"),
            F.create_map(*[item for kv in self.user_metadata.items() for item in (F.lit(kv[0]), F.lit(kv[1]))]).alias(
                "user_metadata"
            ),
        ).cast(dq_result_item_schema)
