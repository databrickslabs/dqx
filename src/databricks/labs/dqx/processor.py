from datetime import datetime
from dataclasses import dataclass
from functools import cached_property

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

from databricks.labs.dqx.rule import (
    DQRule,
    DQDatasetRule,
    DQRowRule,
)
from databricks.labs.dqx.schema.dq_result_schema import df_result_item_schema


@dataclass(frozen=True)
class DQCheckResult:
    condition: Column
    check_df: DataFrame


@dataclass(frozen=True)
class DQRuleProcessor:

    check: DQRule
    df: DataFrame
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
        if isinstance(self.check, DQDatasetRule):
            return self._process_dataset_rule(self.check)
        if isinstance(self.check, DQRowRule):
            return self._process_row_rule(self.check)

        raise ValueError(f"Unsupported rule type: {type(self.check)}")

    def _process_dataset_rule(self, check: DQDatasetRule) -> DQCheckResult:
        condition, check_df = check.apply(self.df, self.ref_dfs)

        result = self._build_result_struct(condition)
        check_result = F.when(self.filter_condition & condition.isNotNull(), result)
        return DQCheckResult(condition=check_result, check_df=check_df)

    def _process_row_rule(self, check: DQRowRule) -> DQCheckResult:
        condition = check.apply()

        result = self._build_result_struct(condition)
        check_result = F.when(self.filter_condition & condition.isNotNull(), result)
        return DQCheckResult(condition=check_result, check_df=self.df)

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
        ).cast(df_result_item_schema)
