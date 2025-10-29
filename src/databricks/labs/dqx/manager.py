import logging
from datetime import datetime
from dataclasses import dataclass
from functools import cached_property

import pyspark.sql.functions as F
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, Column, SparkSession

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.executor import DQCheckResult, DQRuleExecutorFactory
from databricks.labs.dqx.rule import (
    DQRule,
)
from databricks.labs.dqx.schema.dq_result_schema import dq_result_item_schema
from databricks.labs.dqx.utils import get_column_name_or_alias

logger = logging.getLogger(__name__)


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
        run_time_overwrite: Optional timestamp override. If None, current_timestamp() is used for per-micro-batch timestamps.
        ref_dfs: Optional reference DataFrames for dataset-level checks.
        run_id: Optional unique run id.
    """

    check: DQRule
    df: DataFrame
    spark: SparkSession
    engine_user_metadata: dict[str, str]
    run_time_overwrite: datetime | None
    run_id: str
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
    def filter_condition(self) -> Column:
        """
        Returns the filter condition for the check.
        """
        return F.expr(self.check.filter) if self.check.filter else F.lit(True)

    @cached_property
    def invalid_columns(self) -> list[str]:
        """
        Returns list of invalid check columns in the input DataFrame.
        """
        invalid_cols = []

        if self.check.column is not None and self._is_invalid_column(self.check.column):
            invalid_cols.append(get_column_name_or_alias(self.check.column))
        elif self.check.columns is not None:  # either column or columns can be provided, but not both
            for column in self.check.columns:
                if self._is_invalid_column(column):
                    invalid_cols.append(get_column_name_or_alias(column))

        return invalid_cols

    @cached_property
    def has_invalid_columns(self) -> bool:
        """
        Returns a boolean indicating whether any of the specified check columns are invalid in the input DataFrame.
        """
        return bool(self.invalid_columns)

    @cached_property
    def has_invalid_filter(self) -> bool:
        """
        Returns a boolean indicating whether the filter is invalid in the input DataFrame.
        """
        return self._is_invalid_column(self.filter_condition)

    @cached_property
    def invalid_sql_expression(self) -> str | None:
        """
        Returns an invalid expression for sql expression check.
        """
        if self.check.check_func is check_funcs.sql_expression:
            if "expression" in self.check.check_func_kwargs:
                field_value = self.check.check_func_kwargs["expression"]
            elif self.check.check_func_args:
                field_value = self.check.check_func_args[0]
            else:
                return None  # should never happen, as it is validated for correct args when building rules

            if self._is_invalid_column(field_value):
                return field_value
        return None

    def process(self) -> DQCheckResult:
        """
        Process the data quality rule (check) and return results as DQCheckResult containing:
        - Column with the check result
        - optional DataFrame with the results of the check

        Skip the check evaluation if column or columns, or filter in the check cannot be resolved in the input
        DataFrame. Return the check result preserving all fields with message identifying invalid fields.
        """
        invalid_cols_message = self._get_invalid_cols_message()
        if invalid_cols_message:
            # overwrite message but preserve all other fields in the result
            result_struct = self._build_result_struct(condition=F.lit(invalid_cols_message))
            return DQCheckResult(condition=result_struct, check_df=self.df)

        executor = DQRuleExecutorFactory.create(self.check)
        raw_result = executor.apply(self.df, self.spark, self.ref_dfs)
        return self._wrap_result(raw_result)

    def _wrap_result(self, raw_result: DQCheckResult) -> DQCheckResult:
        result_struct = self._build_result_struct(raw_result.condition)
        check_result = F.when(self.filter_condition & raw_result.condition.isNotNull(), result_struct)
        return DQCheckResult(condition=check_result, check_df=raw_result.check_df)

    def _build_result_struct(self, condition: Column) -> Column:
        # Use current_timestamp() to make sure streaming gets per-micro-batch timestamps,
        # or use literal run time if explicitly overridden
        run_time_expr = F.current_timestamp() if self.run_time_overwrite is None else F.lit(self.run_time_overwrite)

        return F.struct(
            F.lit(self.check.name).alias("name"),
            condition.alias("message"),
            self.check.columns_as_string_expr.alias("columns"),
            F.lit(self.check.filter or None).cast("string").alias("filter"),
            F.lit(self.check.check_func.__name__).alias("function"),
            run_time_expr.alias("run_time"),
            F.lit(self.run_id).alias("run_id"),
            F.create_map(*[item for kv in self.user_metadata.items() for item in (F.lit(kv[0]), F.lit(kv[1]))]).alias(
                "user_metadata"
            ),
        ).cast(dq_result_item_schema)

    def _get_invalid_cols_message(self) -> str:
        """
        Returns invalid columns message containing info about invalid columns to check should be applied to or filter.
        """
        invalid_cols_message_parts = []

        if self.has_invalid_columns:
            logger.warning(
                f"Skipping check evaluation '{self.check.name}' due to invalid check columns: {self.invalid_columns}"
            )
            invalid_cols_message_parts.append(
                f"Check evaluation skipped due to invalid check columns: {self.invalid_columns}"
            )

        if self.has_invalid_filter:
            logger.warning(f"Skipping check '{self.check.name}' due to invalid check filter: '{self.check.filter}'")
            invalid_cols_message_parts.append(
                f"Check evaluation skipped due to invalid check filter: '{self.check.filter}'"
            )

        if self.invalid_sql_expression:
            logger.warning(
                f"Skipping check '{self.check.name}' due to invalid sql expression: '{self.invalid_sql_expression}'"
            )
            invalid_cols_message_parts.append(
                f"Check evaluation skipped due to invalid sql expression: '{self.invalid_sql_expression}'"
            )

        invalid_cols_message = "; ".join(invalid_cols_message_parts)

        return invalid_cols_message

    def _is_invalid_column(self, column: str | Column) -> bool:
        """
        Returns True if the specified column is invalid (i.e., cannot be resolved in the input DataFrame),
        otherwise False.
        """
        try:
            col_expr = F.expr(column) if isinstance(column, str) else column
            _ = self.df.select(col_expr).schema  # perform logical plan validation without triggering computation
        except AnalysisException as e:
            # If column is not accessible or column expression cannot be evaluated, an AnalysisException is thrown.
            # Note: This does not cover all error conditions. Some issues only appear during a Spark action.
            logger.debug(
                f"Invalid column '{column}' provided in the check '{self.check.name}'",
                exc_info=e,
            )
            return True
        return False
