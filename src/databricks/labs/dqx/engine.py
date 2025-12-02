import copy
import os
import logging
from concurrent import futures
from collections.abc import Callable
from datetime import datetime
from functools import cached_property
from typing import Any
from uuid import uuid4

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Observation, SparkSession
from pyspark.sql.streaming import StreamingQuery

from databricks.labs.dqx.base import DQEngineBase, DQEngineCoreBase
from databricks.labs.dqx.checks_resolver import resolve_custom_check_functions_from_path
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.labs.dqx.checks_storage import (
    FileChecksStorageHandler,
    BaseChecksStorageHandlerFactory,
    ChecksStorageHandlerFactory,
    is_table_location,
)
from databricks.labs.dqx.config import (
    InputConfig,
    OutputConfig,
    FileChecksStorageConfig,
    BaseChecksStorageConfig,
    RunConfig,
    ExtraParams,
)
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import (
    Criticality,
    ColumnArguments,
    DefaultColumnNames,
    DQRule,
)
from databricks.labs.dqx.checks_validator import ChecksValidator, ChecksValidationStatus
from databricks.labs.dqx.schema import dq_result_schema
from databricks.labs.dqx.metrics_observer import DQMetricsObservation, DQMetricsObserver
from databricks.labs.dqx.metrics_listener import StreamingMetricsListener
from databricks.labs.dqx.io import read_input_data, save_dataframe_as_table, get_reference_dataframes
from databricks.labs.dqx.telemetry import telemetry_logger, log_telemetry, log_dataframe_telemetry
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.errors import InvalidCheckError, InvalidConfigError, InvalidParameterError
from databricks.labs.dqx.utils import list_tables, safe_strip_file_from_path
from databricks.labs.dqx.io import is_one_time_trigger

logger = logging.getLogger(__name__)


class DQEngineCore(DQEngineCoreBase):
    """Core engine to apply data quality checks to a DataFrame.

    Args:
        workspace_client: WorkspaceClient instance used to access the workspace.
        spark: Optional SparkSession to use. If not provided, the active session is used.
        extra_params: Optional extra parameters for the engine, such as result column names and run metadata.
        observer: Optional DQMetricsObserver for tracking data quality summary metrics.
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        extra_params: ExtraParams | None = None,
        observer: DQMetricsObserver | None = None,
    ):
        super().__init__(workspace_client)

        extra_params = extra_params or ExtraParams()

        self._result_column_names = {
            ColumnArguments.ERRORS: extra_params.result_column_names.get(
                ColumnArguments.ERRORS.value, DefaultColumnNames.ERRORS.value
            ),
            ColumnArguments.WARNINGS: extra_params.result_column_names.get(
                ColumnArguments.WARNINGS.value, DefaultColumnNames.WARNINGS.value
            ),
        }

        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark
        self.run_time_overwrite = (
            datetime.fromisoformat(extra_params.run_time_overwrite) if extra_params.run_time_overwrite else None
        )
        self.engine_user_metadata = extra_params.user_metadata

        self.observer = observer
        if self.observer:
            self.observer.set_column_names(
                error_column_name=self._result_column_names[ColumnArguments.ERRORS],
                warning_column_name=self._result_column_names[ColumnArguments.WARNINGS],
            )
            self.observer.id_overwrite = extra_params.run_id_overwrite
            # run id is globally assigned for each engine instance
            self.run_id = self.observer.id
        else:
            self.run_id = extra_params.run_id_overwrite or str(uuid4())  # auto-generate if not provided

    @cached_property
    def result_column_names(self) -> dict[ColumnArguments, str]:
        return self._result_column_names

    def apply_checks(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> DataFrame | tuple[DataFrame, Observation]:
        """Apply data quality checks to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality
            summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

        Raises:
            InvalidCheckError: If any of the checks are invalid.
        """
        if not checks:
            observed_result = self._observe_metrics(self._append_empty_checks(df))
            if isinstance(observed_result, tuple):
                observed_df, observation = observed_result
                return observed_df, observation
            return observed_result

        if not DQEngineCore._all_are_dq_rules(checks):
            raise InvalidCheckError(
                "All elements in the 'checks' list must be instances of DQRule. Use 'apply_checks_by_metadata' to pass checks as list of dicts instead."
            )

        warning_checks = self._get_check_columns(checks, Criticality.WARN.value)
        error_checks = self._get_check_columns(checks, Criticality.ERROR.value)

        result_df = self._create_results_array(
            df, error_checks, self._result_column_names[ColumnArguments.ERRORS], ref_dfs
        )
        result_df = self._create_results_array(
            result_df, warning_checks, self._result_column_names[ColumnArguments.WARNINGS], ref_dfs
        )
        observed_result = self._observe_metrics(result_df)

        if isinstance(observed_result, tuple):
            observed_df, observation = observed_result
            return observed_df, observation

        return observed_result

    def apply_checks_and_split(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]:
        """Apply data quality checks to the given DataFrame and split the results into two DataFrames
        ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows
            with errors or warnings and the corresponding result columns) and an optional Observation which tracks data
            quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

        Raises:
            InvalidCheckError: If any of the checks are invalid.
        """
        if not DQEngineCore._all_are_dq_rules(checks):
            raise InvalidCheckError(
                "All elements in the 'checks' list must be instances of DQRule. Use 'apply_checks_by_metadata_and_split' to pass checks as list of dicts instead."
            )

        observed_result = self.apply_checks(df, checks, ref_dfs)

        if isinstance(observed_result, tuple):
            checked_df, observation = observed_result
            good_df = self.get_valid(checked_df)
            bad_df = self.get_invalid(checked_df)
            return good_df, bad_df, observation

        good_df = self.get_valid(observed_result)
        bad_df = self.get_invalid(observed_result)
        return good_df, bad_df

    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame | tuple[DataFrame, Observation]:
        """Apply data quality checks defined as metadata to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of dictionaries describing checks. Each check dictionary must contain the following:
                - *check* - A check definition including check function and arguments to use.
                - *name* - Optional name for the resulting column. Auto-generated if not provided.
                - *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn*
                  (rows appear in both DataFrames).
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality
            summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """
        dq_rule_checks = deserialize_checks(checks, custom_check_functions)

        return self.apply_checks(df, dq_rule_checks, ref_dfs)

    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]:
        """Apply data quality checks defined as metadata to the given DataFrame and split the results into
        two DataFrames ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of dictionaries describing checks. Each check dictionary must contain the following:
                - *check* - A check definition including check function and arguments to use.
                - *name* - Optional name for the resulting column. Auto-generated if not provided.
                - *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn*
                  (rows appear in both DataFrames).
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows
            with errors or warnings and the corresponding result columns) and an optional Observation which tracks data
            quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

        Raises:
            InvalidCheckError: If any of the checks are invalid.
        """
        dq_rule_checks = deserialize_checks(checks, custom_check_functions)

        good_df, bad_df, *observations = self.apply_checks_and_split(df, dq_rule_checks, ref_dfs)

        if self.observer:
            return good_df, bad_df, observations[0]

        return good_df, bad_df

    @staticmethod
    def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> ChecksValidationStatus:
        """
        Validate checks defined as metadata to ensure they conform to the expected structure and types.

        This method validates the presence of required keys, the existence and callability of functions,
        and the types of arguments passed to those functions.

        Args:
            checks: List of checks to apply to the DataFrame. Each check should be a dictionary.
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            validate_custom_check_functions: If True, validate custom check functions.

        Returns:
            ChecksValidationStatus indicating the validation result.
        """
        return ChecksValidator.validate_checks(checks, custom_check_functions, validate_custom_check_functions)

    def get_invalid(self, df: DataFrame) -> DataFrame:
        """
        Return records that violate data quality checks (rows with warnings or errors).

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with rows that have errors or warnings and the corresponding result columns.
        """
        return df.where(
            F.col(self._result_column_names[ColumnArguments.ERRORS]).isNotNull()
            | F.col(self._result_column_names[ColumnArguments.WARNINGS]).isNotNull()
        )

    def get_valid(self, df: DataFrame) -> DataFrame:
        """
        Return records that do not violate data quality checks (rows with warnings but no errors).

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with warning rows but without the results columns.
        """
        return df.where(F.col(self._result_column_names[ColumnArguments.ERRORS]).isNull()).drop(
            self._result_column_names[ColumnArguments.ERRORS], self._result_column_names[ColumnArguments.WARNINGS]
        )

    @staticmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        """
        Load DQ rules (checks) from a local JSON or YAML file.

        The returned checks can be used as input to *apply_checks_by_metadata*.

        Args:
            filepath: Path to a file containing checks definitions.

        Returns:
            List of DQ rules.
        """
        return FileChecksStorageHandler().load(FileChecksStorageConfig(location=filepath))

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], filepath: str):
        """
        Save DQ rules (checks) to a local YAML or JSON file.

        Args:
            checks: List of DQ rules (checks) to save.
            filepath: Path to a file where the checks definitions will be saved.
        """
        return FileChecksStorageHandler().save(checks, FileChecksStorageConfig(location=filepath))

    @staticmethod
    def _get_check_columns(checks: list[DQRule], criticality: str) -> list[DQRule]:
        """Get check columns based on criticality.

        Args:
            checks: list of checks to apply to the DataFrame
            criticality: criticality

        Returns:
            list of check columns
        """
        return [check for check in checks if check.criticality == criticality]

    @staticmethod
    def _all_are_dq_rules(checks: list[DQRule]) -> bool:
        """Check if all elements in the checks list are instances of DQRule."""
        return all(isinstance(check, DQRule) for check in checks)

    def _append_empty_checks(self, df: DataFrame) -> DataFrame:
        """Append empty checks at the end of DataFrame.

        Args:
            df: DataFrame without checks

        Returns:
            DataFrame with checks
        """
        return df.select(
            "*",
            F.lit(None).cast(dq_result_schema).alias(self._result_column_names[ColumnArguments.ERRORS]),
            F.lit(None).cast(dq_result_schema).alias(self._result_column_names[ColumnArguments.WARNINGS]),
        )

    def _create_results_array(
        self, df: DataFrame, checks: list[DQRule], dest_col: str, ref_dfs: dict[str, DataFrame] | None = None
    ) -> DataFrame:
        """
        Apply a list of data quality checks to a DataFrame and assemble their results into an array column.

        This method:
        - Applies each check using a DQRuleManager.
        - Collects the individual check conditions into an array, filtering out empty results.
        - Adds a new array column that contains only failing checks (if any), or null otherwise.

        Args:
            df: The input DataFrame to which checks are applied.
            checks: List of DQRule instances representing the checks to apply.
            dest_col: Name of the output column where the check results map will be stored.
            ref_dfs: Optional dictionary of reference DataFrames, keyed by name, for use by dataset-level checks.

        Returns:
            DataFrame with an added array column (*dest_col*) containing the results of the applied checks.
        """
        if not checks:
            # No checks then just append a null array result
            empty_result = F.lit(None).cast(dq_result_schema).alias(dest_col)
            return df.select("*", empty_result)

        check_conditions = []
        current_df = df

        for check in checks:
            manager = DQRuleManager(
                check=check,
                df=current_df,
                spark=self.spark,
                run_id=self.run_id,
                engine_user_metadata=self.engine_user_metadata,
                run_time_overwrite=self.run_time_overwrite,
                ref_dfs=ref_dfs,
            )
            log_telemetry(self.ws, "check", check.check_func.__name__)
            result = manager.process()
            check_conditions.append(result.condition)
            # The DataFrame should contain any new columns added by the dataset-level checks
            # to satisfy the check condition.
            current_df = result.check_df

        # Build array of non-null results
        combined_result_array = F.array_compact(F.array(*check_conditions))

        # Add array column with failing checks, or null if none
        result_df = current_df.withColumn(
            dest_col,
            F.when(F.size(combined_result_array) > 0, combined_result_array).otherwise(
                F.lit(None).cast(dq_result_schema)
            ),
        )

        # Ensure the result DataFrame has the same columns as the input DataFrame + the new result column
        return result_df.select(*df.columns, dest_col)

    def _observe_metrics(self, df: DataFrame) -> DataFrame | tuple[DataFrame, Observation]:
        """
        Adds Spark observable metrics to the input DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            The unmodified DataFrame with observed metrics and the corresponding Spark Observation
        """
        if not self.observer:
            return df

        metric_exprs = [F.expr(metric_statement) for metric_statement in self.observer.metrics]
        if not metric_exprs:
            return df

        observation = self.observer.observation
        if df.isStreaming:
            return df.observe(self.observer.id, *metric_exprs), observation

        return df.observe(observation, *metric_exprs), observation


class DQEngine(DQEngineBase):
    """High-level engine to apply data quality checks and manage IO.

    This class delegates core checking logic to *DQEngineCore* while providing helpers to
    read inputs, persist results, and work with different storage backends for checks.

    Args:
        workspace_client: WorkspaceClient instance used to access the Databricks workspace.
        spark: Optional SparkSession to use. If not provided, the active session is used.
        engine: Optional DQEngineCore instance to use. If not provided, a new instance is created.
        extra_params: Optional extra parameters for the engine, such as result column names and run metadata.
        checks_handler_factory: Optional factory to create checks storage handlers. If not provided,
            a default factory is created.
        config_serializer: Optional ConfigSerializer instance to use. If not provided, a new instance is created.
        observer: Optional DQMetricsObserver for tracking data quality summary metrics.
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        engine: DQEngineCore | None = None,
        extra_params: ExtraParams | None = None,
        checks_handler_factory: BaseChecksStorageHandlerFactory | None = None,
        config_serializer: ConfigSerializer | None = None,
        observer: DQMetricsObserver | None = None,
    ):
        super().__init__(workspace_client)

        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark
        self._engine = engine or DQEngineCore(workspace_client, spark, extra_params, observer)
        self._config_serializer = config_serializer or ConfigSerializer(workspace_client)
        self._checks_handler_factory: BaseChecksStorageHandlerFactory = (
            checks_handler_factory or ChecksStorageHandlerFactory(self.ws, self.spark)
        )

    @telemetry_logger("engine", "apply_checks")
    def apply_checks(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> DataFrame | tuple[DataFrame, Observation]:
        """Apply data quality checks to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality
            summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """
        log_dataframe_telemetry(self.ws, self.spark, df)
        return self._engine.apply_checks(df, checks, ref_dfs)

    @telemetry_logger("engine", "apply_checks_and_split")
    def apply_checks_and_split(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]:
        """Apply data quality checks to the given DataFrame and split the results into two DataFrames
        ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows
            with errors or warnings and the corresponding result columns) and an optional Observation which tracks data
            quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

        Raises:
            InvalidCheckError: If any of the checks are invalid.
        """
        log_dataframe_telemetry(self.ws, self.spark, df)
        return self._engine.apply_checks_and_split(df, checks, ref_dfs)

    @telemetry_logger("engine", "apply_checks_by_metadata")
    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame | tuple[DataFrame, Observation]:
        """Apply data quality checks defined as metadata to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of dictionaries describing checks. Each check dictionary must contain the following:
                - *check* - A check definition including check function and arguments to use.
                - *name* - Optional name for the resulting column. Auto-generated if not provided.
                - *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn*
                  (rows appear in both DataFrames).
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality
            summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """
        log_dataframe_telemetry(self.ws, self.spark, df)
        return self._engine.apply_checks_by_metadata(df, checks, custom_check_functions, ref_dfs)

    @telemetry_logger("engine", "apply_checks_by_metadata_and_split")
    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]:
        """Apply data quality checks defined as metadata to the given DataFrame and split the results into
        two DataFrames ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of dictionaries describing checks. Each check dictionary must contain the following:
                - *check* - A check definition including check function and arguments to use.
                - *name* - Optional name for the resulting column. Auto-generated if not provided.
                - *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn*
                  (rows appear in both DataFrames).
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows
            with errors or warnings and the corresponding result columns) and an optional Observation which tracks data
            quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.
        """
        log_dataframe_telemetry(self.ws, self.spark, df)
        return self._engine.apply_checks_by_metadata_and_split(df, checks, custom_check_functions, ref_dfs)

    @telemetry_logger("engine", "apply_checks_and_save_in_table")
    def apply_checks_and_save_in_table(
        self,
        checks: list[DQRule],
        input_config: InputConfig,
        output_config: OutputConfig,
        quarantine_config: OutputConfig | None = None,
        metrics_config: OutputConfig | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
        checks_location: str | None = None,
    ) -> None:
        """
        Apply data quality checks to input data and save results.

        If *quarantine_config* is provided, split the data into valid and invalid records:
        - valid records are written using *output_config*.
        - invalid records are written using *quarantine_config*.

        If *quarantine_config* is not provided, write all rows (including result columns) using *output_config*.

        If *metrics_config* is provided and the `DQEngine` has a valid `observer`, data quality summary metrics will be
        tracked and written using *metrics_config*.

        Args:
            checks: List of *DQRule* checks to apply.
            input_config: Input configuration (e.g., table/view or file location and read options).
            output_config: Output configuration (e.g., table name, mode, and write options).
            quarantine_config: Optional configuration for writing invalid records.
            metrics_config: Optional configuration for writing summary metrics.
            ref_dfs: Optional reference DataFrames used by checks.
            checks_location: Optional location of the checks. Used for reporting in the summary metrics table only.
        """
        logger.info(f"Applying checks to {input_config.location}")

        df = read_input_data(self.spark, input_config)

        batch_observation = None
        output_streaming_query = None
        quarantine_streaming_query = None

        if quarantine_config:
            check_result = self.apply_checks_and_split(df, checks, ref_dfs)
            if self._engine.observer:
                good_df, bad_df, batch_observation = check_result
            else:
                good_df, bad_df = check_result
            output_streaming_query = save_dataframe_as_table(good_df, output_config)
            quarantine_streaming_query = save_dataframe_as_table(bad_df, quarantine_config)
            target_streaming_query = quarantine_streaming_query
        else:
            check_result = self.apply_checks(df, checks, ref_dfs)
            if self._engine.observer:
                checked_df, batch_observation = check_result
            else:
                checked_df = check_result
            output_streaming_query = save_dataframe_as_table(checked_df, output_config)
            target_streaming_query = output_streaming_query

        # Add listener for streaming metrics, targeting the specific query to avoid duplicates
        if self._engine.observer and metrics_config and target_streaming_query is not None:
            listener = self.get_streaming_metrics_listener(
                input_config=input_config,
                output_config=output_config,
                quarantine_config=quarantine_config,
                metrics_config=metrics_config,
                target_query_id=target_streaming_query.id,
                checks_location=checks_location,
            )
            self.spark.streams.addListener(listener)

        self._wait_for_one_time_trigger_streaming_queries(
            output_config, output_streaming_query, quarantine_config, quarantine_streaming_query
        )

        if metrics_config and batch_observation is not None and target_streaming_query is None:
            self.save_summary_metrics(
                observed_metrics=batch_observation.get,
                metrics_config=metrics_config,
                input_config=input_config,
                output_config=output_config,
                quarantine_config=quarantine_config,
                checks_location=checks_location,
            )

    @telemetry_logger("engine", "apply_checks_by_metadata_and_save_in_table")
    def apply_checks_by_metadata_and_save_in_table(
        self,
        checks: list[dict],
        input_config: InputConfig,
        output_config: OutputConfig,
        quarantine_config: OutputConfig | None = None,
        metrics_config: OutputConfig | None = None,
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
        checks_location: str | None = None,
    ) -> None:
        """
        Apply metadata-defined data quality checks to input data and save results.

        If *quarantine_config* is provided, split the data into valid and invalid records:
        - valid records are written using *output_config*;
        - invalid records are written using *quarantine_config*.

        If *quarantine_config* is not provided, write all rows (including result columns) using *output_config*.

        Args:
            checks: List of dicts describing checks. Each check dictionary must contain the following:
                - *check* - A check definition including check function and arguments to use.
                - *name* - Optional name for the resulting column. Auto-generated if not provided.
                - *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn*
                  (rows appear in both DataFrames).
            input_config: Input configuration (e.g., table/view or file location and read options).
            output_config: Output configuration (e.g., table name, mode, and write options).
            quarantine_config: Optional configuration for writing invalid records.
            metrics_config: Optional configuration for writing summary metrics.
            custom_check_functions: Optional mapping of custom check function names
                to callables/modules (e.g., globals()).
            ref_dfs: Optional reference DataFrames used by checks.
            checks_location: Optional location of the checks. Used for reporting in the summary metrics table only.
        """
        logger.info(f"Applying checks to {input_config.location}")

        df = read_input_data(self.spark, input_config)

        batch_observation = None
        output_streaming_query = None
        quarantine_streaming_query = None

        if quarantine_config:
            check_result = self.apply_checks_by_metadata_and_split(df, checks, custom_check_functions, ref_dfs)
            if self._engine.observer:
                good_df, bad_df, batch_observation = check_result
            else:
                good_df, bad_df = check_result
            output_streaming_query = save_dataframe_as_table(good_df, output_config)
            quarantine_streaming_query = save_dataframe_as_table(bad_df, quarantine_config)
            target_streaming_query = quarantine_streaming_query
        else:
            check_result = self.apply_checks_by_metadata(df, checks, custom_check_functions, ref_dfs)
            if self._engine.observer:
                checked_df, batch_observation = check_result
            else:
                checked_df = check_result
            output_streaming_query = save_dataframe_as_table(checked_df, output_config)
            target_streaming_query = output_streaming_query

        # Add listener for streaming metrics, targeting the specific query to avoid duplicates
        if self._engine.observer and metrics_config and target_streaming_query is not None:
            listener = self.get_streaming_metrics_listener(
                input_config=input_config,
                output_config=output_config,
                quarantine_config=quarantine_config,
                metrics_config=metrics_config,
                target_query_id=target_streaming_query.id,
                checks_location=checks_location,
            )
            self.spark.streams.addListener(listener)

        self._wait_for_one_time_trigger_streaming_queries(
            output_config, output_streaming_query, quarantine_config, quarantine_streaming_query
        )

        if metrics_config and batch_observation is not None and target_streaming_query is None:
            self.save_summary_metrics(
                observed_metrics=batch_observation.get,
                metrics_config=metrics_config,
                input_config=input_config,
                output_config=output_config,
                quarantine_config=quarantine_config,
                checks_location=checks_location,
            )

    @telemetry_logger("engine", "apply_checks_and_save_in_tables")
    def apply_checks_and_save_in_tables(
        self,
        run_configs: list[RunConfig],
        max_parallelism: int | None = os.cpu_count(),
    ) -> None:
        """
        Apply data quality checks to multiple tables or views and write the results to output table(s).

        If quarantine tables are provided in the run configuration, the data will be split into
        good and bad records, with good records written to the output table and bad records to the
        quarantine table. If quarantine tables are not provided, all records (with error/warning
        columns) will be written to the output table.

        Args:
            run_configs (list[RunConfig]): List of run configurations containing input configs, output configs,
                quarantine configs, and a checks file location.
            max_parallelism (int, optional): Maximum number of tables to check in parallel. Defaults to the
                number of CPU cores.

        Returns:
            None
        """
        logger.info(f"Applying checks to {len(run_configs)} tables with parallelism {max_parallelism}")
        with futures.ThreadPoolExecutor(max_workers=max_parallelism) as executor:
            apply_checks_runs = [
                executor.submit(self._apply_checks_for_run_config, run_config) for run_config in run_configs
            ]
            for future in futures.as_completed(apply_checks_runs):
                # Retrieve the result to propagate any exceptions
                future.result()

    @telemetry_logger("engine", "apply_checks_and_save_in_tables_for_patterns")
    def apply_checks_and_save_in_tables_for_patterns(
        self,
        patterns: list[str],  # can use wildcard e.g. catalog.schema.*
        checks_location: str,  # use as prefix for checks defined in files
        exclude_patterns: list[str] | None = None,
        exclude_matched: bool = False,
        run_config_template: RunConfig = RunConfig(),
        max_parallelism: int | None = os.cpu_count(),
        output_table_suffix: str = "_dq_output",
        quarantine_table_suffix: str = "_dq_quarantine",
    ) -> None:
        """
        Apply data quality checks to tables or views matching a pattern and write the results to output table(s).

        If quarantine option is enabled the data will be split into
        good and bad records, with good records written to the output table
        (under the same name as input table and "_dq" suffix) and bad records to the
        quarantine table (under the same name as input table and "_quarantine" suffix).
        If quarantine is not enabled, all records (with error/warning columns) will be written to the output table.

        Checks are expected to be available under the same name as the table, with a .yml extension.

        Args:
            patterns: List of table names or filesystem-style wildcards (e.g. 'schema.*') to include.
                If None, all tables are included. By default, tables matching the pattern are included.
            checks_location: Location of the checks files (e.g., absolute workspace or volume directory, or delta table).
                For file based locations, checks are expected to be found under checks_location/table_name.yml.
            exclude_matched (bool): Specifies whether to include tables matched by the pattern.
                If True, matched tables are excluded. If False, matched tables are included.
            exclude_patterns: List of table names or filesystem-style wildcards to exclude.
                If None, no tables are excluded.
            run_config_template: Run configuration template to use for all tables.
                Skip location in the input_config, output_config, and quarantine_config as it is derived from patterns.
                Skip checks_location of the run config as it is derived separately.
                Autogenerate input_config and output_config if not provided.
            max_parallelism (int): Maximum number of tables to check in parallel.
            output_table_suffix: Suffix to append to the original table name for the output table.
            quarantine_table_suffix: Suffix to append to the original table name for the quarantine table.

        Returns:
            None
        """
        if not output_table_suffix:
            raise InvalidParameterError("Output table suffix cannot be empty.")

        if run_config_template.quarantine_config and not quarantine_table_suffix:
            raise InvalidParameterError("Quarantine table suffix cannot be empty.")

        if run_config_template.input_config is None:
            run_config_template.input_config = InputConfig(location="")  # location derived from patterns

        if run_config_template.output_config is None:
            run_config_template.output_config = OutputConfig(location="")  # location derived from patterns

        tables = list_tables(
            workspace_client=self.ws,
            patterns=patterns,
            exclude_matched=exclude_matched,
            exclude_patterns=exclude_patterns,
        )

        run_configs = []
        for table in tables:
            run_config = copy.deepcopy(run_config_template)

            assert run_config.input_config  # to satisfy linter
            assert run_config.output_config  # to satisfy linter

            run_config.name = table
            run_config.input_config.location = table
            run_config.output_config.location = f"{table}{output_table_suffix}"

            if run_config.quarantine_config:
                run_config.quarantine_config.location = f"{table}{quarantine_table_suffix}"

            run_config.checks_location = (
                checks_location
                if is_table_location(checks_location)
                # for file based checks expecting a file per table
                else f"{safe_strip_file_from_path(checks_location)}/{table}.yml"
            )
            run_configs.append(run_config)

        self.apply_checks_and_save_in_tables(run_configs, max_parallelism)

    @staticmethod
    def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> ChecksValidationStatus:
        """
        Validate checks defined as metadata to ensure they conform to the expected structure and types.

        This method validates the presence of required keys, the existence and callability of functions,
        and the types of arguments passed to those functions.

        Args:
            checks: List of checks to apply to the DataFrame. Each check should be a dictionary.
            custom_check_functions: Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
            validate_custom_check_functions: If True, validate custom check functions.

        Returns:
            ChecksValidationStatus indicating the validation result.
        """
        return DQEngineCore.validate_checks(checks, custom_check_functions, validate_custom_check_functions)

    def get_invalid(self, df: DataFrame) -> DataFrame:
        """
        Return records that violate data quality checks (rows with warnings or errors).

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with rows that have errors or warnings and the corresponding result columns.
        """
        return self._engine.get_invalid(df)

    def get_valid(self, df: DataFrame) -> DataFrame:
        """
        Return records that do not violate data quality checks (rows with warnings but no errors).

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with warning rows but without the results columns.
        """
        return self._engine.get_valid(df)

    @telemetry_logger("engine", "save_results_in_table")
    def save_results_in_table(
        self,
        output_df: DataFrame | None = None,
        quarantine_df: DataFrame | None = None,
        observation: Observation | None = None,
        output_config: OutputConfig | None = None,
        quarantine_config: OutputConfig | None = None,
        metrics_config: OutputConfig | None = None,
        run_config_name: str | None = "default",
        product_name: str = "dqx",
        assume_user: bool = True,
        install_folder: str | None = None,
    ):
        """
        Persist result DataFrames using explicit configs or the named run configuration.

        Behavior:
        - If *output_df* is provided and *output_config* is None, load the run config and use its *output_config*.
        - If *quarantine_df* is provided and *quarantine_config* is None, load the run config and use its *quarantine_config*.
        - If *observation* is provided and *metrics_config* is None, load the run config and use its *metrics_config*
        - A write occurs only when both a DataFrame and its corresponding config are available.

        Args:
            output_df: DataFrame with valid rows to be saved (optional).
            quarantine_df: DataFrame with invalid rows to be saved (optional).
            observation: Spark Observation with data quality summary metrics (optional). Supported for batch only. Requires run_config_name or metrics_config to be provided.
            output_config: Configuration describing where/how to write the valid rows. If omitted, falls back to the run config (requires run_config_name).
            quarantine_config: Configuration describing where/how to write the invalid rows (optional). If omitted, falls back to the run config (requires run_config_name).
            metrics_config: Configuration describing where/how to write the summary metrics (optional). If omitted, falls back to the run config (requires run_config_name).
            run_config_name: Name of the run configuration to load when a config parameter is omitted, e.g. input table or job name (use "default" if not provided).
            product_name: Product/installation identifier used to resolve installation paths for config loading in install_folder is not provided (use "dqx" if not provided).
            assume_user: Whether to assume a per-user installation when loading the run configuration (use *True* if not provided, skipped if install_folder is provided).
            install_folder: Custom workspace installation folder. Required if DQX is installed in a custom folder.

        Returns:
            None
        """
        if not output_df and not quarantine_df:
            raise InvalidConfigError("At least one of 'output_df' or 'quarantine_df' Dataframe must be present.")

        if output_df is not None and output_config is None:
            run_config = self._config_serializer.load_run_config(
                run_config_name=run_config_name,
                assume_user=assume_user,
                product_name=product_name,
                install_folder=install_folder,
            )
            output_config = run_config.output_config

        if quarantine_df is not None and quarantine_config is None:
            run_config = self._config_serializer.load_run_config(
                run_config_name=run_config_name,
                assume_user=assume_user,
                product_name=product_name,
                install_folder=install_folder,
            )
            quarantine_config = run_config.quarantine_config

        if self._engine.observer and metrics_config is None:
            run_config = self._config_serializer.load_run_config(
                run_config_name=run_config_name,
                assume_user=assume_user,
                product_name=product_name,
                install_folder=install_folder,
            )
            metrics_config = run_config.metrics_config

        output_query = None
        quarantine_query = None

        if output_df is not None and output_config is not None:
            output_query = save_dataframe_as_table(output_df, output_config)

        if quarantine_df is not None and quarantine_config is not None:
            quarantine_query = save_dataframe_as_table(quarantine_df, quarantine_config)

        # Determine which query to monitor for metrics (prefer quarantine if exists)
        target_query = quarantine_query if quarantine_query else output_query

        # Add listener for streaming metrics, targeting the specific query to avoid duplicates
        if self._engine.observer and metrics_config is not None and target_query is not None:
            listener = self.get_streaming_metrics_listener(
                output_config=output_config,
                quarantine_config=quarantine_config,
                metrics_config=metrics_config,
                target_query_id=target_query.id,
            )
            self.spark.streams.addListener(listener)

        self._wait_for_one_time_trigger_streaming_queries(
            output_config, output_query, quarantine_config, quarantine_query
        )

        if observation is not None and metrics_config is not None and target_query is None:
            self.save_summary_metrics(
                observed_metrics=observation.get,
                metrics_config=metrics_config,
                output_config=output_config,
                quarantine_config=quarantine_config,
            )

    @telemetry_logger("engine", "load_checks")
    def load_checks(self, config: BaseChecksStorageConfig) -> list[dict]:
        """Load DQ rules (checks) from the storage backend described by *config*.

        This method delegates to a storage handler selected by the factory
        based on the concrete type of *config* and returns the parsed list
        of checks (as dictionaries) ready for *apply_checks_by_metadata*.

        Supported storage configurations include, for example:
        - *FileChecksStorageConfig* (local file);
        - *WorkspaceFileChecksStorageConfig* (Databricks workspace file);
        - *TableChecksStorageConfig* (table-backed storage);
        - *LakebaseChecksStorageConfig* (Lakebase table);
        - *InstallationChecksStorageConfig* (installation directory);
        - *VolumeFileChecksStorageConfig* (Unity Catalog volume file);

        Args:
            config: Configuration object describing the storage backend.

        Returns:
            List of DQ rules (checks) represented as dictionaries.

        Raises:
            InvalidConfigError: If the configuration type is unsupported.
        """
        handler = self._checks_handler_factory.create(config)
        return handler.load(config)

    @telemetry_logger("engine", "save_checks")
    def save_checks(self, checks: list[dict], config: BaseChecksStorageConfig) -> None:
        """Persist DQ rules (checks) to the storage backend described by *config*.

        The appropriate storage handler is resolved from the configuration
        type and used to write the provided checks. Any write semantics
        (e.g., append/overwrite) are controlled by fields on *config*
        such as *mode* where applicable.

        Supported storage configurations include, for example:
        - *FileChecksStorageConfig* (local file);
        - *WorkspaceFileChecksStorageConfig* (Databricks workspace file);
        - *TableChecksStorageConfig* (table-backed storage);
        - *LakebaseChecksStorageConfig* (Lakebase table);
        - *InstallationChecksStorageConfig* (installation directory);
        - *VolumeFileChecksStorageConfig* (Unity Catalog volume file);

        Args:
            checks: List of DQ rules (checks) to save (as dictionaries).
            config: Configuration object describing the storage backend and write options.

        Returns:
            None

        Raises:
            InvalidConfigError: If the configuration type is unsupported.
        """
        handler = self._checks_handler_factory.create(config)
        handler.save(checks, config)

    @telemetry_logger("engine", "save_summary_metrics")
    def save_summary_metrics(
        self,
        observed_metrics: dict[str, Any],
        metrics_config: OutputConfig,
        input_config: InputConfig | None = None,
        output_config: OutputConfig | None = None,
        quarantine_config: OutputConfig | None = None,
        checks_location: str | None = None,
    ) -> None:
        """
        Save data quality summary metrics to a table.

        This method extracts observed metrics from a Spark Observation and persists them to a configured
        output destination. Metrics are only saved if an observer is configured on the engine.

        Args:
            observed_metrics: Collected summary metrics from Spark Observation.
            metrics_config: Output configuration specifying where to save the metrics (table name, mode, options).
            input_config: Optional input configuration with source data location (included in metrics for traceability).
            output_config: Optional output configuration with valid records location (included in metrics for traceability).
            quarantine_config: Optional quarantine configuration with invalid records location (included in metrics for traceability).
            checks_location: Location of the checks files (e.g., absolute workspace or volume directory, or delta table).

        Note:
            The observation must have been triggered by an action (e.g., count(), write()) on the observed
            DataFrame before calling this method, otherwise observation.get will be empty.
            This method is only supported by spark batch. Spark query listener must be used for streaming:
            For streaming use spark.streams.addListener(get_streaming_metrics_listener(..))
        """
        if self._engine.observer:
            self._validate_session_for_metrics()
            metrics_observation = DQMetricsObservation(
                run_id=self._engine.run_id,
                run_name=self._engine.observer.name,
                run_time_overwrite=self._engine.run_time_overwrite,
                observed_metrics=observed_metrics,
                error_column_name=self._engine.result_column_names[ColumnArguments.ERRORS],
                warning_column_name=self._engine.result_column_names[ColumnArguments.WARNINGS],
                input_location=input_config.location if input_config else None,
                output_location=output_config.location if output_config else None,
                quarantine_location=quarantine_config.location if quarantine_config else None,
                checks_location=checks_location,
                user_metadata=self._engine.engine_user_metadata,
            )

            metrics_df = self._engine.observer.build_metrics_df(self.spark, metrics_observation)
            save_dataframe_as_table(metrics_df, metrics_config)

    @telemetry_logger("engine", "get_streaming_metrics_listener")
    def get_streaming_metrics_listener(
        self,
        metrics_config: OutputConfig,
        input_config: InputConfig | None = None,
        output_config: OutputConfig | None = None,
        quarantine_config: OutputConfig | None = None,
        checks_location: str | None = None,
        target_query_id: str | None = None,
    ) -> StreamingMetricsListener:
        """
        Gets a `StreamingMetricsListener` object for writing metrics to an output table.

        Args:
            metrics_config: Configuration for writing summary metrics, including table name, mode, and options.
            input_config: Optional configuration for input data containing location.
            output_config: Optional configuration for output data containing location.
            quarantine_config: Optional configuration for quarantine data containing location.
            checks_location: Optional location of the checks files (e.g., absolute workspace or volume directory, or delta table).
            target_query_id: Optional query ID of the specific streaming query to monitor. If provided, metrics will be collected only for this query.

        Returns:
            StreamingMetricsListener: Listener object for monitoring and writing streaming metrics.

        Usage:
            spark.streams.addListener(get_streaming_metrics_listener(..))
        """

        if not isinstance(self._engine, DQEngineCore):
            raise InvalidParameterError(
                f"Metrics cannot be collected for engine with type '{self._engine.__class__.__name__}'"
            )

        if not self._engine.observer:
            raise InvalidParameterError("Metrics cannot be collected for engine with no observer")

        metrics_observation = DQMetricsObservation(
            run_id=self._engine.run_id,
            run_name=self._engine.observer.name,
            error_column_name=self._engine.result_column_names[ColumnArguments.ERRORS],
            warning_column_name=self._engine.result_column_names[ColumnArguments.WARNINGS],
            input_location=input_config.location if input_config else None,
            output_location=output_config.location if output_config else None,
            quarantine_location=quarantine_config.location if quarantine_config else None,
            checks_location=checks_location,
            user_metadata=self._engine.engine_user_metadata,
        )
        return StreamingMetricsListener(metrics_config, metrics_observation, self.spark, target_query_id)

    @telemetry_logger("engine", "apply_checks_for_run_config")
    def _apply_checks_for_run_config(self, run_config: RunConfig) -> None:
        """
        Applies checks based on a given RunConfig.

        This method loads checks from the specified location, reads input data using the input config,
        and writes results using the output and optionally quarantine configs.

        The storage handler is determined by the factory based on the RunConfig. If Lakebase
        connection parameters are present (lakebase_instance_name), checks will be loaded from
        a Lakebase table. Otherwise, the checks location will be inferred from the checks_location string.

        Args:
            run_config (RunConfig): Specifies the inputs, outputs, and checks file.
        """
        if not run_config.input_config:
            raise InvalidConfigError("Input configuration not provided")

        if not run_config.output_config:
            raise InvalidConfigError("Output configuration not provided")

        logger.info(f"Applying checks from {run_config.checks_location} to {run_config.input_config.location}")

        storage_handler, storage_config = self._checks_handler_factory.create_for_run_config(run_config)
        # if checks are not found, return empty list
        # raise an error if checks location not found
        checks = storage_handler.load(storage_config)

        custom_check_functions = resolve_custom_check_functions_from_path(run_config.custom_check_functions)
        ref_dfs = get_reference_dataframes(self.spark, run_config.reference_tables)

        self.apply_checks_by_metadata_and_save_in_table(
            checks=checks,
            input_config=run_config.input_config,
            output_config=run_config.output_config,
            quarantine_config=run_config.quarantine_config,
            metrics_config=run_config.metrics_config,
            custom_check_functions=custom_check_functions,
            ref_dfs=ref_dfs,
            checks_location=storage_config.location,
        )

    def _validate_session_for_metrics(self) -> None:
        """
        Validates the session for metrics collection.

        Raises:
            TypeError: If the session is a SparkConnect session.
        """
        if isinstance(self.spark, pyspark.sql.connect.session.SparkSession):
            raise TypeError(
                "Metrics collection is not supported for SparkConnect sessions. Use a Spark cluster with Dedicated access mode to collect metrics."
            )

    @staticmethod
    def _wait_for_one_time_trigger_streaming_queries(
        output_config: OutputConfig | None,
        output_query: StreamingQuery | None,
        quarantine_config: OutputConfig | None,
        quarantine_query: StreamingQuery | None,
    ) -> None:
        if output_query and output_config and is_one_time_trigger(output_config.trigger):
            output_query.awaitTermination()
        if quarantine_query and quarantine_config and is_one_time_trigger(quarantine_config.trigger):
            quarantine_query.awaitTermination()
