import logging
import warnings
from collections.abc import Callable
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.base import DQEngineBase, DQEngineCoreBase
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.config_loader import RunConfigLoader
from databricks.labs.dqx.checks_storage import (
    FileChecksStorageHandler,
    BaseChecksStorageHandlerFactory,
    ChecksStorageHandlerFactory,
)
from databricks.labs.dqx.config import (
    InputConfig,
    OutputConfig,
    FileChecksStorageConfig,
    BaseChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
    InstallationChecksStorageConfig,
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
from databricks.labs.dqx.utils import read_input_data, save_dataframe_as_table
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class DQEngineCore(DQEngineCoreBase):
    """Core engine to apply data quality checks to a DataFrame.

    Args:
        workspace_client: WorkspaceClient instance used to access the workspace.
        spark: Optional SparkSession to use. If not provided, the active session is used.
        extra_params: Optional extra parameters for the engine, such as result column names and run metadata.
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        extra_params: ExtraParams | None = None,
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
        self.run_time = datetime.fromisoformat(extra_params.run_time)
        self.engine_user_metadata = extra_params.user_metadata

    def apply_checks(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> DataFrame:
        """Apply data quality checks to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            DataFrame with errors and warnings result columns.
        """
        if not checks:
            return self._append_empty_checks(df)

        if not DQEngineCore._all_are_dq_rules(checks):
            raise TypeError(
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

        return result_df

    def apply_checks_and_split(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> tuple[DataFrame, DataFrame]:
        """Apply data quality checks to the given DataFrame and split the results into two DataFrames
        ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and
            "bad" (rows with errors or warnings and the corresponding result columns).
        """
        if not checks:
            return df, self._append_empty_checks(df).limit(0)

        if not DQEngineCore._all_are_dq_rules(checks):
            raise TypeError(
                "All elements in the 'checks' list must be instances of DQRule. Use 'apply_checks_by_metadata_and_split' to pass checks as list of dicts instead."
            )

        checked_df = self.apply_checks(df, checks, ref_dfs)

        good_df = self.get_valid(checked_df)
        bad_df = self.get_invalid(checked_df)

        return good_df, bad_df

    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame:
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
            DataFrame with errors and warnings result columns.
        """
        dq_rule_checks = deserialize_checks(checks, custom_check_functions)

        return self.apply_checks(df, dq_rule_checks, ref_dfs)

    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> tuple[DataFrame, DataFrame]:
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
            DataFrame that includes errors and warnings result columns.
        """
        dq_rule_checks = deserialize_checks(checks, custom_check_functions)

        good_df, bad_df = self.apply_checks_and_split(df, dq_rule_checks, ref_dfs)
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
                engine_user_metadata=self.engine_user_metadata,
                run_time=self.run_time,
                ref_dfs=ref_dfs,
            )
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


class DQEngine(DQEngineBase):
    """High-level engine to apply data quality checks and manage IO.

    This class delegates core checking logic to *DQEngineCore* while providing helpers to
    read inputs, persist results, and work with different storage backends for checks.
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        engine: DQEngineCoreBase | None = None,
        extra_params: ExtraParams | None = None,
        checks_handler_factory: BaseChecksStorageHandlerFactory | None = None,
        run_config_loader: RunConfigLoader | None = None,
    ):
        super().__init__(workspace_client)

        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark
        self._engine = engine or DQEngineCore(workspace_client, spark, extra_params)
        self._run_config_loader = run_config_loader or RunConfigLoader(workspace_client)
        self._checks_handler_factory: BaseChecksStorageHandlerFactory = (
            checks_handler_factory or ChecksStorageHandlerFactory(self.ws, self.spark)
        )

    def apply_checks(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> DataFrame:
        """Apply data quality checks to the given DataFrame.

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            DataFrame with errors and warnings result columns.
        """
        return self._engine.apply_checks(df, checks, ref_dfs)

    def apply_checks_and_split(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> tuple[DataFrame, DataFrame]:
        """Apply data quality checks to the given DataFrame and split the results into two DataFrames
        ("good" and "bad").

        Args:
            df: Input DataFrame to check.
            checks: List of checks to apply. Each check must be a *DQRule* instance.
            ref_dfs: Optional reference DataFrames to use in the checks.

        Returns:
            A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and
            "bad" (rows with errors or warnings and the corresponding result columns).
        """
        return self._engine.apply_checks_and_split(df, checks, ref_dfs)

    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame:
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
            DataFrame with errors and warnings result columns.
        """
        return self._engine.apply_checks_by_metadata(df, checks, custom_check_functions, ref_dfs)

    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> tuple[DataFrame, DataFrame]:
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
            DataFrame that includes errors and warnings result columns.
        """
        return self._engine.apply_checks_by_metadata_and_split(df, checks, custom_check_functions, ref_dfs)

    def apply_checks_and_save_in_table(
        self,
        checks: list[DQRule],
        input_config: InputConfig,
        output_config: OutputConfig,
        quarantine_config: OutputConfig | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> None:
        """
        Apply data quality checks to input data and save results.

        If *quarantine_config* is provided, split the data into valid and invalid records:
        - valid records are written using *output_config*.
        - invalid records are written using *quarantine_config*.

        If *quarantine_config* is not provided, write all rows (including result columns) using *output_config*.

        Args:
            checks: List of *DQRule* checks to apply.
            input_config: Input configuration (e.g., table/view or file location and read options).
            output_config: Output configuration (e.g., table name, mode, and write options).
            quarantine_config: Optional configuration for writing invalid records.
            ref_dfs: Optional reference DataFrames used by checks.
        """
        # Read data from the specified table
        df = read_input_data(self.spark, input_config)

        if quarantine_config:
            # Split data into good and bad records
            good_df, bad_df = self.apply_checks_and_split(df, checks, ref_dfs)
            save_dataframe_as_table(good_df, output_config)
            save_dataframe_as_table(bad_df, quarantine_config)
        else:
            # Apply checks and write all data to single table
            checked_df = self.apply_checks(df, checks, ref_dfs)
            save_dataframe_as_table(checked_df, output_config)

    def apply_checks_by_metadata_and_save_in_table(
        self,
        checks: list[dict],
        input_config: InputConfig,
        output_config: OutputConfig,
        quarantine_config: OutputConfig | None = None,
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
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
            custom_check_functions: Optional mapping of custom check function names
                to callables/modules (e.g., globals()).
            ref_dfs: Optional reference DataFrames used by checks.
        """
        # Read data from the specified table
        df = read_input_data(self.spark, input_config)

        if quarantine_config:
            # Split data into good and bad records
            good_df, bad_df = self.apply_checks_by_metadata_and_split(df, checks, custom_check_functions, ref_dfs)
            save_dataframe_as_table(good_df, output_config)
            save_dataframe_as_table(bad_df, quarantine_config)
        else:
            # Apply checks and write all data to single table
            checked_df = self.apply_checks_by_metadata(df, checks, custom_check_functions, ref_dfs)
            save_dataframe_as_table(checked_df, output_config)

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

    def save_results_in_table(
        self,
        output_df: DataFrame | None = None,
        quarantine_df: DataFrame | None = None,
        output_config: OutputConfig | None = None,
        quarantine_config: OutputConfig | None = None,
        run_config_name: str | None = "default",
        product_name: str = "dqx",
        assume_user: bool = True,
    ):
        """Persist result DataFrames using explicit configs or the named run configuration.

        Behavior:
        - If *output_df* is provided and *output_config* is None, load the run config and use its *output_config*.
        - If *quarantine_df* is provided and *quarantine_config* is None, load the run config and use its *quarantine_config*.
        - A write occurs only when both a DataFrame and its corresponding config are available.

        Args:
            output_df: DataFrame with valid rows to be saved (optional).
            quarantine_df: DataFrame with invalid rows to be saved (optional).
            output_config: Configuration describing where/how to write the valid rows. If omitted, falls back to the run config.
            quarantine_config: Configuration describing where/how to write the invalid rows. If omitted, falls back to the run config.
            run_config_name: Name of the run configuration to load when a config parameter is omitted.
            product_name: Product/installation identifier used to resolve installation paths for config loading.
            assume_user: Whether to assume a per-user installation when loading the run configuration.

        Returns:
            None
        """
        if output_df is not None and output_config is None:
            run_config = self._run_config_loader.load_run_config(run_config_name, assume_user, product_name)
            output_config = run_config.output_config

        if quarantine_df is not None and quarantine_config is None:
            run_config = self._run_config_loader.load_run_config(run_config_name, assume_user, product_name)
            quarantine_config = run_config.quarantine_config

        if output_df is not None and output_config is not None:
            save_dataframe_as_table(output_df, output_config)

        if quarantine_df is not None and quarantine_config is not None:
            save_dataframe_as_table(quarantine_df, quarantine_config)

    def load_checks(self, config: BaseChecksStorageConfig) -> list[dict]:
        """Load DQ rules (checks) from the storage backend described by *config*.

        This method delegates to a storage handler selected by the factory
        based on the concrete type of *config* and returns the parsed list
        of checks (as dictionaries) ready for *apply_checks_by_metadata*.

        Supported storage configurations include, for example:
        - *FileChecksStorageConfig* (local file);
        - *WorkspaceFileChecksStorageConfig* (Databricks workspace file);
        - *TableChecksStorageConfig* (table-backed storage);
        - *InstallationChecksStorageConfig* (installation directory);
        - *VolumeFileChecksStorageConfig* (Unity Catalog volume file);

        Args:
            config: Configuration object describing the storage backend.

        Returns:
            List of DQ rules (checks) represented as dictionaries.

        Raises:
            ValueError: If the configuration type is unsupported.
        """
        handler = self._checks_handler_factory.create(config)
        return handler.load(config)

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
        - *InstallationChecksStorageConfig* (installation directory);
        - *VolumeFileChecksStorageConfig* (Unity Catalog volume file);

        Args:
            checks: List of DQ rules (checks) to save (as dictionaries).
            config: Configuration object describing the storage backend and write options.

        Returns:
            None

        Raises:
            ValueError: If the configuration type is unsupported.
        """
        handler = self._checks_handler_factory.create(config)
        handler.save(checks, config)

    #
    # Deprecated methods for loading and saving checks
    #
    @staticmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        """Deprecated: Use *load_checks* with *FileChecksStorageConfig* instead.

        Load DQ rules (checks) from a local JSON or YAML file.

        Args:
            filepath: Path to a file containing checks definitions.

        Returns:
            List of DQ rules (checks) represented as dictionaries.
        """
        warnings.warn(
            "Use `load_checks` method with `FileChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return DQEngineCore.load_checks_from_local_file(filepath)

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], path: str):
        """Deprecated: Use *save_checks* with *FileChecksStorageConfig* instead.

        Save DQ rules (checks) to a local YAML or JSON file.

        Args:
            checks: List of DQ rules (checks) to save.
            path: File path where the checks definitions will be saved.

        Returns:
            None
        """
        warnings.warn(
            "Use `save_checks` method with `FileChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return DQEngineCore.save_checks_in_local_file(checks, path)

    def load_checks_from_workspace_file(self, workspace_path: str) -> list[dict]:
        """Deprecated: Use *load_checks* with *WorkspaceFileChecksStorageConfig* instead.

        Load checks stored in a Databricks workspace file.

        Args:
            workspace_path: Path to the workspace file containing checks definitions.

        Returns:
            List of DQ rules (checks) represented as dictionaries.
        """
        warnings.warn(
            "Use `load_checks` method with `WorkspaceFileChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.load_checks(WorkspaceFileChecksStorageConfig(location=workspace_path))

    def save_checks_in_workspace_file(self, checks: list[dict], workspace_path: str):
        """Deprecated: Use *save_checks* with *WorkspaceFileChecksStorageConfig* instead.

        Save checks to a Databricks workspace file.

        Args:
            checks: List of DQ rules (checks) to save.
            workspace_path: Path to the workspace file where checks will be saved.

        Returns:
            None
        """
        warnings.warn(
            "Use `save_checks` method with `WorkspaceFileChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.save_checks(checks, WorkspaceFileChecksStorageConfig(location=workspace_path))

    def load_checks_from_table(self, table_name: str, run_config_name: str = "default") -> list[dict]:
        """Deprecated: Use *load_checks* with *TableChecksStorageConfig* instead.

        Load checks from a table.

        Args:
            table_name: Fully qualified table name where checks are stored.
            run_config_name: Name of the run configuration (used by the storage handler if needed).

        Returns:
            List of DQ rules (checks) represented as dictionaries.
        """
        warnings.warn(
            "Use `load_checks` method with `TableChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.load_checks(TableChecksStorageConfig(location=table_name, run_config_name=run_config_name))

    def save_checks_in_table(
        self, checks: list[dict], table_name: str, run_config_name: str = "default", mode: str = "append"
    ):
        """Deprecated: Use *save_checks* with *TableChecksStorageConfig* instead.

        Save checks to a table.

        Args:
            checks: List of DQ rules (checks) to save.
            table_name: Fully qualified table name where checks will be written.
            run_config_name: Name of the run configuration (used by the storage handler if needed).
            mode: Write mode, e.g., "append" or "overwrite".

        Returns:
            None
        """
        warnings.warn(
            "Use `save_checks` method with `TableChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.save_checks(
            checks, TableChecksStorageConfig(location=table_name, run_config_name=run_config_name, mode=mode)
        )

    def load_checks_from_installation(
        self,
        run_config_name: str = "default",
        method: str = "file",
        product_name: str = "dqx",
        assume_user: bool = True,
    ) -> list[dict]:
        """Deprecated: Use *load_checks* with *InstallationChecksStorageConfig* instead.

        Load checks from the installation directory.

        Args:
            run_config_name: Named run configuration to resolve installation paths and defaults.
            method: Deprecated parameter; ignored.
            product_name: Product/installation identifier (e.g., "dqx").
            assume_user: Whether to assume a per-user installation layout.

        Returns:
            List of DQ rules (checks) represented as dictionaries.
        """
        warnings.warn(
            "Use `load_checks` method with `InstallationChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        logger.warning(f"'method' parameter is deprecated: {method}")
        return self.load_checks(
            InstallationChecksStorageConfig(
                run_config_name=run_config_name, product_name=product_name, assume_user=assume_user
            )
        )

    def save_checks_in_installation(
        self,
        checks: list[dict],
        run_config_name: str = "default",
        method: str = "file",
        product_name: str = "dqx",
        assume_user: bool = True,
    ):
        """Deprecated: Use *save_checks* with *InstallationChecksStorageConfig* instead.

        Save checks to the installation directory.

        Args:
            checks: List of DQ rules (checks) to save.
            run_config_name: Named run configuration to resolve installation paths and defaults.
            method: Deprecated parameter; ignored.
            product_name: Product/installation identifier (e.g., "dqx").
            assume_user: Whether to assume a per-user installation layout.

        Returns:
            None
        """
        warnings.warn(
            "Use `save_checks` method with `InstallationChecksStorageConfig` instead. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        logger.warning(f"'method' parameter is deprecated: {method}")
        return self.save_checks(
            checks,
            InstallationChecksStorageConfig(
                run_config_name=run_config_name, product_name=product_name, assume_user=assume_user
            ),
        )

    def load_run_config(
        self, run_config_name: str = "default", assume_user: bool = True, product_name: str = "dqx"
    ) -> RunConfig:
        """Deprecated: Use *RunConfigLoader.load_run_config* directly.

        Load a run configuration by name. This wrapper will be removed in a future version.

        Args:
            run_config_name: Name of the run configuration to load.
            assume_user: Whether to assume a per-user installation when resolving paths.
            product_name: Product/installation identifier (e.g., "dqx").

        Returns:
            Loaded *RunConfig* instance.
        """
        warnings.warn(
            "Use `RunConfigLoader.load_run_config` method instead. This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return RunConfigLoader(self.ws).load_run_config(
            run_config_name=run_config_name, assume_user=assume_user, product_name=product_name
        )
