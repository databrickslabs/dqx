import logging
import warnings
from typing import Any

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
)
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import (
    Criticality,
    ColumnArguments,
    ExtraParams,
    DefaultColumnNames,
    DQRule,
)
from databricks.labs.dqx.checks_validator import ChecksValidator, ChecksValidationStatus
from databricks.labs.dqx.schema import dq_result_schema
from databricks.labs.dqx.utils import read_input_data, save_dataframe_as_table
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class DQEngineCore(DQEngineCoreBase):
    """Data Quality Engine Core class to apply data quality checks to a given dataframe.
    Args:
        workspace_client (WorkspaceClient): WorkspaceClient instance to use for accessing the workspace.
        extra_params (ExtraParams): Extra parameters for the DQEngine.
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
        self.run_time = extra_params.run_time
        self.engine_user_metadata = extra_params.user_metadata

    def apply_checks(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> DataFrame:
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

    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Any] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> tuple[DataFrame, DataFrame]:
        dq_rule_checks = deserialize_checks(checks, custom_check_functions)

        good_df, bad_df = self.apply_checks_and_split(df, dq_rule_checks, ref_dfs)
        return good_df, bad_df

    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Any] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame:
        dq_rule_checks = deserialize_checks(checks, custom_check_functions)

        return self.apply_checks(df, dq_rule_checks, ref_dfs)

    @staticmethod
    def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Any] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> ChecksValidationStatus:
        return ChecksValidator.validate_checks(checks, custom_check_functions, validate_custom_check_functions)

    def get_invalid(self, df: DataFrame) -> DataFrame:
        return df.where(
            F.col(self._result_column_names[ColumnArguments.ERRORS]).isNotNull()
            | F.col(self._result_column_names[ColumnArguments.WARNINGS]).isNotNull()
        )

    def get_valid(self, df: DataFrame) -> DataFrame:
        return df.where(F.col(self._result_column_names[ColumnArguments.ERRORS]).isNull()).drop(
            self._result_column_names[ColumnArguments.ERRORS], self._result_column_names[ColumnArguments.WARNINGS]
        )

    @staticmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        return FileChecksStorageHandler().load(FileChecksStorageConfig(location=filepath))

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], filepath: str):
        return FileChecksStorageHandler().save(checks, FileChecksStorageConfig(location=filepath))

    @staticmethod
    def _get_check_columns(checks: list[DQRule], criticality: str) -> list[DQRule]:
        """Get check columns based on criticality.

        :param checks: list of checks to apply to the dataframe
        :param criticality: criticality
        :return: list of check columns
        """
        return [check for check in checks if check.criticality == criticality]

    @staticmethod
    def _all_are_dq_rules(checks: list[DQRule]) -> bool:
        """Check if all elements in the checks list are instances of DQRule."""
        return all(isinstance(check, DQRule) for check in checks)

    def _append_empty_checks(self, df: DataFrame) -> DataFrame:
        """Append empty checks at the end of dataframe.

        :param df: dataframe without checks
        :return: dataframe with checks
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

        :param df: The input DataFrame to which checks are applied.
        :param checks: List of DQRule instances representing the checks to apply.
        :param dest_col: Name of the output column where the check results map will be stored.
        :param ref_dfs: Optional dictionary of reference DataFrames, keyed by name, for use by dataset-level checks.
        :return: DataFrame with an added array column (`dest_col`) containing the results of the applied checks.
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
    """Data Quality Engine class to apply data quality checks to a given dataframe."""

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
        """Applies data quality checks to a given dataframe.

        :param df: dataframe to check
        :param checks: list of checks to apply to the dataframe. Each check is an instance of DQRule class.
        :param ref_dfs: reference dataframes to use in the checks, if applicable
        :return: dataframe with errors and warning result columns
        """
        return self._engine.apply_checks(df, checks, ref_dfs)

    def apply_checks_and_split(
        self, df: DataFrame, checks: list[DQRule], ref_dfs: dict[str, DataFrame] | None = None
    ) -> tuple[DataFrame, DataFrame]:
        """Applies data quality checks to a given dataframe and split it into two ("good" and "bad"),
        according to the data quality checks.

        :param df: dataframe to check
        :param checks: list of checks to apply to the dataframe. Each check is an instance of DQRule class.
        :param ref_dfs: reference dataframes to use in the checks, if applicable
        :return: two dataframes - "good" which includes warning rows but no result columns, and "data" having
        error and warning rows and corresponding result columns
        """
        return self._engine.apply_checks_and_split(df, checks, ref_dfs)

    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Any] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> tuple[DataFrame, DataFrame]:
        """Wrapper around `apply_checks_and_split` for use in the metadata-driven pipelines. The main difference
        is how the checks are specified - instead of using functions directly, they are described as function name plus
        arguments.

        :param df: dataframe to check
        :param checks: list of dictionaries describing checks. Each check is a dictionary consisting of following fields:
        * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to true -
        it will be used as an error/warning message, or `null` if it's evaluated to `false`
        * `name` - name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) - possible values are `error` (data going only into "bad" dataframe),
        and `warn` (data is going into both dataframes)
        * `filter` (optional) - Expression for filtering data quality checks
        * `user_metadata` (optional) - User-defined key-value pairs added to metadata generated by the check.
        :param custom_check_functions: dictionary with custom check functions (eg. ``globals()`` of the calling module).
        If not specified, then only built-in functions are used for the checks.
        :param ref_dfs: reference dataframes to use in the checks, if applicable
        :return: two dataframes - "good" which includes warning rows but no result columns, and "bad" having
        error and warning rows and corresponding result columns
        """
        return self._engine.apply_checks_by_metadata_and_split(df, checks, custom_check_functions, ref_dfs)

    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Any] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame:
        """Wrapper around `apply_checks` for use in the metadata-driven pipelines. The main difference
        is how the checks are specified - instead of using functions directly, they are described as function name plus
        arguments.

        :param df: dataframe to check
        :param checks: list of dictionaries describing checks. Each check is a dictionary consisting of following fields:
        * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to true -
        it will be used as an error/warning message, or `null` if it's evaluated to `false`
        * `name` - name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) - possible values are `error` (data going only into "bad" dataframe),
        and `warn` (data is going into both dataframes)
        * `filter` (optional) - Expression for filtering data quality checks
        * `user_metadata` (optional) - User-defined key-value pairs added to metadata generated by the check.
        :param custom_check_functions: dictionary with custom check functions (eg. ``globals()`` of calling module).
        :param ref_dfs: reference dataframes to use in the checks, if applicable
        If not specified, then only built-in functions are used for the checks.
        :return: dataframe with errors and warning result columns
        """
        return self._engine.apply_checks_by_metadata(df, checks, custom_check_functions, ref_dfs)

    def apply_checks_and_save_in_table(
        self,
        checks: list[DQRule],
        input_config: InputConfig,
        output_config: OutputConfig,
        quarantine_config: OutputConfig | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> None:
        """
        Apply data quality checks to a table or view and write the result to table(s).

        If quarantine_config is provided, the data will be split into good and bad records,
        with good records written to the output table and bad records to the quarantine table.
        If quarantine_config is not provided, all records (with error/warning columns)
        will be written to the output table.

        :param checks: list of checks to apply to the dataframe. Each check is an instance of DQRule class.
        :param input_config: Input data configuration (e.g. table name or file location, read options)
        :param output_config: Output data configuration (e.g. table name, output mode, write options)
        :param quarantine_config: Optional quarantine data configuration (e.g. table name, output mode, write options)
        :param ref_dfs: Reference dataframes to use in the checks, if applicable
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
        custom_check_functions: dict[str, Any] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> None:
        """
        Apply data quality checks to a table or view and write the result to table(s).

        If quarantine_config is provided, the data will be split into good and bad records,
        with good records written to the output table and bad records to the quarantine table.
        If quarantine_config is not provided, all records (with error/warning columns)
        will be written to the output table.

        :param checks: List of dictionaries describing checks. Each check is a dictionary consisting of following fields:
        * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to true -
        it will be used as an error/warning message, or `null` if it's evaluated to `false`
        * `name` - Name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) -Possible values are `error` (data going only into "bad" dataframe),
        and `warn` (data is going into both dataframes)
        :param input_config: Input data configuration (e.g. table name or file location, read options)
        :param output_config: Output data configuration (e.g. table name, output mode, write options)
        :param quarantine_config: Optional quarantine data configuration (e.g. table name, output mode, write options)
        :param custom_check_functions: Dictionary with custom check functions (eg. ``globals()`` of calling module).
        :param ref_dfs: Reference dataframes to use in the checks, if applicable
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
        custom_check_functions: dict[str, Any] | None = None,
        validate_custom_check_functions: bool = True,
    ) -> ChecksValidationStatus:
        """
        Validate the input dict to ensure they conform to expected structure and types.

        Each check can be a dictionary. The function validates
        the presence of required keys, the existence and callability of functions, and the types
        of arguments passed to these functions.

        :param checks: List of checks to apply to the dataframe. Each check should be a dictionary.
        :param custom_check_functions: Optional dictionary with custom check functions.
        :param validate_custom_check_functions: If True, validate custom check functions.

        :return ValidationStatus: The validation status.
        """
        return DQEngineCore.validate_checks(checks, custom_check_functions, validate_custom_check_functions)

    def get_invalid(self, df: DataFrame) -> DataFrame:
        """
        Get records that violate data quality checks (records with warnings and errors).
        @param df: input DataFrame.
        @return: dataframe with error and warning rows and corresponding result columns.
        """
        return self._engine.get_invalid(df)

    def get_valid(self, df: DataFrame) -> DataFrame:
        """
        Get records that don't violate data quality checks (records with warnings but no errors).
        @param df: input DataFrame.
        @return: dataframe with warning rows but no result columns.
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
        """
        Save quarantine and output data to the `quarantine_table` and `output_table`.

        :param quarantine_df: Optional Dataframe containing the quarantine data
        :param output_df: Optional Dataframe containing the output data. If not provided, use run config
        :param output_config: Optional configuration for saving the output data. If not provided, use run config
        :param quarantine_config: Optional configuration for saving the quarantine data. If not provided, use run config
        :param run_config_name: Optional name of the run (config) to use
        :param product_name: name of the product/installation directory
        :param assume_user: if True, assume user installation
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
        """
        Load checks (dq rules) from the specified source type (file or table).
        :param config: storage configuration
        Allowed configs are:
        - `FileChecksStorageConfig`: for loading checks from a file in the local filesystem
        - `WorkspaceFileChecksStorageConfig`: for loading checks from a workspace file
        - `TableChecksStorageConfig`: for loading checks from a table
        - `InstallationChecksStorageConfig`: for loading checks from the installation directory
        - `VolumeFileChecksStorageConfig`: for loading checks from a Unity Catalog volume file
        - ...
        :raises ValueError: if the source type is unknown
        """
        handler = self._checks_handler_factory.create(config)
        return handler.load(config)

    def save_checks(self, checks: list[dict], config: BaseChecksStorageConfig) -> None:
        """
        Save checks (dq rules) to the specified storage type (file or table).
        :param checks: list of dq rules to save
        :param config: storage configuration
        Allowed configs are:
        - `FileChecksStorageConfig`: for saving checks in a file in the local filesystem
        - `WorkspaceFileChecksStorageConfig`: for saving checks in a workspace file
        - `TableChecksStorageConfig`: for saving checks in a table
        - `InstallationChecksStorageConfig`: for saving checks in the installation directory
        - `VolumeFileChecksStorageConfig`: for saving checks in a Unity Catalog volume file
        - ...
        :raises ValueError: if the storage type is unknown
        """
        handler = self._checks_handler_factory.create(config)
        handler.save(checks, config)

    #
    # Deprecated methods for loading and saving checks
    #
    @staticmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        warnings.warn(
            "Use `load_checks` method instead. This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return DQEngineCore.load_checks_from_local_file(filepath)

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], path: str):
        warnings.warn(
            "Use `save_checks` method instead. This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return DQEngineCore.save_checks_in_local_file(checks, path)

    def load_checks_from_workspace_file(self, workspace_path: str) -> list[dict]:
        warnings.warn(
            "Use `load_checks` method instead. This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.load_checks(WorkspaceFileChecksStorageConfig(location=workspace_path))

    def save_checks_in_workspace_file(self, checks: list[dict], workspace_path: str):
        warnings.warn(
            "Use `save_checks` method instead. This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.save_checks(checks, WorkspaceFileChecksStorageConfig(location=workspace_path))

    def load_checks_from_table(self, table_name: str, run_config_name: str = "default") -> list[dict]:
        warnings.warn(
            "Use `load_checks` method instead. This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.load_checks(TableChecksStorageConfig(location=table_name, run_config_name=run_config_name))

    def save_checks_in_table(
        self, checks: list[dict], table_name: str, run_config_name: str = "default", mode: str = "append"
    ):
        warnings.warn(
            "Use `save_checks` method instead. This method will be removed in future versions.",
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
        warnings.warn(
            "Use `load_checks` method instead. This method will be removed in future versions.",
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
        warnings.warn(
            "Use `save_checks` method instead. This method will be removed in future versions.",
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
        warnings.warn(
            "Use `load_run_config` method from `config_loader.RunConfigLoader` class. "
            "This method will be removed in future versions.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return RunConfigLoader(self.ws).load_run_config(
            run_config_name=run_config_name, assume_user=assume_user, product_name=product_name
        )
