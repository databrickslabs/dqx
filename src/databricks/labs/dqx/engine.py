import logging
import os
import json
import functools as ft
import inspect
import itertools
import warnings
from pathlib import Path
from collections.abc import Callable
from types import UnionType
from typing import Any, get_origin, get_args
import yaml
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.base import DQEngineBase, DQEngineCoreBase
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig, InputConfig, OutputConfig
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import (
    Criticality,
    DQForEachColRule,
    ChecksValidationStatus,
    ColumnArguments,
    ExtraParams,
    DefaultColumnNames,
    DQRule,
    DQRowRule,
    DQDatasetRule,
    CHECK_FUNC_REGISTRY,
)
from databricks.labs.dqx.schema import dq_result_schema
from databricks.labs.dqx.utils import deserialize_dicts, read_input_data, save_dataframe_as_table
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)
COLLECT_LIMIT_WARNING = 500


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
        dq_rule_checks = self.build_quality_rules_by_metadata(checks, custom_check_functions)

        good_df, bad_df = self.apply_checks_and_split(df, dq_rule_checks, ref_dfs)
        return good_df, bad_df

    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: list[dict],
        custom_check_functions: dict[str, Any] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
    ) -> DataFrame:
        dq_rule_checks = self.build_quality_rules_by_metadata(checks, custom_check_functions)

        return self.apply_checks(df, dq_rule_checks, ref_dfs)

    @staticmethod
    def validate_checks(
        checks: list[dict], custom_check_functions: dict[str, Any] | None = None
    ) -> ChecksValidationStatus:
        status = ChecksValidationStatus()

        for check in checks:
            logger.debug(f"Processing check definition: {check}")
            if isinstance(check, dict):
                status.add_errors(DQEngineCore._validate_checks_dict(check, custom_check_functions))
            else:
                status.add_error(f"Unsupported check type: {type(check)}")

        return status

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
        if not filepath:
            raise ValueError("filepath must be provided")

        try:
            checks = Installation.load_local(list[dict[str, str]], Path(filepath))
            return deserialize_dicts(checks)
        except FileNotFoundError:
            msg = f"Checks file {filepath} missing"
            raise FileNotFoundError(msg) from None

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], filepath: str):
        if not filepath:
            raise ValueError("filepath must be provided")

        try:
            with open(filepath, 'w', encoding="utf-8") as file:
                yaml.safe_dump(checks, file)
        except FileNotFoundError:
            msg = f"Checks file {filepath} missing"
            raise FileNotFoundError(msg) from None

    @staticmethod
    def build_quality_rules_from_dataframe(df: DataFrame, run_config_name: str = "default") -> list[dict]:
        """Build checks from a DataFrame based on check specifications, i.e. function name plus arguments.

        :param df: DataFrame with data quality check rules. Each row should define a check. Rows should
        have the following columns:
        * `name` - Name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) - Possible values are `error` (data going only into "bad" dataframe) and `warn` (data is going into both dataframes)
        * `check` - DQX check function used in the check; A `StructType` column defining the data quality check
        * `filter` - Expression for filtering data quality checks
        * `run_config_name` (optional) - Run configuration name for storing checks across runs
        * `user_metadata` (optional) - User-defined key-value pairs added to metadata generated by the check.
        :param run_config_name: Run configuration name for filtering quality rules
        :return: List of data quality check specifications as a Python dictionary
        """
        check_rows = df.where(f"run_config_name = '{run_config_name}'").collect()
        if len(check_rows) > COLLECT_LIMIT_WARNING:
            warnings.warn(
                f"Collecting large number of rows from DataFrame: {len(check_rows)}",
                category=UserWarning,
                stacklevel=2,
            )
        checks = []
        for row in check_rows:
            check_dict = {
                "name": row.name,
                "criticality": row.criticality,
                "check": {
                    "function": row.check["function"],
                    "arguments": (
                        {k: json.loads(v) for k, v in row.check["arguments"].items()}
                        if row.check["arguments"] is not None
                        else {}
                    ),
                },
            }
            if "for_each_column" in row.check and row.check["for_each_column"]:
                check_dict["check"]["for_each_column"] = row.check["for_each_column"]
            if row.filter is not None:
                check_dict["filter"] = row.filter
            if row.user_metadata is not None:
                check_dict["user_metadata"] = row.user_metadata
            checks.append(check_dict)
        return checks

    CHECKS_TABLE_SCHEMA = (
        "name STRING, criticality STRING, check STRUCT<function STRING, for_each_column ARRAY<STRING>,"
        " arguments MAP<STRING, STRING>>, filter STRING, run_config_name STRING, user_metadata MAP<STRING, STRING>"
    )

    @staticmethod
    def build_dataframe_from_quality_rules(
        spark: SparkSession,
        checks: list[dict],
        run_config_name: str = "default",
    ) -> DataFrame:
        """Build a DataFrame from a set of check specifications, i.e. function name plus arguments.

        :param spark: Spark session.
        :param checks: list of check specifications as Python dictionaries. Each check consists of the following fields:
            * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to
               true (it will be used as an error/warning message) or `null` if it's evaluated to `false`
            * `name` - Name that will be given to a resulting column. Autogenerated if not provided
            * `criticality` (optional) - Possible values are `error` (data going only into "bad" dataframe) and `warn`
               (data is going into both dataframes)
            * `filter` (optional) - Expression for filtering data quality checks
            * `user_metadata` (optional) - User-defined key-value pairs added to metadata generated by the check.
        :param run_config_name: Run configuration name for storing quality checks across runs
        :return: DataFrame with data quality check rules
        """
        dq_rule_checks: list[DQRule] = DQEngineCore.build_quality_rules_by_metadata(checks)

        dq_rule_rows = []
        for dq_rule_check in dq_rule_checks:
            arguments = dq_rule_check.check_func_kwargs

            if dq_rule_check.column is not None:
                arguments["column"] = dq_rule_check.column

            if isinstance(dq_rule_check, DQDatasetRule):
                if dq_rule_check.columns is not None:
                    arguments["columns"] = dq_rule_check.columns

            # row_filter is resolved from the check filter so not need to include
            json_arguments = {k: json.dumps(v) for k, v in arguments.items() if k not in {"row_filter"}}
            dq_rule_rows.append(
                [
                    dq_rule_check.name,
                    dq_rule_check.criticality,
                    {"function": dq_rule_check.check_func.__name__, "arguments": json_arguments},
                    dq_rule_check.filter,
                    run_config_name,
                    dq_rule_check.user_metadata,
                ]
            )
        return spark.createDataFrame(dq_rule_rows, DQEngineCore.CHECKS_TABLE_SCHEMA)

    @staticmethod
    def build_quality_rules_by_metadata(
        checks: list[dict], custom_checks: dict[str, Any] | None = None
    ) -> list[DQRule]:
        """Build checks based on check specification, i.e. function name plus arguments.

        :param checks: list of dictionaries describing checks. Each check is a dictionary
        consisting of following fields:
        * `check` - Column expression to evaluate. This expression should return string value if it's evaluated to true
        - it will be used as an error/warning message, or `null` if it's evaluated to `false`
        * `name` - name that will be given to a resulting column. Autogenerated if not provided
        * `criticality` (optional) - possible values are `error` (data going only into "bad" dataframe),
        and `warn` (data is going into both dataframes)
        * `filter` (optional) - Expression for filtering data quality checks
        * `user_metadata` (optional) - User-defined key-value pairs added to metadata generated by the check.
        :param custom_checks: dictionary with custom check functions (eg. ``globals()`` of the calling module).
        If not specified, then only built-in functions are used for the checks.
        :return: list of data quality check rules
        """
        status = DQEngineCore.validate_checks(checks, custom_checks)
        if status.has_errors:
            raise ValueError(str(status))

        dq_rule_checks: list[DQRule] = []
        for check_def in checks:
            logger.debug(f"Processing check definition: {check_def}")

            check = check_def.get("check", {})
            name = check_def.get("name", None)
            func_name = check.get("function")
            func = DQEngineCore.resolve_check_function(func_name, custom_checks, fail_on_missing=True)
            assert func  # should already be validated

            func_args = check.get("arguments", {})
            for_each_column = check.get("for_each_column")
            column = func_args.get("column")  # should be defined for single-column checks only
            columns = func_args.get("columns")  # should be defined for multi-column checks only
            assert not (column and columns)  # should already be validated
            criticality = check_def.get("criticality", "error")
            filter_str = check_def.get("filter")
            user_metadata = check_def.get("user_metadata")

            # Exclude `column` and `columns` from check_func_kwargs
            # as these are always included in the check function call
            check_func_kwargs = {k: v for k, v in func_args.items() if k not in {"column", "columns"}}

            # treat non-registered function as row-level checks
            if for_each_column:
                dq_rule_checks += DQForEachColRule(
                    columns=for_each_column,
                    name=name,
                    check_func=func,
                    criticality=criticality,
                    filter=filter_str,
                    check_func_kwargs=check_func_kwargs,
                    user_metadata=user_metadata,
                ).get_rules()
            else:
                rule_type = CHECK_FUNC_REGISTRY.get(func_name)
                if rule_type == "dataset":
                    dq_rule_checks.append(
                        DQDatasetRule(
                            column=column,
                            columns=columns,
                            check_func=func,
                            check_func_kwargs=check_func_kwargs,
                            name=name,
                            criticality=criticality,
                            filter=filter_str,
                            user_metadata=user_metadata,
                        )
                    )
                else:  # default to row-level rule
                    dq_rule_checks.append(
                        DQRowRule(
                            column=column,
                            columns=columns,
                            check_func=func,
                            check_func_kwargs=check_func_kwargs,
                            name=name,
                            criticality=criticality,
                            filter=filter_str,
                            user_metadata=user_metadata,
                        )
                    )
        return dq_rule_checks

    @staticmethod
    def build_quality_rules_foreach_col(*rules_col_set: DQForEachColRule) -> list[DQRule]:
        """
        Build rules for each column from DQForEachColRule sets.

        :param rules_col_set: list of dq rules which define multiple columns for the same check function
        :return: list of dq rules
        """
        rules_nested = [rule_set.get_rules() for rule_set in rules_col_set]
        flat_rules = list(itertools.chain(*rules_nested))

        return list(filter(None, flat_rules))

    @staticmethod
    def resolve_check_function(
        function_name: str, custom_check_functions: dict[str, Any] | None = None, fail_on_missing: bool = True
    ) -> Callable | None:
        """
        Resolves a function by name from the predefined functions and custom checks.

        :param function_name: name of the function to resolve.
        :param custom_check_functions: dictionary with custom check functions (eg. ``globals()`` of the calling module).
        :param fail_on_missing: if True, raise an AttributeError if the function is not found.
        :return: function or None if not found.
        """
        logger.debug(f"Resolving function: {function_name}")
        func = getattr(check_funcs, function_name, None)  # resolve using predefined checks first
        if not func and custom_check_functions:
            func = custom_check_functions.get(function_name)  # returns None if not found
        if fail_on_missing and not func:
            raise AttributeError(f"Function '{function_name}' not found.")
        logger.debug(f"Function {function_name} resolved successfully: {func}")
        return func

    @staticmethod
    def _get_check_columns(checks: list[DQRule], criticality: str) -> list[DQRule]:
        """Get check columns based on criticality.

        :param checks: list of checks to apply to the dataframe
        :param criticality: criticality
        :return: list of check columns
        """
        return [check for check in checks if check.check_criticality == criticality]

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

    @staticmethod
    def _validate_checks_dict(check: dict, custom_check_functions: dict[str, Any] | None) -> list[str]:
        """
        Validates the structure and content of a given check dictionary.

        Args:
            check (dict): The dictionary to validate.
            custom_check_functions (dict[str, Any] | None): dictionary with custom check functions.

        Returns:
            list[str]: The updated list of error messages.
        """
        errors: list[str] = []

        if "criticality" in check and check["criticality"] not in [c.value for c in Criticality]:
            errors.append(
                f"Invalid 'criticality' value: '{check['criticality']}'. "
                f"Expected '{Criticality.WARN.value}' or '{Criticality.ERROR.value}'. "
                f"Check details: {check}"
            )

        if "check" not in check:
            errors.append(f"'check' field is missing: {check}")
        elif not isinstance(check["check"], dict):
            errors.append(f"'check' field should be a dictionary: {check}")
        else:
            errors.extend(DQEngineCore._validate_check_block(check, custom_check_functions))

        return errors

    @staticmethod
    def _validate_check_block(check: dict, custom_check_functions: dict[str, Any] | None) -> list[str]:
        """
        Validates a check block within a configuration.

        Args:
            check (dict): The entire check configuration.
            custom_check_functions (dict[str, Any] | None): A dictionary with custom check functions.

        Returns:
            list[str]: The updated list of error messages.
        """
        check_block = check["check"]

        if "function" not in check_block:
            return [f"'function' field is missing in the 'check' block: {check}"]

        func_name = check_block["function"]
        func = DQEngineCore.resolve_check_function(func_name, custom_check_functions, fail_on_missing=False)
        if not callable(func):
            return [f"function '{func_name}' is not defined: {check}"]

        arguments = check_block.get("arguments", {})
        for_each_column = check_block.get("for_each_column", [])

        if "for_each_column" in check_block and for_each_column is not None:
            if not isinstance(for_each_column, list):
                return [f"'for_each_column' should be a list in the 'check' block: {check}"]

            if len(for_each_column) == 0:
                return [f"'for_each_column' should not be empty in the 'check' block: {check}"]

        return DQEngineCore._validate_check_function_arguments(arguments, func, for_each_column, check)

    @staticmethod
    def _validate_check_function_arguments(
        arguments: dict, func: Callable, for_each_column: list, check: dict
    ) -> list[str]:
        """
        Validates the provided arguments for a given function and updates the errors list if any validation fails.

        Args:
            arguments (dict): The arguments to validate.
            func (Callable): The function for which the arguments are being validated.
            for_each_column (list): A list of columns to iterate over for the check.
            check (dict): A dictionary containing the validation checks.

        Returns:
            list[str]: The updated list of error messages.
        """
        if not isinstance(arguments, dict):
            return [f"'arguments' should be a dictionary in the 'check' block: {check}"]

        @ft.lru_cache(None)
        def cached_signature(check_func):
            return inspect.signature(check_func)

        func_parameters = cached_signature(func).parameters

        effective_arguments = dict(arguments)  # make a copy to avoid modifying the original
        if for_each_column:
            errors: list[str] = []
            for col_or_cols in for_each_column:
                if "columns" in func_parameters:
                    effective_arguments["columns"] = col_or_cols
                else:
                    effective_arguments["column"] = col_or_cols
                errors.extend(DQEngineCore._validate_func_args(effective_arguments, func, check, func_parameters))
            return errors
        return DQEngineCore._validate_func_args(effective_arguments, func, check, func_parameters)

    @staticmethod
    def _validate_func_args(arguments: dict, func: Callable, check: dict, func_parameters: Any) -> list[str]:
        """
        Validates the arguments passed to a function against its signature.
        Args:
            arguments (dict): A dictionary of argument names and their values to be validated.
            func (Callable): The function whose arguments are being validated.
            check (dict): A dictionary containing additional context or information for error messages.
            func_parameters (Any): The parameters of the function as obtained from its signature.
        Returns:
            list[str]: The updated list of error messages after validation.
        """
        errors: list[str] = []
        if not arguments and func_parameters:
            errors.append(
                f"No arguments provided for function '{func.__name__}' in the 'arguments' block: {check}. "
                f"Expected arguments are: {list(func_parameters.keys())}"
            )
        for arg, value in arguments.items():
            if arg not in func_parameters:
                expected_args = list(func_parameters.keys())
                errors.append(
                    f"Unexpected argument '{arg}' for function '{func.__name__}' in the 'arguments' block: {check}. "
                    f"Expected arguments are: {expected_args}"
                )
            else:
                expected_type = func_parameters[arg].annotation
                if get_origin(expected_type) is list:
                    expected_type_args = get_args(expected_type)
                    errors.extend(DQEngineCore._validate_func_list_args(arg, func, check, expected_type_args, value))
                elif not DQEngineCore._check_type(value, expected_type):
                    expected_type_name = getattr(expected_type, '__name__', str(expected_type))
                    errors.append(
                        f"Argument '{arg}' should be of type '{expected_type_name}' for function '{func.__name__}' "
                        f"in the 'arguments' block: {check}"
                    )
        return errors

    @staticmethod
    def _check_type(value, expected_type) -> bool:
        origin = get_origin(expected_type)
        args = get_args(expected_type)

        if expected_type is inspect.Parameter.empty:
            return True  # no type hint, assume valid

        if origin is UnionType:
            # Handle Optional[X] as Union[X, NoneType]
            return DQEngineCore._check_union_type(args, value)

        if origin is list:
            return DQEngineCore._check_list_type(args, value)

        if origin is dict:
            return DQEngineCore._check_dict_type(args, value)

        if origin is tuple:
            return DQEngineCore._check_tuple_type(args, value)

        if origin:
            return isinstance(value, origin)
        return isinstance(value, expected_type)

    @staticmethod
    def _check_union_type(args, value):
        return any(DQEngineCore._check_type(value, arg) for arg in args)

    @staticmethod
    def _check_list_type(args, value):
        if not isinstance(value, list):
            return False
        if not args:
            return True  # no inner type to check
        return all(DQEngineCore._check_type(item, args[0]) for item in value)

    @staticmethod
    def _check_dict_type(args, value):
        if not isinstance(value, dict):
            return False
        if not args or len(args) != 2:
            return True
        return all(
            DQEngineCore._check_type(k, args[0]) and DQEngineCore._check_type(v, args[1]) for k, v in value.items()
        )

    @staticmethod
    def _check_tuple_type(args, value):
        if not isinstance(value, tuple):
            return False
        if len(args) == 2 and args[1] is Ellipsis:
            return all(DQEngineCore._check_type(item, args[0]) for item in value)
        return len(value) == len(args) and all(DQEngineCore._check_type(item, arg) for item, arg in zip(value, args))

    @staticmethod
    def _validate_func_list_args(
        arguments: dict, func: Callable, check: dict, expected_type_args: tuple[type, ...], value: list[Any]
    ) -> list[str]:
        """
        Validates the list arguments passed to a function against its signature.
        Args:
            arguments (dict): A dictionary of argument names and their values to be validated.
            func (Callable): The function whose arguments are being validated.
            check (dict): A dictionary containing additional context or information for error messages.
            expected_type_args (tuple[type, ...]): Expected types for the list items.
            value (list[Any]): The value of the argument to validate.
        Returns:
            list[str]: list of error messages after validation.
        """
        if not isinstance(value, list):
            return [
                f"Argument '{arguments}' should be of type 'list' for function '{func.__name__}' "
                f"in the 'arguments' block: {check}"
            ]

        errors: list[str] = []
        for i, item in enumerate(value):
            if not isinstance(item, expected_type_args):
                expected_type_name = '|'.join(getattr(arg, '__name__', str(arg)) for arg in expected_type_args)
                errors.append(
                    f"Item {i} in argument '{arguments}' should be of type '{expected_type_name}' "
                    f"for function '{func.__name__}' in the 'arguments' block: {check}"
                )
        return errors


class DQEngine(DQEngineBase):
    """Data Quality Engine class to apply data quality checks to a given dataframe."""

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        engine: DQEngineCoreBase | None = None,
        extra_params: ExtraParams | None = None,
    ):
        super().__init__(workspace_client)

        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark
        self._engine = engine or DQEngineCore(workspace_client, spark, extra_params)

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
        checks: list[dict], custom_check_functions: dict[str, Any] | None = None
    ) -> ChecksValidationStatus:
        """
        Validate the input dict to ensure they conform to expected structure and types.

        Each check can be a dictionary. The function validates
        the presence of required keys, the existence and callability of functions, and the types
        of arguments passed to these functions.

        :param checks: List of checks to apply to the dataframe. Each check should be a dictionary.
        :param custom_check_functions: Optional dictionary with custom check functions.

        :return ValidationStatus: The validation status.
        """
        return DQEngineCore.validate_checks(checks, custom_check_functions)

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

    @staticmethod
    def load_checks_from_local_file(filepath: str) -> list[dict]:
        """
        Load checks (dq rules) from a file (json or yaml) in the local filesystem.

        :param filepath: path to the file containing the checks.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        parsed_checks = DQEngineCore.load_checks_from_local_file(filepath)
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in file: {filepath}")
        return parsed_checks

    def load_checks_from_workspace_file(self, workspace_path: str) -> list[dict]:
        """Load checks (dq rules) from a file (json or yaml) in the workspace.
        This does not require installation of DQX in the workspace.
        The returning checks can be used as input for `apply_checks_by_metadata` function.

        :param workspace_path: path to the file in the workspace.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        workspace_dir = os.path.dirname(workspace_path)
        filename = os.path.basename(workspace_path)
        installation = Installation(self.ws, "dqx", install_folder=workspace_dir)

        logger.info(f"Loading quality rules (checks) from {workspace_path} in the workspace.")
        parsed_checks = self._load_checks_from_file(installation, filename)
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in workspace file: {workspace_path}")
        return parsed_checks

    def load_checks_from_installation(
        self,
        run_config_name: str = "default",
        method: str = "file",
        product_name: str = "dqx",
        assume_user: bool = True,
    ) -> list[dict]:
        """
        Load checks (dq rules) from a file (json or yaml) or table defined in the installation config.
        The returning checks can be used as input for `apply_checks_by_metadata` function.

        :param run_config_name: name of the run (config) to use
        :param method: method to load checks, either 'file' or 'table'
        :param product_name: name of the product/installation directory
        :param assume_user: if True, assume user installation
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        installation = self._get_installation(assume_user, product_name)
        run_config = self._load_run_config(installation, run_config_name)

        if method == "file":
            filename = run_config.checks_file or "checks.yml"
            logger.info(
                f"Loading quality rules (checks) from {installation.install_folder()}/{filename} in the workspace."
            )
            parsed_checks = self._load_checks_from_file(installation, filename)
            if not parsed_checks:
                raise ValueError(f"Invalid or no checks in workspace file: {installation.install_folder()}/{filename}")
            return parsed_checks

        table_name = run_config.checks_table
        if not table_name:
            raise ValueError("Table name must be provided either as a parameter or through run configuration.")

        return self.load_checks_from_table(table_name, run_config_name)

    def load_checks_from_table(self, table_name: str, run_config_name: str = "default") -> list[dict]:
        """
        Load checks (dq rules) from a Delta table in the workspace.
        :param table_name: Unity catalog or Hive metastore table name
        :param run_config_name: Run configuration name for filtering checks
        :return: List of dq rules or raise an error if checks file is missing or is invalid.
        """
        logger.info(f"Loading quality rules (checks) from table {table_name}")
        if not self.ws.tables.exists(table_name).table_exists:
            raise NotFound(f"Table {table_name} does not exist in the workspace")
        return self._load_checks_from_table(table_name, run_config_name)

    @staticmethod
    def save_checks_in_local_file(checks: list[dict], path: str):
        return DQEngineCore.save_checks_in_local_file(checks, path)

    def save_checks_in_installation(
        self,
        checks: list[dict],
        run_config_name: str = "default",
        method: str = "file",
        product_name: str = "dqx",
        assume_user: bool = True,
    ):
        """
        Save checks (dq rules) to yaml file or table in the installation folder.
        This will overwrite existing checks file or table.

        :param checks: list of dq rules to save
        :param run_config_name: name of the run (config) to use
        :param method: method to save checks, either 'file' or 'table'
        :param product_name: name of the product/installation directory
        :param assume_user: if True, assume user installation
        """
        installation = self._get_installation(assume_user, product_name)
        run_config = self._load_run_config(installation, run_config_name)

        if method == "file":
            logger.info(
                f"Saving quality rules (checks) to {installation.install_folder()}/{run_config.checks_file} "
                f"in the workspace."
            )
            return installation.upload(run_config.checks_file, yaml.safe_dump(checks).encode('utf-8'))

        table_name = run_config.checks_table
        if not table_name:
            raise ValueError("Table name must be provided either as a parameter or through run configuration.")

        return self.save_checks_in_table(checks, table_name, run_config_name, mode="overwrite")

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
            installation = self._get_installation(assume_user, product_name)
            run_config = self._load_run_config(installation, run_config_name)
            output_config = run_config.output_config

        if quarantine_df is not None and quarantine_config is None:
            installation = self._get_installation(assume_user, product_name)
            run_config = self._load_run_config(installation, run_config_name)
            quarantine_config = run_config.quarantine_config

        if output_df is not None and output_config is not None:
            save_dataframe_as_table(output_df, output_config)

        if quarantine_df is not None and quarantine_config is not None:
            save_dataframe_as_table(quarantine_df, quarantine_config)

    def save_checks_in_workspace_file(self, checks: list[dict], workspace_path: str):
        """Save checks (dq rules) to yaml file in the workspace.
        This does not require installation of DQX in the workspace.

        :param checks: list of dq rules to save
        :param workspace_path: destination path to the file in the workspace.
        """
        workspace_dir = os.path.dirname(workspace_path)

        logger.info(f"Saving quality rules (checks) to {workspace_path} in the workspace.")
        self.ws.workspace.mkdirs(workspace_dir)
        self.ws.workspace.upload(
            workspace_path, yaml.safe_dump(checks).encode('utf-8'), format=ImportFormat.AUTO, overwrite=True
        )

    def save_checks_in_table(
        self, checks: list[dict], table_name: str, run_config_name: str = "default", mode: str = "append"
    ):
        """
        Save checks to a Delta table in the workspace.
        :param checks: list of dq rules to save
        :param table_name: Unity catalog or Hive metastore fully qualified table name
        :param run_config_name: Run configuration name for identifying groups of checks
        :param mode: Output mode for writing checks to Delta (e.g. `append` or `overwrite`)
        """
        logger.info(f"Saving quality rules (checks) to table {table_name}")
        self._save_checks_in_table(checks, table_name, run_config_name, mode)

    def load_run_config(
        self, run_config_name: str = "default", assume_user: bool = True, product_name: str = "dqx"
    ) -> RunConfig:
        """
        Load run configuration from the installation.

        :param run_config_name: name of the run configuration to use
        :param assume_user: if True, assume user installation
        :param product_name: name of the product
        """
        installation = self._get_installation(assume_user, product_name)
        return self._load_run_config(installation, run_config_name)

    def _get_installation(self, assume_user, product_name):
        if assume_user:
            installation = Installation.assume_user_home(self.ws, product_name)
        else:
            installation = Installation.assume_global(self.ws, product_name)

        # verify the installation
        installation.current(self.ws, product_name, assume_user=assume_user)
        return installation

    @staticmethod
    def _load_run_config(installation, run_config_name):
        """Load run configuration from the installation."""
        config = installation.load(WorkspaceConfig)
        return config.get_run_config(run_config_name)

    @staticmethod
    def _load_checks_from_file(installation: Installation, filename: str) -> list[dict]:
        try:
            checks = installation.load(list[dict[str, str]], filename=filename)
            return deserialize_dicts(checks)
        except NotFound:
            msg = f"Checks file {filename} missing"
            raise NotFound(msg) from None

    def _load_checks_from_table(self, table_name: str, run_config_name: str) -> list[dict]:
        rules_df = self.spark.read.table(table_name)
        return DQEngineCore.build_quality_rules_from_dataframe(rules_df, run_config_name=run_config_name)

    def _save_checks_in_table(self, checks: list[dict], table_name: str, run_config_name: str, mode: str):
        rules_df = DQEngineCore.build_dataframe_from_quality_rules(self.spark, checks, run_config_name=run_config_name)
        rules_df.write.option("replaceWhere", f"run_config_name = '{run_config_name}'").saveAsTable(
            table_name, mode=mode
        )
