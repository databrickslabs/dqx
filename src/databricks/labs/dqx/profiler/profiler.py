import datetime
import decimal
import math
import uuid
import logging
import os
from concurrent import futures
from dataclasses import dataclass
from datetime import timezone
from decimal import Decimal, Context
from difflib import SequenceMatcher
from fnmatch import fnmatch
from typing import Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.config import InputConfig, LLMModelConfig
from databricks.labs.dqx.errors import MissingParameterError, InvalidConfigError
from databricks.labs.dqx.io import read_input_data, STORAGE_PATH_PATTERN
from databricks.labs.dqx.utils import list_tables
from databricks.labs.dqx.telemetry import telemetry_logger
from databricks.labs.dqx.errors import InvalidParameterError

try:
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine

    LLM_ENABLED = True
except ImportError:
    LLM_ENABLED = False

logger = logging.getLogger(__name__)


@dataclass
class DQProfile:
    name: str
    column: str
    description: str | None = None
    parameters: dict[str, Any] | None = None
    filter: str | None = None


class DQProfiler(DQEngineBase):
    """Data Quality Profiler class to profile input data."""

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        spark: SparkSession | None = None,
        llm_model_config: LLMModelConfig | None = None,
    ):
        super().__init__(workspace_client=workspace_client)
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

        llm_model_config = llm_model_config or LLMModelConfig()
        self.llm_engine = DQLLMEngine(model_config=llm_model_config, spark=self.spark) if LLM_ENABLED else None

    default_profile_options = {
        "round": True,  # round the min/max values
        "max_in_count": 10,  # generate is_in if we have less than 1 percent of distinct values
        "distinct_ratio": 0.05,  # generate is_in if we have less than 1 percent of distinct values
        "max_null_ratio": 0.01,  # generate is_not_null if we have less than 1 percent of nulls
        "remove_outliers": True,  # remove outliers
        "outlier_columns": [],  # remove outliers in the columns
        "num_sigmas": 3,  # number of sigmas to use when remove_outliers is True
        "trim_strings": True,  # trim whitespace from strings
        "max_empty_ratio": 0.01,  # generate is_not_null_or_empty rule if we have less than 1 percent of empty strings
        "sample_fraction": 0.3,  # fraction of data to sample (30%)
        "sample_seed": None,  # seed for sampling
        "limit": 1000,  # limit the number of samples
        "filter": None,  # filter to apply to the dataset
        "llm_primary_key_detection": True,  # detect primary keys
    }

    @staticmethod
    def get_columns_or_fields(columns: list[T.StructField]) -> list[T.StructField]:
        """
        Extracts all fields from a list of StructField objects, including nested fields from StructType columns.

        Args:
            columns: A list of StructField objects to process.

        Returns:
            A list of StructField objects, including nested fields with prefixed names.
        """
        out_columns = []
        for column in columns:
            col_name = column.name
            if isinstance(column.dataType, T.StructType):
                out_columns.extend(DQProfiler._get_fields(col_name, column.dataType))
            else:
                out_columns.append(column)

        return out_columns

    # TODO: how to handle maps, arrays & structs?
    @telemetry_logger("profiler", "profile")
    def profile(
        self, df: DataFrame, columns: list[str] | None = None, options: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], list[DQProfile]]:
        """
        Profiles a DataFrame to generate summary statistics and data quality rules.

        Args:
            df: The DataFrame to profile.
            columns: An optional list of column names to include in the profile. If None, all columns are included.
            options: An optional dictionary of options for profiling.

        Returns:
            A tuple containing a dictionary of summary statistics and a list of data quality profiles.
        """

        columns = columns or df.columns
        df_columns = [f for f in df.schema.fields if f.name in columns]
        df = df.select(*[f.name for f in df_columns])

        if options is None:
            options = {}

        options = {**self.default_profile_options, **options}  # merge default options with user-provided options
        df = self._sample(df, options)

        dq_rules: list[DQProfile] = []
        total_count = df.count()
        summary_stats = self._get_df_summary_as_dict(df)
        if total_count == 0:
            return summary_stats, dq_rules

        self._profile(df, df_columns, dq_rules, options, summary_stats, total_count)

        return summary_stats, dq_rules

    @telemetry_logger("profiler", "profile_table")
    def profile_table(
        self,
        input_config: InputConfig,
        columns: list[str] | None = None,
        options: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], list[DQProfile]]:
        """
        Profiles a table to generate summary statistics and data quality rules.

        Args:
            input_config: Input configuration containing the table location.
            columns: An optional list of column names to include in the profile. If None, all columns are included.
            options: An optional dictionary of options for profiling.

        Returns:
            A tuple containing a dictionary of summary statistics and a list of data quality profiles.
        """
        if not input_config or not input_config.location:
            raise MissingParameterError("Input config with location is required")

        logger.info(f"Profiling {input_config.location} with options: {options}")
        df = read_input_data(spark=self.spark, input_config=input_config)
        return self.profile(df=df, columns=columns, options=options)

    @telemetry_logger("profiler", "profile_tables_for_patterns")
    def profile_tables_for_patterns(
        self,
        patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        exclude_matched: bool = False,
        columns: dict[str, list[str]] | None = None,
        options: list[dict[str, Any]] | None = None,
        max_parallelism: int | None = os.cpu_count(),
    ) -> dict[str, tuple[dict[str, Any], list[DQProfile]]]:
        """
        Profiles Delta tables in Unity Catalog to generate summary statistics and data quality rules.

        Args:
            patterns: List of table names or filesystem-style wildcards (e.g. 'schema.*') to include.
                If None, all tables are included. By default, tables matching the pattern are included.
            exclude_patterns: List of table names or filesystem-style wildcards (e.g. 'schema.*') to exclude.
                If None, no tables are excluded.
            exclude_matched: Specifies whether to include tables matched by the pattern. If True, matched tables
                are excluded. If False, matched tables are included.
            columns: A dictionary with column names to include in the profile. Keys should be fully-qualified table
                names (e.g. *catalog.schema.table*) and values should be lists of column names to include in profiling.
            options: A dictionary with options for profiling each table. Keys should be fully-qualified table names
                (e.g. *catalog.schema.table*) and values should be options for profiling.
            max_parallelism: An optional concurrency limit for profiling concurrently

        Returns:
            A dictionary mapping table names to tuples containing summary statistics and data quality profiles.
        """
        tables = list_tables(
            workspace_client=self.ws,
            patterns=patterns,
            exclude_matched=exclude_matched,
            exclude_patterns=exclude_patterns,
        )

        return self._profile_tables(
            tables=tables,
            columns=columns,
            options=options,
            max_parallelism=max_parallelism,
        )

    @telemetry_logger("profiler", "detect_primary_keys_with_llm")
    def detect_primary_keys_with_llm(self, input_config: InputConfig) -> dict[str, Any]:
        """
        Detects primary keys using LLM-based analysis.

        This method analyzes table schema and metadata to identify primary key columns.

        Args:
            input_config: Input configuration containing the table location.

        Returns:
            A dictionary containing the primary key detection result with the following keys:
            - table: The table name
            - success: Whether detection was successful
            - primary_key_columns: List of detected primary key columns (if successful)
            - confidence: Confidence level (high/medium/low)
            - reasoning: LLM reasoning for the selection
            - has_duplicates: Whether duplicates were found (if validation performed)
            - duplicate_count: Number of duplicate combinations (if validation performed)
            - error: Error message (if failed)
        """
        if self.llm_engine is None:
            raise MissingParameterError(
                "LLM engine not available. Make sure LLM dependencies are installed: "
                "pip install 'databricks-labs-dqx[llm]'"
            )

        if not input_config.location:
            raise InvalidConfigError("Input location not configured")

        def _detect_with_temp_view(df: DataFrame, view_name: str) -> dict[str, Any]:
            try:
                df.createOrReplaceTempView(view_name)
                assert self.llm_engine is not None  # for mypy
                return self.llm_engine.detect_primary_keys_with_llm(view_name)
            finally:
                try:
                    self.spark.sql(f"DROP VIEW IF EXISTS {view_name}")
                except AnalysisException:
                    pass

        if STORAGE_PATH_PATTERN.match(input_config.location):
            input_df = read_input_data(self.spark, input_config)
            temp_view_name = f"temp_from_dataframe_{id(input_df)}_{uuid.uuid4().hex}"
            return _detect_with_temp_view(input_df, temp_view_name)

        return self.llm_engine.detect_primary_keys_with_llm(input_config.location)

    def _profile_tables(
        self,
        tables: list[str] | None = None,
        columns: dict[str, list[str]] | None = None,
        options: list[dict[str, Any]] | None = None,
        max_parallelism: int | None = os.cpu_count(),
    ) -> dict[str, tuple[dict[str, Any], list[DQProfile]]]:
        """
        Profiles a list of tables to generate summary statistics and data quality rules.

        Args:
            tables: A list of fully-qualified table names (*catalog.schema.table*) to be profiled
            columns: A dictionary with column names to include in the profile. Keys should be fully-qualified table
                names (e.g. *catalog.schema.table*) and values should be lists of column names to include in profiling.
            options: A dictionary with options for profiling each table. Keys should be fully-qualified table names
                (e.g. *catalog.schema.table*) and values should be options for profiling.
            max_parallelism: An optional concurrency limit for profiling concurrently

        Returns:
            A dictionary mapping table names to tuples containing summary statistics and data quality profiles.
        """
        tables = tables or []
        columns = columns or {}
        options = options or []
        args = []

        for table in tables:
            args.append(
                {
                    "input_config": InputConfig(location=table),
                    "columns": columns.get(table, None),
                    "options": DQProfiler._build_options_from_list(table, options),
                }
            )

        logger.info(f"Profiling tables with {max_parallelism} workers")
        with futures.ThreadPoolExecutor(max_workers=max_parallelism) as executor:
            results = executor.map(lambda arg: self.profile_table(**arg), args)
            return dict(zip(tables, results))

    @staticmethod
    def _build_options_from_list(table: str, options: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Builds the options dictionary from a list of matched options. Options with the highest similarity are
        prioritized in the result.

        Args:
            table: Table name
            options: List of options dictionaries with the following fields:
                * *table* - Table name or wildcard pattern (e.g. *catalog.schema.*) for applying options
                * *options* - Dictionary of profiler options

        Returns:
            Dictionary of profiler options matching the provided table name
        """
        matched_options = DQProfiler._match_options_list(table, options)
        sorted_options = DQProfiler._sort_options_list(table, matched_options)
        built_options = DQProfiler.default_profile_options.copy()
        for opt in sorted_options:
            if opt and isinstance(opt, dict):
                built_options |= opt.get("options") or {}
        return built_options

    @staticmethod
    def _match_options_list(table: str, options: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Returns a subset of options whose 'table' pattern matches the provided table name.

        Args:
            table: Table name
            options: List of options dictionaries with the following fields:
                * *table* - Table name or wildcard pattern (e.g. *catalog.schema.*) for applying options
                * *options* - Dictionary of profiler options

        Returns:
            List of options dictionaries matching the provided table name
        """
        return [opts for opts in options if fnmatch(table, opts.get("table", ""))]

    @staticmethod
    def _sort_options_list(table: str, options: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Sorts the options list by sequence similarity with the provided table name.

        Args:
            table: Table name
            options: List of options dictionaries with the following fields:
                * *table* - Table name or wildcard pattern (e.g. *catalog.schema.*) for applying options
                * *options* - Dictionary of profiler options

        Returns:
            List of options dictionaries sorted by similarity to the provided table name
        """
        return sorted(
            [opts | {"score": SequenceMatcher(None, table, opts.get("table", "")).quick_ratio()} for opts in options],
            key=lambda d: d.get("score", 0),
        )

    @staticmethod
    def _sample(df: DataFrame, opts: dict[str, Any]) -> DataFrame:
        sample_fraction = opts.get("sample_fraction", None)
        sample_seed = opts.get("sample_seed", None)
        limit = opts.get("limit", None)
        filter_dataset = opts.get("filter", None)

        if filter_dataset:
            df = df.filter(filter_dataset)
        if sample_fraction:
            df = df.sample(withReplacement=False, fraction=sample_fraction, seed=sample_seed)
        if limit:
            df = df.limit(limit)

        return df

    def _profile(
        self,
        df: DataFrame,
        df_cols: list[T.StructField],
        dq_rules: list[DQProfile],
        opts: dict[str, Any],
        summary_stats: dict[str, Any],
        total_count: int,
    ):
        # TODO: think, how we can do it in fewer passes. Maybe only for specific things, like, min_max, etc.
        for field in self.get_columns_or_fields(df_cols):
            field_name = field.name
            typ = field.dataType
            if field_name not in summary_stats:
                summary_stats[field_name] = {}
            metrics = summary_stats[field_name]

            self._calculate_metrics(df, dq_rules, field_name, metrics, opts, total_count, typ)

        self._add_llm_primary_key_for_dataframe(df, dq_rules, summary_stats, opts)

    def _add_llm_primary_key_for_dataframe(
        self, df: DataFrame, dq_rules: list[DQProfile], summary_stats: dict[str, Any], opts: dict[str, Any]
    ) -> None:
        """
        Adds LLM-based primary key detection results for DataFrames to summary statistics if enabled.

        Args:
            df: The DataFrame to analyze
            dq_rules: A list to store the generated data quality rules.
            summary_stats: Summary statistics dictionary to update with PK detection results
            opts: A dictionary of options for profiling.
        """
        if not LLM_ENABLED or not opts.get("llm_primary_key_detection", False):
            return

        logger.info("🤖 Starting LLM-based primary key detection for DataFrame")

        temp_view_name = f"temp_from_dataframe_{id(df)}_{uuid.uuid4().hex}"
        pk_result: dict[str, Any] = {}

        try:
            df.createOrReplaceTempView(temp_view_name)
            pk_result = self.detect_primary_keys_with_llm(input_config=InputConfig(location=temp_view_name))
        finally:
            try:  # Clean up the temporary view using SQL (Unity Catalog compatible)
                self.spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
            except AnalysisException:
                pass  # Ignore cleanup errors

        if pk_result and pk_result.get("success", False) and not pk_result.get("has_duplicates", False):
            pk_columns = pk_result.get("primary_key_columns", [])
            if pk_columns and pk_columns != ["none"]:
                # Validate that detected columns actually exist in the DataFrame
                valid_columns = [col for col in pk_columns if col in df.columns]
                if valid_columns:
                    reasoning = pk_result.get("reasoning", "")
                    confidence = pk_result.get("confidence", "unknown")
                    dq_rules.append(
                        DQProfile(
                            name="is_unique",
                            column=",".join(valid_columns),
                            parameters={"nulls_distinct": False, "reasoning": reasoning, "confidence": confidence},
                            filter=opts.get("filter", None),
                            description=f"LLM-detected primary key columns: {', '.join(valid_columns)}",
                        )
                    )
                    # Add to summary stats (but don't automatically generate rules)
                    summary_stats["llm_primary_key_detection"] = {
                        "detected_columns": valid_columns,
                        "confidence": confidence,
                        "reasoning": reasoning,
                        "method": "llm",
                    }
                    logger.info(f"✅ LLM-based primary key detected for DataFrame: {valid_columns}")

    def _calculate_metrics(
        self,
        df: DataFrame,
        dq_rules: list[DQProfile],
        field_name: str,
        metrics: dict[str, Any],
        opts: dict[str, Any],
        total_count: int,
        typ: T.DataType,
    ):
        """
        Calculates various metrics for a given DataFrame column and updates the data quality rules.

        Args:
            df: The DataFrame containing the data.
            dq_rules: A list to store the generated data quality rules.
            field_name: The name of the column to calculate metrics for.
            metrics: A dictionary to store the calculated metrics.
            opts: A dictionary of options for metric calculation.
            total_count: The total number of rows in the DataFrame.
            typ: The data type of the column.
        """
        max_nulls = opts.get("max_null_ratio", 0)
        trim_strings = opts.get("trim_strings", True)

        dst = df.select(field_name).dropna()
        if typ == T.StringType() and trim_strings:
            col_name = dst.columns[0]
            dst = dst.select(F.trim(F.col(col_name)).alias(col_name))

        metrics["count"] = total_count
        count_non_null = dst.count()
        metrics["count_non_null"] = count_non_null
        metrics["count_null"] = total_count - count_non_null
        if count_non_null >= (total_count * (1 - max_nulls)):
            if count_non_null != total_count:
                null_percentage = 1 - (1.0 * count_non_null) / total_count
                dq_rules.append(
                    DQProfile(
                        name="is_not_null",
                        column=field_name,
                        description=f"Column {field_name} has {null_percentage * 100:.1f}% of null values "
                        f"(allowed {max_nulls * 100:.1f}%)",
                        filter=opts.get("filter", None),
                    )
                )
            else:
                dq_rules.append(
                    DQProfile(
                        name="is_not_null",
                        column=field_name,
                        filter=opts.get("filter", None),
                    )
                )
        if self._type_supports_distinct(typ):
            dst2 = dst.dropDuplicates()
            cnt = dst2.count()
            if 0 < cnt < total_count * opts["distinct_ratio"] and cnt < opts["max_in_count"]:
                dq_rules.append(
                    DQProfile(
                        name="is_in",
                        column=field_name,
                        parameters={"in": [row[0] for row in dst2.collect()]},
                        filter=opts.get("filter", None),
                    )
                )

        if (
            typ == T.StringType()
            and not any(  # does not make sense to add is_not_null_or_empty if is_not_null already exists
                rule.name == "is_not_null" and rule.column == field_name for rule in dq_rules
            )
        ):
            dst2 = dst.filter(F.col(field_name) == "")
            cnt = dst2.count()
            if cnt <= (metrics["count"] * opts.get("max_empty_ratio", 0)):
                dq_rules.append(
                    DQProfile(
                        name="is_not_null_or_empty",
                        column=field_name,
                        parameters={"trim_strings": trim_strings},
                        filter=opts.get("filter", None),
                    )
                )
        if metrics["count_non_null"] > 0 and self._type_supports_min_max(typ):
            rule = self._extract_min_max(dst, field_name, typ, metrics, opts)
            if rule:
                dq_rules.append(rule)

    def _get_df_summary_as_dict(self, df: DataFrame) -> dict[str, Any]:
        """
        Generate summary for DataFrame and return it as a dictionary with column name as a key, and dict of metric/value.

        Args:
            df: The DataFrame to profile.

        Returns:
            A dictionary with metrics per column.
        """
        sm_dict: dict[str, dict] = {}
        field_types = {f.name: f.dataType for f in df.schema.fields}
        for row in df.summary().collect():
            row_dict = row.asDict()
            metric = row_dict["summary"]
            self._process_row(row_dict, metric, sm_dict, field_types)
        return sm_dict

    def _process_row(self, row_dict: dict, metric: str, sm_dict: dict, field_types: dict):
        """
        Processes a row from the DataFrame summary and updates the summary dictionary.

        Args:
            row_dict: A dictionary representing a row from the DataFrame summary.
            metric: The metric name (e.g., "mean", "stddev") for the current row.
            sm_dict: The summary dictionary to update with the processed metrics.
            field_types: A dictionary mapping column names to their data types.
        """
        for metric_name, metric_value in row_dict.items():
            if metric_name == "summary":
                continue
            if metric_name not in sm_dict:
                sm_dict[metric_name] = {}
            self._process_metric(metric_name, metric_value, metric, sm_dict, field_types)

    def _process_metric(self, metric_name: str, metric_value: Any, metric: str, sm_dict: dict, field_types: dict):
        """
        Processes a metric value and updates the summary dictionary with the casted value.

        Args:
            metric_name: The name of the metric (e.g., column name).
            metric_value: The value of the metric to process.
            metric: The type of metric (e.g., "stddev", "mean").
            sm_dict: The summary dictionary to update with the processed metric.
            field_types: A dictionary mapping column names to their data types.
        """
        typ = field_types[metric_name]
        if metric_value is not None:
            if metric in {"stddev", "mean"}:
                sm_dict[metric_name][metric] = float(metric_value)
            else:
                sm_dict[metric_name][metric] = self._do_cast(metric_value, typ)
        else:
            sm_dict[metric_name][metric] = None

    def _round_value(self, value: Any, direction: str, opts: dict[str, Any]) -> Any:
        """
        Rounds a value based on the specified direction and options.

        Args:
            value: The value to round.
            direction: The direction to round the value ("up" or "down").
            opts: A dictionary of options, including whether to round the value.

        Returns:
            The rounded value, or the original value if rounding is not enabled.
        """
        if not value or not opts.get("round", False):
            return value

        if isinstance(value, datetime.datetime):
            return self._round_datetime(value, direction)

        if isinstance(value, float):
            return self._round_float(value, direction)

        if isinstance(value, int):
            return value  # already rounded

        if isinstance(value, decimal.Decimal):
            return self._round_decimal(value, direction)

        return value

    def _extract_min_max(
        self,
        dst: DataFrame,
        col_name: str,
        typ: T.DataType,
        metrics: dict[str, Any],
        opts: dict[str, Any] | None = None,
    ) -> DQProfile | None:
        """
        Generates a data quality profile rule for column value ranges.

        Args:
            dst: A single-column DataFrame containing the data to analyze.
            col_name: The name of the column to generate the rule for.
            typ: The data type of the column.
            metrics: A dictionary to store the calculated metrics.
            opts: Optional dictionary of options for rule generation.

        Returns:
            A DQProfile object representing the min/max rule, or None if no rule is generated.
        """
        descr = None
        min_limit = None
        max_limit = None

        if opts is None:
            opts = {}

        outlier_cols = opts.get("outlier_columns", [])
        column = dst.columns[0]
        if opts.get("remove_outliers", True) and (
            len(outlier_cols) == 0 or col_name in outlier_cols
        ):  # detect outliers
            if typ == T.DateType():
                dst = dst.select(F.col(column).cast("timestamp").cast("bigint").alias(column))
            elif typ == T.TimestampType():
                dst = dst.select(F.col(column).cast("bigint").alias(column))
            # TODO: do summary instead? to get percentiles, etc.?
            mn_mx = dst.agg(F.min(column), F.max(column), F.mean(column), F.stddev(column)).collect()
            descr, max_limit, min_limit = self._get_min_max(
                col_name, descr, max_limit, metrics, min_limit, mn_mx, opts, typ
            )
        else:
            mn_mx_df = dst.agg(F.min(column).alias('min_value'), F.max(column).alias('max_value'))
            if typ == T.TimestampType():
                mn_mx_df = mn_mx_df.withColumn(
                    "min_value", F.date_format("min_value", "yyyy-MM-dd HH:mm:ss")
                ).withColumn("max_value", F.date_format("max_value", "yyyy-MM-dd HH:mm:ss"))
            mn_mx = mn_mx_df.collect()
            if mn_mx and len(mn_mx) > 0:
                if typ == T.TimestampType():
                    metrics['min'] = datetime.datetime.strptime(mn_mx[0][0], "%Y-%m-%d %H:%M:%S")
                    metrics['max'] = datetime.datetime.strptime(mn_mx[0][1], "%Y-%m-%d %H:%M:%S")
                else:
                    metrics["min"] = mn_mx[0][0]
                    metrics["max"] = mn_mx[0][1]
                min_limit = self._round_value(metrics.get("min"), "down", opts)
                max_limit = self._round_value(metrics.get("max"), "up", opts)
                descr = "Real min/max values were used"
            else:
                logger.info(f"Can't get min/max for field {col_name}")
        if descr and min_limit and max_limit:
            return DQProfile(
                name="min_max",
                column=col_name,
                parameters={"min": min_limit, "max": max_limit},
                description=descr,
                filter=opts.get("filter", None),
            )

        return None

    def _get_min_max(
        self,
        col_name: str,
        descr: str | None,
        max_limit: Any | None,
        metrics: dict[str, Any],
        min_limit: Any | None,
        mn_mx: list,
        opts: dict[str, Any],
        typ: T.DataType,
    ):
        """
        Calculates the minimum and maximum limits for a column based on the provided metrics and options.

        Args:
            col_name: The name of the column.
            descr: The description of the min/max calculation.
            max_limit: The maximum limit for the column.
            metrics: A dictionary to store the calculated metrics.
            min_limit: The minimum limit for the column.
            mn_mx: A list containing the min, max, mean, and stddev values for the column.
            opts: A dictionary of options for the min/max calculation.
            typ: The data type of the column.

        Returns:
            A tuple containing the description, maximum limit, and minimum limit.
        """
        if mn_mx and len(mn_mx) > 0:
            metrics["min"] = mn_mx[0][0]
            metrics["max"] = mn_mx[0][1]
            sigmas = opts.get("sigmas", 3)
            avg = mn_mx[0][2]
            stddev = mn_mx[0][3]

            if avg is None or stddev is None:
                return descr, max_limit, min_limit

            if isinstance(typ, T.DecimalType):
                context = Context(prec=typ.precision)
                sigmas = Decimal(sigmas, context)
                stddev = Decimal(stddev, context)
                avg = Decimal(avg, context)

            min_limit = avg - sigmas * stddev
            max_limit = avg + sigmas * stddev
            if min_limit > mn_mx[0][0] and max_limit < mn_mx[0][1]:
                descr = (
                    f"Range doesn't include outliers, capped by {sigmas} sigmas. avg={avg}, "
                    f"stddev={stddev}, min={metrics.get('min')}, max={metrics.get('max')}"
                )
            elif min_limit < mn_mx[0][0] and max_limit > mn_mx[0][1]:  #
                min_limit = mn_mx[0][0]
                max_limit = mn_mx[0][1]
                descr = "Real min/max values were used"
            elif min_limit < mn_mx[0][0]:
                min_limit = mn_mx[0][0]
                descr = (
                    f"Real min value was used. Max was capped by {sigmas} sigmas. avg={avg}, "
                    f"stddev={stddev}, max={metrics.get('max')}"
                )
            elif max_limit > mn_mx[0][1]:
                max_limit = mn_mx[0][1]
                descr = (
                    f"Real max value was used. Min was capped by {sigmas} sigmas. avg={avg}, "
                    f"stddev={stddev}, min={metrics.get('min')}"
                )
            # we need to preserve type at the end
            min_limit, max_limit = self._adjust_min_max_limits(min_limit, max_limit, avg, typ, metrics, opts)
        else:
            logger.info(f"Can't get min/max for field {col_name}")
        return descr, max_limit, min_limit

    def _adjust_min_max_limits(
        self, min_limit: Any, max_limit: Any, avg: Any, typ: T.DataType, metrics: dict[str, Any], opts: dict[str, Any]
    ) -> tuple[Any, Any]:
        """
        Adjusts the minimum and maximum limits based on the data type of the column.

        Args:
            min_limit: The minimum limit to adjust.
            max_limit: The maximum limit to adjust.
            avg: The average value of the column.
            typ: The PySpark data type of the column.
            metrics: A dictionary containing the calculated metrics.
            opts: A dictionary of options for min/max limit adjustment.

        Returns:
            A tuple containing the adjusted minimum and maximum limits.
        """
        if isinstance(typ, T.IntegralType):
            min_limit = int(self._round_value(min_limit, "down", opts))
            max_limit = int(self._round_value(max_limit, "up", opts))
        elif typ == T.DateType():
            min_limit = datetime.date.fromtimestamp(int(min_limit))
            max_limit = datetime.date.fromtimestamp(int(max_limit))
            metrics["min"] = datetime.date.fromtimestamp(int(metrics["min"]))
            metrics["max"] = datetime.date.fromtimestamp(int(metrics["max"]))
            metrics["mean"] = datetime.date.fromtimestamp(int(avg))
        elif typ == T.TimestampType():
            min_limit = self._round_value(
                datetime.datetime.fromtimestamp(int(min_limit), tz=timezone.utc), "down", opts
            )
            max_limit = self._round_value(datetime.datetime.fromtimestamp(int(max_limit), tz=timezone.utc), "up", opts)
            metrics["min"] = datetime.datetime.fromtimestamp(int(metrics["min"]), tz=timezone.utc)
            metrics["max"] = datetime.datetime.fromtimestamp(int(metrics["max"]), tz=timezone.utc)
            metrics["mean"] = datetime.datetime.fromtimestamp(int(avg), tz=timezone.utc)
        return min_limit, max_limit

    @staticmethod
    def _get_fields(col_name: str, schema: T.StructType) -> list[T.StructField]:
        """
        Recursively extracts all fields from a nested StructType schema and prefixes them with the given column name.

        Args:
            col_name: The prefix to add to each field name.
            schema: The StructType schema to extract fields from.

        Returns:
            A list of StructField objects with prefixed names.
        """
        fields = []
        for f in schema.fields:
            if isinstance(f.dataType, T.StructType):
                fields.extend(DQProfiler._get_fields(f.name, f.dataType))
            else:
                fields.append(f)

        return [T.StructField(f"{col_name}.{f.name}", f.dataType, f.nullable) for f in fields]

    @staticmethod
    def _do_cast(value: str | None, typ: T.DataType) -> Any | None:
        """
        Casts a string value to a specified PySpark data type.

        Args:
            value: The string value to cast. Can be None.
            typ: The PySpark data type to cast the value to.

        Returns:
            The casted value, or None if the input value is None.
        """
        if not value:
            return None
        if isinstance(typ, T.IntegralType):
            return int(value)
        if typ == T.DoubleType() or typ == T.FloatType():
            return float(value)
        if isinstance(typ, T.DecimalType):
            context = Context(prec=typ.precision)
            return Decimal(value, context)
        if typ == T.StringType():
            return value

        raise InvalidParameterError(f"Unsupported data type for casting: {typ}")

    @staticmethod
    def _type_supports_distinct(typ: T.DataType) -> bool:
        """
        Checks if the given PySpark data type supports distinct operations.

        Args:
            typ: The PySpark data type to check.

        Returns:
            True if the data type supports distinct operations, False otherwise.
        """
        return typ == T.StringType() or typ == T.IntegerType() or typ == T.LongType()

    @staticmethod
    def _type_supports_min_max(typ: T.DataType) -> bool:
        """
        Checks if the given PySpark data type supports min and max operations.

        Args:
            typ: The PySpark data type to check.

        Returns:
            True if the data type supports min and max operations, False otherwise.
        """
        return isinstance(typ, T.NumericType) or typ == T.DateType() or typ == T.TimestampType()

    @staticmethod
    def _round_datetime(value: datetime.datetime, direction: str) -> datetime.datetime:
        """
        Rounds a datetime value to midnight based on the specified direction.

        - "down" → truncate to midnight (00:00:00).
        - "up" → return the next midnight unless value is already midnight.

        Args:
            value: The datetime value to round.
            direction: The rounding direction ("up" or "down").

        Returns:
            The rounded datetime value.

        Raises:
            InvalidParameterError: If direction is not 'up' or 'down'.
        """
        midnight = value.replace(hour=0, minute=0, second=0, microsecond=0)

        if direction == "down":
            return midnight

        if direction == "up":
            if midnight == value:
                return value
            try:
                return midnight + datetime.timedelta(days=1)
            except OverflowError:
                logger.warning("Rounding datetime up caused overflow; returning datetime.max instead.")
                return datetime.datetime.max
        raise InvalidParameterError(f"Invalid rounding direction: {direction}. Use 'up' or 'down'.")

    @staticmethod
    def _round_float(value: float, direction: str) -> float:
        if direction == "down":
            return math.floor(value)
        if direction == "up":
            return math.ceil(value)
        return value

    @staticmethod
    def _round_decimal(value: decimal.Decimal, direction: str) -> decimal.Decimal:
        if direction == "down":
            return value.to_integral_value(rounding=decimal.ROUND_FLOOR)
        if direction == "up":
            return value.to_integral_value(rounding=decimal.ROUND_CEILING)
        return value
