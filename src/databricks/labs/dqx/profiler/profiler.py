import uuid
import logging
import os
from concurrent import futures
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
from databricks.labs.dqx.profiler.profile import DQProfile
from databricks.labs.dqx.profiler.profile_builder import PROFILE_BUILDER_REGISTRY, TEXT_TYPES
from databricks.labs.dqx.utils import list_tables
from databricks.labs.dqx.telemetry import telemetry_logger

try:
    from databricks.labs.dqx.llm.llm_engine import DQLLMEngine

    LLM_ENABLED = True
except ImportError:
    LLM_ENABLED = False

logger = logging.getLogger(__name__)


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
    ) -> None:
        """
        Builds a list of DQProfiles by iterating through DQProfileBuilder builders.

        This method mutates *summary_stats* in place: it writes per-column metrics
        (e.g. count, count_null, count_non_null, empty_count, and min/max when a
        min_max profile is produced) into summary_stats[field_name].

        Args:
            df: Input DataFrame to profile.
            df_cols: List of columns to profile.
            dq_rules: List to store the generated data quality rules.
            opts: Dictionary of options for profiling.
            summary_stats: Summary statistics dictionary to update with profiler results.
            total_count: Total number of rows in the input DataFrame.
        """
        trim_strings = opts.get("trim_strings", True)

        for field in self.get_columns_or_fields(df_cols):
            field_name = field.name
            field_type = field.dataType
            if field_name not in summary_stats:
                summary_stats[field_name] = {}
            metrics = summary_stats[field_name]

            column_df = df.select(field_name).dropna()
            column_label = column_df.columns[0]
            is_text = isinstance(field_type, TEXT_TYPES)
            if is_text and trim_strings:
                column_df = column_df.select(F.trim(F.col(column_label)).alias(column_label))

            aggr_stats = column_df.agg(
                F.count(column_label).alias("cnt"),
                F.countDistinct(column_label).alias("cnt_distinct"),
            ).first()
            count_non_null = aggr_stats[0] if aggr_stats else 0
            metrics["count"] = total_count
            metrics["count_null"] = total_count - count_non_null
            metrics["count_non_null"] = count_non_null
            metrics["count_distinct"] = aggr_stats[1] if aggr_stats else 0
            if is_text:
                metrics["empty_count"] = column_df.filter(F.col(column_label) == "").count()
            else:
                metrics["empty_count"] = 0

            self._build_profiles_for_column(column_df, field_name, field_type, metrics, opts, dq_rules)

        self._add_llm_primary_key_for_dataframe(df, dq_rules, summary_stats, opts)

    def _build_profiles_for_column(
        self,
        column_df: DataFrame,
        field_name: str,
        field_type: T.DataType,
        metrics: dict[str, Any],
        opts: dict[str, Any],
        dq_rules: list[DQProfile],
    ) -> None:
        """Run registered profile builders for a column and append profiles.

        Builders are invoked in PROFILE_BUILDER_REGISTRY insertion order (null_or_empty →
        is_in → min_max). Preserving that order matters: the min_max builder reads summary-stats
        metrics written by earlier passes, so it must run last among the built-in builders.

        After a min_max profile is produced, its resolved min/max values are written back into
        *metrics* so that downstream consumers (e.g. LLM primary-key detection) can read them
        without triggering a second Spark action.
        """
        for profile_type in PROFILE_BUILDER_REGISTRY.values():
            profile = profile_type.builder(column_df, field_name, field_type, metrics, opts)
            if not profile:
                continue
            dq_rules.append(profile)
            # Write resolved min/max back into metrics so callers (e.g. summary_stats consumers)
            # can access the final values without re-running Spark aggregates.
            if profile.name == "min_max" and profile.parameters:
                if profile.parameters.get("min") is not None:
                    metrics["min"] = profile.parameters.get("min")
                if profile.parameters.get("max") is not None:
                    metrics["max"] = profile.parameters.get("max")

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
            if isinstance(typ, TEXT_TYPES) and metric in {"min", "max"}:
                sm_dict[metric_name][metric] = None
            elif metric in {"stddev", "mean"}:
                sm_dict[metric_name][metric] = float(metric_value)
            else:
                sm_dict[metric_name][metric] = self._do_cast(metric_value, typ)
        else:
            sm_dict[metric_name][metric] = None

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
        Casts a string value from Spark's DataFrame.summary() output to a typed Python value.

        Args:
            value: The string value to cast. Can be None or empty.
            typ: The PySpark data type to cast the value to.

        Returns:
            The casted value, or None if the input is None/empty or the type is unsupported.
            Unsupported types (e.g. DateType, TimestampType, BooleanType) return None so that
            profile builders fall back to Spark aggregate actions for min/max computation.
        """
        if not value:
            return None
        if isinstance(typ, T.IntegralType):
            return int(value)
        if isinstance(typ, (T.DoubleType, T.FloatType)):
            return float(value)
        if isinstance(typ, T.DecimalType):
            context = Context(prec=typ.precision)
            return Decimal(value, context)
        if isinstance(typ, (T.StringType, T.CharType, T.VarcharType)):
            return value

        # For unsupported types (e.g. DateType, TimestampType, BooleanType), return None.
        # Profile builders handle missing min/max by falling back to Spark aggregate actions.
        return None
