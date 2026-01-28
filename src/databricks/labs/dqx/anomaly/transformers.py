"""
Feature engineering for anomaly detection.

Provides column type analysis and Spark-native feature transformations.
All transformations are applied in Spark (distributed) for scalability and
Spark Connect compatibility (no custom Python class serialization).
"""

import json
import re
import sys
from dataclasses import dataclass
from io import StringIO
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import coalesce, col, cos, count, dayofweek, hour, lit, month, pi, sin, to_timestamp, when
from pyspark.sql.types import DoubleType, TimestampType

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.telemetry import get_tables_from_spark_plan
from databricks.labs.dqx.utils import get_table_primary_keys


@dataclass
class ColumnTypeInfo:
    """Information about a column's type and encoding strategy."""

    name: str
    spark_type: T.DataType
    category: str  # 'numeric', 'categorical', 'datetime', 'boolean', 'unsupported'
    cardinality: int | None = None
    null_count: int | None = None
    encoding_strategy: str | None = None  # 'onehot', 'frequency', 'cyclical', 'binary', 'none'


@dataclass
class SparkFeatureMetadata:
    """
    Metadata for reconstructing Spark transformations during scoring.

    Stores everything needed to apply the same transformations:
    - Column types and encoding strategies
    - Frequency maps for categorical encoding (high cardinality)
    - OneHot distinct values (low cardinality)
    - Engineered feature names (in order)
    """

    column_infos: list[dict[str, Any]]  # Serializable version of ColumnTypeInfo
    categorical_frequency_maps: dict[str, dict[str, float]]  # col_name -> (value -> frequency)
    onehot_categories: dict[str, list[str]]  # col_name -> [distinct_values] for OneHot encoding
    engineered_feature_names: list[str]  # Final feature names after engineering
    categorical_cardinality_threshold: int = 20  # Threshold used for categorical encoding

    def to_json(self) -> str:
        """Serialize to JSON for storage."""
        return json.dumps(
            {
                "column_infos": self.column_infos,
                "categorical_frequency_maps": self.categorical_frequency_maps,
                "onehot_categories": self.onehot_categories,
                "engineered_feature_names": self.engineered_feature_names,
                "categorical_cardinality_threshold": self.categorical_cardinality_threshold,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "SparkFeatureMetadata":
        """Deserialize from JSON."""
        data = json.loads(json_str)
        if "categorical_cardinality_threshold" not in data:
            data["categorical_cardinality_threshold"] = 20
        return cls(**data)


def reconstruct_column_infos(feature_metadata: SparkFeatureMetadata) -> list[ColumnTypeInfo]:
    """
    Reconstruct ColumnTypeInfo objects from SparkFeatureMetadata.

    Helper function to avoid code duplication when reconstructing column information
    for feature engineering during scoring.

    Args:
        feature_metadata: SparkFeatureMetadata with column information

    Returns:
        List of ColumnTypeInfo objects
    """
    return [
        ColumnTypeInfo(
            name=info["name"],
            spark_type=T.StringType(),  # Type not used in scoring
            category=info["category"],
            cardinality=info.get("cardinality"),
            null_count=info.get("null_count"),
        )
        for info in feature_metadata.column_infos
    ]


class ColumnTypeClassifier:
    """
    Analyzes DataFrame schema and categorizes columns for feature engineering.

    Categories:
    - numeric: int, long, float, double, decimal
    - categorical: string (with reasonable cardinality)
    - datetime: date, timestamp, timestampNTZ
    - boolean: boolean
    - unsupported: array, map, struct, binary, etc.
    """

    def __init__(
        self,
        categorical_cardinality_threshold: int = 20,
        max_input_columns: int = 10,
        max_engineered_features: int = 50,
    ):
        self.categorical_cardinality_threshold = categorical_cardinality_threshold
        self.max_input_columns = max_input_columns
        self.max_engineered_features = max_engineered_features

    def analyze_columns(self, df: DataFrame, columns: list[str]) -> tuple[list[ColumnTypeInfo], list[str]]:
        """
        Analyze columns and return type information and warnings.

        Returns:
            Tuple of (column_type_infos, warnings)
        """
        warnings_list = []

        # Warn if many columns selected (soft limit)
        if len(columns) > self.max_input_columns:
            warnings_list.append(
                f"Training with {len(columns)} columns (recommended max: {self.max_input_columns}). "
                f"More columns may increase training/scoring time and reduce model quality. "
                f"Tip: Omit 'columns' parameter to let DQX auto-select the most relevant columns."
            )

        schema = {f.name: f.dataType for f in df.schema.fields}
        column_infos = []
        unsupported_cols = []

        null_exprs = [F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"{col_name}__nulls") for col_name in columns]
        nulls_row = df.agg(*null_exprs).first()
        if nulls_row is None:
            raise InvalidParameterError("Failed to compute null counts.")
        null_counts = {col_name: nulls_row[f"{col_name}__nulls"] for col_name in columns}

        string_columns = [col_name for col_name in columns if isinstance(schema[col_name], T.StringType)]
        distinct_counts: dict[str, int] = {}
        if string_columns:
            distinct_exprs = [
                F.approx_count_distinct(col_name, rsd=0.05).alias(f"{col_name}__distinct")
                for col_name in string_columns
            ]
            distinct_row = df.agg(*distinct_exprs).first()
            if distinct_row is None:
                raise InvalidParameterError("Failed to compute distinct counts.")
            distinct_counts = {col_name: distinct_row[f"{col_name}__distinct"] for col_name in string_columns}

        for col_name in columns:
            if col_name not in schema:
                raise InvalidParameterError(f"Column '{col_name}' not found in DataFrame")

            col_type = schema[col_name]
            info = self._classify_column(
                col_name,
                col_type,
                null_count=null_counts.get(col_name, 0),
                distinct_count=distinct_counts.get(col_name),
            )

            if info.category == 'unsupported':
                unsupported_cols.append((col_name, type(col_type).__name__))
            else:
                column_infos.append(info)

        # Warn about unsupported columns
        if unsupported_cols:
            type_details = ", ".join(f"{col}({typ})" for col, typ in unsupported_cols)
            warnings_list.append(
                f"Skipping unsupported columns: {type_details}. "
                "Supported types: numeric (int, long, float, double, decimal), "
                "categorical (string), temporal (date, timestamp), boolean."
            )

        # Check for potential ID fields that may lead to poor models
        id_warnings = self._check_for_id_fields(df, column_infos)
        warnings_list.extend(id_warnings)

        # Warn if estimated feature count is high (soft limit)
        estimated_features = self._estimate_feature_count(column_infos)
        if estimated_features > self.max_engineered_features:
            breakdown = self._get_feature_breakdown(column_infos)
            warnings_list.append(
                f"Feature engineering will create {estimated_features} features (recommended max: {self.max_engineered_features}). "
                f"This may increase training/scoring time. Feature breakdown:\n{breakdown}"
            )

        return column_infos, warnings_list

    def _classify_column(
        self, col_name: str, col_type: T.DataType, *, null_count: int, distinct_count: int | None = None
    ) -> ColumnTypeInfo:
        """Classify a single column."""

        # Numeric types
        if isinstance(col_type, T.NumericType):
            return ColumnTypeInfo(
                name=col_name, spark_type=col_type, category='numeric', null_count=null_count, encoding_strategy='none'
            )

        # Boolean
        if isinstance(col_type, T.BooleanType):
            return ColumnTypeInfo(
                name=col_name,
                spark_type=col_type,
                category='boolean',
                null_count=null_count,
                encoding_strategy='binary',
            )

        # Datetime types
        if isinstance(col_type, (T.DateType, T.TimestampType, T.TimestampNTZType)):
            return ColumnTypeInfo(
                name=col_name,
                spark_type=col_type,
                category='datetime',
                null_count=null_count,
                encoding_strategy='cyclical',
            )

        # Handle string columns as categorical features
        if isinstance(col_type, T.StringType):
            if distinct_count is None:
                raise InvalidParameterError(f"Missing distinct count for string column '{col_name}'")
            cardinality = distinct_count

            # Determine encoding strategy based on cardinality
            if cardinality <= self.categorical_cardinality_threshold:
                strategy = 'onehot'
            else:
                strategy = 'frequency'

            return ColumnTypeInfo(
                name=col_name,
                spark_type=col_type,
                category='categorical',
                cardinality=cardinality,
                null_count=null_count,
                encoding_strategy=strategy,
            )

        # Unsupported types
        return ColumnTypeInfo(name=col_name, spark_type=col_type, category='unsupported', null_count=null_count)

    def _estimate_feature_count(self, column_infos: list[ColumnTypeInfo]) -> int:
        """Estimate total engineered features."""
        total = 0
        null_indicators = 0

        for info in column_infos:
            if info.category in {"numeric", "boolean"}:
                total += 1
            elif info.category == 'datetime':
                total += 5  # hour_sin, hour_cos, dow_sin, dow_cos, is_weekend
            elif info.category == 'categorical':
                if info.encoding_strategy == 'onehot':
                    total += (info.cardinality or 0) + 1  # +1 for MISSING category
                else:  # frequency
                    total += 1

            # Add null indicator
            if info.null_count and info.null_count > 0:
                null_indicators += 1

        return total + null_indicators

    def _get_feature_breakdown(self, column_infos: list[ColumnTypeInfo]) -> str:
        """Generate feature count breakdown for error message."""
        counts = {'datetime': 0, 'categorical': 0, 'numeric': 0, 'boolean': 0, 'nulls': 0}
        cat_features = 0

        # Count columns by type
        for info in column_infos:
            counts[info.category] = counts.get(info.category, 0) + 1

            if info.category == 'categorical':
                if info.encoding_strategy == 'onehot':
                    cat_features += (info.cardinality or 0) + 1
                else:
                    cat_features += 1

            if info.null_count and info.null_count > 0:
                counts['nulls'] += 1

        # Build breakdown lines
        breakdown = []
        if counts['datetime'] > 0:
            breakdown.append(f"  - {counts['datetime']} datetime → {counts['datetime'] * 5} features")
        if counts['categorical'] > 0:
            breakdown.append(f"  - {counts['categorical']} categorical → {cat_features} features")
        if counts['numeric'] > 0:
            breakdown.append(f"  - {counts['numeric']} numeric → {counts['numeric']} features")
        if counts['boolean'] > 0:
            breakdown.append(f"  - {counts['boolean']} boolean → {counts['boolean']} features")
        if counts['nulls'] > 0:
            breakdown.append(f"  - {counts['nulls']} null indicators → {counts['nulls']} features")

        return "\n".join(breakdown)

    def _capture_dataframe_explain(self, df: DataFrame) -> str:
        """Capture DataFrame.explain() output as a string."""
        old_stdout = sys.stdout
        sys.stdout = buffer = StringIO()
        df.explain(extended=True)
        sys.stdout = old_stdout
        return buffer.getvalue()

    def _get_primary_key_columns(self, df: DataFrame) -> set[str]:
        """
        Attempt to detect primary key columns from Unity Catalog table metadata.

        This is an OPTIONAL helper for better warnings during training. If the DataFrame
        is backed by a Unity Catalog table with primary key constraints, we can warn
        users that they're including PK columns in their feature set (which would lead
        to overfitting). This is purely for improving warning quality - failure is fine.

        Args:
            df: Input DataFrame (may or may not be from a UC table)

        Returns:
            Set of column names that are primary keys (empty if no PK or not a UC table)
        """
        try:
            # Use the same approach as trainer.py: capture explain() output
            # This avoids accessing protected JVM members
            plan_str = self._capture_dataframe_explain(df)

            # Extract table names from the plan using shared utility
            tables = get_tables_from_spark_plan(plan_str)
            if tables:
                # Use the first table found (typically there's only one)
                table_name = next(iter(tables))
                return get_table_primary_keys(table_name, df.sparkSession)

        except Exception:
            # If we can't infer table name or access metadata, that's fine
            # Just means this DataFrame isn't from a UC table or metadata unavailable
            pass

        return set()

    def _compute_cardinality_stats(self, df: DataFrame, numeric_columns: list[str]) -> tuple[int, dict[str, int]]:
        """
        Compute cardinality statistics for numeric columns in a single aggregation.

        Returns:
            Tuple of (total_rows, cardinality_dict)
        """
        if not numeric_columns:
            return df.count(), {}

        agg_exprs = [F.count("*").alias("_total_rows")]
        for col_name in numeric_columns:
            # Use approx_count_distinct with 5% error (rsd=0.05) for fast cardinality estimation
            # This is accurate enough for ID detection (>1000 or >80% thresholds)
            agg_exprs.append(F.approx_count_distinct(col_name, rsd=0.05).alias(f"_distinct_{col_name}"))

        stats_row = df.agg(*agg_exprs).first()
        if stats_row is None:
            # Empty DataFrame or aggregation failed
            return 0, {col: 0 for col in numeric_columns}

        total_rows = stats_row["_total_rows"]
        cardinality_stats = {col_name: stats_row[f"_distinct_{col_name}"] for col_name in numeric_columns}

        return total_rows, cardinality_stats

    def _check_column_for_id_indicators(
        self,
        info: ColumnTypeInfo,
        pk_columns: set[str],
        id_pattern: re.Pattern,
        cardinality_stats: dict[str, int],
        total_rows: int,
    ) -> list[str]:
        """
        Check a single column for ID field indicators.

        Returns:
            List of issue descriptions (empty if no issues)
        """
        col_name = info.name
        issues = []

        # Check 1: Primary key from Unity Catalog metadata (most reliable!)
        # This is optional - if we can detect PKs from table metadata, use it
        if pk_columns and col_name in pk_columns:
            issues.append("is marked as a primary key in Unity Catalog table metadata")

        # Check 2: Name pattern match (with improved regex)
        if id_pattern.search(col_name):
            issues.append("matches ID naming pattern")

        # Check 3: High cardinality (only for integer/categorical columns, NOT floats)
        # IMPORTANT: Skip float/double columns because:
        # - Float/double values naturally have high cardinality due to decimal precision
        # - ID fields are almost never floating point numbers
        # - Continuous features like speed_mph, price, revenue are legitimately high-cardinality
        is_float_type = isinstance(info.spark_type, (T.FloatType, T.DoubleType))

        if info.category in {'numeric', 'categorical'} and total_rows > 0 and not is_float_type:
            distinct_count = self._get_distinct_count(info, cardinality_stats)
            if distinct_count:
                uniqueness_ratio = distinct_count / total_rows
                # Only warn if BOTH conditions are true (strict AND)
                if distinct_count > 1000 and uniqueness_ratio > 0.8:
                    # Cap display at 100% since approx_count_distinct can slightly exceed total_rows
                    display_ratio = min(uniqueness_ratio, 1.0)
                    issues.append(
                        f"has very high cardinality (~{distinct_count} distinct values, "
                        f"~{display_ratio:.0%} unique)"
                    )

        return issues

    def _get_distinct_count(self, info: ColumnTypeInfo, cardinality_stats: dict[str, int]) -> int | None:
        """Get distinct count for a column from cardinality stats or column info."""
        if info.category == 'categorical' and info.cardinality:
            return info.cardinality
        if info.category == 'numeric':
            return cardinality_stats.get(info.name, 0)
        return None

    def _check_for_id_fields(self, df: DataFrame, column_infos: list[ColumnTypeInfo]) -> list[str]:
        """
        Check for potential ID fields that may degrade model quality.

        Warns about (in priority order):
        1. Columns marked as primary keys in Unity Catalog table metadata (most reliable!)
           Note: This is optional/best-effort - only works if DataFrame is from a UC table
        2. Columns matching ID naming patterns (e.g., _id, _key suffixes)
        3. High cardinality columns (>1000 distinct values AND >80% unique ratio)
           Note: Uses AND condition to avoid false positives on continuous numeric fields
           like expenses, revenue that naturally have high cardinality with decimal precision

        Returns:
            List of warning messages
        """
        warnings = []

        # Try to detect primary keys from Unity Catalog metadata (optional, best-effort)
        # This is the most reliable indicator but only works for UC tables
        pk_columns = self._get_primary_key_columns(df)

        # Improved ID pattern: only match common ID suffixes, not words ending in "id"
        # This avoids false positives like "liquid", "orchid", "valid", etc.
        id_pattern = re.compile(r"(?i)(_id|_key|_uuid|_guid|^id$|^key$|^uuid$|^guid$)")

        # Identify numeric columns that need cardinality checks
        numeric_columns = [info.name for info in column_infos if info.category == 'numeric']

        # Batch compute total rows + approximate distinct counts in single aggregation
        # This is MUCH faster than multiple distinct().count() calls
        total_rows, cardinality_stats = self._compute_cardinality_stats(df, numeric_columns)

        for info in column_infos:
            issues = self._check_column_for_id_indicators(info, pk_columns, id_pattern, cardinality_stats, total_rows)

            # Generate warning if any check fails
            if issues:
                reason = " and ".join(issues)
                warnings.append(
                    f"Column '{info.name}' appears to be an ID field ({reason}). "
                    f"ID fields can lead to poor anomaly detection models due to overfitting. "
                    f"Consider using exclude_columns=['{info.name}'] or removing from columns list."
                )

        return warnings


def _add_null_indicator(
    transformed_df: DataFrame,
    col_name: str,
    null_count: int | None,
    engineered_features: list[str],
) -> DataFrame:
    """Add null indicator column if column has nulls."""
    has_nulls = (null_count or 0) > 0
    if has_nulls:
        null_indicator_col = f"{col_name}_is_null"
        transformed_df = transformed_df.withColumn(null_indicator_col, when(col(col_name).isNull(), 1.0).otherwise(0.0))
        engineered_features.append(null_indicator_col)
    return transformed_df


def _apply_onehot_encoding(
    df: DataFrame,
    transformed_df: DataFrame,
    col_name: str,
    is_training: bool,
    onehot_categories: dict[str, list[str]],
    engineered_features: list[str],
) -> DataFrame:
    """Apply OneHot encoding to a categorical column."""
    if is_training:
        distinct_values = [row[0] for row in df.select(col_name).distinct().collect()]
        distinct_values = [v for v in distinct_values if v is not None]
        if len(distinct_values) == 2:
            distinct_values = distinct_values[:1]
        onehot_categories[col_name] = distinct_values
    else:
        distinct_values = onehot_categories.get(col_name, [])
        if not distinct_values:
            raise ValueError(
                f"OneHot categories for column '{col_name}' not found in metadata. "
                "Model may be from an older version without OneHot category storage."
            )

    for value in distinct_values:
        feature_name = f"{col_name}_{value}"
        transformed_df = transformed_df.withColumn(feature_name, when(col(col_name) == value, 1.0).otherwise(0.0))
        engineered_features.append(feature_name)

    return transformed_df


def _apply_frequency_encoding(
    df: DataFrame,
    transformed_df: DataFrame,
    col_name: str,
    is_training: bool,
    frequency_maps: dict[str, dict[str, float]],
    engineered_features: list[str],
) -> DataFrame:
    """Apply Frequency encoding to a categorical column."""
    if is_training:
        total_count = df.count()
        freq_map = {}
        for row in df.groupBy(col_name).agg(count("*").alias("cnt")).collect():
            value = row[col_name]
            cnt = row["cnt"]
            freq_map[value] = float(cnt) / total_count
        frequency_maps[col_name] = freq_map

    freq_map = frequency_maps[col_name]
    feature_name = f"{col_name}_freq"

    expr = None
    for value, frequency in freq_map.items():
        condition = col(col_name) == lit(value)
        if expr is None:
            expr = when(condition, lit(frequency))
        else:
            expr = expr.when(condition, lit(frequency))

    expr = expr.otherwise(lit(0.0)) if expr is not None else lit(0.0)

    transformed_df = transformed_df.withColumn(feature_name, expr)
    engineered_features.append(feature_name)

    return transformed_df


def _process_categorical_columns(
    df: DataFrame,
    transformed_df: DataFrame,
    categorical_cols: list[ColumnTypeInfo],
    categorical_cardinality_threshold: int,
    is_training: bool,
    frequency_maps: dict[str, dict[str, float]],
    onehot_categories: dict[str, list[str]],
    engineered_features: list[str],
) -> DataFrame:
    """Process categorical columns with OneHot or Frequency encoding."""
    for col_info in categorical_cols:
        col_name = col_info.name
        card = col_info.cardinality or 0

        # Add null indicator and impute nulls
        transformed_df = _add_null_indicator(transformed_df, col_name, col_info.null_count, engineered_features)
        transformed_df = transformed_df.withColumn(col_name, coalesce(col(col_name), lit("MISSING")))

        # Encode based on cardinality
        if card <= categorical_cardinality_threshold:
            transformed_df = _apply_onehot_encoding(
                df, transformed_df, col_name, is_training, onehot_categories, engineered_features
            )
        else:
            transformed_df = _apply_frequency_encoding(
                df, transformed_df, col_name, is_training, frequency_maps, engineered_features
            )

    return transformed_df


def _process_datetime_columns(
    transformed_df: DataFrame,
    datetime_cols: list[ColumnTypeInfo],
    engineered_features: list[str],
) -> DataFrame:
    """Process datetime columns with cyclical encoding."""
    for col_info in datetime_cols:
        col_name = col_info.name

        # Add null indicator if needed
        transformed_df = _add_null_indicator(transformed_df, col_name, col_info.null_count, engineered_features)

        # Impute nulls with epoch
        transformed_df = transformed_df.withColumn(
            col_name, coalesce(col(col_name).cast(TimestampType()), to_timestamp(lit("1970-01-01 00:00:00")))
        )

        # Extract cyclical features
        transformed_df = transformed_df.withColumn(f"{col_name}_hour_sin", sin(hour(col(col_name)) * 2 * pi() / 24))
        transformed_df = transformed_df.withColumn(f"{col_name}_hour_cos", cos(hour(col(col_name)) * 2 * pi() / 24))
        engineered_features.extend([f"{col_name}_hour_sin", f"{col_name}_hour_cos"])

        transformed_df = transformed_df.withColumn(
            f"{col_name}_dow_sin", sin((dayofweek(col(col_name)) - 1) * 2 * pi() / 7)
        )
        transformed_df = transformed_df.withColumn(
            f"{col_name}_dow_cos", cos((dayofweek(col(col_name)) - 1) * 2 * pi() / 7)
        )
        engineered_features.extend([f"{col_name}_dow_sin", f"{col_name}_dow_cos"])

        transformed_df = transformed_df.withColumn(
            f"{col_name}_month_sin", sin((month(col(col_name)) - 1) * 2 * pi() / 12)
        )
        transformed_df = transformed_df.withColumn(
            f"{col_name}_month_cos", cos((month(col(col_name)) - 1) * 2 * pi() / 12)
        )
        engineered_features.extend([f"{col_name}_month_sin", f"{col_name}_month_cos"])

        transformed_df = transformed_df.withColumn(
            f"{col_name}_is_weekend",
            when((dayofweek(col(col_name)) == 1) | (dayofweek(col(col_name)) == 7), 1.0).otherwise(0.0),
        )
        engineered_features.append(f"{col_name}_is_weekend")

        # Drop the original datetime column after feature extraction
        # This ensures the TimestampType column doesn't reach sklearn (which expects float)
        transformed_df = transformed_df.drop(col_name)

    return transformed_df


def _process_boolean_columns(
    transformed_df: DataFrame,
    boolean_cols: list[ColumnTypeInfo],
    engineered_features: list[str],
) -> DataFrame:
    """Process boolean columns by mapping to 0/1."""
    for col_info in boolean_cols:
        col_name = col_info.name

        # Add null indicator if needed
        transformed_df = _add_null_indicator(transformed_df, col_name, col_info.null_count, engineered_features)

        # Map to 0/1 (nulls -> 0)
        transformed_df = transformed_df.withColumn(
            f"{col_name}_bool", when(col(col_name).isNull(), 0.0).when(col(col_name), 1.0).otherwise(0.0)
        )
        engineered_features.append(f"{col_name}_bool")

    return transformed_df


def _process_numeric_columns(
    transformed_df: DataFrame,
    numeric_cols: list[ColumnTypeInfo],
    engineered_features: list[str],
) -> DataFrame:
    """Process numeric columns with null imputation."""
    for col_info in numeric_cols:
        col_name = col_info.name

        # Add null indicator if needed
        transformed_df = _add_null_indicator(transformed_df, col_name, col_info.null_count, engineered_features)

        # Impute nulls with 0
        transformed_df = transformed_df.withColumn(col_name, coalesce(col(col_name).cast(DoubleType()), lit(0.0)))
        engineered_features.append(col_name)

    return transformed_df


def apply_feature_engineering(
    df: DataFrame,
    column_infos: list[ColumnTypeInfo],
    categorical_cardinality_threshold: int = 20,
    frequency_maps: dict[str, dict[str, float]] | None = None,
    onehot_categories: dict[str, list[str]] | None = None,
) -> tuple[DataFrame, SparkFeatureMetadata]:
    """
    Apply feature engineering transformations in Spark (distributed).

    Returns:
        - DataFrame with engineered numeric features
        - Metadata for reconstructing transformations during scoring

    Transformations applied:
    1. Categorical: OneHot (low-card) or Frequency encoding (high-card)
    2. Datetime: Extract hour_sin/cos, dow_sin/cos, month_sin/cos, is_weekend
    3. Boolean: Map to 0/1
    4. Numeric: Keep as-is
    5. Null indicators: Add column_is_null for columns with nulls
    6. Imputation: Fill nulls with 0 (numeric), "MISSING" (categorical), epoch (datetime), 0 (boolean)

    Args:
        df: Input DataFrame with original columns
        column_infos: Column type information from ColumnTypeClassifier
        categorical_cardinality_threshold: Threshold for OneHot vs Frequency encoding
        frequency_maps: Pre-computed frequency maps (for scoring). If None, compute from df (for training).
        onehot_categories: Pre-computed OneHot distinct values (for scoring). If None, compute from df (for training).
    """
    is_training = frequency_maps is None
    if frequency_maps is None:
        frequency_maps = {}
    if onehot_categories is None:
        onehot_categories = {}

    transformed_df = df
    engineered_features: list[str] = []

    # Group columns by type
    categorical_cols = [c for c in column_infos if c.category == "categorical"]
    datetime_cols = [c for c in column_infos if c.category == "datetime"]
    boolean_cols = [c for c in column_infos if c.category == "boolean"]
    numeric_cols = [c for c in column_infos if c.category == "numeric"]

    # Process each column type with dedicated helper functions
    transformed_df = _process_categorical_columns(
        df,
        transformed_df,
        categorical_cols,
        categorical_cardinality_threshold,
        is_training,
        frequency_maps,
        onehot_categories,
        engineered_features,
    )

    transformed_df = _process_datetime_columns(transformed_df, datetime_cols, engineered_features)

    transformed_df = _process_boolean_columns(transformed_df, boolean_cols, engineered_features)

    transformed_df = _process_numeric_columns(transformed_df, numeric_cols, engineered_features)

    # Select engineered features + preserve any extra columns not in column_infos
    # (e.g., __dqx_row_id__ for joining results back)
    feature_col_names = [c.name for c in column_infos]
    extra_cols = [c for c in transformed_df.columns if c not in feature_col_names and c not in engineered_features]
    result_df = transformed_df.select(*engineered_features, *extra_cols)

    # Use only the features that actually exist in the result DataFrame
    # This handles cases where original columns (e.g., datetime) were dropped during transformation
    actual_engineered_features = [f for f in engineered_features if f in result_df.columns]

    # Create metadata for scoring
    metadata = SparkFeatureMetadata(
        column_infos=[
            {
                "name": c.name,
                "category": c.category,
                "cardinality": c.cardinality,
                "null_count": c.null_count,
            }
            for c in column_infos
        ],
        categorical_frequency_maps=frequency_maps,
        onehot_categories=onehot_categories,
        engineered_feature_names=actual_engineered_features,
        categorical_cardinality_threshold=categorical_cardinality_threshold,
    )

    return result_df, metadata
