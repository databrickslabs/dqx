"""
Feature engineering for anomaly detection.

Provides column type analysis and Spark-native feature transformations.
All transformations are applied in Spark (distributed) for scalability and
Spark Connect compatibility (no custom Python class serialization).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql.functions import col, when, lit, coalesce, hour, dayofweek, month, sin, cos, pi, count, to_timestamp
from pyspark.sql.types import DoubleType, TimestampType


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
    categorical_frequency_maps: dict[str, dict[str, float]]  # {col_name: {value: frequency}}
    onehot_categories: dict[str, list[str]]  # {col_name: [distinct_values]} for OneHot encoding
    engineered_feature_names: list[str]  # Final feature names after engineering

    def to_json(self) -> str:
        """Serialize to JSON for storage."""
        return json.dumps(
            {
                "column_infos": self.column_infos,
                "categorical_frequency_maps": self.categorical_frequency_maps,
                "onehot_categories": self.onehot_categories,
                "engineered_feature_names": self.engineered_feature_names,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "SparkFeatureMetadata":
        """Deserialize from JSON."""
        data = json.loads(json_str)
        # Backwards compatibility: old models won't have onehot_categories
        if "onehot_categories" not in data:
            data["onehot_categories"] = {}
        return cls(**data)


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
        from databricks.labs.dqx.errors import InvalidParameterError

        # Check max columns limit
        if len(columns) > self.max_input_columns:
            raise InvalidParameterError(
                f"Anomaly detection supports max {self.max_input_columns} columns, got {len(columns)}. "
                f"Select the most important columns for anomaly detection. "
                f"Tip: Use anomaly.auto_discover() or manually specify columns=['col1', 'col2', ...]"
            )

        schema = {f.name: f.dataType for f in df.schema.fields}
        column_infos = []
        warnings_list = []
        unsupported_cols = []

        for col_name in columns:
            if col_name not in schema:
                raise InvalidParameterError(f"Column '{col_name}' not found in DataFrame")

            col_type = schema[col_name]
            info = self._classify_column(df, col_name, col_type)

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

        # Check estimated feature count
        estimated_features = self._estimate_feature_count(column_infos, df)
        if estimated_features > self.max_engineered_features:
            breakdown = self._get_feature_breakdown(column_infos)
            raise InvalidParameterError(
                f"Feature engineering would create {estimated_features} features (limit: {self.max_engineered_features}). "
                f"Feature breakdown:\n{breakdown}\n"
                f"Suggestions:\n"
                f"  1. Reduce number of categorical columns (highest impact)\n"
                f"  2. Use columns with lower cardinality\n"
                f"  3. Prioritize numeric/boolean columns (1 feature each)"
            )

        return column_infos, warnings_list

    def _classify_column(self, df: DataFrame, col_name: str, col_type: T.DataType) -> ColumnTypeInfo:
        """Classify a single column."""
        import pyspark.sql.functions as F

        # Count nulls
        null_count = df.filter(F.col(col_name).isNull()).count()

        # Numeric types
        if isinstance(
            col_type, (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)
        ):
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

        # String (categorical)
        if isinstance(col_type, T.StringType):
            cardinality_row = df.select(F.countDistinct(col_name)).first()
            assert cardinality_row is not None, "Failed to compute cardinality"
            cardinality = cardinality_row[0]

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

    def _estimate_feature_count(self, column_infos: list[ColumnTypeInfo], df: DataFrame) -> int:
        """Estimate total engineered features."""
        total = 0
        null_indicators = 0

        for info in column_infos:
            if info.category == 'numeric':
                total += 1
            elif info.category == 'boolean':
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

        for info in column_infos:
            if info.category == 'numeric':
                counts['numeric'] += 1
            elif info.category == 'boolean':
                counts['boolean'] += 1
            elif info.category == 'datetime':
                counts['datetime'] += 1
            elif info.category == 'categorical':
                counts['categorical'] += 1
                if info.encoding_strategy == 'onehot':
                    cat_features += (info.cardinality or 0) + 1
                else:
                    cat_features += 1

            if info.null_count and info.null_count > 0:
                counts['nulls'] += 1

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
    5. Null indicators: Add {col}_is_null for columns with nulls
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
    engineered_features = []

    # Group columns by type
    categorical_cols = [c for c in column_infos if c.category == "categorical"]
    datetime_cols = [c for c in column_infos if c.category == "datetime"]
    boolean_cols = [c for c in column_infos if c.category == "boolean"]
    numeric_cols = [c for c in column_infos if c.category == "numeric"]

    # 1. Process categorical columns
    for col_info in categorical_cols:
        col_name = col_info.name
        card = col_info.cardinality or 0

        # Add null indicator if needed
        has_nulls = (col_info.null_count or 0) > 0
        if has_nulls:
            null_indicator_col = f"{col_name}_is_null"
            transformed_df = transformed_df.withColumn(
                null_indicator_col, when(col(col_name).isNull(), 1.0).otherwise(0.0)
            )
            engineered_features.append(null_indicator_col)

        # Impute nulls with "MISSING"
        transformed_df = transformed_df.withColumn(col_name, coalesce(col(col_name), lit("MISSING")))

        # Encode based on cardinality
        if card <= categorical_cardinality_threshold:
            # OneHot encoding (low cardinality)
            if is_training:
                # Training: Compute distinct values from training data
                distinct_values = [row[0] for row in df.select(col_name).distinct().collect()]
                distinct_values = [v for v in distinct_values if v is not None]

                # Drop one category if binary (to avoid redundancy)
                if len(distinct_values) == 2:
                    distinct_values = distinct_values[:1]

                # Store for scoring
                onehot_categories[col_name] = distinct_values
            else:
                # Scoring: Use stored distinct values from training
                distinct_values = onehot_categories.get(col_name, [])
                if not distinct_values:
                    raise ValueError(
                        f"OneHot categories for column '{col_name}' not found in metadata. "
                        "Model may be from an older version without OneHot category storage."
                    )

            for value in distinct_values:
                feature_name = f"{col_name}_{value}"
                transformed_df = transformed_df.withColumn(
                    feature_name, when(col(col_name) == value, 1.0).otherwise(0.0)
                )
                engineered_features.append(feature_name)
        else:
            # Frequency encoding (high cardinality)
            if is_training:
                # Compute frequency map
                total_count = df.count()
                freq_map = {}
                for row in df.groupBy(col_name).agg(count("*").alias("cnt")).collect():
                    value = row[col_name]
                    cnt = row["cnt"]
                    freq_map[value] = float(cnt) / total_count
                frequency_maps[col_name] = freq_map

            # Apply frequency encoding
            freq_map = frequency_maps[col_name]
            feature_name = f"{col_name}_freq"

            # Build CASE WHEN expression
            expr = None
            for value, frequency in freq_map.items():
                condition = col(col_name) == lit(value)
                if expr is None:
                    expr = when(condition, lit(frequency))
                else:
                    expr = expr.when(condition, lit(frequency))

            # Default for unknown values
            if expr is not None:
                expr = expr.otherwise(lit(0.0))
            else:
                # Empty freq_map - use 0.0 for all values
                expr = lit(0.0)

            transformed_df = transformed_df.withColumn(feature_name, expr)
            engineered_features.append(feature_name)

    # 2. Process datetime columns
    for col_info in datetime_cols:
        col_name = col_info.name

        # Add null indicator if needed
        has_nulls = (col_info.null_count or 0) > 0
        if has_nulls:
            null_indicator_col = f"{col_name}_is_null"
            transformed_df = transformed_df.withColumn(
                null_indicator_col, when(col(col_name).isNull(), 1.0).otherwise(0.0)
            )
            engineered_features.append(null_indicator_col)

        # Impute nulls with epoch (1970-01-01)
        transformed_df = transformed_df.withColumn(
            col_name, coalesce(col(col_name).cast(TimestampType()), to_timestamp(lit("1970-01-01 00:00:00")))
        )

        # Extract cyclical features
        # Hour (0-23) -> sin/cos
        transformed_df = transformed_df.withColumn(f"{col_name}_hour_sin", sin(hour(col(col_name)) * 2 * pi() / 24))
        transformed_df = transformed_df.withColumn(f"{col_name}_hour_cos", cos(hour(col(col_name)) * 2 * pi() / 24))
        engineered_features.extend([f"{col_name}_hour_sin", f"{col_name}_hour_cos"])

        # Day of week (1-7 in Spark) -> sin/cos
        transformed_df = transformed_df.withColumn(
            f"{col_name}_dow_sin", sin((dayofweek(col(col_name)) - 1) * 2 * pi() / 7)
        )
        transformed_df = transformed_df.withColumn(
            f"{col_name}_dow_cos", cos((dayofweek(col(col_name)) - 1) * 2 * pi() / 7)
        )
        engineered_features.extend([f"{col_name}_dow_sin", f"{col_name}_dow_cos"])

        # Month (1-12) -> sin/cos
        transformed_df = transformed_df.withColumn(
            f"{col_name}_month_sin", sin((month(col(col_name)) - 1) * 2 * pi() / 12)
        )
        transformed_df = transformed_df.withColumn(
            f"{col_name}_month_cos", cos((month(col(col_name)) - 1) * 2 * pi() / 12)
        )
        engineered_features.extend([f"{col_name}_month_sin", f"{col_name}_month_cos"])

        # Is weekend (Saturday=7, Sunday=1 in Spark)
        transformed_df = transformed_df.withColumn(
            f"{col_name}_is_weekend",
            when((dayofweek(col(col_name)) == 1) | (dayofweek(col(col_name)) == 7), 1.0).otherwise(0.0),
        )
        engineered_features.append(f"{col_name}_is_weekend")

    # 3. Process boolean columns
    for col_info in boolean_cols:
        col_name = col_info.name

        # Add null indicator if needed
        has_nulls = (col_info.null_count or 0) > 0
        if has_nulls:
            null_indicator_col = f"{col_name}_is_null"
            transformed_df = transformed_df.withColumn(
                null_indicator_col, when(col(col_name).isNull(), 1.0).otherwise(0.0)
            )
            engineered_features.append(null_indicator_col)

        # Map to 0/1 (nulls -> 0)
        transformed_df = transformed_df.withColumn(
            f"{col_name}_bool", when(col(col_name).isNull(), 0.0).when(col(col_name), 1.0).otherwise(0.0)
        )
        engineered_features.append(f"{col_name}_bool")

    # 4. Process numeric columns
    for col_info in numeric_cols:
        col_name = col_info.name

        # Add null indicator if needed
        has_nulls = (col_info.null_count or 0) > 0
        if has_nulls:
            null_indicator_col = f"{col_name}_is_null"
            transformed_df = transformed_df.withColumn(
                null_indicator_col, when(col(col_name).isNull(), 1.0).otherwise(0.0)
            )
            engineered_features.append(null_indicator_col)

        # Impute nulls with 0
        transformed_df = transformed_df.withColumn(col_name, coalesce(col(col_name).cast(DoubleType()), lit(0.0)))
        engineered_features.append(col_name)

    # Select engineered features + preserve any extra columns not in column_infos
    # (e.g., __dqx_row_id__ for joining results back)
    feature_col_names = [c.name for c in column_infos]
    extra_cols = [c for c in transformed_df.columns if c not in feature_col_names and c not in engineered_features]
    result_df = transformed_df.select(*engineered_features, *extra_cols)

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
        engineered_feature_names=engineered_features,
    )

    return result_df, metadata
