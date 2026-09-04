"""
Feature engineering for row anomaly detection.

Provides column type analysis and Spark-native feature transformations.
All transformations are applied in Spark (distributed) for scalability and
Spark Connect compatibility (no custom Python class serialization).
"""

import json
import logging
import math
import re
import sys
import threading
from dataclasses import dataclass, field, fields
from io import StringIO
from typing import Any

import numpy as np
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    cos,
    dayofweek,
    hour,
    lit,
    month,
    pi,
    sin,
    to_timestamp,
    when,
)
from pyspark.sql.types import DoubleType, TimestampType

from databricks.labs.dqx.anomaly.segment_utils import BASELINE_KEY_COLUMN, with_baseline_key
from databricks.labs.dqx.anomaly.temporal import TemporalBasis, fit_temporal, select_basis
from databricks.labs.dqx.errors import ComputationError, InvalidParameterError
from databricks.labs.dqx.telemetry import get_tables_from_spark_plan
from databricks.labs.dqx.utils import get_table_primary_keys, sanitize_for_logging

logger = logging.getLogger(__name__)

# Serialize stdout capture so concurrent column analysis is thread-safe
_EXPLAIN_CAPTURE_LOCK = threading.Lock()


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
    # Group conditioning. Every field defaults to empty, so a model trained before these existed
    # deserializes with no grouping, the relative transform returns immediately, and
    # engineered_feature_names is byte-identical to what it was — such models score exactly as
    # they did before. See databrickslabs/dqx#1484.
    baseline_by: list[str] = field(default_factory=list)  # Columns forming the group key
    baseline_medians: dict[str, dict[str, float]] = field(default_factory=dict)  # metric -> (group key -> median)
    global_medians: dict[str, float] = field(default_factory=dict)  # metric -> median, for unseen groups
    # Per-group score calibration: group key -> (quantile key -> score bound). Lives here rather
    # than in the typed training.score_quantiles map<string,double> column, which would need a
    # registry migration to hold a nested map; training.score_quantiles stays the global fallback.
    baseline_score_quantiles: dict[str, dict[str, float]] = field(default_factory=dict)
    # Temporal conditioning, and the same defaults-to-empty contract as the grouping fields above:
    # an empty baseline_over_time makes the temporal transform return immediately, so a model trained
    # before these existed keeps a byte-identical engineered_feature_names and scores exactly as it did.
    baseline_over_time: str = ""  # The time column each metric is judged along
    temporal_basis: dict[str, Any] = field(default_factory=dict)  # TemporalBasis.to_dict()
    temporal_coefficients: dict[str, list[float]] = field(default_factory=dict)  # metric to its coefficients
    # Training window bounds in epoch seconds, for the staleness horizon. A fitted basis extrapolates to
    # any t, but accuracy decays with distance, so scoring needs to know where the evidence ran out.
    temporal_window: dict[str, float] = field(default_factory=dict)  # keys t_min and t_max, epoch seconds

    def to_json(self) -> str:
        """Serialize to JSON for storage.

        Every field of the dataclass is persisted. This is the single writer of the
        ``features.feature_metadata`` column — do not hand-roll the payload elsewhere, or a
        newly added field is silently dropped at persistence and the model scores with a
        different feature set than it trained on.
        """
        return json.dumps({f.name: getattr(self, f.name) for f in fields(self)})

    @classmethod
    def from_json(cls, json_str: str) -> "SparkFeatureMetadata":
        """Deserialize from JSON.

        Unknown keys are ignored rather than raising, so a model trained by a newer DQX
        version stays readable by an older one: the reader falls back to the defaults for
        fields it does not know about instead of failing to load the model at all.
        """
        data = json.loads(json_str)
        known_fields = {f.name for f in fields(cls)}
        unknown = set(data) - known_fields
        if unknown:
            logger.debug(
                f"Ignoring unknown feature metadata keys {sorted(unknown)}; "
                "this model was likely trained by a newer version of DQX."
            )
        return cls(**{k: v for k, v in data.items() if k in known_fields})


@dataclass
class TemporalState:
    """The fitted temporal artefacts, carried through the transform pipeline as one thing.

    Written when training and read when scoring, which is why it is mutable: the transform fills it in
    place exactly as the baseline median dicts are filled. Empty means the feature is unused.

    Not the persisted shape. :class:`SparkFeatureMetadata` keeps these flat, so the JSON payload a model
    carries stays stable regardless of how they are grouped for passing around.
    """

    basis: dict[str, Any] = field(default_factory=dict)  # TemporalBasis.to_dict()
    coefficients: dict[str, list[float]] = field(default_factory=dict)  # metric to its coefficients
    window: dict[str, float] = field(default_factory=dict)  # keys t_min and t_max, epoch seconds


def _spark_type_for_category(category: str) -> T.DataType:
    """Return canonical Spark type for a column category. Keeps reconstructed infos consistent for scoring."""
    return {
        "numeric": T.DoubleType(),
        "categorical": T.StringType(),
        "datetime": T.TimestampType(),
        "boolean": T.BooleanType(),
    }.get(category, T.StringType())


def features_per_numeric_column(baseline_by: list[str] | None, baseline_over_time: str | None) -> int:
    """How many features one numeric column becomes: the metric itself plus one per comparison basis."""
    return 1 + (1 if baseline_by else 0) + (1 if baseline_over_time else 0)


def feature_category_for_type(col_type: T.DataType) -> str:
    """Which family of transforms a column's Spark type puts it in.

    Depends on the type alone. Cardinality decides *how* a categorical column is encoded, and a null
    count decides whether it gets an indicator, but neither changes the category — which is what makes
    this answerable from a schema with no Spark action, and therefore usable by validation before any
    profiling has run.
    """
    if isinstance(col_type, T.NumericType):
        return 'numeric'
    if isinstance(col_type, T.BooleanType):
        return 'boolean'
    if isinstance(col_type, (T.DateType, T.TimestampType, T.TimestampNTZType)):
        return 'datetime'
    if isinstance(col_type, T.StringType):
        return 'categorical'
    return 'unsupported'


def reconstruct_column_infos(feature_metadata: SparkFeatureMetadata) -> list[ColumnTypeInfo]:
    """
    Reconstruct ColumnTypeInfo objects from SparkFeatureMetadata.

    Uses category to set spark_type so that any code (e.g. _classify_column or
    isinstance checks) that runs on reconstructed infos during scoring behaves correctly.

    Args:
        feature_metadata: SparkFeatureMetadata with column information

    Returns:
        List of ColumnTypeInfo objects
    """
    return [
        ColumnTypeInfo(
            name=info["name"],
            spark_type=_spark_type_for_category(info.get("category", "unsupported")),
            category=info.get("category", "unsupported"),
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

    def analyze_columns(
        self,
        df: DataFrame,
        columns: list[str],
        *,
        baseline_by: list[str] | None = None,
        baseline_over_time: str | None = None,
    ) -> tuple[list[ColumnTypeInfo], list[str]]:
        """
        Analyze columns and return type information and warnings.

        Args:
            df: Frame the columns live on.
            columns: Feature columns to analyse.
            baseline_by: Resolved group columns, if any. Needed only so the feature-width estimate can
                account for the derived features they produce; the widths are what the estimate exists
                to warn about, and it silently ignored them before.
            baseline_over_time: Resolved time column, if any. Same reason.

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
        missing = [c for c in columns if c not in schema]
        if missing:
            raise InvalidParameterError(f"Column(s) not found in DataFrame: {missing}")

        column_infos = []
        unsupported_cols = []

        null_exprs = [F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"{col_name}__nulls") for col_name in columns]
        nulls_row = df.agg(*null_exprs).first()
        if nulls_row is None:
            raise ComputationError("Failed to compute null counts for columns")
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
                raise ComputationError("Failed to compute distinct counts for string columns")
            distinct_counts = {col_name: distinct_row[f"{col_name}__distinct"] for col_name in string_columns}

        for col_name in columns:
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
        estimated_features = self._estimate_feature_count(
            column_infos, baseline_by=baseline_by, baseline_over_time=baseline_over_time
        )
        if estimated_features > self.max_engineered_features:
            breakdown = self._get_feature_breakdown(
                column_infos, baseline_by=baseline_by, baseline_over_time=baseline_over_time
            )
            warnings_list.append(
                f"Feature engineering will create {estimated_features} features (recommended max: {self.max_engineered_features}). "
                f"This may increase training/scoring time. Feature breakdown:\n{breakdown}"
            )

        return column_infos, warnings_list

    def _classify_column(
        self, col_name: str, col_type: T.DataType, *, null_count: int, distinct_count: int | None = None
    ) -> ColumnTypeInfo:
        """Classify a single column."""
        category = feature_category_for_type(col_type)

        if category == 'numeric':
            return ColumnTypeInfo(
                name=col_name, spark_type=col_type, category='numeric', null_count=null_count, encoding_strategy='none'
            )

        if category == 'boolean':
            return ColumnTypeInfo(
                name=col_name,
                spark_type=col_type,
                category='boolean',
                null_count=null_count,
                encoding_strategy='binary',
            )

        if category == 'datetime':
            return ColumnTypeInfo(
                name=col_name,
                spark_type=col_type,
                category='datetime',
                null_count=null_count,
                encoding_strategy='cyclical',
            )

        # distinct_count is always set in analyze_columns for string columns.
        if category == 'categorical':
            if distinct_count is None:
                raise InvalidParameterError(f"distinct_count is required for string column {col_name}")

            # Determine encoding strategy based on cardinality
            strategy = 'onehot' if distinct_count <= self.categorical_cardinality_threshold else 'frequency'

            return ColumnTypeInfo(
                name=col_name,
                spark_type=col_type,
                category='categorical',
                cardinality=distinct_count,
                null_count=null_count,
                encoding_strategy=strategy,
            )

        return ColumnTypeInfo(name=col_name, spark_type=col_type, category='unsupported', null_count=null_count)

    def _estimate_feature_count(
        self,
        column_infos: list[ColumnTypeInfo],
        *,
        baseline_by: list[str] | None = None,
        baseline_over_time: str | None = None,
    ) -> int:
        """Estimate total engineered features.

        Counts the derived comparison features, which it did not before: with both bases set, twenty
        numeric columns build sixty features while the estimate said twenty and stayed silent under the
        default recommended maximum of fifty. The warning existed to catch exactly that width.
        """
        total = 0
        null_indicators = 0

        features_per_numeric = features_per_numeric_column(baseline_by, baseline_over_time)

        for info in column_infos:
            if info.category == 'numeric':
                total += features_per_numeric
            elif info.category == 'boolean':
                total += 1
            elif info.category == 'datetime':
                total += len(CALENDAR_FEATURE_SUFFIXES)
            elif info.category == 'categorical':
                # One indicator per distinct non-null value, or one frequency column. Approximate for
                # one-hot: cardinality comes from approx_count_distinct.
                total += (info.cardinality or 0) if info.encoding_strategy == 'onehot' else 1

            # Add null indicator
            if info.null_count and info.null_count > 0:
                null_indicators += 1

        return total + null_indicators

    def _get_feature_breakdown(
        self,
        column_infos: list[ColumnTypeInfo],
        *,
        baseline_by: list[str] | None = None,
        baseline_over_time: str | None = None,
    ) -> str:
        """Generate feature count breakdown for error message."""
        counts = {'datetime': 0, 'categorical': 0, 'numeric': 0, 'boolean': 0, 'nulls': 0}
        cat_features = 0

        # Count columns by type
        for info in column_infos:
            counts[info.category] = counts.get(info.category, 0) + 1

            if info.category == 'categorical':
                cat_features += (info.cardinality or 0) if info.encoding_strategy == 'onehot' else 1

            if info.null_count and info.null_count > 0:
                counts['nulls'] += 1

        # One row per contributing group, as (how many sources, what they are, how many features). The
        # comparison bases are named separately from the numeric line rather than folded into it, because
        # dropping a basis is a different decision from dropping a metric.
        numeric = counts['numeric']
        groups = [
            (counts['datetime'], "datetime", counts['datetime'] * len(CALENDAR_FEATURE_SUFFIXES)),
            (counts['categorical'], "categorical", cat_features),
            (numeric, "numeric", numeric),
            (numeric if baseline_by else 0, "baseline_by comparisons", numeric),
            (numeric if baseline_over_time else 0, "baseline_over_time comparisons", numeric),
            (counts['boolean'], "boolean", counts['boolean']),
            (counts['nulls'], "null indicators", counts['nulls']),
        ]

        return "\n".join(
            f"  - {sources} {label} → {features} features" for sources, label, features in groups if sources > 0
        )

    def _capture_dataframe_explain(self, df: DataFrame) -> str:
        """Capture DataFrame.explain() output as a string. Thread-safe via module lock."""
        with _EXPLAIN_CAPTURE_LOCK:
            old_stdout = sys.stdout
            sys.stdout = buffer = StringIO()
            try:
                df.explain(extended=True)
                return buffer.getvalue()
            finally:
                sys.stdout = old_stdout

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
            raise ComputationError("Failed to compute row and cardinality stats for ID check")
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
        null_indicator_col = f"{col_name}{NULL_INDICATOR_SUFFIX}"
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
        # Sorted, because ``distinct().collect()`` has no ordering: the same table trained twice
        # otherwise produced different feature *names* in different positions, and
        # engineered_feature_names is positional.
        #
        # Every category is retained. Dropping one of a binary pair is the textbook way to avoid the
        # dummy-variable trap, but here it made an unexpected value invisible: the omitted reference
        # category and any value never seen in training both encode as all-zeros, so a brand-new value
        # in a binary column produced no signal at all. With both retained, a known value sets exactly
        # one indicator and anything unseen sets none, which the detectors can tell apart.
        #
        # This makes binary consistent with every other cardinality rather than introducing a new
        # behaviour: three or more categories were always retained in full, so an unseen value has
        # always encoded as all-zeros and scored as a large deviation. Retained categories sum to 1 on
        # every trained row, so the correlation-aware detector sees a zero-variance direction, and a row
        # violating that sum lies off the surface all its training data lay on. Measured: rows that do
        # satisfy it score identically to the one-dummy encoding, so the redundant column is free, while
        # an unseen category scores far above a known one. Both halves are pinned in
        # tests/unit/test_anomaly_mahalanobis_detector.py.
        distinct_values = sorted(row[0] for row in df.select(col_name).distinct().collect() if row[0] is not None)
        onehot_categories[col_name] = distinct_values
    else:
        distinct_values = onehot_categories.get(col_name, [])
        if not distinct_values:
            raise InvalidParameterError(
                f"OneHot categories for column '{col_name}' not found in metadata. "
                "Model may be from an older version without OneHot category storage."
            )

    # The one collision that no schema can predict, so it is caught here rather than in validation:
    # a one-hot name is built from a *value*, and a table can carry both a "region" column with an
    # "east" value and a separate "region_east" column. Writing the indicator would overwrite the real
    # one and leave the model fitted on two copies of the indicator. See
    # ``validation.validate_generated_feature_names`` for the same refusal over the predictable names.
    existing_columns = set(transformed_df.columns)
    for value in distinct_values:
        feature_name = f"{col_name}_{value}"
        if feature_name in existing_columns:
            raise InvalidParameterError(
                f"One-hot encoding column '{col_name}' would create '{feature_name}' for value {value!r}, "
                f"but a column of that name already exists and would be overwritten. Rename it, or exclude "
                f"'{col_name}' from the checked columns."
            )
        transformed_df = transformed_df.withColumn(feature_name, when(col(col_name) == value, 1.0).otherwise(0.0))
        engineered_features.append(feature_name)

    return transformed_df


def _apply_frequency_encoding(
    transformed_df: DataFrame,
    col_name: str,
    is_training: bool,
    frequency_maps: dict[str, dict[str, float]],
    engineered_features: list[str],
) -> DataFrame:
    """Apply Frequency encoding to a categorical column."""
    feature_name = f"{col_name}{FREQUENCY_FEATURE_SUFFIX}"
    lookup_key_col = f"__dqx_{col_name}_category"
    lookup_val_col = f"__dqx_{col_name}_frequency"

    if is_training:
        total_count = transformed_df.count()
        if total_count == 0:
            frequency_maps[col_name] = {}
            transformed_df = transformed_df.withColumn(feature_name, lit(0.0))
            engineered_features.append(feature_name)
            return transformed_df

        freq_counts = transformed_df.groupBy(col_name).agg((F.count("*") / F.lit(total_count)).alias(lookup_val_col))
        freq_map = {row[col_name]: float(row[lookup_val_col]) for row in freq_counts.collect()}
        frequency_maps[col_name] = freq_map

    freq_map = frequency_maps[col_name]
    if not freq_map:
        transformed_df = transformed_df.withColumn(feature_name, lit(0.0))
        engineered_features.append(feature_name)
        return transformed_df

    freq_rows = list(freq_map.items())
    freq_schema = T.StructType(
        [
            T.StructField(lookup_key_col, T.StringType(), False),
            T.StructField(lookup_val_col, T.DoubleType(), False),
        ]
    )
    freq_lookup_df = transformed_df.sparkSession.createDataFrame(freq_rows, schema=freq_schema)
    transformed_df = transformed_df.join(
        broadcast(freq_lookup_df),
        transformed_df[col_name] == freq_lookup_df[lookup_key_col],
        "left",
    )
    transformed_df = transformed_df.withColumn(feature_name, coalesce(col(lookup_val_col), lit(0.0)))
    transformed_df = transformed_df.drop(lookup_key_col, lookup_val_col)
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
                transformed_df, col_name, is_training, frequency_maps, engineered_features
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

        # Extract cyclical features. Paired with their suffixes rather than written out one
        # ``withColumn`` at a time, so the emitted order is the order of CALENDAR_FEATURE_SUFFIXES by
        # construction: that tuple is what collision validation checks against, and
        # engineered_feature_names is positional, so the two must not be able to drift apart.
        timestamp = col(col_name)
        calendar_expressions = (
            sin(hour(timestamp) * 2 * pi() / 24),
            cos(hour(timestamp) * 2 * pi() / 24),
            sin((dayofweek(timestamp) - 1) * 2 * pi() / 7),
            cos((dayofweek(timestamp) - 1) * 2 * pi() / 7),
            sin((month(timestamp) - 1) * 2 * pi() / 12),
            cos((month(timestamp) - 1) * 2 * pi() / 12),
            when((dayofweek(timestamp) == 1) | (dayofweek(timestamp) == 7), 1.0).otherwise(0.0),
        )
        for suffix, expression in zip(CALENDAR_FEATURE_SUFFIXES, calendar_expressions, strict=True):
            feature_name = f"{col_name}{suffix}"
            transformed_df = transformed_df.withColumn(feature_name, expression)
            engineered_features.append(feature_name)

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
        feature_name = f"{col_name}{BOOLEAN_FEATURE_SUFFIX}"
        transformed_df = transformed_df.withColumn(
            feature_name, when(col(col_name).isNull(), 0.0).when(col(col_name), 1.0).otherwise(0.0)
        )
        engineered_features.append(feature_name)

    return transformed_df


def _process_numeric_columns(
    transformed_df: DataFrame,
    numeric_cols: list[ColumnTypeInfo],
    engineered_features: list[str],
) -> DataFrame:
    """Register each numeric column as a feature, cast to double, nulls still null.

    Casting here and imputing in :func:`_impute_numeric_features` at the very end is deliberate, and
    the two used to be one ``coalesce(col.cast(...), 0.0)`` on this line. That imputed before the
    group and time baselines were fitted, so a missing observation contributed a fabricated zero to its
    group's median and then received a fabricated deviation from it -- ``0 - expected(t)`` -- on top of
    its null indicator. Both baselines now see the null and skip the row, which is what the null
    indicator was always supposed to be the sole record of.
    """
    for col_info in numeric_cols:
        col_name = col_info.name

        # Add null indicator if needed
        transformed_df = _add_null_indicator(transformed_df, col_name, col_info.null_count, engineered_features)

        transformed_df = transformed_df.withColumn(col_name, col(col_name).cast(DoubleType()))
        engineered_features.append(col_name)

    return transformed_df


def _signed_log1p(column: Column) -> Column:
    """``signum(x) * log1p(abs(x))`` — a log-like transform defined over all reals.

    Plain ``log1p`` is NaN for ``x <= -1``, so it is only safe for counts and shares. Any signed
    metric (profit, balance, delta) would silently produce NaN features. This form is monotone
    over the whole real line and identical to ``log1p`` for ``x >= 0``.
    """
    return F.signum(column) * F.log1p(F.abs(column))


# Suffix marking a baseline-relative feature. Named here rather than inlined because redaction has
# to be able to recognise a feature as derived from a source column: a caller who redacts "amount"
# means the LLM must not see "amount_rel_baseline" either.
BASELINE_RELATIVE_SUFFIX = "_rel_baseline"
TEMPORAL_RELATIVE_SUFFIX = "_rel_time"
# Every other suffix a transform below appends to a source column's name. Constants rather than
# inline f-strings because ``validation.validate_generated_feature_names`` builds the same names to
# refuse a collision with a real input column, and a suffix that drifted between the two places
# would reopen exactly the silent overwrite that check exists to prevent.
NULL_INDICATOR_SUFFIX = "_is_null"
BOOLEAN_FEATURE_SUFFIX = "_bool"
FREQUENCY_FEATURE_SUFFIX = "_freq"
CALENDAR_FEATURE_SUFFIXES = (
    "_hour_sin",
    "_hour_cos",
    "_dow_sin",
    "_dow_cos",
    "_month_sin",
    "_month_cos",
    "_is_weekend",
)
# Rows collected to the driver to fit the temporal basis. The fit runs on a bucketed aggregate rather
# than raw rows, so this bounds driver memory regardless of table size, exactly as the per-group median
# aggregation does. Each bucket contributes its median, which is robust before Huber even sees it.
TEMPORAL_FIT_BUCKETS = 4000


def _epoch_seconds(time_column: str, t_min: float) -> Column:
    """The time axis the temporal basis was fitted against: seconds since the window start.

    Kept in one place because training and scoring must agree on it exactly. A different origin or a
    different unit produces an expectation for a design the coefficients were never fitted to.
    """
    return F.unix_timestamp(col(time_column).cast(TimestampType())).cast(DoubleType()) - lit(t_min)


def _temporal_expected_column(basis: "TemporalBasis", coefficients: list[float], seconds: Column) -> Column:
    """The fitted expected level, as a Spark expression.

    Deliberately a column expression rather than a UDF: it is a closed-form function of the row's own
    timestamp, so it evaluates per row with no ordering, no neighbours and no state. That is what keeps
    scoring valid on a streaming DataFrame, where a lag or a rolling window would not be.

    Column order here must match :func:`databricks.labs.dqx.anomaly.temporal.design_matrix` term for term.
    """
    scaled = seconds / lit(basis.span) if basis.span > 0 else lit(0.0)
    terms: list[Column] = [lit(1.0)]
    if basis.trend:
        terms.append(scaled)
    for changepoint in basis.changepoints:
        terms.append(F.greatest(lit(0.0), scaled - lit(changepoint)))
    for period in basis.periods:
        for harmonic in range(1, basis.harmonics + 1):
            angle = seconds * lit(2.0 * math.pi * harmonic / period)
            terms.append(F.sin(angle))
            terms.append(F.cos(angle))

    expected = lit(0.0)
    for coefficient, term in zip(coefficients, terms, strict=True):
        expected = expected + lit(float(coefficient)) * term
    return expected


def _process_baseline_relative_features(
    transformed_df: DataFrame,
    numeric_cols: list[ColumnTypeInfo],
    baseline_by: list[str],
    is_training: bool,
    baseline_medians: dict[str, dict[str, float]],
    global_medians: dict[str, float],
    engineered_features: list[str],
) -> DataFrame:
    """Append each numeric metric's deviation from its own group's baseline.

    ``rel = signed_log1p(value) - signed_log1p(group_median(value))``, and the raw metric is kept
    alongside, so globally absurd values stay detectable even where they are ordinary for their group.

    What the ``log1p`` difference buys is behaviour at and below zero: it is finite and monotone for a
    baseline of zero or a negative metric, where a true log ratio is undefined, and it compresses the
    long right tail that a raw difference leaves. What it is **not** is scale-invariant or symmetric, and
    an earlier version of this docstring claimed both. Value 2 against baseline 1 gives
    ``log 3 - log 2 = 0.405``; the same ratio at 100x scale gives ``0.688``; halving gives ``-0.288``
    rather than ``-0.405``. So a group's deviations are comparable within that group but not across
    groups of very different magnitude, and the feature is not dimensionless.

    A median/MAD-standardised deviation would be genuinely dimensionless. It is not done here because it
    needs a per-group MAD persisted alongside every median, roughly doubling the baseline metadata a
    model carries, a rule for a zero MAD, and its own measurement against this form. Tracked separately.

    Follows ``_apply_frequency_encoding``'s shape exactly: compute and persist while training,
    broadcast-join and coalesce the miss while scoring. A row whose group was never trained on
    falls back to the global baseline, which makes it look ordinary rather than extreme — the
    conservative direction, and the case a caller can detect explicitly via ``is_new_baseline``.

    ``engineered_feature_names`` is positional: the sklearn pipeline is handed columns in this order,
    so features may only ever be appended at the tail. Inserting a feature-producing transform before
    this one would silently reorder an already-trained model's inputs. Two things do run afterwards,
    and neither appends a feature: the temporal block, which reads the column produced here, and
    numeric imputation, which must not run before the medians below are collected.
    """
    if not baseline_by or not numeric_cols:
        return transformed_df

    metrics = [c.name for c in numeric_cols]
    group_key_col = BASELINE_KEY_COLUMN
    transformed_df = with_baseline_key(transformed_df, baseline_by)

    if is_training:
        computed_group, computed_global = _compute_baseline_medians(transformed_df, metrics, group_key_col)
        baseline_medians.update(computed_group)
        global_medians.update(computed_global)

    # One broadcast join carrying every metric's baseline as its own column, rather than one join
    # per metric. The per-metric append order below is preserved exactly (the feature list is
    # positional), and each feature's value is unchanged: the same per-group baseline, the same
    # coalesce to the global baseline on a group miss, the same signed-log-ratio.
    metrics_with_baselines = [metric for metric in metrics if baseline_medians.get(metric)]
    baseline_cols = {metric: f"__dqx_{metric}_baseline" for metric in metrics_with_baselines}
    if metrics_with_baselines:
        lookup_df = _baseline_lookup_df(transformed_df, baseline_medians, metrics_with_baselines, group_key_col)
        transformed_df = transformed_df.join(broadcast(lookup_df), on=group_key_col, how="left")

    for metric in metrics:
        feature_name = f"{metric}{BASELINE_RELATIVE_SUFFIX}"
        baselines = baseline_medians.get(metric, {})

        if not baselines:
            # No baseline for this metric at all: the deviation is undefined, so emit a constant
            # rather than a fabricated signal. Still appended, to keep the feature list stable.
            transformed_df = transformed_df.withColumn(feature_name, lit(0.0))
            engineered_features.append(feature_name)
            continue

        global_baseline = global_medians.get(metric, 0.0)
        resolved_baseline = coalesce(col(baseline_cols[metric]), lit(global_baseline))
        transformed_df = transformed_df.withColumn(
            feature_name, _signed_log1p(col(metric)) - _signed_log1p(resolved_baseline)
        )
        engineered_features.append(feature_name)

    if baseline_cols:
        transformed_df = transformed_df.drop(*baseline_cols.values())

    # The key column deliberately survives: it is the frame's only remaining record of the
    # grouping once the raw group columns are dropped, and training-time severity calibration
    # runs on the scored frame, downstream of here. It is not a feature — see the explicit
    # projection in ``prepare_training_features``, which is what keeps it out of the model.
    return transformed_df


def _compute_baseline_medians(
    df: DataFrame, metrics: list[str], group_key_col: str
) -> tuple[dict[str, dict[str, float]], dict[str, float]]:
    """Compute each metric's per-group median, plus a global median for unseen groups.

    One ``groupBy`` over all metrics rather than one per metric, and one global aggregation.
    ``percentile_approx`` rather than an exact median: the baseline only has to be
    representative, and an exact median would need a full sort per group.
    """
    group_exprs = [F.percentile_approx(col(m), 0.5).alias(m) for m in metrics]
    group_rows = df.groupBy(group_key_col).agg(*group_exprs).collect()

    baseline_medians: dict[str, dict[str, float]] = {m: {} for m in metrics}
    for row in group_rows:
        key = row[group_key_col]
        for metric in metrics:
            value = row[metric]
            if value is not None:
                baseline_medians[metric][key] = float(value)

    global_row = df.agg(*[F.percentile_approx(col(m), 0.5).alias(m) for m in metrics]).first()
    global_medians: dict[str, float] = {}
    if global_row is not None:
        for metric in metrics:
            value = global_row[metric]
            global_medians[metric] = float(value) if value is not None else 0.0

    return baseline_medians, global_medians


def _baseline_lookup_df(
    df: DataFrame,
    baseline_medians: dict[str, dict[str, float]],
    metrics: list[str],
    group_key_col: str,
) -> DataFrame:
    """Build one broadcastable lookup: ``(group key, <metric>_baseline per metric)``.

    One row per group key seen for any metric, with each metric's baseline in its own column and
    null where that metric never saw the group — which the caller coalesces to the global baseline,
    exactly as a per-metric left join would. Replaces N single-metric lookups with one.
    """
    all_keys = sorted({key for metric in metrics for key in baseline_medians[metric]})
    baseline_cols = {metric: f"__dqx_{metric}_baseline" for metric in metrics}
    rows = [(key, *(baseline_medians[metric].get(key) for metric in metrics)) for key in all_keys]
    schema = T.StructType(
        [T.StructField(group_key_col, T.StringType(), False)]
        + [T.StructField(baseline_cols[metric], T.DoubleType(), True) for metric in metrics]
    )
    return df.sparkSession.createDataFrame(rows, schema=schema)


def _fit_temporal_from_buckets(
    df: DataFrame,
    time_column: str,
    source_columns: dict[str, str],
) -> tuple["TemporalBasis", dict[str, list[float]], dict[float, str], dict[str, float]]:
    """Fit the temporal basis from a bucketed aggregate of the training frame.

    The fit needs the data on the driver, and a table can be arbitrarily large, so the frame is first
    reduced to at most :data:`TEMPORAL_FIT_BUCKETS` time buckets carrying each metric's median. That
    bounds driver memory the same way ``_compute_baseline_medians`` does, and the per-bucket median is
    itself robust, so gross outliers are attenuated before the Huber fit ever sees them.

    The basis is then selected against the *bucket centres* rather than the raw timestamps, which matters:
    the resolution guard in :func:`~databricks.labs.dqx.anomaly.temporal.candidate_periods` then measures
    the axis actually being fitted. A period the buckets are too coarse to resolve is rejected for that
    reason rather than admitted and quietly fitted to noise.

    Args:
        df: Training frame, carrying *time_column* and every value in *source_columns*.
        time_column: The user's timestamp column.
        source_columns: metric name -> the column its expectation is fitted from. With ``baseline_by``
            in play this is the group-relative feature rather than the raw metric.

    Returns:
        ``(basis, coefficients, rejected_periods, window)``.
    """
    bounds = df.agg(
        F.min(F.unix_timestamp(col(time_column).cast(TimestampType())).cast(DoubleType())).alias("t_min"),
        F.max(F.unix_timestamp(col(time_column).cast(TimestampType())).cast(DoubleType())).alias("t_max"),
    ).first()
    if bounds is None or bounds["t_min"] is None or bounds["t_max"] is None:
        raise ComputationError(
            f"Could not read a time range from column '{time_column}'. Every value is null or unparseable "
            f"as a timestamp, so there is no axis to fit an expected level against."
        )
    t_min, t_max = float(bounds["t_min"]), float(bounds["t_max"])
    span = max(t_max - t_min, 1.0)

    seconds = _epoch_seconds(time_column, t_min)
    bucket_width = span / TEMPORAL_FIT_BUCKETS
    # Rows with no timestamp are dropped from the fit rather than bucketed. A null timestamp yields a null
    # bucket whose min(seconds) is also null, and that null reached float() below as a TypeError -- one
    # unparseable row was enough to fail training outright. Dropping them removes the bucket rather than
    # guarding its symptom; the all-null case is already refused above, with an explanation.
    # Scoring has always tolerated a null timestamp by emitting a zero residual, so this aligns the two.
    bucketed = df.filter(col(time_column).cast(TimestampType()).isNotNull()).withColumn(
        "__dqx_time_bucket", F.floor(seconds / lit(bucket_width))
    )
    aggregations = [F.percentile_approx(col(source), 0.5).alias(name) for name, source in source_columns.items()]
    rows = (
        bucketed.groupBy("__dqx_time_bucket")
        .agg(F.min(seconds).alias("__dqx_bucket_seconds"), *aggregations)
        .orderBy("__dqx_time_bucket")
        .collect()
    )

    bucket_seconds = np.array([float(row["__dqx_bucket_seconds"]) for row in rows], dtype=float)
    metrics = {
        name: np.array([float(row[name]) if row[name] is not None else np.nan for row in rows], dtype=float)
        for name in source_columns
    }
    # A bucket where a metric had no non-null value carries NaN, which would poison the fit. Drop those
    # buckets per metric rather than dropping the metric, so one sparse column does not cost the rest.
    usable = {name: ~np.isnan(values) for name, values in metrics.items()}

    # Every metric, not the first usable one. select_basis masks each on its own NaNs and reads the full
    # bucket axis for the period search, so neither the periods nor the changepoint count depends on
    # schema order any more.
    basis, rejected = select_basis(bucket_seconds, metrics)

    coefficients: dict[str, list[float]] = {}
    for name, values in metrics.items():
        mask = usable[name]
        fitted = fit_temporal(bucket_seconds[mask], {name: values[mask]}, basis)
        coefficients.update(fitted)

    return basis, coefficients, rejected, {"t_min": t_min, "t_max": t_max}


def _process_temporal_baseline_features(
    transformed_df: DataFrame,
    numeric_cols: list[ColumnTypeInfo],
    baseline_over_time: str,
    baseline_by: list[str],
    is_training: bool,
    temporal: TemporalState,
    engineered_features: list[str],
) -> DataFrame:
    """Append each numeric metric's deviation from its own expected level at that point in time.

    ``rel = value - expected(t)``. A plain difference rather than the log-ratio the group-relative
    feature uses, because when the two compose the input here is *already* the signed-log group-relative
    value, so the difference is a log-ratio in that case and a level difference otherwise. Measured that
    way too, which is the more important reason.

    Composition with ``baseline_by`` is not incidental. Fitted on raw values, one pooled trend is wrong for
    every group as soon as their slopes differ: measured, event coverage fell from 80% to 29% as group
    slopes diverged. Fitted on the group-relative residual, which is already on a common scale across
    groups, one pooled fit reached 101% of per-group fits while still training a single model.

    Runs after :func:`_process_baseline_relative_features` for two reasons that happen to agree:
    ``engineered_feature_names`` is positional so features may only be appended, and this transform reads
    the group-relative columns that one produces.

    An empty *baseline_over_time* returns immediately, appending nothing. That is what keeps a model
    trained before this existed byte-identical.
    """
    if not baseline_over_time or not numeric_cols:
        return transformed_df

    metrics = [c.name for c in numeric_cols]
    # With grouping in play the expectation is fitted on the group-relative feature; without it, on the
    # raw metric. Either way the feature appended below is named after the metric.
    source_columns = {
        metric: (
            f"{metric}{BASELINE_RELATIVE_SUFFIX}"
            if baseline_by and f"{metric}{BASELINE_RELATIVE_SUFFIX}" in transformed_df.columns
            else metric
        )
        for metric in metrics
    }

    if is_training:
        basis, coefficients, rejected, window = _fit_temporal_from_buckets(
            transformed_df, baseline_over_time, source_columns
        )
        temporal.basis.update(basis.to_dict())
        temporal.coefficients.update(coefficients)
        temporal.window.update(window)
        _log_temporal_fit(baseline_over_time, basis, coefficients, rejected, metrics)
    else:
        basis = TemporalBasis.from_dict(temporal.basis)

    seconds = _epoch_seconds(baseline_over_time, temporal.window.get("t_min", 0.0))
    for metric in metrics:
        feature_name = f"{metric}{TEMPORAL_RELATIVE_SUFFIX}"
        coefficient_vector = temporal.coefficients.get(metric)
        if not coefficient_vector:
            # No expectation was learned for this metric. Emit a constant rather than a fabricated
            # signal, and still append it, so the feature list stays positionally stable.
            transformed_df = transformed_df.withColumn(feature_name, lit(0.0))
            engineered_features.append(feature_name)
            continue
        expected_level = _temporal_expected_column(basis, coefficient_vector, seconds)
        transformed_df = transformed_df.withColumn(
            feature_name, coalesce(col(source_columns[metric]) - expected_level, lit(0.0))
        )
        engineered_features.append(feature_name)

    return transformed_df


def _impute_numeric_features(
    transformed_df: DataFrame,
    numeric_cols: list[ColumnTypeInfo],
    baseline_by: list[str],
    baseline_over_time: str,
) -> DataFrame:
    """Replace every remaining numeric null with a neutral zero. Must run last.

    Last because both comparison bases are fitted from the raw metric, and imputing first would let a
    missing observation move the very baseline it is then measured against. By the time this runs the
    medians are collected and the temporal coefficients are fitted, so filling in is free of
    consequence: the estimators need a dense matrix, and the ``_is_null`` indicator already carries the
    fact that the value was absent.

    Zero is neutral for the derived features by construction -- it is exactly "no deviation from the
    baseline" -- which the raw metric cannot claim, but the raw metric has the indicator beside it.

    Appends nothing, so the positional ``engineered_feature_names`` contract is untouched.
    """
    for col_info in numeric_cols:
        metric = col_info.name
        # Both derived columns exist for every metric whenever their basis is set: each block above
        # appends one per metric unconditionally, a constant where it learned nothing. Named without a
        # guard so that a future early-exit there fails here loudly rather than skipping an imputation.
        names = [metric]
        if baseline_by:
            names.append(f"{metric}{BASELINE_RELATIVE_SUFFIX}")
        if baseline_over_time:
            names.append(f"{metric}{TEMPORAL_RELATIVE_SUFFIX}")
        for name in names:
            transformed_df = transformed_df.withColumn(name, coalesce(col(name), lit(0.0)))

    return transformed_df


def _log_temporal_fit(
    time_column: str,
    basis: "TemporalBasis",
    coefficients: dict[str, list[float]],
    rejected: dict[float, str],
    metrics: list[str],
) -> None:
    """Say what was fitted, and what was not and why.

    The rejections are the part worth logging loudly. A caller who expected a daily cycle and did not get
    one has no way to find out otherwise, and the silence is the defect: a seasonal term fitted over too
    few cycles measurably costs accuracy, so refusing it is right, but refusing it quietly is not.
    """
    safe_column = sanitize_for_logging(time_column)
    logger.info(
        f"Temporal baseline on '{safe_column}': fitted {basis.describe()} for "
        f"{len(coefficients)} of {len(metrics)} metrics"
    )
    for period, reason in rejected.items():
        logger.info(f"Temporal baseline on '{safe_column}': no {period:g}s seasonal term, {reason}")


def apply_feature_engineering(
    df: DataFrame,
    column_infos: list[ColumnTypeInfo],
    categorical_cardinality_threshold: int = 20,
    frequency_maps: dict[str, dict[str, float]] | None = None,
    onehot_categories: dict[str, list[str]] | None = None,
    baseline_by: list[str] | None = None,
    baseline_medians: dict[str, dict[str, float]] | None = None,
    global_medians: dict[str, float] | None = None,
    baseline_over_time: str | None = None,
    temporal: TemporalState | None = None,
) -> tuple[DataFrame, SparkFeatureMetadata]:
    """
    Apply feature engineering transformations in Spark (distributed).

    Returns:
        - DataFrame with engineered numeric features
        - Metadata for reconstructing transformations during scoring

    Transformations applied, in this order — the order is part of the contract, because
    ``engineered_feature_names`` is positional and the sklearn pipeline is handed columns in it:
    1. Categorical: OneHot (low-card) or Frequency encoding (high-card)
    2. Datetime: Extract hour_sin/cos, dow_sin/cos, month_sin/cos, is_weekend
    3. Boolean: Map to 0/1
    4. Numeric: Keep as-is
    5. Group-relative: deviation of each numeric metric from its own group's baseline
    6. Time-relative: deviation of each numeric metric from its expected level at that timestamp
    7. Null indicators: Add column_is_null for columns with nulls
    8. Imputation: Fill nulls with 0 (numeric), "MISSING" (categorical), epoch (datetime), 0 (boolean)

    New transforms must be appended at the end, never inserted: inserting one shifts the feature
    positions an already-trained model expects.

    Args:
        df: Input DataFrame with original columns
        column_infos: Column type information from ColumnTypeClassifier
        categorical_cardinality_threshold: Threshold for OneHot vs Frequency encoding
        frequency_maps: Pre-computed frequency maps (for scoring). If None, compute from df (for training).
        onehot_categories: Pre-computed OneHot distinct values (for scoring). If None, compute from df (for training).
        baseline_by: Columns forming the group key. Empty disables group-relative features entirely,
            which is what makes a pre-grouping model's feature list byte-identical.
        baseline_medians: Pre-computed per-group medians (for scoring). Computed from df when training.
        global_medians: Pre-computed global medians, used for groups absent from training.
        baseline_over_time: The time column each metric's expected level is fitted along. Empty disables
            time-relative features entirely, which is what makes a pre-temporal model's feature list
            byte-identical.
        temporal: Pre-fitted temporal artefacts (for scoring); filled in place when training.
    """
    is_training = frequency_maps is None
    if frequency_maps is None:
        frequency_maps = {}
    if onehot_categories is None:
        onehot_categories = {}
    baseline_by = list(baseline_by or [])
    if baseline_medians is None:
        baseline_medians = {}
    if global_medians is None:
        global_medians = {}
    baseline_over_time = baseline_over_time or ""
    if temporal is None:
        temporal = TemporalState()

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

    # engineered_feature_names is positional, so features may only be appended past this point.
    transformed_df = _process_baseline_relative_features(
        transformed_df,
        numeric_cols,
        baseline_by,
        is_training,
        baseline_medians,
        global_medians,
        engineered_features,
    )

    # Must stay last, and must stay after the group-relative block above: it reads the columns that one
    # produces, because a pooled trend fitted on raw values is wrong for every group once their slopes
    # differ (measured: event coverage 80% to 29% as slopes diverged).
    transformed_df = _process_temporal_baseline_features(
        transformed_df,
        numeric_cols,
        baseline_over_time,
        baseline_by,
        is_training,
        temporal,
        engineered_features,
    )

    # Strictly after both bases above, which are fitted from the un-imputed metric on purpose.
    transformed_df = _impute_numeric_features(transformed_df, numeric_cols, baseline_by, baseline_over_time)

    return _project_and_describe(
        transformed_df,
        column_infos,
        engineered_features,
        categorical_cardinality_threshold=categorical_cardinality_threshold,
        frequency_maps=frequency_maps,
        onehot_categories=onehot_categories,
        baseline_by=baseline_by,
        baseline_medians=baseline_medians,
        global_medians=global_medians,
        baseline_over_time=baseline_over_time,
        temporal=temporal,
    )


def _project_and_describe(
    transformed_df: DataFrame,
    column_infos: list[ColumnTypeInfo],
    engineered_features: list[str],
    *,
    categorical_cardinality_threshold: int,
    frequency_maps: dict[str, dict[str, float]],
    onehot_categories: dict[str, list[str]],
    baseline_by: list[str],
    baseline_medians: dict[str, dict[str, float]],
    global_medians: dict[str, float],
    baseline_over_time: str,
    temporal: TemporalState,
) -> tuple[DataFrame, SparkFeatureMetadata]:
    """Project the frame down to features, and describe what was built so scoring can replay it.

    Two contracts live here, both about what must *not* reach the model:

    Original columns are dropped, but incidental ones are kept (``__dqx_row_id__`` joins results back).
    A comparison basis is never a feature: the grouping columns define what a metric is compared against,
    and the time column is the axis it is measured along. Either reaching the sklearn pipeline would put
    it in the inferred MLflow signature too, which then has to be honoured at every future scoring call.
    """
    feature_col_names = [c.name for c in column_infos]
    excluded_bases = set(baseline_by) | ({baseline_over_time} if baseline_over_time else set())
    extra_cols = [
        c
        for c in transformed_df.columns
        if c not in feature_col_names and c not in engineered_features and c not in excluded_bases
    ]
    result_df = transformed_df.select(*engineered_features, *extra_cols)

    # Only the features that survived on this frame. Original columns such as a datetime are dropped
    # during transformation, so the persisted list must reflect what the scoring UDF will actually be handed.
    actual_engineered_features = [f for f in engineered_features if f in result_df.columns]

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
        baseline_by=baseline_by,
        baseline_medians=baseline_medians,
        global_medians=global_medians,
        baseline_over_time=baseline_over_time,
        temporal_basis=temporal.basis,
        temporal_coefficients=temporal.coefficients,
        temporal_window=temporal.window,
    )

    return result_df, metadata


def apply_feature_engineering_from_metadata(
    df: DataFrame,
    feature_metadata: SparkFeatureMetadata,
    column_infos: list[ColumnTypeInfo] | None = None,
) -> tuple[DataFrame, SparkFeatureMetadata]:
    """Re-apply the transformations recorded in *feature_metadata* to a new DataFrame.

    This is the derived ("scoring") mode of :func:`apply_feature_engineering`: rather than
    computing encodings from the data, it replays the ones a model was trained with. Every
    caller that scores, or that recomputes engineered features for an already-trained model,
    should go through here so that a newly persisted metadata field has to be threaded in one
    place instead of five.

    Args:
        df: DataFrame to transform.
        feature_metadata: Metadata persisted at training time.
        column_infos: Column infos, reconstructed from *feature_metadata* when omitted.

    Returns:
        The engineered DataFrame and the metadata describing it. The returned metadata's
        ``engineered_feature_names`` reflects the columns that actually survived on *df*,
        which is what the scoring UDF must be handed.
    """
    return apply_feature_engineering(
        df,
        column_infos if column_infos is not None else reconstruct_column_infos(feature_metadata),
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
        baseline_by=feature_metadata.baseline_by,
        baseline_medians=feature_metadata.baseline_medians,
        global_medians=feature_metadata.global_medians,
        baseline_over_time=feature_metadata.baseline_over_time,
        temporal=TemporalState(
            basis=feature_metadata.temporal_basis,
            coefficients=feature_metadata.temporal_coefficients,
            window=feature_metadata.temporal_window,
        ),
    )
