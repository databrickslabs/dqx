"""Scoring utilities for anomaly detection DataFrames."""

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)
import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly.utils.segment_utils import canonicalize_segment_values


def create_null_scored_dataframe(
    df: DataFrame,
    include_contributions: bool,
    include_confidence: bool = False,
    score_col: str = "anomaly_score",
    score_std_col: str = "anomaly_score_std",
    contributions_col: str = "anomaly_contributions",
    severity_col: str = "severity_percentile",
) -> DataFrame:
    """Create a DataFrame with null anomaly scores (for empty segments or filtered rows).

    Args:
        df: Input DataFrame
        include_contributions: Whether to include null contributions column
        include_confidence: Whether to include null confidence/std column
        score_col: Name for the score column
        score_std_col: Name for the standard deviation column
        contributions_col: Name for the contributions column
        severity_col: Name for the severity percentile column

    Returns:
        DataFrame with null anomaly scores and properly structured _dq_info column
    """
    result = df.withColumn(score_col, F.lit(None).cast(DoubleType()))
    if include_confidence:
        result = result.withColumn(score_std_col, F.lit(None).cast(DoubleType()))
    if include_contributions:
        result = result.withColumn(contributions_col, F.lit(None).cast(MapType(StringType(), DoubleType())))
    result = result.withColumn(severity_col, F.lit(None).cast(DoubleType()))

    # Add null _dq_info column with proper schema (direct struct, not array)
    null_anomaly_info = F.lit(None).cast(
        StructType(
            [
                StructField("check_name", StringType(), True),
                StructField("score", DoubleType(), True),
                StructField("severity_percentile", DoubleType(), True),
                StructField("is_anomaly", BooleanType(), True),
                StructField("threshold", DoubleType(), True),
                StructField("model", StringType(), True),
                StructField("segment", MapType(StringType(), StringType()), True),
                StructField("contributions", MapType(StringType(), DoubleType()), True),
                StructField("confidence_std", DoubleType(), True),
            ]
        )
    )

    if "_dq_info" in result.columns:
        # Add or replace the 'anomaly' field in the existing struct
        result = result.withColumn("_dq_info", F.col("_dq_info").withField("anomaly", null_anomaly_info))
    else:
        # Create a new struct with only the 'anomaly' field
        result = result.withColumn("_dq_info", F.struct(null_anomaly_info.alias("anomaly")))

    return result


def add_info_column(
    df: DataFrame,
    model_name: str,
    threshold: float,
    segment_values: dict[str, str] | None = None,
    include_contributions: bool = False,
    include_confidence: bool = False,
    score_col: str = "anomaly_score",
    score_std_col: str = "anomaly_score_std",
    contributions_col: str = "anomaly_contributions",
    severity_col: str = "severity_percentile",
) -> DataFrame:
    """Add _dq_info struct column with anomaly metadata.

    Args:
        df: Scored DataFrame with anomaly_score, prediction, etc.
        model_name: Name of the model used for scoring.
        threshold: Threshold used for anomaly detection.
        segment_values: Segment values if model is segmented (None for global models).
        include_contributions: Whether anomaly_contributions are available (0–100 percent).
        include_confidence: Whether anomaly_score_std is available.
        score_col: Column name for anomaly scores (internal, collision-safe).
        score_std_col: Column name for ensemble std scores (internal, collision-safe).
        contributions_col: Column name for SHAP contributions (internal, collision-safe, 0–100 percent).
        severity_col: Column name for severity percentile (internal, collision-safe).

    Returns:
        DataFrame with _dq_info column added/updated.
    """
    # Build anomaly info struct
    anomaly_info_fields = {
        "check_name": F.lit("has_no_anomalies"),
        "score": F.round(F.col(score_col), 3),
        "severity_percentile": F.round(F.col(severity_col), 1),
        "is_anomaly": F.col(severity_col) >= F.lit(threshold),
        "threshold": F.lit(threshold),
        "model": F.lit(model_name),
    }

    # Add segment as map (null for global models)
    if segment_values:
        canonical_values = canonicalize_segment_values(segment_values)
        anomaly_info_fields["segment"] = F.create_map(
            *[F.lit(item) for pair in canonical_values.items() for item in pair]
        )
    else:
        anomaly_info_fields["segment"] = F.lit(None).cast(MapType(StringType(), StringType()))

    # Add contributions (null if not requested or not available)
    if include_contributions and contributions_col in df.columns:
        anomaly_info_fields["contributions"] = F.col(contributions_col)
    else:
        anomaly_info_fields["contributions"] = F.lit(None).cast(MapType(StringType(), DoubleType()))

    # Add confidence_std (null if not requested or not available)
    if include_confidence and score_std_col in df.columns:
        anomaly_info_fields["confidence_std"] = F.col(score_std_col)
    else:
        anomaly_info_fields["confidence_std"] = F.lit(None).cast(DoubleType())

    # Create anomaly info struct and wrap in array
    anomaly_info = F.struct(*[value.alias(key) for key, value in anomaly_info_fields.items()])

    # Create _dq_info struct with anomaly (direct struct, not array - single result per row)
    info_struct = F.struct(anomaly_info.alias("anomaly"))

    if "_dq_info" in df.columns:
        return df.withColumn("_dq_info", F.col("_dq_info").withField("anomaly", anomaly_info))
    return df.withColumn("_dq_info", info_struct)


def add_severity_percentile_column(
    df: DataFrame,
    *,
    score_col: str,
    severity_col: str,
    quantile_points: list[tuple[float, float]],
) -> DataFrame:
    """Add a severity percentile column using piecewise linear interpolation.

    Args:
        df: DataFrame with anomaly score column.
        score_col: Column name containing anomaly scores.
        severity_col: Output column name for severity percentile (0–100).
        quantile_points: Ordered list of (percentile, score) points.

    Returns:
        DataFrame with severity percentile column added.
    """
    if not quantile_points:
        return df.withColumn(severity_col, F.lit(None).cast(DoubleType()))

    # Ensure points are sorted by percentile
    points = sorted(quantile_points, key=lambda p: p[0])
    score_expr = F.col(score_col)

    # Handle null scores
    expr = F.when(score_expr.isNull(), F.lit(None).cast(DoubleType()))

    prev_p, prev_q = points[0]
    expr = expr.when(score_expr <= F.lit(prev_q), F.lit(float(prev_p)))

    for current_p, current_q in points[1:]:
        if current_q == prev_q:
            interpolated = F.lit(float(current_p))
        else:
            interpolated = F.lit(float(prev_p)) + (
                (score_expr - F.lit(prev_q)) * (float(current_p) - float(prev_p)) / (float(current_q) - float(prev_q))
            )
        expr = expr.when(score_expr <= F.lit(current_q), interpolated)
        prev_p, prev_q = current_p, current_q

    expr = expr.otherwise(F.lit(float(prev_p)))

    return df.withColumn(severity_col, expr)


def create_udf_schema(include_contributions: bool) -> StructType:
    """Create schema for scoring UDF output.

    The anomaly_score is used internally for populating _dq_info.anomaly.score.
    Users should check _dq_info.anomaly.is_anomaly for anomaly status.

    Args:
        include_contributions: Whether to include contributions field

    Returns:
        StructType schema for the UDF output
    """
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
    ]
    if include_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    return StructType(schema_fields)
