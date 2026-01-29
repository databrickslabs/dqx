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


def create_null_scored_dataframe(
    df: DataFrame,
    include_contributions: bool,
    include_confidence: bool = False,
    score_col: str = "anomaly_score",
    score_std_col: str = "anomaly_score_std",
    contributions_col: str = "anomaly_contributions",
) -> DataFrame:
    """Create a DataFrame with null anomaly scores (for empty segments or filtered rows).

    Args:
        df: Input DataFrame
        include_contributions: Whether to include null contributions column
        include_confidence: Whether to include null confidence/std column
        score_col: Name for the score column
        score_std_col: Name for the standard deviation column
        contributions_col: Name for the contributions column

    Returns:
        DataFrame with null anomaly scores and properly structured _dq_info column
    """
    result = df.withColumn(score_col, F.lit(None).cast(DoubleType()))
    if include_confidence:
        result = result.withColumn(score_std_col, F.lit(None).cast(DoubleType()))
    if include_contributions:
        result = result.withColumn(contributions_col, F.lit(None).cast(MapType(StringType(), DoubleType())))

    # Add null _dq_info column with proper schema (direct struct, not array)
    null_anomaly_info = F.lit(None).cast(
        StructType(
            [
                StructField("check_name", StringType(), True),
                StructField("score", DoubleType(), True),
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
) -> DataFrame:
    """Add _dq_info struct column with anomaly metadata.

    Args:
        df: Scored DataFrame with anomaly_score, prediction, etc.
        model_name: Name of the model used for scoring.
        threshold: Threshold used for anomaly detection.
        segment_values: Segment values if model is segmented (None for global models).
        include_contributions: Whether anomaly_contributions are available.
        include_confidence: Whether anomaly_score_std is available.
        score_col: Column name for anomaly scores (internal, collision-safe).
        score_std_col: Column name for ensemble std scores (internal, collision-safe).
        contributions_col: Column name for SHAP contributions (internal, collision-safe).

    Returns:
        DataFrame with _dq_info column added/updated.
    """
    # Build anomaly info struct
    anomaly_info_fields = {
        "check_name": F.lit("has_no_anomalies"),
        "score": F.col(score_col),
        "is_anomaly": F.col(score_col) >= F.lit(threshold),
        "threshold": F.lit(threshold),
        "model": F.lit(model_name),
    }

    # Add segment as map (null for global models)
    if segment_values:
        anomaly_info_fields["segment"] = F.create_map(
            *[F.lit(item) for pair in segment_values.items() for item in pair]
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
