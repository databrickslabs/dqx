"""Anomaly scoring helpers: DataFrame/schema builders, row filter, join, reserved column checks."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.dqx.anomaly.anomaly_info_schema import ai_explanation_struct_schema, anomaly_info_struct_schema
from databricks.labs.dqx.anomaly.segment_utils import canonicalize_segment_values
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.schema.dq_info_schema import (
    build_dq_info_struct,
    register_dq_info_field,
)

# Register anomaly field for the wide _dq_info struct (so merge gets a consistent schema)
register_dq_info_field("anomaly", anomaly_info_struct_schema)


def create_null_scored_dataframe(
    df: DataFrame,
    enable_contributions: bool,
    enable_confidence_std: bool = False,
    score_col: str = "anomaly_score",
    score_std_col: str = "anomaly_score_std",
    contributions_col: str = "anomaly_contributions",
    severity_col: str = "severity_percentile",
    info_col_name: str = "_dq_info",
) -> DataFrame:
    """Create a DataFrame with null anomaly scores (for empty segments or filtered rows).

    Args:
        df: Input DataFrame
        enable_contributions: Whether to include null contributions column
        enable_confidence_std: Whether to include null confidence/std column
        score_col: Name for the score column
        score_std_col: Name for the standard deviation column
        contributions_col: Name for the contributions column
        severity_col: Name for the severity percentile column
        info_col_name: Name for the info struct column (collision-safe UUID name expected).

    Returns:
        DataFrame with null anomaly scores and properly structured info column
    """
    result = df.withColumn(score_col, F.lit(None).cast(DoubleType()))
    if enable_confidence_std:
        result = result.withColumn(score_std_col, F.lit(None).cast(DoubleType()))
    if enable_contributions:
        result = result.withColumn(contributions_col, F.lit(None).cast(MapType(StringType(), DoubleType())))
    result = result.withColumn(severity_col, F.lit(None).cast(DoubleType()))

    null_anomaly_info = F.lit(None).cast(anomaly_info_struct_schema)

    return result.withColumn(info_col_name, build_dq_info_struct(anomaly=null_anomaly_info))


def add_info_column(
    df: DataFrame,
    model_name: str,
    threshold: float,
    info_col_name: str,
    segment_values: dict[str, str] | None = None,
    enable_contributions: bool = False,
    enable_confidence_std: bool = False,
    ai_explanation_col: str | None = None,
    score_col: str = "anomaly_score",
    score_std_col: str = "anomaly_score_std",
    contributions_col: str = "anomaly_contributions",
    severity_col: str = "severity_percentile",
) -> DataFrame:
    """Add info struct column with anomaly metadata.

    Args:
        df: Scored DataFrame with anomaly_score, prediction, etc.
        model_name: Name of the model used for scoring.
        threshold: Threshold used for row anomaly detection.
        info_col_name: Name for the info struct column (collision-safe UUID name expected).
        segment_values: Segment values if model is segmented (None for global models).
        enable_contributions: Whether anomaly_contributions are available (0–100 percent).
        enable_confidence_std: Whether anomaly_score_std is available.
        ai_explanation_col: Optional column name carrying the pre-computed AI explanation struct.
            When provided and present on df, it is packaged into _dq_info.
        score_col: Column name for anomaly scores (internal, collision-safe).
        score_std_col: Column name for ensemble std scores (internal, collision-safe).
        contributions_col: Column name for SHAP contributions (internal, collision-safe, 0–100 percent).
        severity_col: Column name for severity percentile (internal, collision-safe).

    Returns:
        DataFrame with info column added.
    """
    # Build anomaly info struct
    anomaly_info_fields = {
        "check_name": F.lit("has_no_row_anomalies"),
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
    if enable_contributions and contributions_col in df.columns:
        anomaly_info_fields["contributions"] = F.col(contributions_col)
    else:
        anomaly_info_fields["contributions"] = F.lit(None).cast(MapType(StringType(), DoubleType()))

    # Add confidence_std (null if not requested or not available)
    if enable_confidence_std and score_std_col in df.columns:
        anomaly_info_fields["confidence_std"] = F.col(score_std_col)
    else:
        anomaly_info_fields["confidence_std"] = F.lit(None).cast(DoubleType())

    # Add ai_explanation (null when not requested or column not present)
    if ai_explanation_col and ai_explanation_col in df.columns:
        anomaly_info_fields["ai_explanation"] = F.col(ai_explanation_col)
    else:
        anomaly_info_fields["ai_explanation"] = F.lit(None).cast(ai_explanation_struct_schema)

    anomaly_info = F.struct(*[value.alias(key) for key, value in anomaly_info_fields.items()]).cast(
        anomaly_info_struct_schema
    )
    return df.withColumn(info_col_name, build_dq_info_struct(anomaly=anomaly_info))


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


def create_udf_schema(enable_contributions: bool) -> StructType:
    """Create schema for scoring UDF output.

    The anomaly_score is used internally for populating _dq_info (array of structs).
    After merge, first check's anomaly info is at _dq_info[0].anomaly; check _dq_info[0].anomaly.is_anomaly for status.

    Args:
        enable_contributions: Whether to include contributions field

    Returns:
        StructType schema for the UDF output
    """
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
    ]
    if enable_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    return StructType(schema_fields)


def check_reserved_row_id_columns(df: DataFrame) -> None:
    """Raise if DataFrame has reserved _dqx_row_id / __dqx_row_id columns."""
    reserved_prefixes = ("_dqx_row_id", "__dqx_row_id")
    for col_name in df.columns:
        if col_name.startswith(reserved_prefixes) or col_name == "_dqx_row_id":
            raise InvalidParameterError(
                f"Input DataFrame must not contain reserved column '{col_name}'. "
                "Rename or drop this column before running the anomaly check."
            )


def join_filtered_results_back(
    df: DataFrame,
    result: DataFrame,
    merge_columns: list[str],
    score_col: str,
    info_col: str,
) -> DataFrame:
    """Left-join scored result onto df so every input row is preserved.

    Rows that were scored get score/info; rows that were not (e.g. filtered out by
    row_filter) get null. merge_columns (e.g. row_id) must exist on both df and result.
    """
    score_cols_to_join = [score_col, info_col]
    scored_subset = result.select(*merge_columns, *score_cols_to_join)

    agg_exprs = [
        F.max(score_col).alias(score_col),
        F.max_by(info_col, score_col).alias(info_col),
    ]
    scored_subset_unique = scored_subset.groupBy(*merge_columns).agg(*agg_exprs)

    return df.join(scored_subset_unique, on=merge_columns, how="left")


def apply_row_filter(df: DataFrame, row_filter: str | None) -> DataFrame:
    """Return only rows that match row_filter for scoring; if no filter, return df unchanged.

    row_filter is a SQL expression (e.g. \"region = 'US'\"). Only these rows are run
    through anomaly detection; elsewhere we join results back so output has same row count.
    """
    return df.filter(F.expr(row_filter)) if row_filter else df
