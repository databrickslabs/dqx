"""Anomaly scoring helpers: DataFrame/schema builders, row filter, join, reserved column checks."""

from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.dqx.anomaly.anomaly_info_schema import ai_explanation_struct_schema, anomaly_info_struct_schema
from databricks.labs.dqx.anomaly.scoring_config import ScoringOutputColumns
from databricks.labs.dqx.anomaly.segment_utils import canonicalize_segment_values, baseline_key_column
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.utils import safe_filter_expr
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


@dataclass(frozen=True)
class UnseenGroupContext:
    """Where to find the unseen-group verdict, and what it should mean.

    Bundled rather than passed as three more parameters to ``add_info_column``: they are only ever
    meaningful together, and the alternative is a twelve-plus argument signature nobody can read.
    """

    unseen_col: str
    group_key_col: str
    flag_as_violation: bool = False


def add_info_column(
    df: DataFrame,
    model_name: str,
    threshold: float,
    *,
    output_columns: ScoringOutputColumns | None = None,
    info_col_name: str | None = None,
    segment_values: dict[str, str] | None = None,
    enable_contributions: bool = False,
    enable_confidence_std: bool = False,
    ai_explanation_col: str | None = None,
    unseen: UnseenGroupContext | None = None,
) -> DataFrame:
    """Add info struct column with anomaly metadata.

    Args:
        df: Scored DataFrame with anomaly_score, prediction, etc.
        model_name: Name of the model used for scoring.
        threshold: Threshold used for row anomaly detection.
        output_columns: Internal column names to read scores, severity, contributions and std
            from, and where to write the info struct. Defaults to the standard names.
        info_col_name: Overrides ``output_columns.info`` when given (collision-safe UUID name).
        segment_values: Segment values if model is segmented (None for global models).
        enable_contributions: Whether anomaly_contributions are available (0–100 percent).
        enable_confidence_std: Whether anomaly_score_std is available.
        ai_explanation_col: Optional column name carrying the pre-computed AI explanation struct.
            When provided and present on df, it is packaged into _dq_info.
        unseen: Where the unseen-group verdict lives and whether it counts as a violation. None
            means the model is not grouped, so no row is unseen.

    Returns:
        DataFrame with info column added.
    """
    output_columns = output_columns or ScoringOutputColumns()
    score_col = output_columns.score
    score_std_col = output_columns.score_std
    contributions_col = output_columns.contributions
    severity_col = output_columns.severity
    info_col_name = info_col_name or output_columns.info

    # Build anomaly info struct
    unseen_col = unseen.unseen_col if unseen else None
    group_key_col = unseen.group_key_col if unseen else None
    flag_unseen_baseline_as_violation = unseen.flag_as_violation if unseen else False
    unseen_expr = F.col(unseen_col) if unseen_col and unseen_col in df.columns else F.lit(False)

    # An unseen group has a null severity, so `severity >= threshold` is null rather than False.
    # Decide it explicitly: flag_unseen_baseline_as_violation says whether "we cannot judge this row"
    # should count as a violation, which is a policy question only the caller can answer.
    is_anomaly = F.when(unseen_expr, F.lit(bool(flag_unseen_baseline_as_violation))).otherwise(
        F.col(severity_col) >= F.lit(threshold)
    )

    anomaly_info_fields = {
        "check_name": F.lit("has_no_row_anomalies"),
        "score": F.round(F.col(score_col), 3),
        "severity_percentile": F.round(F.col(severity_col), 1),
        "is_anomaly": is_anomaly,
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

    # Add contributions (null if not requested or not available). Contributions are only
    # surfaced for anomalous rows: SHAP is computed just for rows at or above the threshold
    # (see compute_gated_shap_contributions) and this gate makes the observable contract
    # exact — non-anomalous rows always carry a null map.
    if enable_contributions and contributions_col in df.columns:
        anomaly_info_fields["contributions"] = F.when(
            F.col(severity_col) >= F.lit(threshold), F.col(contributions_col)
        ).otherwise(F.lit(None).cast(MapType(StringType(), DoubleType())))
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

    # Surface the unseen-group verdict, and the key that was not recognised so the caller can act
    # on it (retrain, or investigate an unexpected dimension value) without re-deriving it.
    anomaly_info_fields["is_new_baseline"] = unseen_expr
    if group_key_col and group_key_col in df.columns:
        anomaly_info_fields["new_baseline_key"] = F.when(unseen_expr, F.col(group_key_col)).otherwise(
            F.lit(None).cast(StringType())
        )
    else:
        anomaly_info_fields["new_baseline_key"] = F.lit(None).cast(StringType())

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

    points = sorted(quantile_points, key=lambda p: p[0])
    expr = _piecewise_severity_expr(F.col(score_col), [(p, F.lit(float(q))) for p, q in points])
    return df.withColumn(severity_col, expr)


def _piecewise_severity_expr(score_expr: Column, points: list[tuple[float, Column]]) -> Column:
    """Map a score onto 0–100 by piecewise linear interpolation between *points*.

    The score bounds are Columns rather than floats so the same interpolation serves both the
    global calibration, where each bound is a literal, and per-group calibration, where each
    bound is a column read from a broadcast lookup of that row's group.

    Args:
        score_expr: The score to map.
        points: ``(percentile, score bound)`` pairs, ordered by percentile.
    """
    expr = F.when(score_expr.isNull(), F.lit(None).cast(DoubleType()))

    prev_p, prev_q = points[0]
    expr = expr.when(score_expr <= prev_q, F.lit(float(prev_p)))

    for current_p, current_q in points[1:]:
        span = current_q - prev_q
        # A degenerate segment (equal bounds) would divide by zero. It means every score in this
        # band sits on one point, so the upper percentile is the answer outright.
        interpolated = F.when(span == F.lit(0.0), F.lit(float(current_p))).otherwise(
            F.lit(float(prev_p)) + ((score_expr - prev_q) * F.lit(float(current_p) - float(prev_p)) / span)
        )
        expr = expr.when(score_expr <= current_q, interpolated)
        prev_p, prev_q = current_p, current_q

    return expr.otherwise(F.lit(float(prev_p)))


def add_baseline_severity_percentile_column(
    df: DataFrame,
    *,
    score_col: str,
    severity_col: str,
    baseline_by: list[str],
    group_quantile_points: dict[str, list[tuple[float, float]]],
    fallback_quantile_points: list[tuple[float, float]],
) -> DataFrame:
    """Add a severity percentile calibrated against each row's own group.

    A raw anomaly score is not comparable across groups: severity is a percentile of a score
    distribution, and each group has its own. Calibrating per group is what makes "severity 97"
    mean the same thing in a high-volume group as in a quiet one.

    Implemented as a broadcast join of a ``(group key, p00 … p100)`` lookup plus one piecewise
    expression over those columns — deliberately *not* a nested
    ``when(group_key == lit(k), <interpolation>)`` chain, which at 90 groups and 11 points is
    around a thousand branches of generated code and blows up compilation.

    Groups absent from *group_quantile_points* fall back to the global calibration, so a group
    seen at scoring but not at training still gets a severity rather than a null.
    """
    if not group_quantile_points:
        return add_severity_percentile_column(
            df, score_col=score_col, severity_col=severity_col, quantile_points=fallback_quantile_points
        )

    percentiles = [p for p, _ in sorted(fallback_quantile_points, key=lambda point: point[0])]
    fallback_by_percentile = dict(fallback_quantile_points)
    quantile_cols = {p: f"__dqx_group_q{int(p * 100):05d}" for p in percentiles}

    group_key_col = "__dqx_severity_group_key"
    df = df.withColumn(group_key_col, baseline_key_column(baseline_by))

    schema = StructType(
        [StructField(group_key_col, StringType(), False)]
        + [StructField(quantile_cols[p], DoubleType(), True) for p in percentiles]
    )
    rows = []
    for key, points in group_quantile_points.items():
        by_percentile = dict(points)
        rows.append((key, *[by_percentile.get(p) for p in percentiles]))
    lookup_df = df.sparkSession.createDataFrame(rows, schema=schema)

    df = df.join(F.broadcast(lookup_df), on=group_key_col, how="left")

    bounds = [(p, F.coalesce(F.col(quantile_cols[p]), F.lit(float(fallback_by_percentile[p])))) for p in percentiles]
    df = df.withColumn(severity_col, _piecewise_severity_expr(F.col(score_col), bounds))

    return df.drop(group_key_col, *quantile_cols.values())


#: Above this many known group keys, membership is tested with a broadcast anti-join rather than
#: an `isin` list. An `isin` of thousands of literals bloats the query plan.
_MAX_ISIN_GROUP_KEYS = 200


def mark_unseen_baselines(
    df: DataFrame,
    baseline_by: list[str],
    known_group_keys: list[str],
    *,
    unseen_col: str,
    group_key_col: str,
) -> DataFrame:
    """Add a boolean *unseen_col*, True where the row's group was absent from training.

    Both categorical encoders mishandle an unseen value, in opposite directions, and neither
    tells the caller:

    - One-hot emits all zeros. Because each one-hot column is 0 for most training rows, an
      all-zeros row looks like the majority on *every* axis, so axis-aligned splits cannot see
      that no category is set at all. Result: a false negative — the most normal-looking score
      in the table.
    - Frequency encoding coalesces the miss to 0.0, which sits *below every frequency seen in
      training*. Result: a false positive, a fabricated extreme. This is the branch taken above
      the cardinality threshold, so it is the one that fires on wide groupings.

    Either way the score is not meaningful, so the honest answer is to flag it rather than emit a
    number. Uses ``isin`` for a modest number of known keys and a broadcast left-join for many,
    since an ``isin`` over thousands of literals bloats the query plan.
    """
    if not baseline_by:
        return df.withColumn(unseen_col, F.lit(False)).withColumn(group_key_col, F.lit(None).cast(StringType()))

    df = df.withColumn(group_key_col, baseline_key_column(baseline_by))

    if not known_group_keys:
        # A grouped model with no recorded groups cannot judge membership; treating every row as
        # unseen would null the whole frame, so trust the score instead.
        return df.withColumn(unseen_col, F.lit(False))

    if len(known_group_keys) <= _MAX_ISIN_GROUP_KEYS:
        return df.withColumn(unseen_col, ~F.col(group_key_col).isin(known_group_keys))

    known_col = "__dqx_known_group_key"
    known_df = df.sparkSession.createDataFrame(
        [(key,) for key in known_group_keys],
        schema=StructType([StructField(known_col, StringType(), False)]),
    )
    joined = df.join(F.broadcast(known_df), F.col(group_key_col) == F.col(known_col), "left")
    return joined.withColumn(unseen_col, F.col(known_col).isNull()).drop(known_col)


def null_out_unseen_baseline_scores(
    df: DataFrame,
    *,
    unseen_col: str,
    score_col: str,
    severity_col: str,
    contributions_col: str | None,
    score_std_col: str | None,
) -> DataFrame:
    """Null the score, severity and contributions of unseen-group rows, in place.

    Deliberately not routed through ``create_null_scored_dataframe``: that builds the anomaly
    struct as ``lit(None).cast(schema)``, a wholly null struct, which cannot carry
    ``is_new_baseline``. Overwriting the individual columns keeps the struct intact so the flag
    survives to the caller.
    """
    unseen = F.col(unseen_col)
    df = df.withColumn(score_col, F.when(unseen, F.lit(None).cast(DoubleType())).otherwise(F.col(score_col)))
    df = df.withColumn(severity_col, F.when(unseen, F.lit(None).cast(DoubleType())).otherwise(F.col(severity_col)))
    if contributions_col and contributions_col in df.columns:
        df = df.withColumn(
            contributions_col,
            F.when(unseen, F.lit(None).cast(MapType(StringType(), DoubleType()))).otherwise(F.col(contributions_col)),
        )
    if score_std_col and score_std_col in df.columns:
        df = df.withColumn(
            score_std_col, F.when(unseen, F.lit(None).cast(DoubleType())).otherwise(F.col(score_std_col))
        )
    return df


def permissive_quantile_points(
    group_quantile_points: dict[str, list[tuple[float, float]]],
    fallback_quantile_points: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    """Per-percentile *minimum* score bound across all groups.

    Used only to decide which rows are worth computing SHAP contributions for. Taking the
    minimum makes the gate deliberately permissive: a row that any group would consider
    anomalous passes it. That is safe because the observable contract is re-enforced downstream
    by ``add_info_column``, which masks contributions on severity against the real threshold —
    so a permissive gate can only cost a little wasted SHAP, while a strict one would silently
    drop contributions from genuinely anomalous rows in the groups with the widest score ranges.
    """
    if not group_quantile_points:
        return fallback_quantile_points

    minima: dict[float, float] = {}
    for points in group_quantile_points.values():
        for percentile, bound in points:
            existing = minima.get(percentile)
            minima[percentile] = bound if existing is None else min(existing, bound)

    for percentile, bound in fallback_quantile_points:
        minima.setdefault(percentile, bound)

    return sorted(minima.items())


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
        # max_by returns null when every score in the group is null, which would throw away the
        # info struct for rows that are deliberately unscored — an unseen group carries a null
        # score but still has is_new_baseline to report. Fall back to any non-null info in that case.
        F.coalesce(F.max_by(info_col, score_col), F.first(info_col, ignorenulls=True)).alias(info_col),
    ]
    scored_subset_unique = scored_subset.groupBy(*merge_columns).agg(*agg_exprs)

    return df.join(scored_subset_unique, on=merge_columns, how="left")


def apply_row_filter(df: DataFrame, row_filter: str | None) -> DataFrame:
    """Return only rows that match row_filter for scoring; if no filter, return df unchanged.

    row_filter is a SQL expression (e.g. \"region = 'US'\"). Only these rows are run
    through anomaly detection; elsewhere we join results back so output has same row count.
    """
    return df.filter(safe_filter_expr(row_filter)) if row_filter else df
