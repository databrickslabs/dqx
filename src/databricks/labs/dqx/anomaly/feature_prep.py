"""Prepare feature metadata and apply feature engineering for anomaly scoring."""

import collections.abc
import uuid

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    SparkFeatureMetadata,
    apply_feature_engineering_from_metadata,
    reconstruct_column_infos,
)
from databricks.labs.dqx.errors import InvalidParameterError


def prepare_feature_metadata(feature_metadata_json: str) -> tuple[list[ColumnTypeInfo], SparkFeatureMetadata]:
    """Load and prepare feature metadata from JSON."""
    feature_metadata = SparkFeatureMetadata.from_json(feature_metadata_json)
    column_infos = reconstruct_column_infos(feature_metadata)
    return column_infos, feature_metadata


def scoring_input_columns(
    feature_cols: collections.abc.Iterable[str],
    merge_columns: collections.abc.Iterable[str],
    feature_metadata: SparkFeatureMetadata,
    passthrough_columns: collections.abc.Iterable[str] | None = None,
) -> list[str]:
    """The columns the scoring transform must be handed, in the order it expects them.

    A comparison basis is not a feature, but it is still *read*: the grouping columns say what a metric is
    compared against and the time column is the axis it is measured along, so both have to survive the
    narrowing even though feature engineering drops them again before the model sees anything. Leaving
    either off does not degrade the score, it fails the query outright on an unresolved column.

    Deduplicated while preserving order, since a column may legitimately appear in more than one role.
    """
    time_cols = [feature_metadata.baseline_over_time] if feature_metadata.baseline_over_time else []
    return list(
        dict.fromkeys(
            [
                *feature_cols,
                *feature_metadata.baseline_by,
                *time_cols,
                *merge_columns,
                *(passthrough_columns or []),
            ]
        )
    )


def apply_feature_engineering_for_scoring(
    df: DataFrame,
    feature_cols: list[str],
    merge_columns: list[str],
    column_infos: list[ColumnTypeInfo],
    feature_metadata: SparkFeatureMetadata,
    passthrough_columns: list[str] | None = None,
) -> DataFrame:
    """Apply feature engineering to DataFrame for scoring.

    Note: the internal row identifier must exist in the DataFrame as it is required for
    joining results back in row_filter cases. *passthrough_columns* are carried through
    the transformation untouched (feature engineering preserves columns it does not know
    about).
    """
    missing_cols = [c for c in merge_columns if c not in df.columns]
    if missing_cols:
        raise InvalidParameterError(
            f"Internal row identifier {missing_cols} not found in DataFrame. "
            f"Available columns: {df.columns}. "
            "Ensure the anomaly check is applied to the same DataFrame instance."
        )

    cols_to_select = scoring_input_columns(feature_cols, merge_columns, feature_metadata, passthrough_columns)

    engineered_df, _ = apply_feature_engineering_from_metadata(
        df.select(*cols_to_select), feature_metadata, column_infos=column_infos
    )

    return engineered_df


def apply_feature_engineering_with_row_passthrough(
    df: DataFrame,
    feature_cols: list[str],
    merge_columns: list[str],
    column_infos: list[ColumnTypeInfo],
    feature_metadata: SparkFeatureMetadata,
) -> tuple[DataFrame, str]:
    """Apply feature engineering while carrying every original column through unchanged.

    Feature engineering mutates feature columns in place (imputation, encodings) and drops
    some of them (e.g. datetime), so scorers used to re-join scores onto the caller's
    DataFrame to restore the original rows — recomputing the source a second time and
    shuffling on a non-deterministic row id. Instead, pack the pristine original row into a
    collision-proof struct column that rides through the transformation untouched; after
    scoring, selecting ``<struct>.*`` restores the exact original columns without a join.

    Returns:
        The engineered DataFrame and the name of the struct column holding the original row.
    """
    original_row_col = f"__dqx_orig_{uuid.uuid4().hex}"
    packed = df.withColumn(original_row_col, F.struct(*[F.col(c) for c in df.columns]))
    engineered_df = apply_feature_engineering_for_scoring(
        packed,
        feature_cols,
        merge_columns,
        column_infos,
        feature_metadata,
        passthrough_columns=[original_row_col],
    )
    return engineered_df, original_row_col
