"""Prepare feature metadata and apply feature engineering for anomaly scoring."""

import uuid

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    SparkFeatureMetadata,
    apply_feature_engineering,
    reconstruct_column_infos,
)
from databricks.labs.dqx.errors import InvalidParameterError


def prepare_feature_metadata(feature_metadata_json: str) -> tuple[list[ColumnTypeInfo], SparkFeatureMetadata]:
    """Load and prepare feature metadata from JSON."""
    feature_metadata = SparkFeatureMetadata.from_json(feature_metadata_json)
    column_infos = reconstruct_column_infos(feature_metadata)
    return column_infos, feature_metadata


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

    cols_to_select = list(dict.fromkeys([*feature_cols, *merge_columns, *(passthrough_columns or [])]))

    engineered_df, _ = apply_feature_engineering(
        df.select(*cols_to_select),
        column_infos,
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
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
