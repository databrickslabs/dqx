"""Prepare feature metadata and apply feature engineering for anomaly scoring."""

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
) -> DataFrame:
    """Apply feature engineering to DataFrame for scoring.

    Note: the internal row identifier must exist in the DataFrame as it is required for
    joining results back in row_filter cases.
    """
    missing_cols = [c for c in merge_columns if c not in df.columns]
    if missing_cols:
        raise InvalidParameterError(
            f"Internal row identifier {missing_cols} not found in DataFrame. "
            f"Available columns: {df.columns}. "
            "Ensure the anomaly check is applied to the same DataFrame instance."
        )

    cols_to_select = list(dict.fromkeys([*feature_cols, *merge_columns]))

    engineered_df, _ = apply_feature_engineering(
        df.select(*cols_to_select),
        column_infos,
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    return engineered_df
