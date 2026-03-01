"""Ensemble anomaly scoring (distributed UDF and driver-local)."""

from typing import cast

import cloudpickle
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.dqx.anomaly.feature_prep import (
    apply_feature_engineering_for_scoring,
    prepare_feature_metadata,
)
from databricks.labs.dqx.anomaly.model_loader import load_and_validate_model
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.shap_contributions import (
    compute_shap_values,
    format_shap_contributions,
)


def serialize_ensemble_models(
    model_uris: list[str],
    model_record: AnomalyModelRecord,
) -> list[bytes]:
    """Load and serialize ensemble models for UDF."""
    models_bytes = []
    for uri in model_uris:
        model = load_and_validate_model(uri, model_record)
        models_bytes.append(cloudpickle.dumps(model))
    return models_bytes


def prepare_ensemble_scoring_schema(include_contributions: bool) -> StructType:
    """Prepare schema for ensemble scoring UDF."""
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
        StructField("anomaly_score_std", DoubleType(), True),
    ]
    if include_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    return StructType(schema_fields)


def join_ensemble_scores(
    df_filtered: DataFrame,
    scored_df: DataFrame,
    merge_columns: list[str],
    include_contributions: bool,
) -> DataFrame:
    """Join scores back to original DataFrame."""
    cols_to_select = [*merge_columns, "_scores.anomaly_score", "_scores.anomaly_score_std"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    return df_filtered.join(scored_df.select(*cols_to_select), on=merge_columns, how="left")


def create_ensemble_scoring_udf(
    models_bytes: list[bytes],
    engineered_feature_cols: list[str],
    schema: StructType,
):
    """Create ensemble scoring UDF."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def ensemble_scoring_udf(*cols: pd.Series) -> pd.DataFrame:
        models = [cloudpickle.loads(mb) for mb in models_bytes]
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])
        mean_scores = scores_matrix.mean(axis=0)
        std_scores = scores_matrix.std(axis=0, ddof=1)

        return pd.DataFrame({"anomaly_score": mean_scores, "anomaly_score_std": std_scores})

    return ensemble_scoring_udf


def create_ensemble_scoring_udf_with_contributions(
    models_bytes: list[bytes],
    engineered_feature_cols: list[str],
    schema: StructType,
):
    """Create ensemble scoring UDF with SHAP contributions."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def ensemble_scoring_udf(*cols: pd.Series) -> pd.DataFrame:
        models = [cloudpickle.loads(mb) for mb in models_bytes]
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])
        mean_scores = scores_matrix.mean(axis=0)
        std_scores = scores_matrix.std(axis=0, ddof=1)

        result = {"anomaly_score": mean_scores, "anomaly_score_std": std_scores}

        model_local = models[0]
        shap_values, valid_indices = compute_shap_values(
            model_local,
            feature_matrix,
            engineered_feature_cols,
        )
        result["anomaly_contributions"] = format_shap_contributions(
            shap_values, valid_indices, len(feature_matrix), engineered_feature_cols
        )

        return pd.DataFrame(result)

    return ensemble_scoring_udf


def score_ensemble_models(
    model_uris: list[str],
    df_filtered: DataFrame,
    columns: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool,
    *,
    model_record: AnomalyModelRecord,
) -> DataFrame:
    """Score DataFrame with multiple ensemble models and compute statistics."""
    models_bytes = serialize_ensemble_models(model_uris, model_record)

    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df = apply_feature_engineering_for_scoring(
        df_filtered, columns, merge_columns, column_infos, feature_metadata
    )
    engineered_feature_cols = feature_metadata.engineered_feature_names

    schema = prepare_ensemble_scoring_schema(include_contributions)
    if include_contributions:
        ensemble_scoring_udf = create_ensemble_scoring_udf_with_contributions(
            models_bytes, engineered_feature_cols, schema
        )
    else:
        ensemble_scoring_udf = create_ensemble_scoring_udf(models_bytes, engineered_feature_cols, schema)

    input_cols = [col(c) for c in engineered_feature_cols]
    scored_df = engineered_df.withColumn("_scores", ensemble_scoring_udf(*input_cols))

    return join_ensemble_scores(df_filtered, scored_df, merge_columns, include_contributions)


def score_ensemble_models_local(
    model_uris: list[str],
    df_filtered: DataFrame,
    columns: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool,
    *,
    model_record: AnomalyModelRecord,
) -> DataFrame:
    """Score ensemble models locally on the driver."""
    models = [load_and_validate_model(uri, model_record) for uri in model_uris]
    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df = apply_feature_engineering_for_scoring(
        df_filtered, columns, merge_columns, column_infos, feature_metadata
    )
    engineered_feature_cols = feature_metadata.engineered_feature_names
    local_pdf = cast(pd.DataFrame, engineered_df.select(*merge_columns, *engineered_feature_cols).toPandas())

    feature_matrix = local_pdf[engineered_feature_cols]
    scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])

    result = {col_name: local_pdf[col_name] for col_name in merge_columns}
    result["anomaly_score"] = scores_matrix.mean(axis=0)
    result["anomaly_score_std"] = scores_matrix.std(axis=0, ddof=1)

    if include_contributions:
        shap_values, valid_indices = compute_shap_values(
            models[0],
            feature_matrix,
            engineered_feature_cols,
        )
        result["anomaly_contributions"] = format_shap_contributions(
            shap_values, valid_indices, len(local_pdf), engineered_feature_cols
        )

    result_pdf = pd.DataFrame(result)
    result_schema = StructType(
        [
            *[df_filtered.schema[c] for c in merge_columns],
            StructField("anomaly_score", DoubleType(), True),
            StructField("anomaly_score_std", DoubleType(), True),
            *(
                [StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)]
                if include_contributions
                else []
            ),
        ]
    )
    scored_df = df_filtered.sparkSession.createDataFrame(result_pdf, schema=result_schema)
    return df_filtered.join(scored_df, on=merge_columns, how="left")
