"""Single-model anomaly scoring (distributed UDF and driver-local)."""

from typing import cast

import cloudpickle
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
    apply_feature_engineering_with_row_passthrough,
    prepare_feature_metadata,
)
from databricks.labs.dqx.anomaly.model_loader import load_and_validate_model
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.scoring_utils import create_udf_schema
from databricks.labs.dqx.anomaly.explainability import compute_gated_shap_contributions


def create_scoring_udf(
    model_bytes: bytes,
    engineered_feature_cols: list[str],
    schema: StructType,
):
    """Create pandas UDF for distributed scoring."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_udf(*cols: pd.Series) -> pd.DataFrame:
        model_local = cloudpickle.loads(model_bytes)
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols
        scores = -model_local.score_samples(feature_matrix)
        return pd.DataFrame({"anomaly_score": scores})

    return predict_udf


def create_scoring_udf_with_contributions(
    model_bytes: bytes,
    engineered_feature_cols: list[str],
    schema: StructType,
    quantile_points: list[tuple[float, float]] | None = None,
    threshold: float | None = None,
):
    """Create pandas UDF for distributed scoring with SHAP contributions.

    When *quantile_points* and *threshold* are provided, SHAP runs only for rows whose
    severity reaches the threshold (contributions are only surfaced for anomalous rows);
    other rows get a null contributions map.
    """

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_with_shap_udf(*cols: pd.Series) -> pd.DataFrame:
        model_local = cloudpickle.loads(model_bytes)
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols
        scores = -model_local.score_samples(feature_matrix)

        contributions_list = compute_gated_shap_contributions(
            model_local,
            feature_matrix,
            engineered_feature_cols,
            scores,
            quantile_points,
            threshold,
        )

        return pd.DataFrame({"anomaly_score": scores, "anomaly_contributions": contributions_list})

    return predict_with_shap_udf


def score_with_sklearn_model(
    model_uri: str,
    df: DataFrame,
    feature_cols: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    enable_contributions: bool = False,
    *,
    model_record: AnomalyModelRecord,
    quantile_points: list[tuple[float, float]] | None = None,
    threshold: float | None = None,
) -> DataFrame:
    """Score DataFrame using scikit-learn model with distributed pandas UDF.

    The original row rides through feature engineering inside a struct column and is
    restored after scoring, so scores are attached in the same pass — no join back onto
    the caller's DataFrame (which would recompute the source and shuffle on the row id).
    """
    sklearn_model = load_and_validate_model(model_uri, model_record)
    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df, original_row_col = apply_feature_engineering_with_row_passthrough(
        df, feature_cols, merge_columns, column_infos, feature_metadata
    )

    engineered_feature_cols = feature_metadata.engineered_feature_names
    model_bytes = cloudpickle.dumps(sklearn_model)

    schema = create_udf_schema(enable_contributions)
    if enable_contributions:
        predict_udf = create_scoring_udf_with_contributions(
            model_bytes, engineered_feature_cols, schema, quantile_points, threshold
        )
    else:
        predict_udf = create_scoring_udf(model_bytes, engineered_feature_cols, schema)

    scored_df = engineered_df.withColumn("_scores", predict_udf(*[col(c) for c in engineered_feature_cols]))

    cols_to_select = [f"{original_row_col}.*", "_scores.anomaly_score"]
    if enable_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    return scored_df.select(*cols_to_select)


def score_with_sklearn_model_local(
    model_uri: str,
    df: DataFrame,
    feature_cols: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    enable_contributions: bool = False,
    *,
    model_record: AnomalyModelRecord,
    quantile_points: list[tuple[float, float]] | None = None,
    threshold: float | None = None,
) -> DataFrame:
    """Score DataFrame using scikit-learn model locally on the driver."""
    sklearn_model = load_and_validate_model(model_uri, model_record)
    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df = apply_feature_engineering_for_scoring(
        df, feature_cols, merge_columns, column_infos, feature_metadata
    )

    engineered_feature_cols = feature_metadata.engineered_feature_names
    local_pdf = cast(pd.DataFrame, engineered_df.select(*merge_columns, *engineered_feature_cols).toPandas())

    feature_matrix = local_pdf[engineered_feature_cols]
    scores = -sklearn_model.score_samples(feature_matrix)

    result = {col_name: local_pdf[col_name] for col_name in merge_columns}
    result["anomaly_score"] = scores

    if enable_contributions:
        result["anomaly_contributions"] = compute_gated_shap_contributions(
            sklearn_model,
            feature_matrix,
            engineered_feature_cols,
            scores,
            quantile_points,
            threshold,
        )

    result_pdf = pd.DataFrame(result)
    result_schema = StructType(
        [
            *[df.schema[c] for c in merge_columns],
            StructField("anomaly_score", DoubleType(), True),
            *(
                [StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)]
                if enable_contributions
                else []
            ),
        ]
    )
    scored_df = df.sparkSession.createDataFrame(result_pdf, schema=result_schema)
    return df.join(scored_df, on=merge_columns, how="left")
