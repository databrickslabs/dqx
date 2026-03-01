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
    prepare_feature_metadata,
)
from databricks.labs.dqx.anomaly.model_loader import load_and_validate_model
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.scoring_utils import create_udf_schema
from databricks.labs.dqx.anomaly.explainability import (
    compute_shap_values,
    format_shap_contributions,
)


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
):
    """Create pandas UDF for distributed scoring with SHAP contributions."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_with_shap_udf(*cols: pd.Series) -> pd.DataFrame:
        model_local = cloudpickle.loads(model_bytes)
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols
        scores = -model_local.score_samples(feature_matrix)

        shap_values, valid_indices = compute_shap_values(
            model_local,
            feature_matrix,
            engineered_feature_cols,
        )
        contributions_list = format_shap_contributions(
            shap_values, valid_indices, len(feature_matrix), engineered_feature_cols
        )

        return pd.DataFrame({"anomaly_score": scores, "anomaly_contributions": contributions_list})

    return predict_with_shap_udf


def score_with_sklearn_model(
    model_uri: str,
    df: DataFrame,
    feature_cols: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool = False,
    *,
    model_record: AnomalyModelRecord,
) -> DataFrame:
    """Score DataFrame using scikit-learn model with distributed pandas UDF."""
    sklearn_model = load_and_validate_model(model_uri, model_record)
    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df = apply_feature_engineering_for_scoring(
        df, feature_cols, merge_columns, column_infos, feature_metadata
    )

    engineered_feature_cols = feature_metadata.engineered_feature_names
    model_bytes = cloudpickle.dumps(sklearn_model)

    schema = create_udf_schema(include_contributions)
    if include_contributions:
        predict_udf = create_scoring_udf_with_contributions(model_bytes, engineered_feature_cols, schema)
    else:
        predict_udf = create_scoring_udf(model_bytes, engineered_feature_cols, schema)

    scored_df = engineered_df.withColumn("_scores", predict_udf(*[col(c) for c in engineered_feature_cols]))

    cols_to_select = [*merge_columns, "_scores.anomaly_score"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    return df.join(scored_df.select(*cols_to_select), on=merge_columns, how="left")


def score_with_sklearn_model_local(
    model_uri: str,
    df: DataFrame,
    feature_cols: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool = False,
    *,
    model_record: AnomalyModelRecord,
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

    if include_contributions:
        shap_values, valid_indices = compute_shap_values(
            sklearn_model,
            feature_matrix,
            engineered_feature_cols,
        )
        result["anomaly_contributions"] = format_shap_contributions(
            shap_values, valid_indices, len(local_pdf), engineered_feature_cols
        )

    result_pdf = pd.DataFrame(result)
    result_schema = StructType(
        [
            *[df.schema[c] for c in merge_columns],
            StructField("anomaly_score", DoubleType(), True),
            *(
                [StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)]
                if include_contributions
                else []
            ),
        ]
    )
    scored_df = df.sparkSession.createDataFrame(result_pdf, schema=result_schema)
    return df.join(scored_df, on=merge_columns, how="left")
