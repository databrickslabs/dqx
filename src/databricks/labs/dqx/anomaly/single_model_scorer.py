"""Single-model anomaly scoring (distributed UDF and driver-local)."""

import uuid

import cloudpickle
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StructType

from databricks.labs.dqx.anomaly.feature_prep import (
    apply_feature_engineering_for_scoring,
    apply_feature_engineering_with_row_passthrough,
    prepare_feature_metadata,
    collect_feature_matrix,
)
from databricks.labs.dqx.anomaly.model_loader import load_and_validate_model
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.scoring_utils import create_udf_schema
from databricks.labs.dqx.anomaly.scoring_config import ScoringOutputColumns
from databricks.labs.dqx.anomaly.explainability import AttributionGate, compute_gated_shap_contributions
from databricks.labs.dqx.anomaly.feature_naming import AttributionKeys
from databricks.labs.dqx.check_funcs import join_results_on_null_safe_columns
from databricks.labs.dqx.utils import quote_column_name


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
    keys: AttributionKeys | None = None,
    gate: AttributionGate | None = None,
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

        contributions = compute_gated_shap_contributions(
            [model_local],
            feature_matrix,
            engineered_feature_cols,
            scores,
            quantile_points,
            threshold,
            keys,
            gate,
        )

        return pd.DataFrame({"anomaly_score": scores, **contributions.as_columns()})

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
    output_columns: ScoringOutputColumns | None = None,
    gate: AttributionGate | None = None,
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
        # Blocks are a pure function of the persisted metadata, so they are built once here and closed
        # over rather than rebuilt per partition.
        predict_udf = create_scoring_udf_with_contributions(
            model_bytes,
            engineered_feature_cols,
            schema,
            quantile_points=quantile_points,
            threshold=threshold,
            keys=AttributionKeys.from_metadata(feature_metadata),
            gate=gate,
        )
    else:
        predict_udf = create_scoring_udf(model_bytes, engineered_feature_cols, schema)

    scores_col = f"__dqx_scores_{uuid.uuid4().hex}"
    scored_df = engineered_df.withColumn(
        scores_col, predict_udf(*[col(quote_column_name(c)) for c in engineered_feature_cols])
    )
    aliases = (output_columns or ScoringOutputColumns()).result_aliases(enable_contributions)
    return scored_df.select(
        f"{original_row_col}.*", *[col(f"{scores_col}.{name}").alias(alias) for name, alias in aliases.items()]
    )


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
    output_columns: ScoringOutputColumns | None = None,
    gate: AttributionGate | None = None,
) -> DataFrame:
    """Score DataFrame using scikit-learn model locally on the driver."""
    sklearn_model = load_and_validate_model(model_uri, model_record)
    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df = apply_feature_engineering_for_scoring(
        df, feature_cols, merge_columns, column_infos, feature_metadata
    )

    engineered_feature_cols = feature_metadata.engineered_feature_names
    local_pdf = collect_feature_matrix(engineered_df, [*merge_columns, *engineered_feature_cols])

    feature_matrix = local_pdf[engineered_feature_cols]
    scores = -sklearn_model.score_samples(feature_matrix)

    result = {col_name: local_pdf[col_name] for col_name in merge_columns}
    result["anomaly_score"] = scores

    if enable_contributions:
        contributions = compute_gated_shap_contributions(
            [sklearn_model],
            feature_matrix,
            engineered_feature_cols,
            scores,
            quantile_points=quantile_points,
            threshold=threshold,
            keys=AttributionKeys.from_metadata(feature_metadata),
            gate=gate,
        )
        result.update(contributions.as_columns())

    scored_df = df.sparkSession.createDataFrame(
        pd.DataFrame(result),
        schema=StructType([*[df.schema[c] for c in merge_columns], *create_udf_schema(enable_contributions).fields]),
    )
    aliases = (output_columns or ScoringOutputColumns()).result_aliases(enable_contributions)
    scored_df = scored_df.select(*merge_columns, *[col(name).alias(alias) for name, alias in aliases.items()])
    return join_results_on_null_safe_columns(df, scored_df, merge_columns, list(aliases.values()))
