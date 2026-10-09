"""Ensemble anomaly scoring (distributed UDF and driver-local)."""

import uuid

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
    apply_feature_engineering_with_row_passthrough,
    prepare_feature_metadata,
    collect_feature_matrix,
)
from databricks.labs.dqx.anomaly.model_loader import load_and_validate_model
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.explainability import AttributionGate, compute_gated_shap_contributions
from databricks.labs.dqx.anomaly.feature_naming import AttributionKeys
from databricks.labs.dqx.anomaly.scoring_config import ScoringOutputColumns
from databricks.labs.dqx.check_funcs import join_results_on_null_safe_columns
from databricks.labs.dqx.utils import quote_column_name


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


def prepare_ensemble_scoring_schema(enable_contributions: bool) -> StructType:
    """Prepare schema for ensemble scoring UDF."""
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
        StructField("anomaly_score_std", DoubleType(), True),
    ]
    if enable_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
        # Emitted alongside, for the same reason as in scoring_utils.create_udf_schema: the basis split is
        # derived from the attribution enable_contributions already pays for.
        schema_fields.append(StructField("anomaly_basis_contributions", MapType(StringType(), DoubleType()), True))
    return StructType(schema_fields)


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
    quantile_points: list[tuple[float, float]] | None = None,
    threshold: float | None = None,
    keys: AttributionKeys | None = None,
    gate: AttributionGate | None = None,
):
    """Create ensemble scoring UDF with feature contributions.

    When *quantile_points* and *threshold* are provided, attribution runs only for rows whose
    mean-score severity reaches the threshold; other rows get a null contributions map.

    Contributions are the mean across every member, matching the score and *anomaly_score_std*, which are
    also aggregates over all of them. Explaining one member while scoring with all of them made the
    explanation depend on which member happened to be trained first.
    """

    @pandas_udf(schema)  # type: ignore[call-overload]
    def ensemble_scoring_udf(*cols: pd.Series) -> pd.DataFrame:
        models = [cloudpickle.loads(mb) for mb in models_bytes]
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])
        mean_scores = scores_matrix.mean(axis=0)
        std_scores = scores_matrix.std(axis=0, ddof=1)

        contributions = compute_gated_shap_contributions(
            models,
            feature_matrix,
            engineered_feature_cols,
            mean_scores,
            quantile_points,
            threshold,
            keys,
            gate,
        )
        result = {
            "anomaly_score": mean_scores,
            "anomaly_score_std": std_scores,
            **contributions.as_columns(),
        }

        return pd.DataFrame(result)

    return ensemble_scoring_udf


def score_ensemble_models(
    model_uris: list[str],
    df_filtered: DataFrame,
    columns: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    enable_contributions: bool,
    *,
    model_record: AnomalyModelRecord,
    quantile_points: list[tuple[float, float]] | None = None,
    threshold: float | None = None,
    output_columns: ScoringOutputColumns | None = None,
    gate: AttributionGate | None = None,
) -> DataFrame:
    """Score DataFrame with multiple ensemble models and compute statistics.

    The original row rides through feature engineering inside a struct column and is
    restored after scoring, so scores are attached in the same pass — no join back onto
    the caller's DataFrame.
    """
    models_bytes = serialize_ensemble_models(model_uris, model_record)

    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df, original_row_col = apply_feature_engineering_with_row_passthrough(
        df_filtered, columns, merge_columns, column_infos, feature_metadata
    )
    engineered_feature_cols = feature_metadata.engineered_feature_names

    schema = prepare_ensemble_scoring_schema(enable_contributions)
    if enable_contributions:
        # Blocks are a pure function of the persisted metadata, so they are built once on the driver and
        # closed over rather than rebuilt per partition -- the same reasoning as the single-model scorer.
        ensemble_scoring_udf = create_ensemble_scoring_udf_with_contributions(
            models_bytes,
            engineered_feature_cols,
            schema,
            quantile_points,
            threshold,
            AttributionKeys.from_metadata(feature_metadata),
            gate,
        )
    else:
        ensemble_scoring_udf = create_ensemble_scoring_udf(models_bytes, engineered_feature_cols, schema)

    input_cols = [col(quote_column_name(c)) for c in engineered_feature_cols]
    scores_col = f"__dqx_scores_{uuid.uuid4().hex}"
    scored_df = engineered_df.withColumn(scores_col, ensemble_scoring_udf(*input_cols))
    aliases = (output_columns or ScoringOutputColumns()).result_aliases(enable_contributions, include_std=True)
    return scored_df.select(
        f"{original_row_col}.*", *[col(f"{scores_col}.{name}").alias(alias) for name, alias in aliases.items()]
    )


def score_ensemble_models_local(
    model_uris: list[str],
    df_filtered: DataFrame,
    columns: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    enable_contributions: bool,
    *,
    model_record: AnomalyModelRecord,
    quantile_points: list[tuple[float, float]] | None = None,
    threshold: float | None = None,
    output_columns: ScoringOutputColumns | None = None,
    gate: AttributionGate | None = None,
) -> DataFrame:
    """Score ensemble models locally on the driver."""
    models = [load_and_validate_model(uri, model_record) for uri in model_uris]
    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df = apply_feature_engineering_for_scoring(
        df_filtered, columns, merge_columns, column_infos, feature_metadata
    )
    engineered_feature_cols = feature_metadata.engineered_feature_names
    local_pdf = collect_feature_matrix(engineered_df, [*merge_columns, *engineered_feature_cols])

    feature_matrix = local_pdf[engineered_feature_cols]
    scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])
    mean_scores = scores_matrix.mean(axis=0)

    result = {col_name: local_pdf[col_name] for col_name in merge_columns}
    result["anomaly_score"] = mean_scores
    result["anomaly_score_std"] = scores_matrix.std(axis=0, ddof=1)

    if enable_contributions:
        contributions = compute_gated_shap_contributions(
            models,
            feature_matrix,
            engineered_feature_cols,
            mean_scores,
            quantile_points,
            threshold,
            AttributionKeys.from_metadata(feature_metadata),
            gate,
        )
        result.update(contributions.as_columns())

    scored_df = df_filtered.sparkSession.createDataFrame(
        pd.DataFrame(result),
        schema=StructType(
            [
                *[df_filtered.schema[c] for c in merge_columns],
                *prepare_ensemble_scoring_schema(enable_contributions).fields,
            ]
        ),
    )
    aliases = (output_columns or ScoringOutputColumns()).result_aliases(enable_contributions, include_std=True)
    scored_df = scored_df.select(*merge_columns, *[col(name).alias(alias) for name, alias in aliases.items()])
    return join_results_on_null_safe_columns(df_filtered, scored_df, merge_columns, list(aliases.values()))
