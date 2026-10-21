"""Orchestrates anomaly scoring: resolve the registered model and run the scoring pipeline."""

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_strategies import resolve_scoring_strategy
from databricks.labs.dqx.errors import InvalidParameterError


def run_anomaly_scoring(
    df_to_score: DataFrame,
    config: ScoringConfig,
    registry_table: str,
    model_name: str,
) -> DataFrame:
    """Score with the registered model and return the scored DataFrame (caller drops row_id_col).

    There is one model per registered name. The routing that used to choose between segmented and
    global scoring, and the fallback that looked for ``__seg_`` models when a name was not found,
    both went with the ``segment_by`` path: a grouping is now expressed as features on a single
    model.
    """
    registry_client = AnomalyModelRegistry(df_to_score.sparkSession)
    record = registry_client.get_active_model(registry_table, model_name)
    if not record:
        raise InvalidParameterError(
            f"Model '{model_name}' not found in '{registry_table}'. Train first using anomaly.train(...)."
        )
    strategy = resolve_scoring_strategy(record.identity.algorithm)
    return strategy.score_global(df_to_score, record, config)
