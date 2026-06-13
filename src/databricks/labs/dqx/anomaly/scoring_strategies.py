"""Scoring strategy interface and implementations for row anomaly models."""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_run import score_global_model, score_segmented
from databricks.labs.dqx.errors import InvalidParameterError


class AnomalyScoringStrategy(ABC):
    """Scoring strategy interface for row anomaly models.

    Implementations that bypass `score_global_model` / `score_segmented` must call
    `add_explanation_column` themselves when `config.enable_ai_explanation` is True;
    otherwise the `_dq_info.anomaly.ai_explanation` struct will always be null.
    """

    @abstractmethod
    def supports(self, algorithm: str) -> bool:
        """Return True if the strategy supports the given algorithm."""

    @abstractmethod
    def score_global(self, df: DataFrame, record: AnomalyModelRecord, config: ScoringConfig) -> DataFrame:
        """Score a global model."""

    @abstractmethod
    def score_segmented(
        self,
        df: DataFrame,
        config: ScoringConfig,
        registry_client: AnomalyModelRegistry,
        all_segments: list[AnomalyModelRecord],
    ) -> DataFrame:
        """Score a segmented model."""


class IsolationForestScoringStrategy(AnomalyScoringStrategy):
    """IsolationForest scoring strategy (default)."""

    def supports(self, algorithm: str) -> bool:
        return algorithm.startswith("IsolationForest")

    def score_global(self, df: DataFrame, record: AnomalyModelRecord, config: ScoringConfig) -> DataFrame:
        return score_global_model(df, record, config)

    def score_segmented(
        self,
        df: DataFrame,
        config: ScoringConfig,
        registry_client: AnomalyModelRegistry,
        all_segments: list[AnomalyModelRecord],
    ) -> DataFrame:
        return score_segmented(df, config, registry_client, all_segments)


_SCORING_STRATEGIES: list[AnomalyScoringStrategy] = [IsolationForestScoringStrategy()]


def resolve_scoring_strategy(algorithm: str) -> AnomalyScoringStrategy:
    """Return the first strategy that supports the given algorithm."""
    for strategy in _SCORING_STRATEGIES:
        if strategy.supports(algorithm):
            return strategy
    raise InvalidParameterError(
        f"Unsupported model algorithm '{algorithm}'. Add a scoring strategy for this algorithm."
    )
