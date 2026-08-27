"""Scoring strategy interface and implementations for row anomaly models."""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_run import score_global_model
from databricks.labs.dqx.errors import InvalidParameterError


class AnomalyScoringStrategy(ABC):
    """Scoring strategy interface for row anomaly models.

    Implementations that bypass `score_global_model` must call `add_explanation_column` themselves
    when `config.enable_ai_explanation` is True; otherwise the `_dq_info.anomaly.ai_explanation`
    struct will always be null.
    """

    @abstractmethod
    def supports(self, algorithm: str) -> bool:
        """Return True if the strategy supports the given algorithm."""

    @abstractmethod
    def score_global(self, df: DataFrame, record: AnomalyModelRecord, config: ScoringConfig) -> DataFrame:
        """Score the model."""


# Algorithms scored by one pooled sklearn-compatible model. All of them reach the model through
# ``score_samples`` and are therefore indistinguishable to the scoring path — the estimator differs,
# the scoring mechanics do not. Existing registry rows carry "IsolationForest" or
# "IsolationForest_Ensemble_N", so prefix matching keeps them resolving unchanged.
_GLOBAL_ALGORITHM_PREFIXES = ("IsolationForest", "Mahalanobis")


class GlobalModelScoringStrategy(AnomalyScoringStrategy):
    """Scoring for any algorithm represented by a single pooled model.

    One class rather than one per algorithm on purpose: a second class whose body was also
    ``return score_global_model(...)`` would read as design while testing as duplication. The strategy
    interface still earns its place for a future path that genuinely scores differently.
    """

    def supports(self, algorithm: str) -> bool:
        return algorithm.startswith(_GLOBAL_ALGORITHM_PREFIXES)

    def score_global(self, df: DataFrame, record: AnomalyModelRecord, config: ScoringConfig) -> DataFrame:
        return score_global_model(df, record, config)


_SCORING_STRATEGIES: list[AnomalyScoringStrategy] = [GlobalModelScoringStrategy()]


def resolve_scoring_strategy(algorithm: str) -> AnomalyScoringStrategy:
    """Return the first strategy that supports the given algorithm."""
    for strategy in _SCORING_STRATEGIES:
        if strategy.supports(algorithm):
            return strategy
    raise InvalidParameterError(
        f"Unsupported model algorithm '{algorithm}'. Add a scoring strategy for this algorithm."
    )
