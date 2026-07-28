---
sidebar_label: scoring_strategies
title: databricks.labs.dqx.anomaly.scoring_strategies
---

Scoring strategy interface and implementations for row anomaly models.

## AnomalyScoringStrategy Objects

```python
class AnomalyScoringStrategy(ABC)
```

Scoring strategy interface for row anomaly models.

Implementations that bypass `score_global_model` / `score_segmented` must call
`add_explanation_column` themselves when `config.enable_ai_explanation` is True;
otherwise the `_dq_info.anomaly.ai_explanation` struct will always be null.

#### supports

```python
@abstractmethod
def supports(algorithm: str) -> bool
```

Return True if the strategy supports the given algorithm.

#### score\_global

```python
@abstractmethod
def score_global(df: DataFrame, record: AnomalyModelRecord,
                 config: ScoringConfig) -> DataFrame
```

Score a global model.

#### score\_segmented

```python
@abstractmethod
def score_segmented(df: DataFrame, config: ScoringConfig,
                    registry_client: AnomalyModelRegistry,
                    all_segments: list[AnomalyModelRecord]) -> DataFrame
```

Score a segmented model.

## IsolationForestScoringStrategy Objects

```python
class IsolationForestScoringStrategy(AnomalyScoringStrategy)
```

IsolationForest scoring strategy (default).

#### resolve\_scoring\_strategy

```python
def resolve_scoring_strategy(algorithm: str) -> AnomalyScoringStrategy
```

Return the first strategy that supports the given algorithm.

