# databricks.labs.dqx.anomaly.scoring\_strategies

Scoring strategy interface and implementations for row anomaly models.

## AnomalyScoringStrategy Objects[​](#anomalyscoringstrategy-objects "Direct link to AnomalyScoringStrategy Objects")

```python
class AnomalyScoringStrategy(ABC)

```

Scoring strategy interface for row anomaly models.

### supports[​](#supports "Direct link to supports")

```python
@abstractmethod
def supports(algorithm: str) -> bool

```

Return True if the strategy supports the given algorithm.

### score\_global[​](#score_global "Direct link to score_global")

```python
@abstractmethod
def score_global(df: DataFrame, record: AnomalyModelRecord,
                 config: ScoringConfig) -> DataFrame

```

Score a global model.

### score\_segmented[​](#score_segmented "Direct link to score_segmented")

```python
@abstractmethod
def score_segmented(df: DataFrame, config: ScoringConfig,
                    registry_client: AnomalyModelRegistry,
                    all_segments: list[AnomalyModelRecord]) -> DataFrame

```

Score a segmented model.

## IsolationForestScoringStrategy Objects[​](#isolationforestscoringstrategy-objects "Direct link to IsolationForestScoringStrategy Objects")

```python
class IsolationForestScoringStrategy(AnomalyScoringStrategy)

```

IsolationForest scoring strategy (default).

### resolve\_scoring\_strategy[​](#resolve_scoring_strategy "Direct link to resolve_scoring_strategy")

```python
def resolve_scoring_strategy(algorithm: str) -> AnomalyScoringStrategy

```

Return the first strategy that supports the given algorithm.
