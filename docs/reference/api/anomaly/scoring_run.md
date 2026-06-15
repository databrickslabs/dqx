# databricks.labs.dqx.anomaly.scoring\_run

Global and segmented anomaly model scoring.

Provides score\_global\_model, score\_segmented, and load\_segment\_models. Kept in one module to avoid over-fragmentation of the scoring layer.

### score\_global\_model[​](#score_global_model "Direct link to score_global_model")

```python
def score_global_model(df: DataFrame, record: AnomalyModelRecord,
                       config: ScoringConfig) -> DataFrame

```

Score using a global (non-segmented) model.

### load\_segment\_models[​](#load_segment_models "Direct link to load_segment_models")

```python
def load_segment_models(registry_client: AnomalyModelRegistry,
                        config: ScoringConfig) -> list[AnomalyModelRecord]

```

Load all segment models for a base model from the registry.

### score\_single\_segment[​](#score_single_segment "Direct link to score_single_segment")

```python
def score_single_segment(segment_df: DataFrame,
                         segment_model: AnomalyModelRecord,
                         config: ScoringConfig,
                         max_groups_override: int | None = None,
                         endpoint_reachable: bool | None = None) -> DataFrame

```

Score a single segment with its specific model.

*max\_groups\_override*, when set, replaces *config.max\_groups* in the ExplanationContext for this segment only. Used by *score\_segmented* to enforce a *global* cap on LLM calls across segments — without it, *config.max\_groups* applies independently per segment and the worst-case total is `num_segments * max_groups`.

*endpoint\_reachable* is the serving-endpoint reachability probed once by *score\_segmented* for the whole run; threading it through avoids one billable `ai_query` probe per segment.

### score\_segmented[​](#score_segmented "Direct link to score_segmented")

```python
def score_segmented(
        df: DataFrame,
        config: ScoringConfig,
        registry_client: AnomalyModelRegistry,
        all_segments: list[AnomalyModelRecord] | None = None) -> DataFrame

```

Score DataFrame using segment-specific models.
