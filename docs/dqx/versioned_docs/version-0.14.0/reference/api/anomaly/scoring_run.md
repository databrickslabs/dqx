---
sidebar_label: scoring_run
title: databricks.labs.dqx.anomaly.scoring_run
---

Global and segmented anomaly model scoring.

Provides score_global_model, score_segmented, and load_segment_models.
Kept in one module to avoid over-fragmentation of the scoring layer.

#### score\_global\_model

```python
def score_global_model(df: DataFrame, record: AnomalyModelRecord,
                       config: ScoringConfig) -> DataFrame
```

Score using a global (non-segmented) model.

#### load\_segment\_models

```python
def load_segment_models(registry_client: AnomalyModelRegistry,
                        config: ScoringConfig) -> list[AnomalyModelRecord]
```

Load all segment models for a base model from the registry.

#### score\_single\_segment

```python
def score_single_segment(segment_df: DataFrame,
                         segment_model: AnomalyModelRecord,
                         config: ScoringConfig) -> DataFrame
```

Score a single segment with its specific model.

#### score\_segmented

```python
def score_segmented(
        df: DataFrame,
        config: ScoringConfig,
        registry_client: AnomalyModelRegistry,
        all_segments: list[AnomalyModelRecord] | None = None) -> DataFrame
```

Score DataFrame using segment-specific models.

