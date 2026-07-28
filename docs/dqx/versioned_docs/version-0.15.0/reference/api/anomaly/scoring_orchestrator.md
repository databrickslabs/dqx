---
sidebar_label: scoring_orchestrator
title: databricks.labs.dqx.anomaly.scoring_orchestrator
---

Orchestrates anomaly scoring: route global vs segmented, run global model pipeline.

#### run\_anomaly\_scoring

```python
def run_anomaly_scoring(df_to_score: DataFrame, config: ScoringConfig,
                        registry_table: str, model_name: str) -> DataFrame
```

Route to segmented or global scoring and return scored DataFrame (caller drops row_id_col).

#### try\_segmented\_scoring\_fallback

```python
def try_segmented_scoring_fallback(
        df: DataFrame, config: ScoringConfig,
        registry_client: AnomalyModelRegistry) -> DataFrame | None
```

Try to score using segmented models as fallback. Returns None if no segments found.

