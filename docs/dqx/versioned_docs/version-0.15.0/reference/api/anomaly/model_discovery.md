---
sidebar_label: model_discovery
title: databricks.labs.dqx.anomaly.model_discovery
---

Discover model columns, segments, and quantile points from the anomaly registry.

#### get\_record\_for\_discovery

```python
def get_record_for_discovery(registry_client: AnomalyModelRegistry,
                             registry_table: str,
                             model_name_local: str) -> AnomalyModelRecord
```

Get model record for auto-discovery, checking global and segmented models.

#### select\_segment\_record

```python
def select_segment_record(
        all_segments: list[AnomalyModelRecord]) -> AnomalyModelRecord
```

Select a deterministic segment record (latest training_time, tie-breaker by model_name).

#### get\_quantile\_points\_for\_severity

```python
def get_quantile_points_for_severity(
        record: AnomalyModelRecord) -> list[tuple[float, float]]
```

Extract percentile-&gt;score points for severity mapping.

Used internally for scoring and exposed for testing and advanced use.

#### extract\_quantile\_points

```python
def extract_quantile_points(
        record: AnomalyModelRecord) -> list[tuple[float, float]]
```

Extract percentile-&gt;score points for severity mapping.

#### fetch\_model\_columns\_and\_segments

```python
def fetch_model_columns_and_segments(
        df: DataFrame, model_name: str,
        registry_table: str) -> tuple[list[str], list[str] | None]
```

Auto-discover columns and segmentation from the model registry.

**Returns**:

  Tuple of (columns, segment_by).

