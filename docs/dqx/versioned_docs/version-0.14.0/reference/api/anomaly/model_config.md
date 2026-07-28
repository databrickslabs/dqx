---
sidebar_label: model_config
title: databricks.labs.dqx.anomaly.model_config
---

Model configuration and record structure for row anomaly detection.

Single responsibility: model record structure (dataclasses) and config identity
(compute_config_hash). Persistence lives in model_registry.

## ModelIdentity Objects

```python
@dataclass
class ModelIdentity()
```

Core model identification (5 fields).

## TrainingMetadata Objects

```python
@dataclass
class TrainingMetadata()
```

Training configuration and metrics (7 fields).

## FeatureEngineering Objects

```python
@dataclass
class FeatureEngineering()
```

Feature engineering metadata (5 fields).

## SegmentationConfig Objects

```python
@dataclass
class SegmentationConfig()
```

Segmentation configuration (5 fields).

## AnomalyModelRecord Objects

```python
@dataclass
class AnomalyModelRecord()
```

Registry record for a trained anomaly model using composition.

Composed of 4 focused components, each under the 16-attribute limit:
- identity: Core model identification (5 fields)
- training: Training configuration and metrics (6 fields)
- features: Feature engineering metadata (5 fields)
- segmentation: Segmentation configuration (5 fields)

Stored as nested structs in Delta tables (no flattening needed).

#### compute\_config\_hash

```python
def compute_config_hash(columns: list[str],
                        segment_by: list[str] | None) -> str
```

Generate stable hash of model configuration.

**Arguments**:

- `columns` - List of column names used for training
- `segment_by` - List of columns used for segmentation, or None
  

**Returns**:

  16-character hex string (first 16 chars of SHA256 hash)
  

**Notes**:

  This hash uniquely identifies a model configuration based on:
  - Sorted list of columns (order-independent)
  - Sorted list of segment_by columns (order-independent)
  Used for collision detection when same model_name is reused with different configs.

