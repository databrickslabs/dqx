---
sidebar_label: training_strategies
title: databricks.labs.dqx.anomaly.training_strategies
---

Training strategy pattern for row anomaly detection.

Enables different anomaly detection algorithms through a common interface.
Currently implements IsolationForest, but designed for extensibility.

Uses dependency injection for the model registry, enabling:
- Consistent registration path with EnsembleTrainer
- Easy mocking/testing
- Potential for alternative backends

## AnomalyTrainingStrategy Objects

```python
class AnomalyTrainingStrategy(ABC)
```

Training strategy interface for row anomaly models.

Implement this interface to add new anomaly detection algorithms.
Uses dependency injection for the model registry.

#### \_\_init\_\_

```python
def __init__(registry: ModelRegistryBase | None = None) -> None
```

Initialize strategy with optional registry.

**Arguments**:

- `registry` - Model registry to use. Defaults to MLflow/Unity Catalog.

#### train

```python
@abstractmethod
def train(train_df: DataFrame, val_df: DataFrame, columns: list[str],
          params: AnomalyParams, model_name: str, *,
          allow_ensemble: bool) -> TrainingResult
```

Train an anomaly detection model.

**Arguments**:

- `train_df` - Training DataFrame
- `val_df` - Validation DataFrame
- `columns` - Feature columns to use
- `params` - Training parameters
- `model_name` - Name for registered model
- `allow_ensemble` - Whether to allow ensemble training
  

**Returns**:

  TrainingResult with model URI, metrics, and metadata

## IsolationForestTrainingStrategy Objects

```python
class IsolationForestTrainingStrategy(AnomalyTrainingStrategy)
```

IsolationForest training strategy (default).

Uses sklearn&#x27;s IsolationForest algorithm with optional ensemble training.
Both single-model and ensemble paths use the same ModelRegistryBase abstraction.

#### train

```python
def train(train_df: DataFrame, val_df: DataFrame, columns: list[str],
          params: AnomalyParams, model_name: str, *,
          allow_ensemble: bool) -> TrainingResult
```

Train IsolationForest model(s).

If allow_ensemble and params.ensemble_size &gt; 1, trains an ensemble.
Otherwise trains a single model using the registry abstraction.

