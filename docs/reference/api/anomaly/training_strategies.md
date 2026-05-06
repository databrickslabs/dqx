# databricks.labs.dqx.anomaly.training\_strategies

Training strategy pattern for row anomaly detection.

Enables different anomaly detection algorithms through a common interface. Currently implements IsolationForest, but designed for extensibility.

Uses dependency injection for the model registry, enabling:

* Consistent registration path with EnsembleTrainer
* Easy mocking/testing
* Potential for alternative backends

## AnomalyTrainingStrategy Objects[​](#anomalytrainingstrategy-objects "Direct link to AnomalyTrainingStrategy Objects")

```python
class AnomalyTrainingStrategy(ABC)

```

Training strategy interface for row anomaly models.

Implement this interface to add new anomaly detection algorithms. Uses dependency injection for the model registry.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(registry: ModelRegistryBase | None = None) -> None

```

Initialize strategy with optional registry.

**Arguments**:

* `registry` - Model registry to use. Defaults to MLflow/Unity Catalog.

### train[​](#train "Direct link to train")

```python
@abstractmethod
def train(train_df: DataFrame, val_df: DataFrame, columns: list[str],
          params: AnomalyParams, model_name: str, *,
          allow_ensemble: bool) -> TrainingResult

```

Train an anomaly detection model.

**Arguments**:

* `train_df` - Training DataFrame
* `val_df` - Validation DataFrame
* `columns` - Feature columns to use
* `params` - Training parameters
* `model_name` - Name for registered model
* `allow_ensemble` - Whether to allow ensemble training

**Returns**:

TrainingResult with model URI, metrics, and metadata

## IsolationForestTrainingStrategy Objects[​](#isolationforesttrainingstrategy-objects "Direct link to IsolationForestTrainingStrategy Objects")

```python
class IsolationForestTrainingStrategy(AnomalyTrainingStrategy)

```

IsolationForest training strategy (default).

Uses sklearn's IsolationForest algorithm with optional ensemble training. Both single-model and ensemble paths use the same ModelRegistryBase abstraction.

### train[​](#train-1 "Direct link to train")

```python
def train(train_df: DataFrame, val_df: DataFrame, columns: list[str],
          params: AnomalyParams, model_name: str, *,
          allow_ensemble: bool) -> TrainingResult

```

Train IsolationForest model(s).

If allow\_ensemble and params.ensemble\_size > 1, trains an ensemble. Otherwise trains a single model using the registry abstraction.
