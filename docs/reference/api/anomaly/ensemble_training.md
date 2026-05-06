# databricks.labs.dqx.anomaly.ensemble\_training

Ensemble trainer for row anomaly detection models.

Encapsulates the logic for training multiple models with different random seeds to create a robust ensemble.

## EnsembleTrainer Objects[​](#ensembletrainer-objects "Direct link to EnsembleTrainer Objects")

```python
class EnsembleTrainer()

```

Trains ensemble of anomaly detection models with different random seeds.

Responsibilities:

* Feature engineering (done once, reused for all models)
* Training multiple models with varied seeds
* Computing validation metrics for each model
* Registering models to the registry
* Aggregating ensemble metrics

Uses dependency injection for the model registry, enabling testing with mocks.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(registry: ModelRegistryBase | None = None) -> None

```

Initialize ensemble trainer.

**Arguments**:

* `registry` - Model registry to use. Defaults to MLflow/Unity Catalog.

### train[​](#train "Direct link to train")

```python
def train(train_df: DataFrame, val_df: DataFrame, columns: list[str],
          params: AnomalyParams, ensemble_size: int,
          model_name: str) -> EnsembleTrainingResult

```

Train an ensemble of models.

**Arguments**:

* `train_df` - Training DataFrame
* `val_df` - Validation DataFrame
* `columns` - Feature columns to use
* `params` - Training parameters
* `ensemble_size` - Number of models in ensemble
* `model_name` - Base name for registered models

**Returns**:

EnsembleTrainingResult with model URIs, metrics, and metadata

### train\_ensemble[​](#train_ensemble "Direct link to train_ensemble")

```python
def train_ensemble(
    train_df: DataFrame, val_df: DataFrame, columns: list[str],
    params: AnomalyParams, ensemble_size: int, model_name: str
) -> tuple[list[str], dict[str, Any], dict[str, float], dict[str, float],
           SparkFeatureMetadata]

```

Train ensemble of models with different random seeds.
