---
sidebar_label: mlflow_registry
title: databricks.labs.dqx.anomaly.mlflow_registry
---

Model registry abstraction for row anomaly detection.

Provides an abstract interface for model registration, with MLflow/Unity Catalog
as the default implementation. This abstraction enables:
- Unit testing with mock registries
- Potential support for alternative backends
- Clean separation of concerns

## ModelRegistryBase Objects

```python
class ModelRegistryBase(ABC)
```

Abstract base for model registration backends.

Implementations should handle model persistence and versioning.

#### register\_model

```python
@abstractmethod
def register_model(model: TrainedModel,
                   model_name: str,
                   signature: MLflowSignature,
                   hyperparams: dict[str, Any],
                   metrics: dict[str, float],
                   tags: dict[str, Any] | None = None) -> str
```

Register a model and return its URI.

**Arguments**:

- `model` - Trained sklearn-compatible model
- `model_name` - Fully qualified model name (catalog.schema.model)
- `signature` - MLflow signature for input/output schema
- `hyperparams` - Model hyperparameters to log
- `metrics` - Validation metrics to log
- `tags` - Additional metadata tags
  

**Returns**:

  Model URI in format models:/&lt;name&gt;/&lt;version&gt;

#### register\_model\_with\_signature\_inference

```python
@abstractmethod
def register_model_with_signature_inference(
        model: TrainedModel, model_name: str, train_pandas: pd.DataFrame,
        hyperparams: dict[str, Any], metrics: dict[str,
                                                   float]) -> tuple[str, str]
```

Register a model, inferring signature from training data.

**Arguments**:

- `model` - Trained sklearn-compatible model
- `model_name` - Fully qualified model name (catalog.schema.model)
- `train_pandas` - Training data for signature inference
- `hyperparams` - Model hyperparameters to log
- `metrics` - Validation metrics to log
  

**Returns**:

  Tuple of (model_uri, run_id)

#### ensure\_registry\_configured

```python
@abstractmethod
def ensure_registry_configured() -> None
```

Ensure the registry is properly configured for the environment.

## MLflowModelRegistry Objects

```python
class MLflowModelRegistry(ModelRegistryBase)
```

MLflow/Unity Catalog implementation of model registry.

Uses MLflow&#x27;s sklearn integration for model logging and Unity Catalog
for model versioning and governance.

#### ensure\_registry\_configured

```python
def ensure_registry_configured() -> None
```

Configure MLflow for Unity Catalog.

Sets registry URI to &#x27;databricks-uc&#x27; (or MLFLOW_REGISTRY_URI env var).
Also sets tracking URI if MLFLOW_TRACKING_URI is set.
Ensures an experiment is set (create if missing) so start_run() works in
job contexts (e.g. Databricks jobs) where no experiment is active.

#### register\_model

```python
def register_model(model: TrainedModel,
                   model_name: str,
                   signature: MLflowSignature,
                   hyperparams: dict[str, Any],
                   metrics: dict[str, float],
                   tags: dict[str, Any] | None = None) -> str
```

Register model to MLflow/Unity Catalog.

Creates a new MLflow run, logs the model with signature, hyperparameters,
and metrics, then registers it to Unity Catalog.

#### register\_model\_with\_signature\_inference

```python
def register_model_with_signature_inference(
        model: TrainedModel, model_name: str, train_pandas: pd.DataFrame,
        hyperparams: dict[str, Any], metrics: dict[str,
                                                   float]) -> tuple[str, str]
```

Register model, inferring signature from training data.

Creates a new MLflow run, infers the model signature from training data
and predictions, logs the model with hyperparameters and metrics.

#### log\_sklearn\_model\_compatible

```python
def log_sklearn_model_compatible(*, model: TrainedModel, model_name: str,
                                 signature: MLflowSignature)
```

Log sklearn model with compatibility across MLflow API variants.

Some runtimes accept `name=...` while others require `artifact_path=...`.

## \_RegistryHolder Objects

```python
class _RegistryHolder()
```

Holder for the default registry instance. Avoids global statement.

#### get

```python
@classmethod
def get(cls) -> ModelRegistryBase
```

Get the default registry, creating if needed.

#### set

```python
@classmethod
def set(cls, registry: ModelRegistryBase) -> None
```

Set the default registry (useful for testing).

#### get\_default\_registry

```python
def get_default_registry() -> ModelRegistryBase
```

Get the default model registry instance.

#### set\_default\_registry

```python
def set_default_registry(registry: ModelRegistryBase) -> None
```

Set the default model registry (useful for testing).

**Arguments**:

- `registry` - ModelRegistryBase implementation to use as default

