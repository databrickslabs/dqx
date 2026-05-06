# databricks.labs.dqx.anomaly.types

Type definitions for row anomaly detection module.

Contains:

* Type protocols for duck typing (TrainedModel, MLflowSignature)
* Immutable data classes for training results and context

## TrainedModel Objects[​](#trainedmodel-objects "Direct link to TrainedModel Objects")

```python
@runtime_checkable
class TrainedModel(Protocol)

```

Protocol for trained sklearn-compatible models.

Any object with predict, decision\_function, and fit methods satisfies this protocol. Uses sklearn naming convention where input features are passed as 'data'.

### predict[​](#predict "Direct link to predict")

```python
def predict(data: pd.DataFrame) -> np.ndarray

```

Predict anomaly labels (-1 for anomaly, 1 for normal).

### decision\_function[​](#decision_function "Direct link to decision_function")

```python
def decision_function(data: pd.DataFrame) -> np.ndarray

```

Return anomaly scores (lower = more anomalous).

### fit[​](#fit "Direct link to fit")

```python
def fit(data: pd.DataFrame) -> "TrainedModel"

```

Fit the model on training data.

## MLflowSignature Objects[​](#mlflowsignature-objects "Direct link to MLflowSignature Objects")

```python
class MLflowSignature(Protocol)

```

Protocol for MLflow model signatures.

## TrainingResult Objects[​](#trainingresult-objects "Direct link to TrainingResult Objects")

```python
@dataclass(frozen=True)
class TrainingResult()

```

Result of single model training.

## EnsembleTrainingResult Objects[​](#ensembletrainingresult-objects "Direct link to EnsembleTrainingResult Objects")

```python
@dataclass(frozen=True)
class EnsembleTrainingResult()

```

Result of ensemble training.

## AnomalyTrainingContext Objects[​](#anomalytrainingcontext-objects "Direct link to AnomalyTrainingContext Objects")

```python
@dataclass(frozen=True)
class AnomalyTrainingContext()

```

Context containing all inputs needed for training.

## TrainingArtifacts Objects[​](#trainingartifacts-objects "Direct link to TrainingArtifacts Objects")

```python
@dataclass(frozen=True)
class TrainingArtifacts()

```

Artifacts produced by training a single model or segment.
