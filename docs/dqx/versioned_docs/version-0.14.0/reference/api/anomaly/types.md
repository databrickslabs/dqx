---
sidebar_label: types
title: databricks.labs.dqx.anomaly.types
---

Type definitions for row anomaly detection module.

Contains:
- Type protocols for duck typing (TrainedModel, MLflowSignature)
- Immutable data classes for training results and context

## TrainedModel Objects

```python
@runtime_checkable
class TrainedModel(Protocol)
```

Protocol for trained sklearn-compatible models.

Any object with predict, decision_function, and fit methods satisfies this protocol.
Uses sklearn naming convention where input features are passed as &#x27;data&#x27;.

#### predict

```python
def predict(data: pd.DataFrame) -> np.ndarray
```

Predict anomaly labels (-1 for anomaly, 1 for normal).

#### decision\_function

```python
def decision_function(data: pd.DataFrame) -> np.ndarray
```

Return anomaly scores (lower = more anomalous).

#### fit

```python
def fit(data: pd.DataFrame) -> "TrainedModel"
```

Fit the model on training data.

## MLflowSignature Objects

```python
class MLflowSignature(Protocol)
```

Protocol for MLflow model signatures.

## TrainingResult Objects

```python
@dataclass(frozen=True)
class TrainingResult()
```

Result of single model training.

## EnsembleTrainingResult Objects

```python
@dataclass(frozen=True)
class EnsembleTrainingResult()
```

Result of ensemble training.

## AnomalyTrainingContext Objects

```python
@dataclass(frozen=True)
class AnomalyTrainingContext()
```

Context containing all inputs needed for training.

## TrainingArtifacts Objects

```python
@dataclass(frozen=True)
class TrainingArtifacts()
```

Artifacts produced by training a single model or segment.

