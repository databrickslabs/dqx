---
sidebar_label: model_loader
title: databricks.labs.dqx.anomaly.model_loader
---

Load and validate sklearn anomaly models from MLflow.

#### load\_sklearn\_model\_with\_error\_handling

```python
def load_sklearn_model_with_error_handling(
        model_uri: str, model_record: AnomalyModelRecord) -> Any
```

Load sklearn model from MLflow with graceful error handling.

**Arguments**:

- `model_uri` - MLflow model URI
- `model_record` - Model record with metadata for error messages
  

**Returns**:

  Loaded sklearn model
  

**Raises**:

  ModelLoadError with actionable error message if loading fails

#### load\_and\_validate\_model

```python
def load_and_validate_model(model_uri: str,
                            model_record: AnomalyModelRecord) -> Any
```

Load model with validation and error handling.

**Arguments**:

- `model_uri` - MLflow model URI
- `model_record` - Model record for version validation
  

**Returns**:

  Loaded sklearn model

#### check\_model\_staleness

```python
def check_model_staleness(record: AnomalyModelRecord, model_name: str) -> None
```

Check model training age and issue warning if stale (&gt;30 days).

