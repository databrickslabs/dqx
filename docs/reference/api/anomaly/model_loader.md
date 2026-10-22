# databricks.labs.dqx.anomaly.model_loader

Load and validate sklearn anomaly models from MLflow.

### load_sklearn_model_with_error_handling[​](#load_sklearn_model_with_error_handling "Direct link to load_sklearn_model_with_error_handling")

```python
def load_sklearn_model_with_error_handling(
        model_uri: str, model_record: AnomalyModelRecord) -> Any

```

Load sklearn model from MLflow with graceful error handling.

**Arguments**:

* `model_uri` - MLflow model URI
* `model_record` - Model record with metadata for error messages

**Returns**:

Loaded sklearn model

**Raises**:

ModelLoadError with actionable error message if loading fails

### load_and_validate_model[​](#load_and_validate_model "Direct link to load_and_validate_model")

```python
def load_and_validate_model(model_uri: str,
                            model_record: AnomalyModelRecord) -> Any

```

Load model with validation and error handling.

**Arguments**:

* `model_uri` - MLflow model URI
* `model_record` - Model record for version validation

**Returns**:

Loaded sklearn model

### check_model_staleness[​](#check_model_staleness "Direct link to check_model_staleness")

```python
def check_model_staleness(record: AnomalyModelRecord, model_name: str) -> None

```

Check model training age and issue warning if stale (>30 days).
