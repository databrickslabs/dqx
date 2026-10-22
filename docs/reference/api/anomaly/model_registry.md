# databricks.labs.dqx.anomaly.model_registry

Model registry utilities for row anomaly detection.

Persistence only: schema and AnomalyModelRegistry. Record types live in model_config.

## AnomalyModelRegistry Objects[​](#anomalymodelregistry-objects "Direct link to AnomalyModelRegistry Objects")

```python
class AnomalyModelRegistry()

```

Manage anomaly model metadata in a Delta table.

### convert_decimals[​](#convert_decimals "Direct link to convert_decimals")

```python
@staticmethod
def convert_decimals(obj: Any) -> Any

```

Recursively convert Decimal values to float for PyArrow compatibility.

This is a public utility method that can be used by tests and other code to handle Decimal to float conversions for PyArrow compatibility.

### build_model_df[​](#build_model_df "Direct link to build_model_df")

```python
@staticmethod
def build_model_df(spark: SparkSession,
                   record: AnomalyModelRecord) -> DataFrame

```

Convert a registry record into a DataFrame with nested structure.

### save_model[​](#save_model "Direct link to save_model")

```python
def save_model(record: AnomalyModelRecord, table: str) -> None

```

Archive previous active model with the same name and insert the new record.

### get_active_model[​](#get_active_model "Direct link to get_active_model")

```python
def get_active_model(table: str, model_name: str) -> AnomalyModelRecord | None

```

Fetch the active model for a given name.

### get_segment_model[​](#get_segment_model "Direct link to get_segment_model")

```python
def get_segment_model(
        table: str, base_model_name: str,
        segment_values: dict[str, str]) -> AnomalyModelRecord | None

```

Fetch model for specific segment combination.

### get_all_segment_models[​](#get_all_segment_models "Direct link to get_all_segment_models")

```python
def get_all_segment_models(table: str,
                           base_model_name: str) -> list[AnomalyModelRecord]

```

Fetch all segment models for a base name.
