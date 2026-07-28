---
sidebar_label: validation
title: databricks.labs.dqx.anomaly.validation
---

Validation for anomaly detection: training inputs and model record compatibility.

- Training-time: Spark version, fully qualified names, columns, and training params.
- Inference-time: compatibility checks when using an AnomalyModelRecord
  (e.g. sklearn version mismatch). Registry types and persistence live in model_registry.

#### validate\_spark\_version

```python
def validate_spark_version(spark: SparkSession) -> None
```

Validate Spark version is compatible with anomaly detection.

#### validate\_fully\_qualified\_name

```python
def validate_fully_qualified_name(value: str, *, label: str) -> None
```

Validate that a name is in catalog.schema.table format (exactly three non-empty parts).

#### validate\_columns

```python
def validate_columns(df: DataFrame,
                     columns: collections.abc.Iterable[str],
                     params: AnomalyParams | None = None) -> list[str]
```

Validate columns for row anomaly detection with multi-type support.

#### validate\_training\_params

```python
def validate_training_params(params: AnomalyParams,
                             expected_anomaly_rate: float) -> None
```

Validate training parameters with strict fail-fast checks.

#### validate\_sklearn\_compatibility

```python
def validate_sklearn_compatibility(model_record: AnomalyModelRecord) -> None
```

Validate sklearn version compatibility between training and inference.

**Arguments**:

- `model_record` - Model record containing sklearn_version from training
  

**Raises**:

  Warning if minor version mismatch detected (e.g., 1.2.x vs 1.3.x)
  

**Example**:

  &gt;&gt;&gt; record = AnomalyModelRecord(...)
  &gt;&gt;&gt; validate_sklearn_compatibility(record)
  # Warns if sklearn versions don&#x27;t match

