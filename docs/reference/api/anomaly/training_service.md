# databricks.labs.dqx.anomaly.training\_service

Anomaly training service - Main orchestration layer.

Provides the high-level API for training anomaly detection models, including context building, validation, and both global and segmented training. All training logic lives on AnomalyTrainingService (public and private methods).

## AnomalyTrainingService Objects[​](#anomalytrainingservice-objects "Direct link to AnomalyTrainingService Objects")

```python
class AnomalyTrainingService()

```

Service for building training context and orchestrating model training.

Provides the main entry point for training anomaly detection models. Supports both global models and segment-specific models.

Extension point: To add new algorithms, implement AnomalyTrainingStrategy and pass to constructor.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(spark: SparkSession,
             strategy: AnomalyTrainingStrategy | None = None) -> None

```

Initialize the training service.

### apply\_expected\_anomaly\_rate\_if\_default\_contamination[​](#apply_expected_anomaly_rate_if_default_contamination "Direct link to apply_expected_anomaly_rate_if_default_contamination")

```python
@staticmethod
def apply_expected_anomaly_rate_if_default_contamination(
        params: AnomalyParams | None,
        expected_anomaly_rate: float) -> AnomalyParams

```

Apply expected\_anomaly\_rate to params if contamination is not explicitly set.

### build\_context[​](#build_context "Direct link to build_context")

```python
def build_context(df: DataFrame, model_name: str, registry_table: str, *,
                  columns: list[str] | None, segment_by: list[str] | None,
                  params: AnomalyParams | None,
                  exclude_columns: list[str] | None,
                  expected_anomaly_rate: float) -> AnomalyTrainingContext

```

Build training context with all validated inputs.

### train[​](#train "Direct link to train")

```python
def train(context: AnomalyTrainingContext) -> str

```

Train model(s) based on context.
