# databricks.labs.dqx.anomaly.anomaly\_engine

AnomalyEngine entrypoint for row anomaly detection.

## AnomalyEngine Objects[​](#anomalyengine-objects "Direct link to AnomalyEngine Objects")

```python
class AnomalyEngine(DQEngineBase)

```

Engine for row anomaly detection model lifecycle management.

This class provides methods for training, managing, and working with row anomaly detection models.

**Arguments**:

* `workspace_client` - WorkspaceClient instance used to access the Databricks workspace.
* `spark` - Optional SparkSession to use. If not provided, the active session is used.

**Examples**:

# Initialize engine

from databricks.sdk import WorkspaceClient from databricks.labs.dqx.anomaly.anomaly\_engine import AnomalyEngine

ws = WorkspaceClient() anomaly\_engine = AnomalyEngine(ws)

# Train a model with auto-discovery

model\_name = anomaly\_engine.train( df, model\_name="catalog.schema.my\_anomaly\_model", registry\_table="catalog.schema.dqx\_anomaly\_models", )

# Train with specific configuration

model\_name = anomaly\_engine.train( df=df, model\_name="catalog.schema.regional\_model", registry\_table="catalog.schema.dqx\_anomaly\_models", columns=\["revenue", "transactions"], segment\_by=\["region"] )

### train[​](#train "Direct link to train")

```python
@telemetry_logger("anomaly", "train")
def train(df: DataFrame,
          model_name: str,
          registry_table: str,
          columns: list[str] | None = None,
          segment_by: list[str] | None = None,
          params: AnomalyParams | None = None,
          exclude_columns: list[str] | None = None,
          expected_anomaly_rate: float = 0.02) -> str

```

Train row anomaly detection model(s) with intelligent auto-discovery.

Requires Spark >= 3.4 and the 'anomaly' extras installed: pip install 'databricks-labs-dqx\[anomaly]'

Auto-discovery behavior:

* columns=None, segment\_by=None: Auto-discovers both (simplest)
* columns specified, segment\_by=None: Uses columns, no segmentation
* columns=None, segment\_by specified: Auto-discovers columns, uses segments

**Arguments**:

* `df` - Input DataFrame containing historical "normal" data.

* `model_name` - Model name (REQUIRED). Must be fully qualified Unity Catalog name as 'catalog.schema.model'.

* `registry_table` - Registry table (REQUIRED). Must be fully qualified Unity Catalog table as 'catalog.schema.table'.

* `columns` - Columns to use for row anomaly detection (auto-discovered if omitted).

* `segment_by` - Segment columns (auto-discovered if both columns and segment\_by omitted).

* `params` - Optional anomaly parameters for tuning training behavior.

* `exclude_columns` - Columns to exclude from training (e.g., IDs, labels, ground truth). Exclusions always take precedence over `columns` if both are provided. Useful with auto-discovery to filter out unwanted columns without specifying all desired columns manually.

* `expected_anomaly_rate` - Expected fraction of anomalies in your data (default: 0.02 = 2%). Used as the default contamination parameter for the Isolation Forest algorithm, which controls the proportion of training data that the model treats as outliers when learning the decision boundary. A higher value makes the model flag more rows as anomalous. Common values: 0.01-0.02 (fraud), 0.03-0.05 (quality issues), 0.10 (exploration). Overridden if params.algorithm\_config.contamination is set explicitly. Important Notes:

  <!-- -->

  * Avoid ID columns (user\_id, order\_id, etc.) - use exclude\_columns to filter them out.
  * Choose behavioral columns, not identifiers. Good: amount, quantity. Bad: user\_id.
  * See documentation for detailed column selection best practices.

**Returns**:

Base model name (e.g., 'catalog.schema.model\_name'). For segmented models, individual segments are stored with suffixes like '\_\_seg\_region=APAC', but the base name is returned for simplified API usage.

**Examples**:

# Auto-discovery with default 2% expected anomaly rate (simplest)

anomaly\_engine.train( df, model\_name="catalog.schema.my\_model", registry\_table="catalog.schema.dqx\_anomaly\_models", )

# Exclude ID fields (recommended)

anomaly\_engine.train( df, model\_name="catalog.schema.my\_model", registry\_table="catalog.schema.dqx\_anomaly\_models", exclude\_columns=\["user\_id", "order\_id"], )

# Adjust expected anomaly rate for specific use cases

anomaly\_engine.train( df, model\_name="catalog.schema.fraud\_detector", registry\_table="catalog.schema.dqx\_anomaly\_models", expected\_anomaly\_rate=0.01, # 1% fraud ) anomaly\_engine.train( df, model\_name="catalog.schema.quality\_monitor", registry\_table="catalog.schema.dqx\_anomaly\_models", expected\_anomaly\_rate=0.10, # 10% defects )

# Explicit columns

anomaly\_engine.train( df, model\_name="catalog.schema.sales\_monitor", registry\_table="catalog.schema.dqx\_anomaly\_models", columns=\["revenue", "transactions"], )
