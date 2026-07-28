---
sidebar_label: anomaly_engine
title: databricks.labs.dqx.anomaly.anomaly_engine
---

AnomalyEngine entrypoint for row anomaly detection.

## AnomalyEngine Objects

```python
class AnomalyEngine(DQEngineBase)
```

Engine for row anomaly detection model lifecycle management.

This class provides methods for training, managing, and working with row anomaly detection models.

**Arguments**:

- `workspace_client` - WorkspaceClient instance used to access the Databricks workspace.
- `spark` - Optional SparkSession to use. If not provided, the active session is used.
  

**Examples**:

  # Initialize engine
  from databricks.sdk import WorkspaceClient
  from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
  
  ws = WorkspaceClient()
  anomaly_engine = AnomalyEngine(ws)
  
  # Train a model with auto-discovery
  model_name = anomaly_engine.train(
  df,
  model_name=&quot;catalog.schema.my_anomaly_model&quot;,
  registry_table=&quot;catalog.schema.dqx_anomaly_models&quot;,
  )
  
  # Train with specific configuration
  model_name = anomaly_engine.train(
  df=df,
  model_name=&quot;catalog.schema.regional_model&quot;,
  registry_table=&quot;catalog.schema.dqx_anomaly_models&quot;,
  columns=[&quot;revenue&quot;, &quot;transactions&quot;],
  segment_by=[&quot;region&quot;]
  )

#### train

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

Requires Spark &gt;= 3.4 and the &#x27;anomaly&#x27; extras installed:
pip install &#x27;databricks-labs-dqx[anomaly]&#x27;

Auto-discovery behavior:
- columns=None, segment_by=None: Auto-discovers both (simplest)
- columns specified, segment_by=None: Uses columns, no segmentation
- columns=None, segment_by specified: Auto-discovers columns, uses segments

**Arguments**:

- `df` - Input DataFrame containing historical &quot;normal&quot; data.
- `model_name` - Model name (REQUIRED). Must be fully qualified Unity Catalog name as
  &#x27;catalog.schema.model&#x27;.
- `registry_table` - Registry table (REQUIRED). Must be fully qualified Unity Catalog table as
  &#x27;catalog.schema.table&#x27;.
- `columns` - Columns to use for row anomaly detection (auto-discovered if omitted).
- `segment_by` - Segment columns (auto-discovered if both columns and segment_by omitted).
- `params` - Optional anomaly parameters for tuning training behavior.
- `exclude_columns` - Columns to exclude from training (e.g., IDs, labels, ground truth).
  Exclusions always take precedence over `columns` if both are provided.
  Useful with auto-discovery to filter out unwanted columns without
  specifying all desired columns manually.
- `expected_anomaly_rate` - Expected fraction of anomalies in your data (default: 0.02 = 2%).
  Used as the default contamination parameter for the Isolation Forest
  algorithm, which controls the proportion of training data that the model
  treats as outliers when learning the decision boundary. A higher value
  makes the model flag more rows as anomalous.
  Common values: 0.01-0.02 (fraud), 0.03-0.05 (quality issues), 0.10 (exploration).
  Overridden if params.algorithm_config.contamination is set explicitly.
  Important Notes:
  - Avoid ID columns (user_id, order_id, etc.) - use exclude_columns to filter them out.
  - Choose behavioral columns, not identifiers. Good: amount, quantity. Bad: user_id.
  - See documentation for detailed column selection best practices.
  

**Returns**:

  Base model name (e.g., &#x27;catalog.schema.model_name&#x27;). For segmented models,
  individual segments are stored with suffixes like &#x27;__seg_region=APAC&#x27;, but
  the base name is returned for simplified API usage.
  

**Examples**:

  # Auto-discovery with default 2% expected anomaly rate (simplest)
  anomaly_engine.train(
  df,
  model_name=&quot;catalog.schema.my_model&quot;,
  registry_table=&quot;catalog.schema.dqx_anomaly_models&quot;,
  )
  
  # Exclude ID fields (recommended)
  anomaly_engine.train(
  df,
  model_name=&quot;catalog.schema.my_model&quot;,
  registry_table=&quot;catalog.schema.dqx_anomaly_models&quot;,
  exclude_columns=[&quot;user_id&quot;, &quot;order_id&quot;],
  )
  
  # Adjust expected anomaly rate for specific use cases
  anomaly_engine.train(
  df,
  model_name=&quot;catalog.schema.fraud_detector&quot;,
  registry_table=&quot;catalog.schema.dqx_anomaly_models&quot;,
  expected_anomaly_rate=0.01,  # 1% fraud
  )
  anomaly_engine.train(
  df,
  model_name=&quot;catalog.schema.quality_monitor&quot;,
  registry_table=&quot;catalog.schema.dqx_anomaly_models&quot;,
  expected_anomaly_rate=0.10,  # 10% defects
  )
  
  # Explicit columns
  anomaly_engine.train(
  df,
  model_name=&quot;catalog.schema.sales_monitor&quot;,
  registry_table=&quot;catalog.schema.dqx_anomaly_models&quot;,
  columns=[&quot;revenue&quot;, &quot;transactions&quot;],
  )

