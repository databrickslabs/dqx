# Troubleshooting Row Anomaly Detection

This guide helps you resolve common issues when using DQX row anomaly detection.

## Common Errors[​](#common-errors "Direct link to Common Errors")

### Training Errors[​](#training-errors "Direct link to Training Errors")

#### Error: "No columns provided or auto-discovered. Provide columns explicitly."[​](#error-no-columns-provided-or-auto-discovered-provide-columns-explicitly "Direct link to Error: \"No columns provided or auto-discovered. Provide columns explicitly.\"")

**Cause**: Auto-discovery found no suitable columns (all IDs, constants, or unsupported types).

**Solution**: Explicitly specify behavioral columns.

```python
# Explicitly specify behavioral columns
anomaly_engine.train(
  df=df,
  columns=["amount", "quantity"],  # specify selected columns
  model_name=model_name,  # must be fully qualified Unity Catalog name: catalog.schema.model
  registry_table=registry_table,
)

```

Avoid ID columns

Avoid ID columns (`order_id`, `user_id`) - they're always unique and hurt model quality.

***

#### Error: "Columns not found in DataFrame: \['column\_name']"[​](#error-columns-not-found-in-dataframe-column_name "Direct link to Error: \"Columns not found in DataFrame: \['column_name']\"")

**Cause**: Specified column doesn't exist in training data.

**Solution**:

1. Check column names: `df.columns`
2. Verify no typos in column list
3. Ensure columns exist after any filters

```python
# Check what columns are available
print(df.columns)

# Then specify correct column names
anomaly_engine.train(
  df=df,
  columns=["actual_column_name"],  # Use correct names
  model_name=model_name,
  registry_table=registry_table,
)

```

Note also that columns can only be defined when training the model. Scoring uses the columns the model was trained on. Ensure scoring DataFrame has all columns used during training.

***

#### Error: "exclude\_columns contains columns not in DataFrame: \['column\_name']"[​](#error-exclude_columns-contains-columns-not-in-dataframe-column_name "Direct link to Error: \"exclude_columns contains columns not in DataFrame: \['column_name']\"")

**Cause**: You're trying to exclude a column that doesn't exist.

**Solution**: Remove non-existent columns from `exclude_columns` list.

```python
# Check existing columns first
existing_cols = set(df.columns)
exclude_list = ["order_id", "user_id"]
valid_excludes = [col for col in exclude_list if col in existing_cols]

anomaly_engine.train(
  df=df,
  exclude_columns=valid_excludes,
  model_name=model_name,
  registry_table=registry_table,
)

```

***

### Scoring Errors[​](#scoring-errors "Direct link to Scoring Errors")

#### Error: Model not found in registry[​](#error-model-not-found-in-registry "Direct link to Error: Model not found in registry")

**Cause**: Model name mismatch or registry table doesn't exist.

**Solution**: Verify model exists in registry.

```python
import pyspark.sql.functions as F

# Verify model exists
display(spark.table("catalog.schema.anomaly_registry").filter(
  F.col("identity.model_name") == "your_model_name"
))

# Check for typos in model_name (case-sensitive!)

```

***

#### Error: "Column 'feature\_name' not found in DataFrame"[​](#error-column-feature_name-not-found-in-dataframe "Direct link to Error: \"Column 'feature_name' not found in DataFrame\"")

**Cause**: Scoring data missing columns that were used in training.

**Solution**: Ensure scoring DataFrame has all columns used during training.

```python
# Check training columns from registry
registry_df = spark.table("catalog.schema.anomaly_registry")
model_info = registry_df.filter(
  F.col("identity.model_name") == "your_model"
).select("training.columns").first()

training_cols = model_info[0] if model_info else []
print(f"Required columns: {training_cols}")

# Verify scoring data has all required columns
scoring_cols = set(df.columns)
missing = set(training_cols) - scoring_cols
if missing:
  print(f"Missing columns: {missing}")

```

***

## Common Issues[​](#common-issues "Direct link to Common Issues")

### Q: Training is very slow (> 30 minutes)[​](#q-training-is-very-slow--30-minutes "Direct link to Q: Training is very slow (> 30 minutes)")

**Likely causes**:

1. Large dataset (millions of rows)
2. Many columns (50+) → generates many features
3. Small cluster

**Solutions**:

**Option 1: Sample during training** (recommended)

```python
from databricks.labs.dqx.config import AnomalyParams

anomaly_engine.train(
  df=df,
  model_name=model_name,
  registry_table=registry_table,
  params=AnomalyParams(sample_fraction=0.1),
)

```

**Option 2: Cap maximum rows**

```python
from databricks.labs.dqx.config import AnomalyParams

anomaly_engine.train(
  df=df,
  model_name=model_name,
  registry_table=registry_table,
  params=AnomalyParams(max_rows=100000),
)

```

**Option 3: Explicit column selection** (reduces features)

```python
anomaly_engine.train(
  df=df,
  columns=["amount", "quantity"],  # Only key columns
  model_name=model_name,
  registry_table=registry_table,
)

```

Sampling doesn't hurt quality

Sampling doesn't hurt model quality if the sample is representative!

***

### Q: Scoring is slow (with contributions enabled)[​](#q-scoring-is-slow-with-contributions-enabled "Direct link to Q: Scoring is slow (with contributions enabled)")

**Cause**: SHAP explainability is compute-intensive (5-10x slower than scoring without contributions).

**Solution**: Disable for production scoring, enable only for investigation.

```python
# Production: fast scoring, no explanations
has_no_row_anomalies(
  model_name=model_name,
  registry_table=registry_table,
  enable_contributions=False,  # 10x faster, default is False
)

# Investigation: slower, with explanations
has_no_row_anomalies(
  model_name=model_name,
  registry_table=registry_table,
  enable_contributions=True,  # Optional: for explainability (slower)
)

```

***

### Q: Too many false positives (everything flagged as anomaly)[​](#q-too-many-false-positives-everything-flagged-as-anomaly "Direct link to Q: Too many false positives (everything flagged as anomaly)")

**Likely causes**:

1. Distribution shift between training and scoring data
2. Threshold too low
3. Training data contained anomalies

**Solutions**:

**Option 1: Raise threshold** (fewer alerts)

```python
has_no_row_anomalies(
  model_name=model_name,
  registry_table=registry_table,
  threshold=98,
)

```

**Option 2: Enable drift detection** (warns if data changed)

```python
has_no_row_anomalies(
  model_name=model_name,
  registry_table=registry_table,
  drift_threshold=3.0,  # Warn on distribution shift
)

```

**Option 3: Retrain on clean, recent data**

```python
# Filter to recent, known-good data
clean_data = spark.table("orders").filter(
  F.col("order_date") >= F.current_date() - F.expr("INTERVAL 30 DAYS")
)
anomaly_engine.train(df=clean_data, ...)

```

***

### Q: No anomalies detected but I see unusual data[​](#q-no-anomalies-detected-but-i-see-unusual-data "Direct link to Q: No anomalies detected but I see unusual data")

**Likely causes**:

1. Threshold too high (95 = only top 5%)
2. Unusual data was in training set: model learned it as "normal"

**Solutions**:

**Option 1: Lower threshold**

```python
has_no_row_anomalies(
  model_name=model_name,
  registry_table=registry_table,
  threshold=90,
)

```

**Option 2: Check severity percentiles** (even "normal" rows in the output have scores). For the full list of fields in `_dq_info` and `anomaly`, see [Schema of the info column (\_dq\_info)](/dqx/docs/guide/row_anomaly_detection.md#schema-of-the-info-column-_dq_info).

```python
import pyspark.sql.functions as F

df_scored = dq_engine.apply_checks(df, checks)

# View top 20 by severity (regardless of threshold)
# Use element_at(_, 1) instead of getItem(0) if using Spark Connect to avoid resolution issues
display(df_scored.orderBy(
  F.col("_dq_info").getItem(0).getField("anomaly").getField("severity_percentile").desc()
).select(
  "transaction_id",
  F.col("_dq_info").getItem(0).getField("anomaly").getField("severity_percentile").alias("severity_percentile"),
  F.col("_dq_info").getItem(0).getField("anomaly").getField("score").alias("score"),
).limit(20))

```

***

### Q: Memory errors during training[​](#q-memory-errors-during-training "Direct link to Q: Memory errors during training")

**Cause**: Dataset too large for the cluster.

**Solutions**:

1. Use `sample_fraction` and `max_rows` via the `params` argument (pass an `AnomalyParams` instance) to reduce data used for training
2. Scale up cluster (more memory)

```python
from databricks.labs.dqx.config import AnomalyParams

# Conservative settings for large datasets
anomaly_engine.train(
  df=df,
  columns=["amount", "quantity"],  # Select only key columns
  model_name=model_name,
  registry_table=registry_table,
  params=AnomalyParams(sample_fraction=0.05, max_rows=50000),  # 5% sample, cap at 50K
)

```

***

## Streaming-Specific Issues[​](#streaming-specific-issues "Direct link to Streaming-Specific Issues")

### Q: Can I use row anomaly detection with streaming?[​](#q-can-i-use-row-anomaly-detection-with-streaming "Direct link to Q: Can I use row anomaly detection with streaming?")

**Yes!** Train on batch, score on streaming data.

**Best practices**:

* Train model on historical batch data first
* Disable contributions (default) to improve performance

**Example 1: DIY Approach** (you manage readStream/writeStream):

```python
# 1. Train on batch
model_name = anomaly_engine.train(
  df=spark.table("catalog.schema.orders_historical"),
  model_name="catalog.schema.orders_monitor",
  registry_table="catalog.schema.anomaly_registry",
)

# 2. Apply to streaming DataFrame
streaming_df = spark.readStream.table("catalog.schema.orders_stream")

from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies

checks = [
  DQDatasetRule(
    criticality="error",
    check_func=has_no_row_anomalies,
    check_func_kwargs={
      "model_name": model_name,
      "registry_table": "catalog.schema.anomaly_registry",
      "enable_contributions": False, # Recommended: too slow for real-time
    }
  )
]

result_stream = dq_engine.apply_checks(streaming_df, checks)

# 3. Write results
result_stream.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/checkpoint") \
  .table("catalog.schema.orders_scored")

```

**Example 2: End-to-End Approach** (DQX manages streaming):

```python
from databricks.labs.dqx.config import InputConfig, OutputConfig

# 1. Train model (same as above)

# 2. Configure streaming input/output
input_config = InputConfig(
  location="catalog.schema.orders_stream",
  format="delta",
  is_streaming=True  # This flag enables streaming
)

output_config = OutputConfig(
  location="catalog.schema.orders_scored",
  format="delta",
  mode="append",
  trigger={"availableNow": True},
  options={"checkpointLocation": "/tmp/checkpoint"}
)

# 3. Apply checks with streaming config
dq_engine.apply_checks_by_metadata_and_save_in_table(
  checks=checks_metadata,  # List of check dicts
  input_config=input_config,
  output_config=output_config,
)

```

Streaming recommendation

Disable contributions for streaming workloads. SHAP is compute-intensive (5-10x slower) and not suitable for real-time processing.

***

### Q: Should I use contributions in streaming?[​](#q-should-i-use-contributions-in-streaming "Direct link to Q: Should I use contributions in streaming?")

**Not recommended**: SHAP explainability (`enable_contributions=True`) works with streaming but is too slow for real-time processing (5-10x overhead).

**Best practice**:

* Use `enable_contributions=False` for streaming workloads
* For investigation, score a batch sample with contributions enabled

***

## Getting Help[​](#getting-help "Direct link to Getting Help")

If you encounter an issue not covered here:

1. **Check GitHub Issues**: <https://github.com/databrickslabs/dqx/issues>
2. **Search closed issues**: Your problem might be solved already
3. **Open a new issue**: Include error message, code snippet, and DQX version

**Include in bug reports**:

* DQX version: `pip show databricks-labs-dqx`
* Databricks Runtime version
* Full error message and stack trace
* Minimal reproducible example
