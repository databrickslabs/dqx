---
sidebar_label: check_funcs
title: databricks.labs.dqx.anomaly.check_funcs
---

Check functions for row anomaly detection.

Facade: public rule entry point. Orchestration and scoring live in sibling modules.

#### has\_no\_row\_anomalies

```python
@register_rule("dataset")
def has_no_row_anomalies(model_name: str,
                         registry_table: str,
                         threshold: float = 95.0,
                         row_filter: str | None = None,
                         drift_threshold: float | None = None,
                         enable_contributions: bool = False,
                         enable_confidence_std: bool = False,
                         *,
                         driver_only: bool = False) -> tuple[Column, Any, str]
```

Check that records are not anomalous according to a trained model(s).

Auto-discovery:
- columns: Inferred from model registry
- segmentation: Inferred from model registry (checks if model is segmented)

Output columns:
- _dq_info: Array of structs (one element per dataset-level check). For example:
- _dq_info[0].anomaly.score: Raw anomaly score (model-relative)
- _dq_info[0].anomaly.severity_percentile: Severity percentile (0–100)
- _dq_info[0].anomaly.is_anomaly: Boolean flag
- _dq_info[0].anomaly.threshold: Severity percentile threshold used (0–100)
- _dq_info[0].anomaly.model: Model name
- _dq_info[0].anomaly.segment: Segment values (if segmented)
- _dq_info[0].anomaly.contributions: SHAP contributions as percentages (0–100)
- _dq_info[0].anomaly.confidence_std: Ensemble std (if requested)

**Notes**:

  DQX always scores using the columns the model was trained on.
  DQX aligns scored rows back to the input using an internal row id and removes it before returning.
  Segmentation is inferred from the trained model configuration.
  

**Arguments**:

- `model_name` - Model name (REQUIRED). Provide the fully qualified model name
  in catalog.schema.table format returned from train().
- `registry_table` - Registry table (REQUIRED). Provide the fully qualified table
  name in catalog.schema.table format.
- `threshold` - Severity percentile threshold (0–100, default 95).
  Records with severity_percentile &gt;= threshold are flagged as anomalous.
  Higher threshold = stricter detection (fewer anomalies).
- `row_filter` - Optional SQL expression (e.g. &quot;region = &#x27;US&#x27;&quot;). Only rows matching
  this expression are scored; others are left in the output with null anomaly
  result. Auto-injected from the check filter.
- `drift_threshold` - Drift detection threshold (default 3.0, None to disable).
- `enable_contributions` - Include SHAP feature contributions for explainability (default False).
  Set True to get per-feature contributions in _dq_info; adds significant scoring cost.
  Requires SHAP library when True.
- `enable_confidence_std` - Include ensemble confidence scores in _dq_info and top-level (default False).
  Automatically available when training with ensemble_size &gt; 1 (default is 3).
- `driver_only` - If True, score on the driver (no UDF). Use for tests or Spark Connect when
  worker UDF dependencies are not available. Default False for production.
  

**Returns**:

  Tuple of condition expression, apply function and info column name.
  

**Example**:

  Access anomaly metadata via _dq_info (array; first check = index 0):
  &gt;&gt;&gt; df_scored.select(col(&quot;_dq_info&quot;).getItem(0).getField(&quot;anomaly&quot;).getField(&quot;score&quot;), ...)
  &gt;&gt;&gt; df_scored.filter(col(&quot;_dq_info&quot;).getItem(0).getField(&quot;anomaly&quot;).getField(&quot;is_anomaly&quot;))

