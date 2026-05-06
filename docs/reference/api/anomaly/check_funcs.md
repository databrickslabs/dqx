# databricks.labs.dqx.anomaly.check\_funcs

Check functions for row anomaly detection.

Facade: public rule entry point. Orchestration and scoring live in sibling modules.

### has\_no\_row\_anomalies[​](#has_no_row_anomalies "Direct link to has_no_row_anomalies")

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

* columns: Inferred from model registry
* segmentation: Inferred from model registry (checks if model is segmented)

Output columns:

* \_dq\_info: Array of structs (one element per dataset-level check). For example:
* \_dq\_info\[0].anomaly.score: Raw anomaly score (model-relative)
* \_dq\_info\[0].anomaly.severity\_percentile: Severity percentile (0–100)
* \_dq\_info\[0].anomaly.is\_anomaly: Boolean flag
* \_dq\_info\[0].anomaly.threshold: Severity percentile threshold used (0–100)
* \_dq\_info\[0].anomaly.model: Model name
* \_dq\_info\[0].anomaly.segment: Segment values (if segmented)
* \_dq\_info\[0].anomaly.contributions: SHAP contributions as percentages (0–100)
* \_dq\_info\[0].anomaly.confidence\_std: Ensemble std (if requested)

**Notes**:

DQX always scores using the columns the model was trained on. DQX aligns scored rows back to the input using an internal row id and removes it before returning. Segmentation is inferred from the trained model configuration.

**Arguments**:

* `model_name` - Model name (REQUIRED). Provide the fully qualified model name in catalog.schema.table format returned from train().
* `registry_table` - Registry table (REQUIRED). Provide the fully qualified table name in catalog.schema.table format.
* `threshold` - Severity percentile threshold (0–100, default 95). Records with severity\_percentile >= threshold are flagged as anomalous. Higher threshold = stricter detection (fewer anomalies).
* `row_filter` - Optional SQL expression (e.g. "region = 'US'"). Only rows matching this expression are scored; others are left in the output with null anomaly result. Auto-injected from the check filter.
* `drift_threshold` - Drift detection threshold (default 3.0, None to disable).
* `enable_contributions` - Include SHAP feature contributions for explainability (default False). Set True to get per-feature contributions in \_dq\_info; adds significant scoring cost. Requires SHAP library when True.
* `enable_confidence_std` - Include ensemble confidence scores in \_dq\_info and top-level (default False). Automatically available when training with ensemble\_size > 1 (default is 3).
* `driver_only` - If True, score on the driver (no UDF). Use for tests or Spark Connect when worker UDF dependencies are not available. Default False for production.

**Returns**:

Tuple of condition expression, apply function and info column name.

**Example**:

Access anomaly metadata via \_dq\_info (array; first check = index 0): >>> df\_scored.select(col("\_dq\_info").getItem(0).getField("anomaly").getField("score"), ...) >>> df\_scored.filter(col("\_dq\_info").getItem(0).getField("anomaly").getField("is\_anomaly"))
