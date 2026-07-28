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
                         enable_contributions: bool = True,
                         enable_confidence_std: bool = False,
                         enable_ai_explanation: bool = True,
                         ai_explanation_llm_model_config: LLMModelConfig | dict
                         | None = None,
                         redact_columns: list[str] | None = None,
                         max_groups: int = 500,
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
- _dq_info[0].anomaly.contributions: SHAP contributions as percentages (0–100); populated
only for anomalous rows, null otherwise
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
- `enable_contributions` - Include SHAP feature contributions for explainability (default True).
  Per-feature contributions are added to _dq_info for anomalous rows only (severity at or
  above the threshold; other rows get a null map), so the SHAP cost scales with the number
  of anomalies rather than the table size. Requires the SHAP library (installed with the
  anomaly extra). Set False to skip the SHAP cost entirely (this also disables AI
  explanations, since they use contributions as input).
- `enable_confidence_std` - Include ensemble confidence scores in _dq_info and top-level (default False).
  Automatically available when training with ensemble_size &gt; 1 (default is 3).
- `enable_ai_explanation` - Add a human-readable LLM explanation for each anomalous row
  (default True). Uses enable_contributions as input; if contributions are off, explanations
  are disabled (with a warning) rather than erroring. The LLM call runs in Spark via
  ``ai_query`` against a Databricks Model Serving endpoint — no extra dependencies. If that
  endpoint is unreachable (e.g. no Foundation Model APIs in the workspace), explanations are
  skipped with a warning and scoring still completes. Output is in
  _dq_info[0].anomaly.ai_explanation, and is AI-generated from the anomaly signal (feature
  names + SHAP + severity), not grounded in catalog metadata.
- `registry_table`0 - LLM model configuration for AI explanations (named
  distinctly from the check&#x27;s *model_name* to avoid confusion). Defaults to
  LLMModelConfig() (model_name=&#x27;databricks/databricks-claude-sonnet-4-5&#x27;). Its *model_name*
  must resolve to a Databricks Model Serving endpoint (with or without the ``databricks/``
  prefix); the ``ai_query`` call uses the bare endpoint name.
  
  When wrapping this check in DQDatasetRule / DQRowRule, applying it via
  apply_checks / apply_checks_by_metadata, or declaring it in YAML: **pass a dict**
  with keys ``{&quot;model_name&quot;, &quot;api_key&quot;, &quot;api_base&quot;}``. The rule pipeline
  normalizes check arguments and does not accept custom dataclass instances, so an
  LLMModelConfig object there raises ``TypeError: Unsupported type for normalization``.
  The dict is coerced back to LLMModelConfig inside the check.
  
  When calling this function directly (not via a rule), either a dict or an
  LLMModelConfig instance is accepted. The simplest dict form sets only *model_name*
  to a Databricks Model Serving endpoint. See the AI Explanations section of the Row
  Anomaly Detection reference docs for a full example.
- `registry_table`9 - Column names to exclude from the LLM prompt. Filters SHAP contribution
  map keys, the top-2 pattern key, and — when the scored model is segmented — any
  matching segment key (emitted as ``key=&lt;redacted&gt;`` so sensitive segmentation values
  never reach the prompt).
- `threshold`2 - Maximum number of distinct (segment, pattern) groups the LLM is called for
  per scoring run (default 500). Groups beyond this cap — ranked by
  group_size * group_avg_severity — get a null ai_explanation; a warning is logged.
- `threshold`3 - for segmented models the cap is split across eligible segments with a floor of
  one call each, so when ``max_groups`` is smaller than the number of eligible segments
  the effective call count is the segment count (a warning is logged). Size
  ``max_groups`` at or above your expected eligible-segment count to keep cost bounded.
- `threshold`8 - If True, score on the driver (no UDF). Use for tests or Spark Connect when
  worker UDF dependencies are not available. Default False for production.
  

**Returns**:

  Tuple of condition expression, apply function and info column name.
  

**Example**:

  Access anomaly metadata via _dq_info (array; first check = index 0):
  &gt;&gt;&gt; df_scored.select(col(&quot;_dq_info&quot;).getItem(0).getField(&quot;anomaly&quot;).getField(&quot;score&quot;), ...)
  &gt;&gt;&gt; df_scored.filter(col(&quot;_dq_info&quot;).getItem(0).getField(&quot;anomaly&quot;).getField(&quot;is_anomaly&quot;))

