---
sidebar_label: anomaly_llm_explainer
title: databricks.labs.dqx.anomaly.anomaly_llm_explainer
---

LLM-based group explanation for row anomaly detection.

The algorithm is group-based: anomalous rows are grouped by a deterministic
(segment, pattern) key — pattern being the sorted top-2 contributing features —
and the LLM is invoked once per group. Every row in a
group shares the same narrative/business_impact/action; group_size and
group_avg_severity signal that the explanation describes a pattern, not a row.

The LLM call runs entirely inside Spark via the SQL ``ai_query`` function against a
Databricks Model Serving endpoint — no driver collect of LLM output, scales with the
cluster, and needs no extra Python dependency.

## ExplanationContext Objects

```python
@dataclass(frozen=True)
class ExplanationContext()
```

Decoupled inputs for the LLM group explainer.

Any anomaly check that produces a severity column + contributions map can build one of
these and call ``add_explanation_column`` directly — no ScoringConfig required.

#### probe\_endpoint\_reachable

```python
def probe_endpoint_reachable(spark: object,
                             llm_model_config: LLMModelConfig | None) -> bool
```

Resolve the serving endpoint from *llm_model_config* and probe its reachability once.

Public entry point so a caller that invokes ``add_explanation_column`` repeatedly within a
single scoring run (e.g. *score_segmented*, once per segment) can probe **once** up front and
pass the result down via ``add_explanation_column(..., endpoint_reachable=...)``, instead of
paying one billable 1-token ``ai_query`` probe per segment.

**Raises**:

- `InvalidParameterError` - When *model_name* does not resolve to a Databricks serving endpoint.

#### add\_explanation\_column

```python
def add_explanation_column(
        df: DataFrame,
        ctx: ExplanationContext,
        segment_values: dict[str, str] | None,
        is_ensemble: bool,
        drift_summary: str = "none",
        endpoint_reachable: bool | None = None) -> DataFrame
```

Add the AI explanation column to df using the group-based algorithm.

Anomalous rows are bucketed by a deterministic (segment, pattern) key — pattern =
sorted top-2 contributing SHAP features. The LLM is called once per group via the Spark SQL
``ai_query`` function against a Databricks Model Serving endpoint, and every row in that group
receives the same narrative/business_impact/action, plus the group&#x27;s size and mean severity.
Rows below threshold or in groups exceeding ``ctx.max_groups`` receive a null struct.

Preconditions (caller&#x27;s responsibility):
- df has ctx.score_std_col, ctx.severity_col, and ctx.contributions_col.

**Arguments**:

- `df` - Scored DataFrame to annotate with the explanation column.
- `ctx` - Explanation inputs (columns, threshold, model, redaction, budget).
- `segment_values` - Segment key/value pairs for this run, or None for a global model.
- `is_ensemble` - Whether the scoring model is an ensemble (drives the confidence label).
- `drift_summary` - Baseline-drift summary string for the prompt, or &quot;none&quot;.
- `endpoint_reachable` - Pre-computed serving-endpoint reachability. When None (default) the
  endpoint is probed here with a single 1-token ai_query call. Callers that invoke this
  repeatedly in one scoring run (e.g. once per segment) should probe once via
  probe_endpoint_reachable and pass the result to avoid one billable probe per call.
  

**Raises**:

- ``0 - When *model_name* does not resolve to a Databricks serving endpoint.

