# AI-Explained Anomalies — Implementation Plan

> **Issue**: #957 — related to #959 (ML anomaly detection)
> **Branch**: `feature/AI_Explained_Anomalies_for_has_no_anomalies`
> **Status**: Planning

---

## 1. Problem Statement

The current anomaly output from `has_no_row_anomalies` is technically rich but hard for non-ML users to interpret:

```
_dq_info[0].anomaly.score               → 0.721
_dq_info[0].anomaly.severity_percentile → 98.3
_dq_info[0].anomaly.is_anomaly          → true
_dq_info[0].anomaly.threshold           → 95.0
_dq_info[0].anomaly.model               → catalog.schema.orders_anomaly_model
_dq_info[0].anomaly.segment             → {region: US}
_dq_info[0].anomaly.contributions       → {amount: 85.0, quantity: 10.0, discount: 5.0}
_dq_info[0].anomaly.confidence_std      → 0.04
```

Teams need faster root-cause triage. A data analyst should be able to read one sentence and know why a group of rows was flagged. The feature request in #957 explicitly calls for *"grouping similar anomalies into 'patterns' with summary labels"* — which is also the right answer economically: per-row LLM calls on millions of rows is neither affordable nor necessary when anomalies cluster into a small number of root-cause patterns.

---

## 2. Proposed Solution

Add an optional **group-based** LLM step at the tail of the scoring pipeline:

1. Compute a deterministic `pattern` key per anomalous row (sorted top-2 contributing features, joined with `+`).
2. Group anomalous rows by `(segment, pattern)`.
3. Call the LLM **once per group** (not per row) with aggregate contributions, group size, and severity range.
4. Fan the group's explanation back to each row via a LEFT JOIN.

Per-row LLM calls are eliminated. A 10M-row table with 50k anomalies typically produces 20–200 distinct `(segment, pattern)` groups → 20–200 LLM calls for the entire run.

Each anomalous row receives:

```
_dq_info[0].anomaly.ai_explanation.narrative          → plain-English "why this group was flagged"
_dq_info[0].anomaly.ai_explanation.business_impact    → one-sentence likely business impact
_dq_info[0].anomaly.ai_explanation.pattern            → deterministic grouping key ("amount+quantity")
_dq_info[0].anomaly.ai_explanation.action             → recommended follow-up
_dq_info[0].anomaly.ai_explanation.group_size         → number of rows in this (segment, pattern) group
_dq_info[0].anomaly.ai_explanation.group_avg_severity → mean severity_percentile across the group
```

Rows within the same `(segment, pattern)` group share the same `narrative`, `business_impact`, and `action` strings — intentionally. That is the whole point of grouping: one story per root cause, replicated wherever that root cause appears. The `group_size` and `group_avg_severity` fields signal to users that the explanation describes a *pattern*, not a specific row.

**Key design constraints:**
- Opt-in only (`enable_ai_explanation=False` by default)
- No raw data values sent to the LLM — only aggregate SHAP percentages, segment labels, group statistics
- Configurable redaction: `redact_columns` excludes specified feature names from the LLM prompt
- Requires `enable_contributions=True` (SHAP contributions are the primary grouping and LLM input)
- Driver-side reduce — no `pandas_udf` LLM calls; group counts are bounded and small

---

## 3. How the Anomaly Pipeline Works Today

### 3.1 Entry point: `anomaly/check_funcs.py:25`

```python
@register_rule("dataset")
def has_no_row_anomalies(
    model_name: str,
    registry_table: str,
    threshold: float = 95.0,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    enable_contributions: bool = False,
    enable_confidence_std: bool = False,
    *,
    driver_only: bool = False,
) -> tuple[Column, Any, str]:
```

Returns `(condition_expr, apply_fn, info_col_name)`. The `apply_fn` transforms the user's DataFrame.

### 3.2 Full pipeline (inside `apply_fn`)

```
Input DataFrame (user columns)
    ↓
Add __dqx_row_id_<uuid>
    ↓
fetch_model_columns_and_segments()  — queries registry for model metadata
    ↓
ScoringConfig created (internal columns uuid-suffixed to avoid collisions)
    ↓
run_anomaly_scoring()                 [scoring_orchestrator.py]
  └─ IF global model:  score_global_model()         [scoring_run.py:35]
  └─ IF segmented:     score_segmented()             [scoring_run.py:247]
       └─ per segment: score_single_segment()        [scoring_run.py:175]

  Inside score_global_model() / score_single_segment():
    1. apply_row_filter()            — optional SQL filter
    2. check_and_warn_drift()        — distribution shift warning (captured for LLM)
    3. score_with_sklearn_model() / score_ensemble_models()
       → adds: anomaly_score, anomaly_score_std, anomaly_contributions (if enabled)
    4. rename to uuid-suffixed names (score_col, score_std_col, contributions_col)
    5. add_severity_percentile_column()
       → adds: __dq_severity_percentile_<uuid>  (0–100)
    6. *** GROUP-BASED LLM STEP GOES HERE ***
    7. add_info_column()
       → builds _dq_info[0].anomaly struct
       → is_anomaly computed inline: severity_col >= threshold
    8. drop internal cols (score_std_col, severity_col, contributions_col, ai_explanation_col)
    9. join_filtered_results_back()  — if row_filter was used
   10. drop score_col, row_id
    ↓
Output: user columns + _dq_info
```

### 3.3 Internal columns at the LLM insertion point

| Internal column name | Type | Maps to `_dq_info` field |
|---|---|---|
| `__dq_anomaly_score_<uuid>` | `double` | `.score` |
| `__dq_severity_percentile_<uuid>` | `double` | `.severity_percentile` |
| `__dq_anomaly_contributions_<uuid>` | `map<string, double>` | `.contributions` |
| `__dq_anomaly_score_std_<uuid>` | `double` | `.confidence_std` |

Python-level values (not columns): `config.threshold`, `config.model_name`, `segment_model.segmentation.segment_values`, `drift_report` from step 2.

> **Important**: `is_anomaly` and `segment` do **not** exist as DataFrame columns at this point.
> - `is_anomaly` is computed inline inside `add_info_column()`.
> - `segment_values` is a Python dict passed through into the group-reduce step.

### 3.4 `anomaly_info_schema.py` — schema after this change

```python
anomaly_info_struct_schema = StructType([
    StructField("check_name",          StringType(),                         True),
    StructField("score",               DoubleType(),                         True),
    StructField("severity_percentile", DoubleType(),                         True),
    StructField("is_anomaly",          BooleanType(),                        True),
    StructField("threshold",           DoubleType(),                         True),
    StructField("model",               StringType(),                         True),
    StructField("segment",             MapType(StringType(), StringType()),  True),
    StructField("contributions",       MapType(StringType(), DoubleType()),  True),
    StructField("confidence_std",      DoubleType(),                         True),
    StructField("ai_explanation",      ai_explanation_struct_schema,         True),   # NEW
])
```

---

## 4. New Output: `ai_explanation` Struct

### Schema

```python
ai_explanation_struct_schema = StructType([
    StructField("narrative",           StringType(),  True),
    StructField("business_impact",     StringType(),  True),
    StructField("pattern",             StringType(),  True),   # deterministic
    StructField("action",              StringType(),  True),
    StructField("group_size",          LongType(),    True),   # rows sharing this (segment, pattern)
    StructField("group_avg_severity",  DoubleType(),  True),   # mean severity in the group
])
```

### Example values (shared by every row in the `US / amount+quantity` group)

```
narrative:          "Amount and quantity jointly drive this pattern: 312 US orders show amount
                     contributing ~82% and quantity ~11% of the anomaly signal, at a mean severity
                     of 97.4. The ensemble is in high agreement, pointing to a likely unit
                     mismatch at ingestion."

business_impact:    "Potential revenue miscalculation or inventory overcommit across US orders
                     if the unit mismatch propagates downstream."

pattern:            "amount+quantity"      # deterministic, sorted top-2
action:             "Verify the unit of measure for 'amount' on US orders ingested in this window."
group_size:         312
group_avg_severity: 97.4
```

### Nullability

| Condition | `ai_explanation` |
|---|---|
| `enable_ai_explanation=False` (default) | `null` |
| `severity_percentile < threshold` (row not flagged) | `null` |
| Flagged row belongs to a group that exceeded `max_groups` budget | `null` (count surfaced via warning) |
| Otherwise | Populated struct |

---

## 5. New Parameters on `has_no_row_anomalies()`

```python
def has_no_row_anomalies(
    model_name: str,
    registry_table: str,
    threshold: float = 95.0,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    enable_contributions: bool = False,
    enable_confidence_std: bool = False,
    # ↓ NEW
    enable_ai_explanation: bool = False,
    llm_model_config: LLMModelConfig | None = None,
    redact_columns: list[str] | None = None,
    max_groups: int = 500,
    *,
    driver_only: bool = False,
) -> tuple[Column, Any, str]:
```

### Validation

| Condition | Error |
|---|---|
| `enable_ai_explanation=True` and `enable_contributions=False` | `InvalidParameterError`: contributions required as LLM input |
| `redact_columns` provided but not a `list` | `InvalidParameterError` |
| `max_groups` not a positive int | `InvalidParameterError` |
| `enable_ai_explanation=True` and `dspy` not importable | `InvalidParameterError`: "requires the `[llm]` extra. Install: `pip install databricks-labs-dqx[anomaly,llm]`" |

### Defaults

- `llm_model_config=None` → `LLMModelConfig()` → `"databricks/databricks-claude-sonnet-4-5"`
- `redact_columns=None` → no redaction
- `max_groups=500` — groups beyond this cap (ranked by `group_size × avg_severity`) get null explanations. A warning is logged with the count of uncovered groups and rows. **No silent truncation.**

---

## 6. DSPy Signature — The Group-Level Prompt

```python
class AnomalyGroupExplanationSignature(dspy.Signature):
    """You are a data quality analyst. Given aggregate metadata for a GROUP of anomalous rows
    sharing the same root-cause pattern, explain in plain business language why this group was
    flagged. Your explanation will be shown for every row in the group — describe the pattern,
    not a specific row."""

    # ── Inputs ───────────────────────────────────────────────────────────────

    feature_contributions: str = dspy.InputField(
        desc="Mean SHAP contributions across the group, e.g. "
             "'amount (82%), quantity (11%), discount (5%)'. "
             "These are aggregated relative importances — not raw data values."
    )
    group_size: str = dspy.InputField(
        desc="Number of rows in this group, e.g. '312 rows'."
    )
    severity_range: str = dspy.InputField(
        desc="Severity percentile range across the group, e.g. 'mean 97.4, min 95.1, max 99.8'."
    )
    confidence: str = dspy.InputField(
        desc="Model confidence label across the group. 'high' / 'mixed' / 'low' for ensemble, "
             "'n/a' for single-model scoring."
    )
    segment: str = dspy.InputField(
        desc="Data segment this group belongs to, e.g. 'region=US, product=electronics'. "
             "Empty string if no segmentation was used."
    )
    threshold: str = dspy.InputField(
        desc="The severity percentile threshold configured by the user (0–100)."
    )
    model_name: str = dspy.InputField(
        desc="Name of the anomaly detection model that scored this group."
    )
    drift_summary: str = dspy.InputField(
        desc="Baseline drift signal from the scoring run, e.g. "
             "'drift detected: amount KS=0.42; quantity KS=0.31' or 'none'. "
             "If drift is present, explicitly frame the narrative vs baseline."
    )

    # ── Outputs ──────────────────────────────────────────────────────────────

    narrative: str = dspy.OutputField(
        desc="Max 2 sentences, max 40 words total. Describe the GROUP pattern, not a single row. "
             "Reference the top contributing features and the group size. "
             "If drift_summary != 'none', frame at least one feature vs baseline."
    )
    business_impact: str = dspy.OutputField(
        desc="One sentence, max 25 words. Likely downstream business impact if this group of "
             "rows is processed unchanged. Concrete, tied to the contributing features."
    )
    action: str = dspy.OutputField(
        desc="One sentence, max 20 words. What a data analyst should investigate for this group."
    )

    # NOTE: `pattern`, `group_size`, `group_avg_severity` are NOT LLM outputs.
    # They are computed deterministically outside the signature.
```

`dspy.Predict` (single-shot) is used — no few-shot training needed for this task.

---

## 7. New File: `anomaly/anomaly_llm_explainer.py`

### 7.1 Public entry point — `add_explanation_column()`

This is the single public function. Any current or future scoring path that produces the required columns can call it.

```python
def add_explanation_column(
    df: DataFrame,
    config: ScoringConfig,
    segment_values: dict[str, str] | None,
    is_ensemble: bool,
    drift_report: DriftReport | None,
) -> DataFrame:
    """
    Add the AI explanation column to df by grouping anomalous rows on (segment, pattern),
    calling the LLM once per group, and joining the result back.

    Preconditions (caller's responsibility):
      - config.enable_ai_explanation is True
      - config.enable_contributions is True
      - dspy is importable
      - df has score_col, score_std_col, severity_col, contributions_col
    """
```

### 7.2 Algorithm

```
1. DETERMINISTIC PATTERN (distributed, cheap)
   Add a temporary `pattern` column via a small pandas_udf over contributions_col.
   pattern = "+".join(sorted(top-2 feature names by SHAP %))
   Features in `redact_columns` are excluded BEFORE the top-2 selection.
   Empty contributions → "unknown".

2. FILTER + AGGREGATE (distributed)
   anomalous = df.filter(severity_col >= threshold)
   groups = anomalous.groupBy(pattern).agg(
       F.count("*").alias("group_size"),
       F.avg(severity_col).alias("group_avg_severity"),
       F.min(severity_col).alias("severity_min"),
       F.max(severity_col).alias("severity_max"),
       F.avg(score_std_col).alias("mean_std"),
       mean_map_agg(contributions_col).alias("mean_contributions"),  # UDAF or explode+avg+collect
   )
   groups_pd = groups.toPandas()                  # bounded: N distinct patterns

   NOTE: `segment` is a Python dict passed in — it is the SAME for every row in this
   scoring call (global model or single segment), so we do NOT groupBy on it here.
   score_segmented() calls add_explanation_column() once per segment.

3. BUDGET (driver)
   Rank groups by `group_size * group_avg_severity` desc.
   Take top `config.max_groups`. Log warning if any groups were dropped:
     "ai_explanation: {dropped_n} groups covering {dropped_rows} rows exceeded
      max_groups={max_groups}; their ai_explanation will be null."

4. LLM LOOP (driver — sequential, one call per group)
   For each kept group row:
     - format feature_contributions string (top-5, redacted)
     - derive confidence (see is_ensemble rules, aggregated across group)
     - call predictor(...) once
     - collect (pattern, narrative, business_impact, action)

5. BUILD RESULT DF + JOIN (distributed)
   result_df = spark.createDataFrame(
       [(pattern, group_size, group_avg_severity, narrative, business_impact, action), ...],
       schema=explanation_groups_schema,
   )
   return df.withColumn("pattern", F.col("pattern")) \
            .join(result_df, on="pattern", how="left") \
            .withColumn(
                config.ai_explanation_col,
                F.when(
                    F.col(config.severity_col) >= config.threshold,
                    F.struct(...),  # narrative, business_impact, pattern, action, group_size, group_avg_severity
                ).otherwise(F.lit(None).cast(ai_explanation_struct_schema)),
            ) \
            .drop("pattern")
```

### 7.3 Confidence aggregation (ensemble)

Across a group, `mean_std` determines confidence:
- single-model (`is_ensemble=False`) → `"n/a"`
- `is_ensemble=True` and `mean_std < 0.05` → `"high"`
- `is_ensemble=True` and `mean_std < 0.15` → `"mixed"`
- else → `"low"`

### 7.4 `driver_only=True` path

Identical algorithm — step 1's `pattern` computation runs as a local pandas transform via `.collect()` → Spark DataFrame round-trip, matching `score_with_sklearn_model_local` style. Steps 2–5 are unchanged because they are already driver-side after the aggregation.

---

## 8. Insertion Points in `scoring_run.py`

> **Strategy-pattern note:** `scoring_orchestrator.py` routes through `IsolationForestScoringStrategy` which delegates to the functions below. Strategies that bypass `score_global_model` / `score_single_segment` must call `add_explanation_column()` themselves.

### `score_global_model()` (line 35)

```python
# EXISTING: after add_severity_percentile_column()
scored_df = add_severity_percentile_column(scored_df, ...)

# NEW
if config.enable_ai_explanation:
    is_ensemble = len(record.identity.model_uri.split(",")) > 1
    scored_df = add_explanation_column(
        scored_df, config, segment_values=None, is_ensemble=is_ensemble,
        drift_report=drift_report,
    )

# EXISTING: add_info_column() — now also receives ai_explanation_col
scored_df = add_info_column(scored_df, ..., ai_explanation_col=config.ai_explanation_col)

# EXISTING: cleanup
internal_to_remove = [config.score_std_col, config.severity_col]
if config.enable_contributions:
    internal_to_remove.append(config.contributions_col)
if config.enable_ai_explanation:
    internal_to_remove.append(config.ai_explanation_col)
```

### `score_single_segment()` (line 175)

```python
segment_scored = add_severity_percentile_column(segment_scored, ...)

if config.enable_ai_explanation:
    is_ensemble = len(segment_model.identity.model_uri.split(",")) > 1
    segment_scored = add_explanation_column(
        segment_scored, config,
        segment_values=segment_model.segmentation.segment_values,
        is_ensemble=is_ensemble,
        drift_report=segment_drift_report,
    )

segment_scored = add_info_column(segment_scored, ..., ai_explanation_col=config.ai_explanation_col)
```

The `ai_explanation_col` drop for segmented scoring happens in `score_segmented()` ([scoring_run.py:293-297](src/databricks/labs/dqx/anomaly/scoring_run.py#L293-L297)) alongside the other internal-column drops.

---

## 9. `ScoringConfig` Changes (`scoring_config.py`)

```python
@dataclass
class ScoringConfig:
    # ... all existing fields unchanged ...

    # NEW
    enable_ai_explanation: bool = False
    ai_explanation_col: str = "ai_explanation"        # set to uuid-suffixed name in check_funcs.py
    llm_model_config: LLMModelConfig | None = None
    redact_columns: list[str] | None = None
    max_groups: int = 500
```

---

## 10. `scoring_utils.py` — `add_info_column()` Change

```python
def add_info_column(
    df: DataFrame,
    model_name: str,
    threshold: float,
    info_col_name: str,
    segment_values: dict[str, str] | None = None,
    enable_contributions: bool = False,
    enable_confidence_std: bool = False,
    ai_explanation_col: str | None = None,          # NEW
    score_col: str = "anomaly_score",
    score_std_col: str = "anomaly_score_std",
    contributions_col: str = "anomaly_contributions",
    severity_col: str = "severity_percentile",
) -> DataFrame:
    ...
    if ai_explanation_col and ai_explanation_col in df.columns:
        anomaly_info_fields["ai_explanation"] = F.col(ai_explanation_col)
    else:
        anomaly_info_fields["ai_explanation"] = F.lit(None).cast(ai_explanation_struct_schema)
```

---

## 11. Files Changed Summary

| File | Type | What changes |
|---|---|---|
| `anomaly/anomaly_llm_explainer.py` | **New** | `AnomalyGroupExplanationSignature`, `add_explanation_column()` (group-based), helper for mean-contributions aggregation |
| `anomaly/anomaly_info_schema.py` | Modified | Add `ai_explanation_struct_schema` (6 fields); add `ai_explanation` field to `anomaly_info_struct_schema` |
| `anomaly/scoring_utils.py` | Modified | `add_info_column()` gains `ai_explanation_col` param; null struct when absent |
| `anomaly/scoring_run.py` | Modified | Insert group-based LLM step in `score_global_model()` and `score_single_segment()`; drop `ai_explanation_col` in cleanup |
| `anomaly/scoring_config.py` | Modified | 5 new fields: `enable_ai_explanation`, `ai_explanation_col`, `llm_model_config`, `redact_columns`, `max_groups` |
| `anomaly/check_funcs.py` | Modified | 4 new params, validation guard, wire `ScoringConfig`, set uuid-suffixed `ai_explanation_col` |

---

## 12. YAML Usage Example

```yaml
- criticality: warn
  check:
    function: has_no_row_anomalies
    arguments:
      model_name: catalog.schema.orders_anomaly_model
      registry_table: catalog.schema.dqx_anomaly_registry
      threshold: 95.0
      enable_contributions: true          # required — SHAP feeds the grouping and LLM
      enable_ai_explanation: true         # opt-in
      redact_columns: [customer_id, ssn]  # feature names excluded from LLM prompt
      max_groups: 500                     # cap LLM calls per scoring run
```

### Accessing the output

```python
from pyspark.sql import functions as F

ai = F.col("_dq_info")[0]["anomaly"]["ai_explanation"]

df.select(
    F.col("_dq_info")[0]["anomaly"]["severity_percentile"].alias("severity"),
    ai["pattern"].alias("pattern"),
    ai["group_size"].alias("group_size"),
    ai["group_avg_severity"].alias("group_avg_severity"),
    ai["narrative"].alias("why_flagged"),
    ai["business_impact"].alias("impact"),
    ai["action"].alias("next_step"),
).filter(F.col("_dq_info")[0]["anomaly"]["is_anomaly"]).distinct().show(truncate=False)
```

Because rows in the same `(segment, pattern)` group share the same explanation strings, `.distinct()` on `(pattern, narrative, action)` gives you a natural root-cause summary table.

---

## 13. Data Governance — Redaction

Only **feature names, aggregate SHAP percentages, group counts, severity statistics, and segment labels** reach the LLM. No raw data values are ever included. The group-based aggregation strengthens this guarantee — by construction, inputs to the LLM are statistics over many rows.

`redact_columns` removes specific feature names from the `feature_contributions` string **before** the deterministic `pattern` computation, so a redacted column never appears in `pattern` either:

```
Without redaction:             pattern="customer_id+amount",  contributions="customer_id (62%), amount (28%), region (10%)"
With redact_columns=[customer_id]:  pattern="amount+region",   contributions="amount (28%), region (10%)"
```

Remaining percentages are passed as-is (not re-normalized).

---

## 14. Cost and Performance Profile

**LLM calls per run:**
- Upper bound: `min(distinct (segment, pattern) groups in flagged rows, max_groups)`
- Typical: 20–200 for a 10M-row table with 50k anomalies

**Driver memory:** Groups are aggregated distributed, then `.toPandas()` on a result bounded by `max_groups` — trivial memory.

**Latency:** Dominated by sequential LLM calls on the driver. ~1–3s per group → a 200-group run completes in 3–10 minutes. Future optimization: thread-pool parallel calls within the `max_groups` budget (follow-up F5).

**Row coverage:** Rows in over-budget groups get `ai_explanation=null` and the row is still flagged by the rest of the `_dq_info` struct. A warning is logged with the uncovered group and row counts. No silent data loss.

---

## 15. Tests

### 15.1 Unit tests — validation (`tests/unit/test_anomaly_check_funcs_validation.py`)

- `enable_ai_explanation=True` + `enable_contributions=False` → `InvalidParameterError`
- `redact_columns=123` (not a list) → `InvalidParameterError`
- `max_groups=0` or negative → `InvalidParameterError`
- `enable_ai_explanation=False` (default) → `ai_explanation` null in schema, no DSPy import triggered

### 15.2 New unit test file: `tests/unit/test_anomaly_llm_explainer.py`

DSPy `Predict` is mocked; tests assert on the grouping/aggregation/join mechanics and on prompt inputs.

- **Pattern determinism**: same contributions map → same `pattern` key (e.g. `amount+quantity`), stable across rows
- **Pattern is redaction-aware**: `redact_columns=["customer_id"]` → `customer_id` never appears in any `pattern` or `feature_contributions` string
- **One LLM call per group**: 1000 input rows across 3 patterns → exactly 3 `predictor()` calls
- **Shared explanation within group**: all rows with the same `(segment, pattern)` receive identical `narrative`/`business_impact`/`action`
- **Non-anomalous rows** (`severity < threshold`) → `ai_explanation` is null; those rows do NOT contribute to group statistics
- **Row count invariant**: `output.count() == input.count()`
- **`group_size` correctness**: equals number of flagged rows with that `(segment, pattern)`
- **`group_avg_severity` correctness**: equals the mean severity of rows in the group (within float tolerance)
- **`max_groups` budget**: given 10 groups with `max_groups=3`, the top 3 by `size * avg_severity` get populated structs, the other 7 get null `ai_explanation` AND a warning is emitted with the dropped count
- **Confidence aggregation**: single-model → `"n/a"`; ensemble mean_std 0.02 → `"high"`; 0.10 → `"mixed"`; 0.30 → `"low"`
- **Drift plumbing**: `drift_report=None` → `drift_summary="none"` in prompt; populated report → formatted string in prompt
- **Segment formatting**: `{region: US, product: electronics}` → `"region=US, product=electronics"` (comma-space, NOT underscores from `build_segment_name()`)

### 15.3 Integration test: `tests/integration_anomaly/test_anomaly_ai_explanation.py`

_(Requires live Databricks workspace — skipped without env)_

- Train model → score with `enable_contributions=True, enable_ai_explanation=True`
- Assert `_dq_info[0].anomaly.ai_explanation.narrative` is non-empty for `is_anomaly=True` rows
- Assert `_dq_info[0].anomaly.ai_explanation` is null for `is_anomaly=False` rows
- Assert rows sharing the same `pattern` also share the same `narrative` text
- Assert redacted column names absent from both `narrative` and `pattern`
- Assert `pattern` matches `/^[a-z_]+(\+[a-z_]+)?$/` (sorted feature names joined with `+`, or `"unknown"`)
- Segmented model: assert the segment context appears in at least one flagged group's `narrative`

---

## 16. Verification Commands

```bash
make fmt && make lint
make test
hatch run pytest tests/unit/test_anomaly_check_funcs_validation.py -v
hatch run pytest tests/unit/test_anomaly_llm_explainer.py -v
make integration
```

---

## 17. Scope Decision: `has_no_row_anomalies` vs `AnomalyEngine`

`AnomalyEngine` is currently training-only (`.train()`, no `.score()`). All scoring goes through `has_no_row_anomalies` in the DQX rule framework, so the explanation feature lives in the scoring pipeline. A future PR can add `AnomalyEngine.score()` exposing the same group-based explanation directly for ad-hoc use outside `apply_checks`. Out of scope here — see §18 F1.

---

## 18. Follow-ups (Out of Scope for This PR)

| # | Item | Why deferred |
|---|---|---|
| F1 | **`AnomalyEngine.score()` with AI explanation** — expose scoring + explanation directly on the engine for users not using the DQX rule framework. | Requires designing `AnomalyEngine.score()` from scratch. Own API design decision. |
| F2 | **Semantic cross-pattern clustering** — instead of deterministic top-2 pattern keys, cluster groups by SHAP vector similarity before LLM summarization. | Needs a driver-side clustering step and a tuning story. The deterministic `(segment, pattern)` key already collapses the LLM-call budget by 100–1000×. |
| F3 | **Surface `ai_explanation` in DQX dashboard and workflow reports.** | Separate dashboards-module change. The struct is already queryable in `_dq_info` today. |
| F4 | **Configurable confidence cutoffs** (currently hardcoded 0.05 / 0.15). | No user signal yet that defaults are wrong. |
| F5 | **Thread-pooled parallel LLM calls** within the `max_groups` budget. | Sequential is simple and already cheap post-grouping. Parallelize only if latency becomes a complaint. |
| F6 | **Expose `top_n` (SHAP features in prompt)** as a public parameter. | Hardcoded 5 is a reasonable default. |

---

## 19. Open Questions

| # | Question | Current assumption |
|---|---|---|
| 1 | Confidence cutoffs (0.05 / 0.15) — configurable? | Hardcoded |
| 2 | `top_n` (max SHAP features in prompt) — expose as parameter? | Hardcoded at 5 |
| 3 | DSPy `max_retries=3` sufficient for provider rate limits? | Yes for now; revisit after first real run |
| 4 | Should `ai_explanation` appear in DQX dashboard/report outputs? | Follow-up F3 |
| 5 | Default `max_groups=500` — too high / too low? | Revisit after first production run |
