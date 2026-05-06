# Row Anomaly Detection

Use row anomaly detection to automatically find unusual rows in your data using ML (per‑record anomalies with explanations) without manually specifying thresholds so you can catch issues that rule-based checks miss. You provide recent good data; DQX trains a model and flags rows that don't fit typical patterns. No ML expertise required. Each flagged row includes an explainable breakdown of which columns drove the score, so you can see why it was flagged.

### Spot the odd banana.

Row anomaly detection in data quality — in a few minutes.

🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌

🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌

$ 

pip install 'databricks-labs-dqx\[anomaly]'pip install 'databricks-labs-dqx\[anomaly]'

BackNext

1

<!-- -->

/

<!-- -->

11

Use ← → arrow keys or Space to move between slides.

## What it is and why it helps[​](#what-it-is-and-why-it-helps "Direct link to What it is and why it helps")

Use row anomaly detection to learn typical patterns from recent "good" data and highlight rows that look unusual when you consider multiple columns together. You do not need to define thresholds or write rules; the model learns from your data and surfaces what stands out.

It works alongside your existing rule-based checks:

* **Rules** catch the issues you can anticipate and describe (for example, "amount must be positive").
* **Row anomaly detection** surfaces unusual patterns you did not think to write rules for, especially combinations across columns.

Example: amount, quantity, and discount might all pass your rules individually, but a row where all three are at the extreme end for your business could be rare. Anomaly detection can flag that row so you can review it.

## Why to use it[​](#why-to-use-it "Direct link to Why to use it")

Row anomaly detection finds issues that look "valid" to static rules but are still suspicious when you look at the full row. This is especially useful for:

* unexpected spikes or dips that are hard to encode as a rule
* unusual combinations across columns
* silent pipeline changes that shift data behavior without breaking schemas

In practice, it helps reduce blind spots and speeds up triage because you can focus on the top-ranked unusual rows first. It also helps you find the issues you didn't plan for. Manual rules can only catch what you anticipate; row anomaly detection surfaces what stands out as unusual in your data so you can catch problems before they affect downstream dashboards, reports, or AI models.

Because results are **explainable**, you don't just get a list of flagged rows. You see *why* each one was flagged, which makes investigation faster and builds trust in the output.

## When to use it[​](#when-to-use-it "Direct link to When to use it")

### Decision Matrix[​](#decision-matrix "Direct link to Decision Matrix")

| Scenario                      | Use Rule Checks           | Use Row Anomaly Detection    | Recommended Approach                         |
| ----------------------------- | ------------------------- | ---------------------------- | -------------------------------------------- |
| **Null values**               | Yes                       | Yes                          | Both may be useful depending on the scenario |
| **Schema validation**         | Yes, explicit             | No                           | Rules only                                   |
| **Known valid ranges**        | Yes, easy to define       | No (overkill)                | Rules only                                   |
| **Compliance checks**         | Yes, required for audit   | Partial (not audit-friendly) | Rules only                                   |
| **Unusual combinations**      | Partial (hard to specify) | Yes, natural fit             | Anomaly + Rules                              |
| **Temporal patterns**         | Partial (complex rules)   | Yes, auto-learns             | Anomaly + Rules                              |
| **Unknown/unexpected issues** | No (can't anticipate)     | Yes, discovers               | Anomaly only                                 |
| **Cross-column dependencies** | Partial (very complex)    | Yes, excels                  | Anomaly + Rules                              |

### Data Quality vs Domain-Specific Detection[​](#data-quality-vs-domain-specific-detection "Direct link to Data Quality vs Domain-Specific Detection")

**DQX Row Anomaly Detection focuses on data quality issues**:

* Unusual data patterns (statistical outliers)
* Distribution shifts (drift detection)
* Cross-column inconsistencies
* Temporal anomalies (timing-based patterns)

**Not designed for**:

* Completeness and freshness using table-level signals such as row counts or commit patterns
* Fraud detection (requires labeled fraud examples)
* Predictive maintenance (needs failure labels)
* Sentiment analysis (domain-specific NLP)
* Medical diagnosis (specialized models)

**When domain expertise is critical**: DQX can identify unusual patterns, but domain-specific fine-tuned models will outperform for specialized use cases (for example, credit card fraud, network intrusion).

**Use DQX for**: "Is this data unusual?"<br />**Use domain models for**: "Is this data fraudulent, faulty, or malicious?"

## Complements Databricks Data Quality Monitoring[​](#complements-databricks-data-quality-monitoring "Direct link to Complements Databricks Data Quality Monitoring")

[Databricks Data Quality Monitoring (DQM)](https://docs.databricks.com/aws/en/data-quality-monitoring/anomaly-detection) focuses on table-level signals like freshness and completeness. DQX row anomaly detection focuses on unusual rows and cross-column patterns. Use both together to cover:

* **Data availability**: Is new data arriving as expected?
* **Data content quality**: Are the rows themselves unusual or inconsistent?

Example: Data Quality Monitoring tells you new data arrived as expected; DQX flags a spike in unusual orders inside it.

Use both together

DQM and DQX each provide distinct capabilities. Together, they complement one another. Use Databricks data quality monitoring for table health (freshness/completeness) and DQX for discovering anomalies in the data.

## What you get out of the box in DQX[​](#what-you-get-out-of-the-box-in-dqx "Direct link to What you get out of the box in DQX")

Each row is scored and enriched with:

* **Severity percentile (0–100)**: how unusual the row is compared to training data.
* **Anomaly flag**: whether it crosses your chosen score threshold (default 95). You can tune this to control how many alerts you get.
* **Top contributors (explainability)**: which fields most influenced the anomaly score, so you can see *why* a row was flagged. Powered by SHAP, this turns a black-box score into an actionable insight.

You can tune the threshold and other options later if you need to reduce alert noise or catch more edge cases, but the defaults should work well for most use cases.

## Prerequisites[​](#prerequisites "Direct link to Prerequisites")

Install DQX with anomaly support:

```bash
pip install 'databricks-labs-dqx[anomaly]'

```

## Quick start[​](#quick-start "Direct link to Quick start")

Getting started is two steps: train once on recent "good" data, then run the same model as a check on new or incoming data. No configuration files or tuning required to begin. The example below uses defaults that work for most tables.

Training data requirements

**Quantity**: 1,000+ rows preferred; quality and coverage matter more than raw count. **Quality**: Use clean, representative data; anomalies in training are learned as "normal." **Coverage**: Include all realistic scenarios (regions, time ranges, categories). Have a very large table? DQX automatically samples a representative portion (default 30%, capped at 1M rows) before training. See [Row Anomaly Detection in Quality Checks](/dqx/docs/reference/quality_checks.md#row-anomaly-detection) for full parameter details.

Exclude identifier columns

Avoid ID-like columns (for example, `order_id`, `user_id`) in anomaly training. IDs often look unique and can drown out real patterns, which makes the model less useful. If you need to keep them in your table, use `exclude_columns` or provide an explicit `columns` list of behavioral fields (amounts, counts, rates, timestamps).

* Python
* Workflows

```python
import yaml
from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
anomaly_engine = AnomalyEngine(ws)

# ----------------------------------------
# Train row anomaly detection model
# ----------------------------------------

model_name = anomaly_engine.train(
  df=spark.table("catalog.schema.orders_training"),
  model_name="catalog.schema.orders_monitor",  # stored in Unity Catalog and must be fully qualified name
  columns=["amount", "quantity"],  # optional; omit to use all supported columns
  registry_table="catalog.schema.dqx_anomaly_models",  # stored in Unity Catalog and must be fully qualified name
)

# ----------------------------------------
# Define using classes and apply checks
# ----------------------------------------

# Note that columns are not specified in the check; Scoring uses the columns the model was trained on
checks = [
  DQDatasetRule(
    criticality="error",
    check_func=has_no_row_anomalies,
    check_func_kwargs={
      "model_name": "catalog.schema.orders_monitor",
      "registry_table": "catalog.schema.dqx_anomaly_models",
      # "threshold": 98,
    }
  ),
  # you can define other checks as well
]

dq_engine = DQEngine(ws, spark)
scored_df = dq_engine.apply_checks(spark.table("catalog.schema.orders"), checks)

# ----------------------------------------
# Define using metadata and apply checks
# ----------------------------------------

checks = yaml.safe_load("""
  - criticality: error
    check:
      function: has_no_row_anomalies
      arguments:
        model_name: catalog.schema.orders_monitor
        registry_table: catalog.schema.dqx_anomaly_models
        threshold: 98.0  # optional
  """)

dq_engine = DQEngine(ws, spark)
scored_df = dq_engine.apply_checks_by_metadata(spark.table("catalog.schema.orders"), checks)

```

Configure anomaly training per run config:

```yaml
run_configs:
- name: orders
  input_config:
    location: catalog.schema.orders
    anomaly_config:
      columns: [amount, quantity]  # optional; omit to use all supported columns
      model_name: catalog.schema.orders_monitor
      registry_table: catalog.schema.dqx_anomaly_models

```

Then trigger the anomaly-trainer workflow via the CLI:

```bash
# Train for all run configs
databricks labs dqx train-anomaly --timeout-minutes 60

# Train for a specific run config
databricks labs dqx train-anomaly --run-config orders --timeout-minutes 60

```

When not using serverless clusters (`serverless_clusters: false`), you can override cluster configuration for the anomaly-trainer workflow in `config.yml` using `anomaly_override_clusters` (map job cluster names to existing cluster IDs) and optionally `anomaly_spark_conf` for Spark settings. See [Configuration file](/dqx/docs/installation.md#configuration-file) for the full example.

Define checks separately (YAML, Python, etc.). See [Row Anomaly Detection in Quality Checks](/dqx/docs/reference/quality_checks.md#row-anomaly-detection).

## Investigate anomalies[​](#investigate-anomalies "Direct link to Investigate anomalies")

Anomalies are reported the same way as other checks, using the `_errors` and `_warnings` reporting columns in the output data. However, when you run a row anomaly detection check, DQX also adds a `_dq_info` column that contains structured metadata from the ML model scoring which is useful if we want to understand why a particular row was marked as anomalous.

The info column is an array of structs, with one element per anomaly detection check that was applied.

Feature contributions are disabled by default for faster scoring. You can enable with `enable_contributions` parameter:

```python
DQDatasetRule(
  criticality="error",
  check_func=has_no_row_anomalies,
  check_func_kwargs={
    "model_name": "catalog.schema.orders_monitor",  # fully qualified name
    "registry_table": "catalog.schema.dqx_anomaly_models",   # fully qualified name
    "enable_contributions": True,
  }
)

```

To review the top 20 unusual rows:

```python
import pyspark.sql.functions as F

top = scored_df.orderBy(
  F.col("_dq_info").getItem(0).getField("anomaly").getField("severity_percentile").desc()
).limit(20)

display(
  top.select(
    "transaction_id", "date", "amount", "quantity",
    F.col("_dq_info").getItem(0).getField("anomaly").getField("severity_percentile").alias("severity_percentile"),
    F.col("_dq_info").getItem(0).getField("anomaly").getField("contributions").alias("contributions"),
  )
)

```

Scores are normalized into `severity_percentile` (0–100). The anomaly threshold is a percentile cutoff (default 95) that you should tune to your data.

## How to choose a threshold[​](#how-to-choose-a-threshold "Direct link to How to choose a threshold")

The threshold controls how many rows are flagged as anomalies (percentile cutoff; default 95). Lower values (for example 90) flag more rows; higher values (for example 98) flag fewer (only extreme anomalies). Start with the default (95). If you get too many alerts, raise the threshold; if you are missing issues you care about, lower it.

## How it works under the hood[​](#how-it-works-under-the-hood "Direct link to How it works under the hood")

For full parameter and schema details, see [Row Anomaly Detection in Quality Checks](/dqx/docs/reference/quality_checks.md#row-anomaly-detection).

### Architecture overview[​](#architecture-overview "Direct link to Architecture overview")

1. **Feature engineering** (automatic): DQX detects column types and creates features (numerical standardized, categorical one-hot or frequency-encoded, temporal expanded to hour/day/month/weekend).
2. **Smart sampling and training**: DQX samples your data (default 30%, capped at 1M rows), trains an ensemble of Isolation Forest models, and captures baseline statistics for drift detection.
3. **Model registry**: Models and metadata live in MLflow and a Delta table; segmented models use deterministic names (for example `__seg_region=US_tier=gold`).
4. **Scoring and explanation**: Raw scores are normalized to a 0–100 severity percentile. Set `enable_contributions=True` for SHAP contributions (which features drove each score); default is `False` for faster scoring.
5. **Auto-discovery of columns and segments**: When you call `train()` without `columns` or `segment_by`, DQX automatically discovers both. It selects numeric columns with enough variance as features and may **auto-segment** when it finds suitable segment columns: categorical or low-cardinality columns with 2–50 distinct values, low null rate, and enough rows per segment (for example region, product category). If your auto-trained model is segmented, that is expected. To force a single global model, pass `segment_by=[]` (or omit segment columns from the data used for discovery) or set `columns` and `segment_by` explicitly.

### Why Isolation Forest?[​](#why-isolation-forest "Direct link to Why Isolation Forest?")

Isolation Forest measures how "easy" it is to isolate a data point; anomalies are isolated in few splits, normal points need many. It is fast, handles mixed types, is robust to noise, and is explainable via SHAP. DQX uses it by default because it fits data quality use cases without tuning.

### Output structure and options[​](#output-structure-and-options "Direct link to Output structure and options")

The `_dq_info` column is an array of structs (one element per dataset-level check that produces info; for row anomaly, one per `has_no_row_anomalies` check). Use `severity_percentile` for threshold decisions; raw `score` is for diagnostics only. Enable `drift_threshold` (for example `3.0`) to get warnings when the scoring distribution shifts from training so you know when to retrain.

Anomalous records can be identified using the standard reporting columns (`_errors` and `_warnings`). The `_dq_info[0].anomaly.is_anomaly` field provides additional detail for in-depth analysis and is set to `False` for records that are not anomalous.

### Schema of the info column (\_dq\_info)[​](#schema-of-the-info-column-_dq_info "Direct link to Schema of the info column (_dq_info)")

`_dq_info` has type **array of structs**. Each array element corresponds to one dataset-level check that writes info (for example one `has_no_row_anomalies` check). Element order matches the order of checks; the first anomaly check is at index `0`.

Each element is a **struct** with a shared “wide” schema. Currently, the only field populated by row anomaly detection is **`anomaly`**. Other check types may add more top-level fields in the future.

**Nested `anomaly` struct** (when the check is row anomaly detection):

| Field                 | Type                 | Description                                                                                                |
| --------------------- | -------------------- | ---------------------------------------------------------------------------------------------------------- |
| `check_name`          | string               | Always `"has_no_row_anomalies"` for this check.                                                            |
| `score`               | double               | Raw model score (0–1). Use for diagnostics only.                                                           |
| `severity_percentile` | double               | Normalized score 0–100. **Use this for thresholds and ordering.**                                          |
| `is_anomaly`          | boolean              | `true` if `severity_percentile` ≥ threshold.                                                               |
| `threshold`           | double               | Severity percentile threshold used (e.g. 95.0).                                                            |
| `model`               | string               | Full model name (e.g. Unity Catalog name).                                                                 |
| `segment`             | map\<string, string> | Segment key-value pairs for segmented models; `null` for global models.                                    |
| `contributions`       | map\<string, double> | Per-feature contribution percentages (0–100). Present when `enable_contributions=True`; `null` by default. |
| `confidence_std`      | double               | Ensemble score standard deviation. Present when `enable_confidence_std=True`; `null` otherwise.            |

**Access in PySpark:** use `F.element_at(F.col("_dq_info"), 1)` for the first element (1-based), then `.getField("anomaly").getField("severity_percentile")` etc. Alternatively `F.col("_dq_info").getItem(0)` for 0-based index (see [Troubleshooting](/dqx/docs/guide/row_anomaly_detection/troubleshooting.md) for Spark Connect–friendly patterns).

## Practical examples (non-technical)[​](#practical-examples-non-technical "Direct link to Practical examples (non-technical)")

* A sudden surge of high-value orders in a region that usually has low spend.
* Orders placed at unusual hours for a business that operates during the day.
* A big jump in quantity for a category that normally sells in small units.

## When to retrain[​](#when-to-retrain "Direct link to When to retrain")

DQX in the current version, does not retrain the model automatically. Retrain the model when "normal" changes: seasonality, new pricing rules, new products, or major pipeline changes. A simple cadence (monthly or quarterly) is often enough.

If you want to quarantine anomalies for investigation, use DQX quarantine pattern (see [Applying Checks](/dqx/docs/guide/quality_checks_apply.md)).

When to use row anomaly detection

Use row anomaly detection when you want to catch unusual combinations across columns that are hard to capture with static rules.

## Frequently Asked Questions[​](#frequently-asked-questions "Direct link to Frequently Asked Questions")

**Q: How much training data do I really need?**

See the **Training data requirements** tip under Quick start. In short: 1,000+ rows preferred; quality and coverage matter more than quantity. For segmented models, use at least 100+ rows per segment. Ensure training data includes all realistic values for categorical columns (regions, types, etc.).

**Q: How often should I retrain?**

**Retrain when**:

* Drift warnings appear (distribution changed). Enable `drift_threshold=3.0` to get warnings when retraining is needed
* Business logic changes (new products, pricing, processes)
* Seasonality shifts (quarterly/annual patterns)
* Major data pipeline changes

**Typical cadence**:

* Stable data: Monthly or quarterly
* Changing data: Weekly
* High volatility: Daily (with automation)

**Q: Why is my auto-trained model segmented?**

When you train without specifying `columns` or `segment_by`, DQX auto-discovers both. If your data has columns that look like good segment dimensions (for example region, category) — low cardinality (2–50 distinct values), low null rate, and enough rows per segment — DQX will train **one model per segment**. That is intentional: segmented models often fit better when behavior differs by segment. To get a single global model instead, pass `segment_by=[]` or provide explicit `columns` (and no segment columns) when calling `train()`.

**Q: Does this work with streaming data?**

**Yes!** Train on batch, score on streaming:

1. Train model on historical batch data
2. Apply checks to streaming DataFrame
3. **Recommended**: Disable contributions (default): `enable_contributions=False` (SHAP is too slow for real-time processing)

**Q: Can I use this with PII/sensitive data?**

**Yes**, with considerations:

**Safe**:

* Training happens in your Databricks environment (data never leaves).
* Models are stored in Unity Catalog that you control.
* There are no external API calls.

**Consider**:

* SHAP contributions may expose sensitive patterns in explanations.
* Model metadata includes column names and statistics.
* Use column-level security for model registry if needed.

**Recommendation**: Train on pseudonymized features or aggregate metrics when possible.

**Q: How does this compare to Databricks Data Quality Monitoring?**

**They're complementary, not competing**:

**Data Quality Monitoring**:

* Table-level metrics (completeness and freshness)
* Column-level statistics (mean, stddev, null rate)

**DQX Row Anomaly Detection**:

* Row-level anomaly scores
* Cross-column pattern detection
* Per-row explanations (SHAP contributions)
* Can be applied together with rule-based checks

**Use both**: Data Quality Monitoring for table health + DQX for row-level issues inside the data.

**Q: Why am I getting different results each training run?**

**Cause**: Isolation Forest uses randomness (bootstrap sampling, random splits) for ensemble diversity.

**Use seed to get reproducible results** (testing/debugging only):

```python
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig

# Set fixed base random seed in algorithm config
params = AnomalyParams(
  algorithm_config=IsolationForestConfig(
    random_seed=42  # base seed for reproducibility
  )
)

anomaly_engine.train(
  df=df,
  model_name=model_name,
  registry_table=registry_table,
  params=params,  # pass params with fixed seed
)

```

Ensemble seed handling

Each ensemble model automatically gets a different seed (base\_seed + model\_index). With `random_seed=42` and `ensemble_size=3`:

* Model 0 → seed 42
* Model 1 → seed 43
* Model 2 → seed 44

This ensures ensemble diversity while maintaining reproducibility.

**Important**: This is NOT recommended for production. Small variations between runs are:

* **Expected**: Due to ensemble randomness (default: 3 models with seeds 42, 43, 44)
* **Healthy**: Ensemble diversity improves robustness
* **Acceptable**: Focus on trend consistency (for example, "top 10 anomalies stay in top 20"), not exact scores

**Production approach**: If you need stability, retrain less frequently and version your models using the registry.

**Q: What's the difference between columns and features?**

**Columns** = Raw DataFrame columns you specify in `columns=["a", "b", "c"]`

**Features** = Engineered inputs to the ML model (after transformation)

**Example**:

```python
# You specify 3 columns
columns=["amount", "order_time", "region"]

# Model sees ~8 features after transformation:
# - amount → 1 feature (scaled)
# - order_time → 4 features (hour, day_of_week, month, is_weekend)
# - region → 3 features (if 3 unique regions: region_north, region_south, region_east)

```

**Why it matters**: Training time depends on number of features (not columns). More categorical/temporal columns → more features → slower training.

**Optimization**: If training is slow, reduce high-cardinality categorical columns or exclude unnecessary temporal columns.

## Troubleshooting[​](#troubleshooting "Direct link to Troubleshooting")

Having issues? See the [Troubleshooting Guide](/dqx/docs/guide/row_anomaly_detection/troubleshooting.md) for solutions to common problems.

## Next steps[​](#next-steps "Direct link to Next steps")

For training options, parameters, and YAML examples, see [Row Anomaly Detection in Quality Checks](/dqx/docs/reference/quality_checks.md#row-anomaly-detection) section.
