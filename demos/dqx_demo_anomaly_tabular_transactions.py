# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Finding the Transactions Your Rules Will Never Catch
# MAGIC
# MAGIC ## Learn Row Anomaly Detection on Business Records in 10 Minutes
# MAGIC
# MAGIC **What you'll do:**
# MAGIC - Write the quality rules a good payments team would already have
# MAGIC - Watch them pass a batch that contains real problems
# MAGIC - Train a DQX anomaly model with no thresholds and no labels
# MAGIC - Read *why* each row was flagged, in plain language
# MAGIC
# MAGIC **Dataset**: Card transactions across twelve merchant categories (no domain expertise required)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## The problem: a row can be wrong without any value being wrong
# MAGIC
# MAGIC A payments team has good rules. Amount is positive and under the card limit. Quantity is at least
# MAGIC one. The merchant category is one they support. Every rule passes, every day, and the dashboards
# MAGIC are green.
# MAGIC
# MAGIC Then a reconciliation breaks, and someone finds a **£4 grocery basket with 38 items** in it, and a
# MAGIC **£900 coffee**. Both were inside every threshold. Neither was flagged.
# MAGIC
# MAGIC **Known vs unknown issues**
# MAGIC - **Known unknowns**: nulls, ranges, formats. Write a rule — it is cheap, clear and versioned.
# MAGIC - **Unknown unknowns**: a combination of values that is individually ordinary and jointly absurd.
# MAGIC   There is no single column to write the rule against.
# MAGIC
# MAGIC **"Normal" also depends on context.** £900 is unremarkable for electronics and absurd for coffee. A
# MAGIC threshold that catches the coffee rejects half the laptops. DQX handles this with `baseline_by`,
# MAGIC which judges every row against **its own group's** normal rather than the whole table's.
# MAGIC
# MAGIC Use rules *and* anomaly detection. Rules catch what you can describe; anomaly detection covers
# MAGIC what is left.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites: Install DQX with Anomaly Support
# MAGIC
# MAGIC ```python
# MAGIC %pip install 'databricks-labs-dqx[anomaly]'
# MAGIC ```
# MAGIC
# MAGIC **Note**: On ML Runtime or Serverless most dependencies are already present.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Install DQX

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[anomaly] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install 'databricks-labs-dqx[anomaly]'

%restart_python

# COMMAND ----------
# DBTITLE 1,Configure catalog and schema

dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 1: Setup & Data Generation
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `transaction_id` | string | Unique transaction reference |
# MAGIC | `transaction_time` | timestamp | When the card was used |
# MAGIC | `amount` | double | Total basket value, GBP |
# MAGIC | `item_count` | int | Items in the basket |
# MAGIC | `merchant_category` | string | One of twelve categories — the **baseline group** |
# MAGIC | `channel` | string | `chip_and_pin`, `contactless` or `online` |
# MAGIC | `is_anomaly` | double | Ground truth, for this demo only — never given to the model |
# MAGIC
# MAGIC Each category has its own **typical basket**: a coffee is a couple of pounds for one item, a laptop
# MAGIC several hundred for one, a weekly shop tens of pounds across dozens. That structure is the point —
# MAGIC it is what makes a single global threshold useless.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Setup engines

from datetime import datetime, timedelta

import numpy as np
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.check_funcs import is_in_range, is_not_null
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")

ws = WorkspaceClient()
dq_engine = DQEngine(ws)
anomaly_engine = AnomalyEngine(ws)

print(f"✅ Setup complete — writing to {catalog}.{schema}")

# COMMAND ----------
# DBTITLE 1,Prepare a clean model registry

# Drop the registry so each run of this notebook starts from nothing. Without this, re-running leaves
# every previous run's rows behind and the "registered model" cell below shows a pile of stale
# configurations rather than the one just trained.
registry_table = f"{catalog}.{schema}.dqx_anomaly_models"
spark.sql(f"DROP TABLE IF EXISTS {registry_table}")

print(f"📋 Model registry: {registry_table}")
print("✅ Registry reset — ready for this run's model")

# COMMAND ----------
# DBTITLE 1,Typical basket per merchant category

# (typical unit price, typical item count). These are the patterns a model has to learn;
# nobody writes them down as rules.
CATEGORY_BASKETS = {
    "coffee_shop": (3.20, 1.4),
    "grocery": (2.10, 24.0),
    "fuel": (68.00, 1.0),
    "electronics": (420.00, 1.1),
    "pharmacy": (8.50, 2.6),
    "restaurant": (23.00, 2.2),
    "clothing": (38.00, 2.4),
    "transport": (2.80, 1.0),
    "streaming": (9.99, 1.0),
    "hardware": (14.00, 3.8),
    "books": (11.00, 1.7),
    "gym": (42.00, 1.0),
}
CHANNELS = ("chip_and_pin", "contactless", "online")
START = datetime(2024, 1, 1)
SCHEMA = (
    "transaction_id string, transaction_time timestamp, amount double, "
    "item_count int, merchant_category string, channel string, is_anomaly double"
)

print(f"📊 {len(CATEGORY_BASKETS)} merchant categories, each with its own basket shape")

# COMMAND ----------
# DBTITLE 1,The three shapes of implausible row


def make_implausible(rng, category: str, items: int):
    """Return (category, item_count, amount) for a row that is ordinary per column and absurd overall."""
    kind = rng.integers(3)
    if kind == 0:
        # A grocery-sized basket at a coffee-shop price. £4 and 38 items are each ordinary
        # somewhere in this table; together they are not.
        return category, int(rng.integers(30, 45)), round(rng.uniform(3.0, 6.0), 2)
    if kind == 1:
        # An electronics-sized amount on a single coffee — still inside the global amount range.
        return "coffee_shop", 1, round(rng.uniform(600.0, 950.0), 2)
    # A plausible amount and count, for the wrong category: a £420 single grocery item.
    return "grocery", 1, round(rng.uniform(380.0, 460.0), 2)


# COMMAND ----------
# DBTITLE 1,Generate transactions


def generate_transactions(n_rows: int, seed: int, inject: bool = False):
    """Transactions whose amount and item count follow their category's basket shape."""
    rng = np.random.default_rng(seed)
    categories = list(CATEGORY_BASKETS)
    rows = []

    for i in range(n_rows):
        category = categories[rng.integers(len(categories))]
        unit_price, typical_items = CATEGORY_BASKETS[category]
        items = max(1, int(rng.normal(typical_items, max(0.4, typical_items * 0.25))))
        amount = round(items * unit_price * rng.uniform(0.82, 1.18), 2)
        is_anomaly = 0.0

        if inject and rng.random() < 0.02:
            category, items, amount = make_implausible(rng, category, items)
            is_anomaly = 1.0

        when = START + timedelta(days=int(rng.integers(0, 90)), hours=int(rng.integers(7, 22)))
        channel = CHANNELS[rng.integers(len(CHANNELS))]
        rows.append((f"TXN{i:06d}", when, amount, items, category, channel, is_anomaly))

    return spark.createDataFrame(rows, SCHEMA)


# COMMAND ----------
# DBTITLE 1,Create the training table

print("🔄 Generating three months of clean history...\n")

history_df = generate_transactions(6000, seed=11)
history_table = f"{catalog}.{schema}.card_transactions_history"
history_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(history_table)

print("📊 Sample of historical transactions:")
display(history_df.limit(10))

print(f"\n✅ {history_df.count():,} transactions saved to {history_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 2: The Rules a Good Team Already Has
# MAGIC
# MAGIC These are sensible rules, not strawmen — amount present, amount in range, item count in range.
# MAGIC They are exactly what you should write, and they will catch a great deal of real breakage.
# MAGIC
# MAGIC They will not catch what we are about to inject.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Define the rules

rules = [
    DQRowRule(check_func=is_not_null, column="amount", criticality="error"),
    DQRowRule(
        check_func=is_in_range,
        column="amount",
        check_func_kwargs={"min_limit": 0.01, "max_limit": 2000.0},
        criticality="error",
    ),
    DQRowRule(
        check_func=is_in_range,
        column="item_count",
        check_func_kwargs={"min_limit": 1, "max_limit": 60},
        criticality="error",
    ),
]

print(f"✅ {len(rules)} rules defined")

# COMMAND ----------
# DBTITLE 1,Generate a new batch containing real problems

# Nothing is cached in this notebook: PERSIST is unsupported on serverless compute, which is what most
# readers will run this on. These frames are small local relations built from seeded RNGs, so
# recomputation is both cheap and deterministic.
print("🔄 Generating a new batch with problems injected...\n")

new_df = generate_transactions(1500, seed=99, inject=True)
new_table = f"{catalog}.{schema}.card_transactions_new"
new_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(new_table)

total_new = new_df.count()
injected = new_df.filter(F.col("is_anomaly") == 1.0).count()

print(f"✅ {total_new:,} new transactions saved to {new_table}")
print(f"   {injected} of them jointly implausible")

# COMMAND ----------
# DBTITLE 1,Apply the rules

print("🔍 Applying the rule-based checks...\n")

rule_results = dq_engine.apply_checks(new_df, rules)
caught_by_rules = rule_results.filter(F.col("_errors").isNotNull() & (F.col("is_anomaly") == 1.0)).count()

print(f"⚠️  Rules caught {caught_by_rules} of the {injected} implausible transactions.")
print("   Every injected row sits inside every threshold — each value is ordinary on its own.")
print("   Widening the rules cannot help; tightening them would reject legitimate transactions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 3: Train the Anomaly Model
# MAGIC
# MAGIC Note what is **not** passed: no thresholds, no per-category limits, no labels. DQX learns the
# MAGIC patterns from the history table.
# MAGIC
# MAGIC `baseline_by=["merchant_category"]` is the one modelling decision, and it says what a payments
# MAGIC analyst already knows: **judge each transaction against its own category**. DQX then adds, for every
# MAGIC metric, its deviation from that category's own median — so one model can hold "£900 is normal for
# MAGIC electronics and extreme for coffee".
# MAGIC
# MAGIC `profile="tabular"` is the default and is right for independent records like these. Use
# MAGIC `profile="timeseries"` for repeated multivariate measurements such as machine telemetry — see the
# MAGIC companion notebook.
# MAGIC
# MAGIC **Why `transaction_time` is not in `columns`.** A datetime column becomes seven features (cyclical
# MAGIC hour, day of week and month, plus a weekend flag). That is valuable when *when* something happened
# MAGIC carries meaning — off-hours activity, weekend spikes. In this dataset it does not, so those seven
# MAGIC features would be noise, and noise costs you twice: it dilutes the columns that do carry signal, and
# MAGIC it manufactures "unusual timing" alerts that spend your alert budget. Measured on this data,
# MAGIC excluding it lifts recall at threshold 98 from **77% to 100%** and precision from 38% to 50%.
# MAGIC
# MAGIC The rule is general: **feed a column only if it relates to the anomalies you care about.** Every
# MAGIC extra column adds features, and features you do not need make the ones you do harder to see. Note
# MAGIC that auto-discovery — `train()` with no `columns` — would have included the timestamp here.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Train the model

print("🎯 Training the anomaly model...\n")

model_name = f"{catalog}.{schema}.card_transactions_monitor"

trained = anomaly_engine.train(
    df=spark.table(history_table),
    model_name=model_name,
    registry_table=registry_table,
    # transaction_time is deliberately excluded — see the note above.
    columns=["amount", "item_count"],
    baseline_by=["merchant_category"],
    profile="tabular",
)

print(f"\n✅ Model trained: {trained}")

# COMMAND ----------
# DBTITLE 1,What DQX engineered for you

print("📋 Registered model and its engineered features:\n")

display(
    spark.table(registry_table)
    .filter(F.col("identity.model_name") == trained)
    .selectExpr(
        "identity.algorithm",
        "training.columns",
        "grouping.baseline_by",
        "training.training_rows",
        "from_json(features.feature_metadata, 'engineered_feature_names array<string>')"
        ".engineered_feature_names as engineered_features",
    )
)

print("💡 Note the `_rel_baseline` features — each metric's deviation from its own category's median.")
print("   Four features from two columns, and every one of them carries signal.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 4: Score and Triage
# MAGIC
# MAGIC One check. Feature contributions and AI explanations are **on by default**.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Apply the anomaly check

print("🔍 Scoring the new batch...\n")

anomaly_check = [
    DQDatasetRule(
        criticality="error",
        check_func=has_no_row_anomalies,
        check_func_kwargs={
            "model_name": trained,
            "registry_table": registry_table,
            "threshold": 95.0,
        },
    )
]

# One DQX call: name the input table, name the output table. Writing the result rather than keeping a
# lazy DataFrame also matters here — AI explanations call an LLM through ai_query *inside* the scoring
# plan, so each action on an unmaterialised result would call the model again.
scored_table = f"{catalog}.{schema}.transactions_scored"

dq_engine.apply_checks_and_save_in_table(
    input_config=InputConfig(location=new_table),
    output_config=OutputConfig(location=scored_table, mode="overwrite", options={"overwriteSchema": "true"}),
    checks=anomaly_check,
)

scored = spark.table(scored_table)
anomaly = F.element_at(F.col("_dq_info"), 1).getField("anomaly")
flagged = scored.filter(anomaly.getField("is_anomaly"))

print(f"✅ Scoring complete — {flagged.count()} of {total_new:,} rows flagged")

# COMMAND ----------
# DBTITLE 1,Which columns combined badly

caught = flagged.filter(F.col("is_anomaly") == 1.0).count()
print(f"🔝 Anomaly detection caught {caught} of the {injected} implausible transactions.\n")

display(
    flagged.select(
        "transaction_id",
        "merchant_category",
        "amount",
        "item_count",
        anomaly.getField("severity_percentile").alias("severity"),
        anomaly.getField("contributions").alias("contributions"),
    )
    .orderBy(F.desc("severity"))
    .limit(10)
)

# COMMAND ----------
# DBTITLE 1,Why each group was flagged, in plain language

print("🤖 AI explanations, one per group of similar anomalies:\n")

display(
    flagged.select(
        "transaction_id",
        "merchant_category",
        "amount",
        "item_count",
        anomaly.getField("ai_explanation").getField("top_features").alias("pattern"),
        anomaly.getField("ai_explanation").getField("narrative").alias("narrative"),
        anomaly.getField("ai_explanation").getField("action").alias("action"),
    )
    .filter(F.col("narrative").isNotNull())
    .orderBy("pattern")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 5: (Optional) Tune the Threshold
# MAGIC
# MAGIC **The threshold is an alert budget, not a confidence score.** `threshold=95` means "flag the rows
# MAGIC above the 95th percentile of *training* severity" — so on 1,500 rows it flags roughly 75 before a
# MAGIC single anomaly exists. A row at severity 97 is not "97% likely to be a problem"; it is in the top 3%
# MAGIC most unusual. This is the most commonly misread number in the feature.
# MAGIC
# MAGIC That also puts a hard ceiling on precision. Ask for the top 5% of 1,500 rows and you get 75 alerts;
# MAGIC if only 30 rows are genuinely bad, the best precision anyone could achieve is 30/75 = **40%**. The
# MAGIC table below prints that ceiling next to what the model actually achieved, which is the only fair way
# MAGIC to read the number.
# MAGIC
# MAGIC Severity is computed for **every** row, so you can count would-be anomalies at other thresholds
# MAGIC without rescoring.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Threshold tradeoffs

severity = anomaly.getField("severity_percentile")
truth = F.col("is_anomaly") == 1.0

print("🎚️  Testing different thresholds:\n")
print("Threshold | Alerts | Caught | Precision | Best possible | Recall")
print("-" * 68)

for threshold in (90.0, 95.0, 98.0):
    alerts = scored.filter(severity >= threshold)
    n_alerts = alerts.count()
    n_caught = alerts.filter(truth).count()
    # The ceiling: you cannot be more precise than "every alert is a real anomaly".
    ceiling = min(1.0, injected / n_alerts) if n_alerts else 0.0
    precision = n_caught / n_alerts if n_alerts else 0.0
    print(
        f"   {threshold:>5.0f} | {n_alerts:>6d} | {n_caught:>4d}/{injected:<3d}|"
        f"   {precision:>6.1%}  |    {ceiling:>7.1%}    | {n_caught / injected:>5.1%}"
    )

print("\n💡 Read precision against the ceiling, not against 100%. Where the two are equal, every")
print("   planted anomaly is inside the model's ranking and no alert is wasted — the ranking is")
print("   optimal for that budget. A tighter threshold then trades recall for precision; it does")
print("   not reveal a better model.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Summary & Next Steps
# MAGIC
# MAGIC **Key takeaways:**
# MAGIC - Rules catch what you can name in advance. They caught **none** of these rows, and no threshold
# MAGIC   would have, because every individual value was ordinary.
# MAGIC - Row anomaly detection finds implausible **combinations**, with no thresholds to choose.
# MAGIC - `baseline_by` makes "normal" contextual, so one model covers a coffee shop and an electronics store.
# MAGIC - Contributions and AI explanations tell you *which columns combined badly*, so a flagged row is
# MAGIC   actionable rather than merely suspicious.
# MAGIC - The threshold is an **alert budget**, not a confidence score. Judge precision against the ceiling
# MAGIC   that budget implies.
# MAGIC - Feed the model only columns that relate to what you are looking for. Here, excluding a timestamp
# MAGIC   whose values carried no meaning took recall from 77% to 100%.
# MAGIC
# MAGIC **Apply to your data:**
# MAGIC ```python
# MAGIC model = anomaly_engine.train(
# MAGIC     df=spark.table("your_catalog.your_schema.your_table"),
# MAGIC     model_name="your_catalog.your_schema.your_model",
# MAGIC     registry_table="your_catalog.your_schema.dqx_anomaly_models",
# MAGIC     baseline_by=["your_grouping_column"],   # judge each row against its own group
# MAGIC )
# MAGIC
# MAGIC checks = [
# MAGIC     DQDatasetRule(
# MAGIC         criticality="error",
# MAGIC         check_func=has_no_row_anomalies,
# MAGIC         check_func_kwargs={
# MAGIC             "model_name": model,
# MAGIC             "registry_table": "your_catalog.your_schema.dqx_anomaly_models",
# MAGIC         },
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC # Name the input table and the output table — DQX reads, scores and writes in one call.
# MAGIC dq_engine.apply_checks_and_save_in_table(
# MAGIC     input_config=InputConfig(location="your_catalog.your_schema.new_data"),
# MAGIC     output_config=OutputConfig(location="your_catalog.your_schema.scored"),
# MAGIC     checks=checks,
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Optional next steps:**
# MAGIC - Add `drift_threshold=3.0` to be warned when the input distribution moves away from training.
# MAGIC - Quarantine flagged rows with `apply_checks_and_split` instead of tagging them in place.
# MAGIC - Schedule retraining as "normal" changes — new products, new pricing, new processes.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Resources
# MAGIC
# MAGIC - [Row Anomaly Detection guide](https://databrickslabs.github.io/dqx/docs/guide/row_anomaly_detection)
# MAGIC - [`has_no_row_anomalies` reference](https://databrickslabs.github.io/dqx/docs/reference/quality_checks#row-anomaly-detection)
# MAGIC - [Choosing a profile](https://databrickslabs.github.io/dqx/docs/guide/row_anomaly_detection#choosing-a-profile)
# MAGIC
# MAGIC ### 🎉 You're Ready!
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ Why rule-based checks cannot catch implausible combinations
# MAGIC - ✅ How to train an anomaly model with no thresholds and no labels
# MAGIC - ✅ How `baseline_by` makes "normal" depend on context
# MAGIC - ✅ How to read contributions and AI explanations to triage a flagged row
# MAGIC
# MAGIC **Start finding the rows your rules miss!** 🚀
# MAGIC
