# Databricks notebook source
# MAGIC %md
# MAGIC # The transaction that passed every rule
# MAGIC
# MAGIC A payments team has good rules. Amount is positive, under the card limit. Quantity is at least one.
# MAGIC The merchant category is one of the twelve they support. Every rule passes, every day, and the
# MAGIC dashboards are green.
# MAGIC
# MAGIC Then a reconciliation breaks, and someone finds a £4 grocery basket with 38 items in it, and a £900
# MAGIC coffee. Both were inside every threshold. Neither was flagged.
# MAGIC
# MAGIC That is the gap this notebook is about, and it has two halves:
# MAGIC
# MAGIC 1. **A row can be wrong in the *combination* of its values** while every value is individually fine.
# MAGIC    No single-column rule sees it, because there is no single column to write the rule against.
# MAGIC 2. **"Normal" depends on context.** £900 is unremarkable for electronics and absurd for coffee. A
# MAGIC    threshold that catches the coffee rejects half the laptops.
# MAGIC
# MAGIC DQX row anomaly detection handles both, with no thresholds to pick. This is the **`tabular`** profile,
# MAGIC which is the default — see the companion notebook `dqx_demo_anomaly_timeseries_fleet.py` for the case
# MAGIC that needs the other one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[anomaly] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install 'databricks-labs-dqx[anomaly]'

%restart_python

# COMMAND ----------

dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The data
# MAGIC
# MAGIC Card transactions across twelve merchant categories. Each category has its own **typical basket** —
# MAGIC a coffee is a couple of pounds for one item, a laptop is several hundred for one item, a weekly
# MAGIC grocery shop is tens of pounds across dozens of items. That per-category structure is the whole
# MAGIC point: it is what makes a single global threshold useless.

# COMMAND ----------
# DBTITLE 1,Generate three months of clean history

import numpy as np
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule
from databricks.labs.dqx.check_funcs import is_in_range, is_not_null

# Per-category basket shape: (typical unit price, typical item count).
# These are the patterns a model has to learn; nobody writes them down as rules.
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


def generate_transactions(n_rows: int, seed: int, inject: bool = False):
    """Card transactions whose amount and item count follow their category's basket shape.

    When *inject* is set, a small number of rows are made **jointly** implausible while every individual
    value stays inside the range that category, or some other category, occupies normally. That is the
    point: an injected row must not be catchable by a threshold on one column.
    """
    rng = np.random.default_rng(seed)
    categories = list(CATEGORY_BASKETS)
    rows, labels = [], []

    for i in range(n_rows):
        category = categories[rng.integers(len(categories))]
        unit_price, typical_items = CATEGORY_BASKETS[category]

        items = max(1, int(rng.normal(typical_items, max(0.4, typical_items * 0.25))))
        amount = round(items * unit_price * rng.uniform(0.82, 1.18), 2)
        channel = CHANNELS[rng.integers(len(CHANNELS))]
        is_anomaly = 0.0

        if inject and rng.random() < 0.02:
            kind = rng.integers(3)
            if kind == 0:
                # A grocery-sized basket at a coffee-shop price. £4 and 38 items are each ordinary
                # somewhere in this table; together they are not.
                items = int(rng.integers(30, 45))
                amount = round(rng.uniform(3.0, 6.0), 2)
            elif kind == 1:
                # An electronics-sized amount on a single coffee. Inside the global amount range.
                category = "coffee_shop"
                items = 1
                amount = round(rng.uniform(600.0, 950.0), 2)
            else:
                # A plausible amount and count, but for the wrong category: a £420 grocery single item.
                category = "grocery"
                items = 1
                amount = round(rng.uniform(380.0, 460.0), 2)
            is_anomaly = 1.0

        rows.append(
            (
                f"TXN{i:06d}",
                START + timedelta(days=int(rng.integers(0, 90)), hours=int(rng.integers(7, 22))),
                amount,
                items,
                category,
                channel,
                is_anomaly,
            )
        )
        labels.append(is_anomaly)

    schema = (
        "transaction_id string, transaction_time timestamp, amount double, "
        "item_count int, merchant_category string, channel string, is_anomaly double"
    )
    return spark.createDataFrame(rows, schema), int(sum(labels))


history_df, _ = generate_transactions(6000, seed=11)
history_df.createOrReplaceTempView("history")
print(f"✅ {history_df.count():,} historical transactions across {len(CATEGORY_BASKETS)} categories")
display(history_df.limit(5))

# COMMAND ----------
# DBTITLE 1,Why rules cannot catch this
# MAGIC %md
# MAGIC Before training anything, here is the honest version of the problem. These are sensible rules, and
# MAGIC they are the rules a good team would already have:

# COMMAND ----------

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")

ws = WorkspaceClient()
dq_engine = DQEngine(ws)
anomaly_engine = AnomalyEngine(ws)

# The rules a payments team would already have. Deliberately reasonable, not strawmen.
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

# Nothing is persisted or cached in this notebook: PERSIST is unsupported on serverless compute,
# which is what most readers will run this on. These frames are small local relations built from
# seeded RNGs, so recomputation is both cheap and deterministic.
new_df, injected = generate_transactions(1500, seed=99, inject=True)
print(f"✅ {new_df.count():,} new transactions, {injected} of them jointly implausible\n")

rule_results = dq_engine.apply_checks(new_df, rules)
caught_by_rules = rule_results.filter(F.col("_errors").isNotNull() & (F.col("is_anomaly") == 1.0)).count()

print(f"Rules caught {caught_by_rules} of the {injected} implausible transactions.")
print("Every injected row sits inside every threshold — each value is ordinary on its own.")
print("Widening the rules cannot help; tightening them would reject legitimate transactions.")

# COMMAND ----------
# DBTITLE 1,Train: no thresholds, no labels
# MAGIC %md
# MAGIC Now the model. Note what is *not* being passed: no thresholds, no per-category limits, no labels.
# MAGIC `baseline_by=["merchant_category"]` is the one modelling decision, and it says the thing a payments
# MAGIC analyst already knows — **judge each transaction against its own category**, not against the table.

# COMMAND ----------

model_name = f"{catalog}.{schema}.card_transactions_monitor"
registry_table = f"{catalog}.{schema}.dqx_anomaly_models"

trained = anomaly_engine.train(
    df=history_df,
    model_name=model_name,
    registry_table=registry_table,
    columns=["amount", "item_count", "transaction_time"],
    baseline_by=["merchant_category"],
    # profile="tabular" is the default: independent records, anomalies are unusual values or
    # unusual combinations of them. Stated here only to make the choice visible.
    profile="tabular",
)
print(f"\n✅ trained: {trained}")

# COMMAND ----------
# DBTITLE 1,What conditioning bought
# MAGIC %md
# MAGIC `baseline_by` appends, for each metric, its deviation from **that category's own** median. So the
# MAGIC model sees both the raw amount and "how this amount compares to a typical basket in this category" —
# MAGIC which is how £900 can be normal for electronics and extreme for coffee inside one model.

# COMMAND ----------

display(
    spark.table(registry_table)
    .filter(F.col("identity.model_name") == trained)
    .selectExpr(
        "identity.algorithm",
        "training.columns",
        "grouping.baseline_by",
        "training.training_rows",
        "from_json(features.feature_metadata, 'engineered_feature_names array<string>')"
        ".engineered_feature_names as features",
    )
)

# COMMAND ----------
# DBTITLE 1,Score, and read why
# MAGIC %md
# MAGIC One check. Contributions and AI explanations are on by default.

# COMMAND ----------

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

# Written to a table rather than left lazy: AI explanations call an LLM through ai_query inside the
# scoring plan, so each action on an unmaterialised result would call the model again -- paying repeatedly
# and getting a different answer each time. `.cache()` cannot be used (PERSIST is unsupported on
# serverless), so writing once is both the fix and the pattern to copy in a real pipeline.
scored_table = f"{catalog}.{schema}.transactions_scored"
dq_engine.apply_checks(new_df, anomaly_check).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(scored_table)
scored = spark.table(scored_table)
anomaly = F.element_at(F.col("_dq_info"), 1).getField("anomaly")

flagged = scored.filter(anomaly.getField("is_anomaly"))
caught = scored.filter(anomaly.getField("is_anomaly") & (F.col("is_anomaly") == 1.0)).count()

print(f"Rules caught      {caught_by_rules:>3} of {injected}")
print(f"Anomaly detection caught {caught:>3} of {injected}")
print(f"\n{flagged.count()} rows flagged in total out of {new_df.count():,} ({flagged.count() / new_df.count():.1%})")

# COMMAND ----------
# DBTITLE 1,The explanations name the columns that combined badly

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
# DBTITLE 1,And in plain language, per group of similar anomalies

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
# DBTITLE 1,Does conditioning actually matter? Train without it and compare.
# MAGIC %md
# MAGIC The claim above was that comparing each transaction against its own category is what makes this
# MAGIC work. Worth testing rather than asserting: the same data, the same everything, `baseline_by=[]`.

# COMMAND ----------

pooled = anomaly_engine.train(
    df=history_df,
    model_name=f"{catalog}.{schema}.card_transactions_pooled",
    registry_table=registry_table,
    columns=["amount", "item_count", "transaction_time"],
    baseline_by=[],  # compare against the whole table instead
)

pooled_scored = dq_engine.apply_checks(
    new_df,
    [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_row_anomalies,
            check_func_kwargs={
                "model_name": pooled,
                "registry_table": registry_table,
                "threshold": 95.0,
                # This model exists only to produce one comparison number, so skip the attribution and
                # the LLM call. Both are on by default.
                "enable_contributions": False,
                "enable_ai_explanation": False,
            },
        )
    ],
)
pooled_anomaly = F.element_at(F.col("_dq_info"), 1).getField("anomaly")
pooled_caught = pooled_scored.filter(pooled_anomaly.getField("is_anomaly") & (F.col("is_anomaly") == 1.0)).count()

print(f"conditioned on merchant_category : {caught:>3} of {injected} caught")
print(f"compared against whole table     : {pooled_caught:>3} of {injected} caught")
print("\nBoth are real models on identical data. The difference is the basis of comparison.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to take away
# MAGIC
# MAGIC - Rules catch what you can **name in advance**. They caught none of these, and no threshold would
# MAGIC   have, because every individual value was ordinary.
# MAGIC - Row anomaly detection catches **implausible combinations**, with no thresholds to choose.
# MAGIC - `baseline_by` is how "normal" becomes contextual, and it helps here rather than transforming the
# MAGIC   result: the comparison above is a real but modest gain. That is honest and expected — some of these
# MAGIC   injected rows (a £900 coffee) are extreme enough to stand out against the whole table too.
# MAGIC   Conditioning earns its keep on the ones that are not, like a 40-item basket costing £4.
# MAGIC - Contributions tell you **which columns combined badly**, so a flagged row is actionable rather
# MAGIC   than just suspicious.
# MAGIC
# MAGIC Use rules *and* this. Rules are cheaper, clearer and versioned; they should catch everything you can
# MAGIC describe. Anomaly detection is for what is left.
# MAGIC
# MAGIC ### Where this profile is the wrong tool
# MAGIC
# MAGIC These transactions are **independent records** — each row stands on its own. When rows are instead
# MAGIC repeated measurements of the same thing over time, and the failure is metrics that normally move
# MAGIC together drifting apart, the `tabular` profile is close to blind to it. That is
# MAGIC `dqx_demo_anomaly_timeseries_fleet.py`.
# MAGIC
# MAGIC Neither profile models trend or forecasts the next value. See
# MAGIC [Choosing a profile](https://databrickslabs.github.io/dqx/docs/guide/row_anomaly_detection#choosing-a-profile).
