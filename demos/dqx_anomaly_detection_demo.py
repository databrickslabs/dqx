# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Anomaly Detection Demo
# MAGIC
# MAGIC ## Learn Anomaly Detection in 15 Minutes
# MAGIC
# MAGIC **Quickstart (5–10 minutes):**
# MAGIC - Train an anomaly model on sample data using DQX - Anomaly Detection Engine
# MAGIC - Score records and see flagged anomalies
# MAGIC - View severity percentiles and top contributors
# MAGIC
# MAGIC **Dataset**: Simple sales transactions (universally relatable, no domain expertise required)
# MAGIC
# MAGIC **Optional deep dive**: Conceptual background and Unity Catalog comparison are provided later.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What is Anomaly Detection?
# MAGIC
# MAGIC - Rule checks catch *known* issues (nulls, ranges, formats).
# MAGIC - Anomaly detection finds *unknown* patterns across multiple columns.
# MAGIC - Use both together for better coverage.
# MAGIC
# MAGIC **If you want the deep dive**, see the Appendix at the end of this notebook or DQX Docs[https://databrickslabs.github.io/dqx/docs/guide/].
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites: Install DQX with Anomaly Support
# MAGIC
# MAGIC (If you're already running DQX in Databricks, you can skip this install cell.)
# MAGIC
# MAGIC Before running this demo, install DQX with anomaly detection extras:
# MAGIC
# MAGIC ```python
# MAGIC %pip install 'databricks-labs-dqx[anomaly]'
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC **What's included in `[anomaly]` extras:**
# MAGIC - `scikit-learn` - Machine learning algorithms used for anomaly detection
# MAGIC - `mlflow` - Model tracking and registry
# MAGIC - `shap` - Feature contributions for explainability
# MAGIC - `cloudpickle` - Model serialization
# MAGIC
# MAGIC **Note**: On ML Runtimes and Serverless compute, most dependencies are already pre-installed.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Prerequisites: Install DQX with Anomaly Support

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[anomaly] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[anomaly]

%restart_python

# COMMAND ----------
# DBTITLE 1,Prerequisites: Install DQX with Anomaly Support

default_catalog = "main"
default_schema = "default"

# Configure widgets for catalog and schema
dbutils.widgets.text("demo_catalog", default_catalog, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema, "Schema Name")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup & Data Generation
# MAGIC
# MAGIC We’ll create historical data, train a model, then score a new batch with injected anomalies.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Setup & Data Generation

# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import numpy as np

from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null, is_in_range
from databricks.sdk import WorkspaceClient

# Initialize
ws = WorkspaceClient()
anomaly_engine = AnomalyEngine(ws)
dq_engine = DQEngine(ws)

# Set seeds for reproducibility for demo purposes
random.seed(42)
np.random.seed(42)

print("✅ Setup complete!")


# COMMAND ----------
# DBTITLE 1,Setup & Data Generation

# Generate historical (training) data
def generate_historical_sales_data(
    num_rows: int = 1000,
):
    """
    Generate historical sales data (no synthetic anomalies).
    """
    data = []
    categories = ["Electronics", "Clothing", "Food", "Books", "Home"]
    regions = ["North", "South", "East", "West"]
    
    # Regional pricing patterns (normal baseline)
    region_patterns = {
        "North": {"base_amount": 200, "quantity": 5},
        "South": {"base_amount": 150, "quantity": 4},
        "East": {"base_amount": 180, "quantity": 4},
        "West": {"base_amount": 220, "quantity": 6},
    }
    
    start_date = datetime(2024, 1, 1, 9, 0)  # Jan 1, 2024, 9am
    
    for i in range(num_rows):
        transaction_id = f"TXN{i:06d}"
        category = random.choice(categories)
        region = random.choice(regions)
        pattern = region_patterns[region]
        
        # Generate timestamp (mostly business hours weekdays)
        days_offset = random.randint(0, 90)  # 3 months of data
        hours_offset = random.randint(0, 9)  # 9am-6pm = 9 hours
        date = start_date + timedelta(days=days_offset, hours=hours_offset)
        
        # Skip weekends for normal transactions
        if date.weekday() >= 5:  # Saturday=5, Sunday=6
            date = date - timedelta(days=date.weekday() - 4)  # Move to Friday
        
        # Normal transaction (tighter variance for more consistent patterns)
        amount = round(pattern["base_amount"] * random.uniform(0.85, 1.15), 2)
        quantity = max(1, int(np.random.normal(pattern["quantity"], 1)))
        
        # Ensure valid ranges (skip for injected nulls/negatives)
        if amount is not None:
            amount = max(10, min(10000, amount))
        if quantity is not None:
            quantity = max(1, min(150, quantity))  # Allow bulk orders up to 150
        
        data.append((transaction_id, date, amount, quantity, category, region))
    
    return data

# Generate historical data
print("🔄 Generating historical (training) data...\n")
train_rows = 5000
historical_data = generate_historical_sales_data(num_rows=train_rows)

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("date", TimestampType(), False),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("category", StringType(), False),
    StructField("region", StringType(), False),
])

df_train = spark.createDataFrame(historical_data, schema)

print("📊 Sample of sales transactions:")
display(df_train.orderBy("date"))

total_train = df_train.count()
print(f"\n✅ Generated {total_train} historical transactions (for training)")


# COMMAND ----------
# DBTITLE 1,Setup & Data Generation

# Get catalog and schema from widgets
catalog = dbutils.widgets.get("demo_catalog")
schema_name = dbutils.widgets.get("demo_schema")

print(f"📂 Using catalog: {catalog}")
print(f"📂 Using schema: {schema_name}\n")

# Save data to table
train_table = f"{catalog}.{schema_name}.sales_transactions_train"
df_train.write.mode("overwrite").saveAsTable(train_table)

print(f"✅ Training data saved to: {train_table}")


# COMMAND ----------
# DBTITLE 1,Setup & Data Generation

# Set up registry table for tracking trained models
registry_table = f"{catalog}.{schema_name}.anomaly_model_registry_101"
print(f"📋 Model registry table: {registry_table}")

# Clean up any existing registry from previous runs
spark.sql(f"DROP TABLE IF EXISTS {registry_table}")
print(f"✅ Registry ready for new models")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 2: Train the Anomaly Model
# MAGIC
# MAGIC We’ll run:
# MAGIC - Simple rule checks (nulls, ranges)
# MAGIC - Anomaly detection for unusual multi‑column patterns
# MAGIC

# COMMAND ----------
# DBTITLE 1,Train the Anomaly Model

# Train anomaly detection model with zero configuration
print("🎯 Training anomaly detection model...")
print("   DQX will automatically discover patterns in your data\n")

model_name_auto = f"{catalog}.{schema_name}.sales_auto"
model_uri_auto = anomaly_engine.train(
    df=spark.table(train_table),
    model_name=model_name_auto,
    registry_table=registry_table
)

print(f"✅ Model trained successfully!")
print(f"   Model URI: {model_uri_auto}")

# View what DQX created for you
print(f"\n📋 Trained Models:\n")

display(
    spark.table(registry_table)
    .filter(F.col("identity.model_name").contains(model_name_auto))
    .select(
        "identity.model_name",
        "training.columns", 
        "segmentation.segment_by",
        "segmentation.segment_values",
        "training.training_rows",
        "training.training_time",
        "identity.status"
    )
    .orderBy("identity.model_name")
)

print("\n💡 DQX auto-discovered patterns and registered a model you can score immediately.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: View Models in the UI
# MAGIC
# MAGIC Your models are registered in Unity Catalog / MLflow.
# MAGIC If you want to inspect them, open **Catalog Explorer** or **Experiments**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 3: Generate a New Batch (with Injected Anomalies)
# MAGIC
# MAGIC We score with rule checks + anomaly detection.
# MAGIC `_dq_info.anomaly` includes:
# MAGIC - `severity_percentile` (0–100): percentile of anomaly severity
# MAGIC - `score`: raw model score (diagnostic only)
# MAGIC - `contributions`: feature-level explanations
# MAGIC

# COMMAND ----------
# DBTITLE 1,Generate a New Batch (with Injected Anomalies)

def inject_anomalies_and_dq_issues(
    base_rows: list[tuple],
    anomaly_rate: float = 0.02,
    dq_null_amount_rate: float = 0.01,
    dq_null_quantity_rate: float = 0.005,
    dq_negative_amount_rate: float = 0.005,
):
    """
    Take clean (normal) rows and inject anomalies + simple DQ issues.
    """
    rows = []
    for idx, row in enumerate(base_rows):
        transaction_id, date, amount, quantity, category, region = row
        transaction_id = f"NEW{idx:06d}"
        is_synthetic_anomaly = False

        dq_roll = random.random()
        if dq_roll < dq_null_amount_rate:
            amount = None
        elif dq_roll < dq_null_amount_rate + dq_null_quantity_rate:
            quantity = None
        elif dq_roll < dq_null_amount_rate + dq_null_quantity_rate + dq_negative_amount_rate:
            amount = -abs(amount)
        elif random.random() < anomaly_rate:
            is_synthetic_anomaly = True
            anomaly_type = random.choices(
                ["extreme_scale", "mismatch_pair", "timing_spike"],
                weights=[3, 3, 2],
            )[0]

            if anomaly_type == "extreme_scale":
                amount = round(amount * random.uniform(15, 25), 2)
                quantity = int(quantity * random.uniform(15, 25))
            elif anomaly_type == "mismatch_pair":
                # Large amount with tiny quantity (or vice versa)
                if random.random() < 0.5:
                    amount = round(amount * random.uniform(12, 20), 2)
                    quantity = max(1, int(quantity * random.uniform(0.05, 0.2)))
                else:
                    amount = round(amount * random.uniform(0.05, 0.2), 2)
                    quantity = int(quantity * random.uniform(12, 20))
            else:
                # Off-hours + large spike
                amount = round(amount * random.uniform(10, 18), 2)
                quantity = int(quantity * random.uniform(10, 18))
                date = date.replace(hour=random.choice([2, 3, 4, 22, 23]))

        if amount is not None:
            amount = max(10, min(10000, amount))
        if quantity is not None:
            quantity = max(1, min(150, quantity))

        rows.append((transaction_id, date, amount, quantity, category, region, is_synthetic_anomaly))
    return rows

print("🔄 Generating new batch data with injected anomalies...\n")

new_rows = 1000
anomaly_rate = 0.02
dq_null_amount_rate = 0.01
dq_null_quantity_rate = 0.005
dq_negative_amount_rate = 0.005
dq_issue_rate = dq_null_amount_rate + dq_null_quantity_rate + dq_negative_amount_rate

new_batch_base = generate_historical_sales_data(num_rows=new_rows)
new_batch = inject_anomalies_and_dq_issues(
    base_rows=new_batch_base,
    anomaly_rate=anomaly_rate,
    dq_null_amount_rate=dq_null_amount_rate,
    dq_null_quantity_rate=dq_null_quantity_rate,
    dq_negative_amount_rate=dq_negative_amount_rate,
)

new_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("date", TimestampType(), False),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("category", StringType(), False),
    StructField("region", StringType(), False),
    StructField("is_synthetic_anomaly", BooleanType(), False),
])

df_new = spark.createDataFrame(new_batch, new_schema)

print("📊 Sample of NEW batch:")
display(df_new.orderBy("date"))

total_new = df_new.count()
print(f"\n✅ Generated {total_new} NEW transactions")
print(f"   Injected anomalies: ~{int(total_new * anomaly_rate)} ({anomaly_rate*100:.0f}%)")
print(f"   Injected rule issues: ~{int(total_new * dq_issue_rate)} ({dq_issue_rate*100:.1f}%)")

new_table = f"{catalog}.{schema_name}.sales_transactions_new"
df_new.write.mode("overwrite").saveAsTable(new_table)
print(f"✅ New batch saved to: {new_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 4: Score the New Batch (Default Settings)
# MAGIC
# MAGIC Now apply anomaly detection + rule checks to the **new batch**.

# COMMAND ----------
# DBTITLE 1,Score the New Batch (Default Settings)

print("🔍 Applying quality checks to NEW batch...\n")

# Define all quality checks
checks_combined = [
    # Rule-based checks for known issues
    DQRowRule(check_func=is_not_null, check_func_kwargs={"column": "transaction_id"}),
    DQRowRule(check_func=is_not_null, check_func_kwargs={"column": "amount"}),
    DQRowRule(check_func=is_in_range, check_func_kwargs={"column": "amount", "min_limit": 0, "max_limit": 100000}),
    DQRowRule(check_func=is_not_null, check_func_kwargs={"column": "quantity"}),
    DQRowRule(check_func=is_in_range, check_func_kwargs={"column": "quantity", "min_limit": 1, "max_limit": 1000}),
    
    # ML anomaly detection for unusual patterns
    DQDatasetRule(
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": model_name_auto,
            "registry_table": registry_table
        }
    )
]

df_valid, df_anomalies = dq_engine.apply_checks_and_split(df_new, checks_combined)
df_scored = df_valid.unionByName(df_anomalies, allowMissingColumns=True)

display(df_scored)

print("\n💡 Summary:")
print("   • We trained on historical data and scored a new batch.")
print("   • Default threshold 95 flags the top 5% most unusual records.")
print("   • Threshold is a percentile cutoff — tune it based on your data and alert tolerance.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 5: Review Results
# MAGIC
# MAGIC You’ll see flagged anomalies, severity percentiles, and top contributors.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Review Results

df_anomalies = df_anomalies.filter(F.col("_dq_info.anomaly.is_anomaly") == True)
score_col = F.col("_dq_info.anomaly.score")
severity_col = F.col("_dq_info.anomaly.severity_percentile")
percentile_band = (
    F.when(severity_col >= 98, F.lit("p98+ (top 2%)"))
    .when(severity_col >= 95, F.lit("p95-98 (top 5%)"))
    .when(severity_col >= 90, F.lit("p90-95 (top 10%)"))
    .otherwise(F.lit("<p90 (bottom 90%)"))
)
total_scored = df_scored.count()
anomalies_count = df_anomalies.count()

print(f"✅ Quality checks complete!")
print(f"\n📊 Results:")
print(f"   Total new transactions: {total_new}")
print(f"   Anomalies found: {anomalies_count} ({(anomalies_count / total_new) * 100:.1f}%)")
print("ℹ️  Each record receives a severity percentile.")
print("   The percentile threshold decides whether a record is flagged as anomalous.")
print("   Even records that are not flagged still get severity for analysis.")
print("   Higher percentile = more severe relative to training data.")

# Sanity check: did we catch the injected anomalies?
synthetic_total = df_scored.filter(F.col("is_synthetic_anomaly") == True).count()
synthetic_caught = df_scored.filter(
    (F.col("is_synthetic_anomaly") == True) & (F.col("_dq_info.anomaly.is_anomaly") == True)
).count()
if synthetic_total > 0:
    recall = synthetic_caught / synthetic_total * 100
    print(f"\n✅ Synthetic anomalies injected: {synthetic_total}")
    print(f"   Synthetic anomalies caught: {synthetic_caught} ({recall:.1f}% recall)")
print(f"\n🔝 Top 10 anomalies:\n")

display(df_anomalies.orderBy(severity_col.desc()).select(
    "transaction_id", "date", "amount", "quantity", "category", "region",
    F.round(severity_col, 1).alias("severity_percentile"),
    F.round(score_col, 3).alias("anomaly_score"),
    percentile_band.alias("severity_band"),
    F.col("_dq_info.anomaly.contributions").alias("why_anomalous")
).limit(10))

print("   Use the existing 'why_anomalous' display column above (from _dq_info.anomaly.contributions) to understand drivers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Threshold Tradeoffs (Optional)
# MAGIC
# MAGIC This section is optional. Skip if you only want the quickstart.

# COMMAND ----------
# DBTITLE 1,Threshold Tradeoffs (Optional)

print("📌 Summary:")
print("   • Default threshold = 95 (top 5%).")
print("   • Raise it to reduce alerts; lower it to catch more.")
print("   • The right setting depends on your data distribution and risk tolerance.")


# COMMAND ----------
# DBTITLE 1,Threshold Tradeoffs (Optional)

# (Optional) Quick normal vs anomaly sanity check
print("🔍 Sanity check (severity < 95 vs ≥ 95):\n")
normal_count = df_scored.filter(severity_col < 95).count()
anomaly_count = df_scored.filter(severity_col >= 95).count()
print(f"   Normal: {normal_count} ({normal_count/total_scored*100:.1f}%)")
print(f"   Anomaly: {anomaly_count} ({anomaly_count/total_scored*100:.1f}%)")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### Tuning the Threshold
# MAGIC
# MAGIC Threshold is a percentile cutoff:
# MAGIC - Lower (e.g., 90) = more alerts
# MAGIC - Higher (e.g., 98) = fewer alerts
# MAGIC
# MAGIC We already scored all records, so you can change thresholds without re‑scoring.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Tuning the Threshold

# Try different thresholds
print("🎚️  Testing Different Thresholds:\n")
print("Threshold | Anomalies | % of Data | Interpretation")
print("-" * 70)

thresholds = [90, 95, 98]
total_count = total_scored

for threshold in thresholds:
    anomaly_count = df_scored.filter(severity_col >= threshold).count()
    percentage = (anomaly_count / total_count) * 100

    if threshold < 95:
        interpretation = "Sensitive (more alerts)"
    elif threshold == 95:
        interpretation = "Balanced (default)"
    else:
        interpretation = "Strict (fewer alerts)"

    print(f"   {threshold:>3d}   |   {anomaly_count:4d}    |  {percentage:5.1f}%  | {interpretation}")

print("\n💡 Start at 95, then explore thresholds on your data to balance noise vs. missed anomalies.")


# COMMAND ----------
# DBTITLE 1,Tuning the Threshold

# Borderline slice (optional)
borderline = df_scored.filter((severity_col >= 90) & (severity_col < 95)).orderBy(severity_col.desc())
print(f"\nBorderline (90-<95) examples: {borderline.count()}")
display(borderline.select(
    "transaction_id", "amount", "quantity",
    F.round(severity_col, 1).alias("severity_percentile"),
    F.round(score_col, 3).alias("score"),
).limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 7: Manual Column Selection (Optional)
# MAGIC
# MAGIC Skip this if you only want the quickstart.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Manual Column Selection (Optional)

# Train with manual column selection
print("🎯 Training model with manual column selection...\n")

model_name_manual = f"{catalog}.{schema_name}.sales_manual"
model_uri_manual = anomaly_engine.train(
    df=spark.table(train_table),
    columns=["amount", "quantity"],  # Explicitly specify numeric columns only
    model_name=model_name_manual,
    registry_table=registry_table
)

print(f"✅ Manual model trained!")
print(f"   Model URI: {model_uri_manual}")
print(f"\n💡 Manual selection is useful in production when you want strict feature control.")

# Compare auto vs manual in the registry
print(f"\n📊 Auto vs Manual Comparison:")
print(f"   View both models side-by-side in the registry:\n")

display(
    spark.table(registry_table)
    .filter(
        (F.col("identity.model_name") == model_name_auto) |
        (F.col("identity.model_name") == model_name_manual)
    )
    .select(
        "identity.model_name",
        "training.columns",
        "segmentation.segment_by",
        "training.training_rows",
        "identity.status"
    )
    .orderBy("identity.model_name", "training.training_time")
)

print(f"\n💡 Auto vs manual: auto = fast exploration, manual = tight control.")


# COMMAND ----------
# DBTITLE 1,Manual Column Selection (Optional)

# Score with manual model
print("🔍 Scoring with manual model...\n")

checks_manual = [
    DQDatasetRule(
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": model_name_manual,
            "threshold": 95.0,
            "registry_table": registry_table
        }
    )
]

df_valid, df_anomalies_manual = dq_engine.apply_checks_and_split(df_new, checks_manual)

print(f"⚠️  Manual model found {df_anomalies_manual.count()} anomalies")
print(f"   (Auto model found {df_anomalies.count()} anomalies)")
print(f"\n🔝 Top 5 anomalies from manual model:\n")


display(df_anomalies_manual.orderBy(F.col("_dq_info.anomaly.severity_percentile").desc()).select(
    "transaction_id", "amount", "quantity", "date",
    F.round("_dq_info.anomaly.severity_percentile", 1).alias("severity_percentile"),
    F.round("_dq_info.anomaly.score", 3).alias("score")
).limit(5))

print("\n💡 Different features → different anomalies. That’s expected.")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 8: Feature Contributions (Optional)
# MAGIC
# MAGIC Contributions are already shown in Section 5. This section is a deeper look.
# MAGIC
# MAGIC ### Advanced Options (Reference)
# MAGIC
# MAGIC **Scoring options (`has_no_anomalies`):**
# MAGIC - `threshold` (float, 0–100): percentile cutoff (default 95)
# MAGIC - `include_contributions` (bool): feature contributions in `_dq_info.anomaly`
# MAGIC - `include_confidence` (bool): confidence estimate (std dev across ensemble)
# MAGIC - `drift_threshold` (float): drift detection sensitivity
# MAGIC - `row_filter` (str): SQL filter applied before scoring
# MAGIC
# MAGIC **Training options (`AnomalyEngine.train` / `AnomalyParams`):**
# MAGIC - `columns` (list[str]): explicit feature list (disables auto‑discovery)
# MAGIC - `segment_by` (list[str]): explicit segmentation columns
# MAGIC - `sample_fraction`, `max_rows`: training sample controls
# MAGIC - `ensemble_size`: number of models in the ensemble
# MAGIC - `expected_anomaly_rate`: expected anomaly rate for calibration
# MAGIC
# MAGIC These are optional — the demo uses defaults for simplicity.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Advanced Options (Reference)

# Score with feature contributions
print("🔍 Scoring with feature contributions (explainability)...\n")

checks_with_contrib = [
    DQDatasetRule(
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": model_name_manual,
            "threshold": 95.0,
            "include_contributions": True,  # On by default; explicit here for clarity.
            "registry_table": registry_table
        }
    )
]

df_with_contrib = dq_engine.apply_checks(df_new, checks_with_contrib)

print("✅ Scored with feature contributions!")
print("\n🎯 Top Anomalies with Explanations:\n")

# Filter by _errors column (standard DQX pattern) to get flagged anomalies
anomalies_explained = df_with_contrib.filter(
    F.size(F.col("_errors")) > 0
).orderBy(F.col("_dq_info.anomaly.severity_percentile").desc()).limit(5)

display(anomalies_explained.select(
    "transaction_id",
    "amount",
    "quantity",
    F.date_format("date", "yyyy-MM-dd HH:mm").alias("date"),
    F.round("_dq_info.anomaly.severity_percentile", 1).alias("severity_percentile"),
    F.round("_dq_info.anomaly.score", 3).alias("score"),
    F.col("_dq_info.anomaly.contributions").alias("contributions")
))

print("\n💡 Contributions show which features most influenced the anomaly.")
print("   Focus on features with the highest % contribution.")


# COMMAND ----------
# DBTITLE 1,Advanced Options (Reference)

# Show one detailed example
print("🔎 Detailed Example - Top Anomaly:\n")

# Extract flat columns for easier access
anomalies_flattened = anomalies_explained.select(
    "transaction_id",
    "amount",
    "quantity",
    "date",
    F.col("_dq_info.anomaly.severity_percentile").alias("severity_percentile"),
    F.col("_dq_info.anomaly.score").alias("score"),
    F.col("_dq_info.anomaly.contributions").alias("contributions")
)

top_anomaly = anomalies_flattened.first()

print(f"Transaction ID: {top_anomaly['transaction_id']}")
print(f"Severity Percentile: {top_anomaly['severity_percentile']:.1f}")
print(f"Anomaly Score (raw): {top_anomaly['score']:.3f}")
print(f"\nTransaction Details:")
print(f"   Amount: ${top_anomaly['amount']:.2f}")
print(f"   Quantity: {top_anomaly['quantity']}")
print(f"   Date: {top_anomaly['date']}")
print(f"\nFeature Contributions:")

contributions = top_anomaly['contributions']
if contributions:
    # Sort by contribution value
    sorted_contrib = sorted(contributions.items(), key=lambda x: abs(x[1]), reverse=True)
    for feature, value in sorted_contrib[:3]:  # Top 3
        print(f"   {feature}: {abs(value):.1f}% contribution")
    
    print(f"\n🎯 Investigation Tip:")
    top_feature = sorted_contrib[0][0]
    if "amount" in top_feature:
        print(f"   → Check for pricing errors or incorrect price feeds")
    elif "quantity" in top_feature:
        print(f"   → Investigate bulk order or inventory issue")
    elif "date" in top_feature or "hour" in top_feature:
        print(f"   → Review transaction timing - off-hours activity?")
else:
    print("   (No detailed contributions available)")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 9: Using in Production (Optional)
# MAGIC
# MAGIC This section shows a simple production‑style pattern (clean + quarantine tables).
# MAGIC
# MAGIC **Workflow tip**: Provide fully qualified names for both `model_name` and `registry_table`
# MAGIC (e.g., `catalog.schema.model_name`, `catalog.schema.registry_table`). DQX will not
# MAGIC auto-derive these in workflows.
# MAGIC

# COMMAND ----------
# DBTITLE 1,Using in Production (Optional)

# Automatically separate clean data from anomalies
print("📦 Separating clean data from anomalies...\n")

# Save clean and quarantined records using the built-in helper
clean_table = f"{catalog}.{schema_name}.sales_transactions_clean"
quarantine_table = f"{catalog}.{schema_name}.sales_anomalies_quarantine"

dq_engine.apply_checks_and_save_in_table(
    checks=checks_combined,
    input_config=InputConfig(location=new_table),
    output_config=OutputConfig(location=clean_table, mode="overwrite"),
    quarantine_config=OutputConfig(location=quarantine_table, mode="overwrite"),
)

good_df = spark.table(clean_table)
bad_df = spark.table(quarantine_table)

print(f"✅ Automatically saved clean and quarantined data using apply_checks_and_save_in_table():")
print(f"   Clean records: {good_df.count()}")
print(f"   Quarantined: {bad_df.count()}")
print(f"\n💡 Benefits:")
print(f"   • Automatically routes failed checks to quarantine")
print(f"   • Handles anomalies + rule violations in one pass")
print(f"\nTables created:")
print(f"   Input: {new_table}")
print(f"   Clean: {clean_table}")
print(f"   Quarantine: {quarantine_table}")
print(f"\n📋 Access quarantined records (includes DQX _errors/_warnings metadata):")
print(f"   spark.table('{quarantine_table}')")


# COMMAND ----------
# DBTITLE 1,Using in Production (Optional)

# Pattern 2: Use in downstream pipelines
print("🔄 Downstream usage:")
print("   • Use clean data for analytics/ML")
print("   • Investigate quarantined records")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Summary & Next Steps
# MAGIC
# MAGIC **Key takeaways:**
# MAGIC - Use anomaly detection + rules together.
# MAGIC - Start with threshold 95, tune as needed.
# MAGIC - Use contributions to triage anomalies faster.
# MAGIC
# MAGIC **Apply to your data:**
# MAGIC ```python
# MAGIC # Replace with your table
# MAGIC model = anomaly_engine.train(
# MAGIC     df=spark.table("your_catalog.your_schema.your_table"),
# MAGIC     model_name="your_catalog.your_schema.your_model_name",
# MAGIC     registry_table="your_catalog.your_schema.dqx_anomaly_models",
# MAGIC )
# MAGIC
# MAGIC checks = [
# MAGIC     has_no_anomalies(
# MAGIC         model="your_catalog.your_schema.your_model_name",
# MAGIC         registry_table="your_catalog.your_schema.dqx_anomaly_models",
# MAGIC     )
# MAGIC ]
# MAGIC df_scored = dq_engine.apply_checks(your_df, checks)
# MAGIC ```
# MAGIC
# MAGIC **Optional next steps:**
# MAGIC - Add segmentation, drift detection, and scheduled scoring.
# MAGIC - Automate retraining and alerting.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Appendix (Optional): Concepts & Unity Catalog Comparison
# MAGIC
# MAGIC **Known vs Unknown Issues**
# MAGIC - **Known unknowns**: rule‑based checks (nulls, ranges, formats).
# MAGIC - **Unknown unknowns**: multi‑column or subtle patterns you didn’t anticipate.
# MAGIC
# MAGIC **Why anomaly detection**
# MAGIC - Learns “normal” from data
# MAGIC - Flags deviations without manual rules
# MAGIC - Complements rule checks rather than replacing them
# MAGIC
# MAGIC **Unity Catalog Monitoring vs DQX Anomaly**
# MAGIC - **Unity Catalog**: table‑level signals (row counts, freshness, commit patterns).
# MAGIC - **DQX Anomaly**: row‑level patterns (per‑record anomalies with explanations).
# MAGIC
# MAGIC **Where to learn more**
# MAGIC - Unity Catalog monitoring: https://docs.databricks.com/aws/en/data-quality-monitoring/
# MAGIC - DQX anomaly docs: see `docs/dqx/docs/guide/anomaly_detection.mdx`
# MAGIC - Retrain models as patterns change
# MAGIC - Combine with Unity Catalog's table-level monitoring
# MAGIC
# MAGIC ### 📚 Resources
# MAGIC
# MAGIC - [DQX Anomaly Detection Documentation](https://databrickslabs.github.io/dqx/guide/anomaly_detection)
# MAGIC - [API Reference](https://databrickslabs.github.io/dqx/reference/quality_checks#has_no_anomalies)
# MAGIC - [Unity Catalog Anomaly Detection](https://docs.databricks.com/aws/en/data-quality-monitoring/anomaly-detection/#-table-quality-details)
# MAGIC - [GitHub Repository](https://github.com/databrickslabs/dqx)
# MAGIC
# MAGIC ### 🎉 You're Ready!
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ What anomaly detection is and when to use it
# MAGIC - ✅ How to implement it with minimal configuration
# MAGIC - ✅ How to interpret and tune results
# MAGIC - ✅ How to integrate it into production
# MAGIC
# MAGIC **Start detecting anomalies in your data today!** 🚀
# MAGIC
