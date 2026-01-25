# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Anomaly Detection Demo
# MAGIC
# MAGIC ## Learn Anomaly Detection in 15 Minutes
# MAGIC
# MAGIC This beginner-friendly demo shows you how to:
# MAGIC - Understand what anomaly detection is and why it matters
# MAGIC - Detect unusual patterns in your data with zero configuration
# MAGIC - Tune detection sensitivity to your needs
# MAGIC - Understand why specific records are flagged
# MAGIC - Integrate anomaly detection into production workflows
# MAGIC
# MAGIC **Dataset**: Simple sales transactions (universally relatable, no domain expertise required)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 0: What is Anomaly Detection in Data Quality?
# MAGIC
# MAGIC Before we dive into the code, let's understand what anomaly detection is and why it's valuable.
# MAGIC
# MAGIC ### The Data Quality Challenge: Known vs Unknown Issues
# MAGIC
# MAGIC #### 🎯 Known Unknowns (Traditional Data Quality)
# MAGIC
# MAGIC These are issues **you can anticipate** and write rules for:
# MAGIC
# MAGIC | Issue Type | Example | DQX Rule |
# MAGIC |------------|---------|----------|
# MAGIC | Null values | `amount` is NULL | `is_not_null(column="amount")` |
# MAGIC | Out of range | Price is negative | `is_in_range(column="price", min=0)` |
# MAGIC | Invalid format | Email without @ symbol | Regex validation |
# MAGIC
# MAGIC **Works great when you know what to look for!**
# MAGIC
# MAGIC #### 🔍 Unknown Unknowns (Anomaly Detection)
# MAGIC
# MAGIC These are issues **you DON'T know to look for**:
# MAGIC
# MAGIC - Unusual **patterns** across multiple columns
# MAGIC - Outlier **combinations** that are individually valid
# MAGIC - Subtle **data corruption** that passes all rules
# MAGIC
# MAGIC **Problem**: You can't write rules for things you haven't thought of!
# MAGIC
# MAGIC **Solution**: ML-based anomaly detection learns "normal" patterns from your data and flags deviations.
# MAGIC
# MAGIC ### Concrete Example
# MAGIC
# MAGIC ```
# MAGIC Known Unknown:  "Amount must be positive"
# MAGIC                 → is_in_range(min=0)
# MAGIC                 ✅ Catches: amount = -50
# MAGIC
# MAGIC Unknown Unknown: "Transaction for $47,283 at 3am on Sunday for 2 items"
# MAGIC                  → Anomaly detection
# MAGIC                  ✅ Catches: All fields valid individually, but pattern is unusual
# MAGIC ```
# MAGIC
# MAGIC ### Why Anomaly Detection Matters
# MAGIC
# MAGIC - ✅ **Catches issues before they become problems** - Early warning system
# MAGIC - ✅ **No need to anticipate every failure mode** - Adapts to your data
# MAGIC - ✅ **Learns patterns automatically** - No manual rule writing
# MAGIC - ✅ **Complements rule-based checks** - Use both together for comprehensive quality
# MAGIC
# MAGIC ### Unity Catalog Integration
# MAGIC
# MAGIC #### Built-in Quality Monitoring (Unity Catalog)
# MAGIC
# MAGIC Unity Catalog includes **table-level** anomaly detection by analyzing table metadata and state:
# MAGIC - Monitors table freshness by analyzing the history of table commits.
# MAGIC - Monitors table completeness by tracking historical row counts and alerts if the current row count falls outside the predicted range.
# MAGIC - Focuses on row count and commit timing metrics, but not table content (data).
# MAGIC - Great for monitoring table health for completeness and freshness.
# MAGIC
# MAGIC More [here](https://docs.databricks.com/aws/en/data-quality-monitoring/).
# MAGIC
# MAGIC #### When to Use DQX Anomaly Detection
# MAGIC
# MAGIC DQX provides **row-level** anomaly detection by analyzing the actual data content:
# MAGIC - **Detects unusual patterns in individual records/transactions** by analyzing the data content across the dataset.
# MAGIC - **Provides row-level anomaly reporting** with a detailed log identifying the specific records or transactions flagged as anomalous.
# MAGIC - **Multi-column pattern** detection (e.g., price + quantity + time)
# MAGIC - **Custom models per segment** (e.g., different regions, categories)
# MAGIC - **Feature contributions** to understand WHY records are anomalous
# MAGIC - **Integrated** and executable with other DQX quality rules types
# MAGIC
# MAGIC #### Complementary Approach
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │  Rule-Based Checks (Known Unknowns)                     │
# MAGIC │  • is_not_null, is_in_range, regex validation           │
# MAGIC │  • Schema validation, referential integrity             │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                            +
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │  ML Anomaly Detection (Unknown Unknowns)                │
# MAGIC │  • Pattern detection, outlier identification            │
# MAGIC │  • Multi-column relationship validation                 │
# MAGIC │  DQX: Row-level anomaly detection                       │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                            +
# MAGIC ┌───────────────────────────────────────────────────────────────┐
# MAGIC │  Unity Catalog Monitoring (Table Health)                      │
# MAGIC │  • Metadata and table state tracking (e.g. commits, row count)│
# MAGIC │  Table freshness and completeness for Unity Catalog tables    │
# MAGIC └───────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC - 💡 **Anomaly detection finds issues in the data you didn't know to look for**
# MAGIC - 💡 **Complements (doesn't replace) rule-based checks - use both!**
# MAGIC - 💡 **Unity Catalog monitors table freshness and completeness of tables, DQX monitors data inside the tables**
# MAGIC - 💡 **Together, they provide comprehensive quality coverage**
# MAGIC
# MAGIC Let's see how easy it is to add DQX anomaly detection to your pipeline! 🚀
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites: Install DQX with Anomaly Support
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

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[anomaly] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[anomaly]

%restart_python

# COMMAND ----------

# Configure widgets for catalog and schema
dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup & Data Generation
# MAGIC
# MAGIC First, let's set up our environment and create simple sales transaction data.
# MAGIC

# COMMAND ----------

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

# Generate simple sales transaction data
def generate_sales_data(
    num_rows:int=1000,
    anomaly_rate:float=0.02,
    dq_null_amount_rate:float=0.01,
    dq_null_quantity_rate:float=0.005,
    dq_negative_amount_rate:float=0.005,
):
    """
    Generate sales transaction data with injected anomalies.
    
    Normal patterns:
    - Amount: $10-500 per transaction
    - Quantity: 1-10 items
    - Business hours: 9am-6pm weekdays
    - Regional consistency
    
    Anomalies (2% - matches default expected_anomaly_rate):
    - Pricing errors (VERY extreme: 40-50x or 1/40 of normal amounts)
    - Quantity spikes (bulk orders 100-150 items = 20-30x normal)
    - Timing anomalies (off-hours + 5-8x amount + 25-40 quantity)
    - Multi-factor (6-10x amount + 35-60 quantity + always off-hours)
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
        
        # Inject a few simple DQ issues (nulls/negatives) for rule-based checks
        dq_issue_roll = random.random()
        dq_issue_type = None
        if dq_issue_roll < dq_null_amount_rate:
            dq_issue_type = "null_amount"
        elif dq_issue_roll < dq_null_amount_rate + dq_null_quantity_rate:
            dq_issue_type = "null_quantity"
        elif dq_issue_roll < dq_null_amount_rate + dq_null_quantity_rate + dq_negative_amount_rate:
            dq_issue_type = "negative_amount"

        if dq_issue_type == "null_amount":
            amount = None
            quantity = max(1, int(np.random.normal(pattern["quantity"], 1)))
        elif dq_issue_type == "null_quantity":
            amount = round(pattern["base_amount"] * random.uniform(0.85, 1.15), 2)
            quantity = None
        elif dq_issue_type == "negative_amount":
            amount = -abs(round(pattern["base_amount"] * random.uniform(0.5, 1.5), 2))
            quantity = max(1, int(np.random.normal(pattern["quantity"], 1)))

        # Inject anomalies (unusual patterns not caught by simple rules)
        elif random.random() < anomaly_rate:
            # Bias towards more extreme anomaly types for better detection
            anomaly_type = random.choices(
                ["pricing", "quantity", "timing", "multi_factor"],
                weights=[2, 2, 1, 3]  # Favor pricing, quantity, and multi-factor
            )[0]
            
            if anomaly_type == "pricing":
                # Pricing error: VERY extreme amounts (40-50x or 1/40 of normal)
                multiplier = random.choice([random.uniform(40, 50), 1.0 / random.uniform(35, 45)])
                amount = round(pattern["base_amount"] * multiplier, 2)
                quantity = int(np.random.normal(pattern["quantity"], 0.3))  # Near-normal quantity
            
            elif anomaly_type == "quantity":
                # Bulk order spike (100-150 items = 20-30x normal) 
                amount = round(pattern["base_amount"] * random.uniform(0.95, 1.05), 2)  # Normal amount
                quantity = random.randint(100, 150)
            
            elif anomaly_type == "timing":
                # Off-hours transaction WITH very unusual amount (multi-factor)
                amount = round(pattern["base_amount"] * random.uniform(5.0, 8.0), 2)  # 5-8x normal
                quantity = random.randint(25, 40)  # 5-8x normal
                date = date.replace(hour=random.choice([2, 3, 4, 22, 23]))  # Late night/early morning
                # Or make it weekend
                if random.random() > 0.5:
                    date = date + timedelta(days=(5 - date.weekday()))  # Move to Saturday
            
            else:  # multi-factor: EXTREME multi-dimensional anomaly
                # Extreme regional mismatch + very unusual quantity + off-hours
                other_region = random.choice([r for r in regions if r != region])
                amount = round(region_patterns[other_region]["base_amount"] * random.uniform(6.0, 10.0), 2)
                quantity = random.randint(35, 60)  # 7-12x normal
                date = date.replace(hour=random.choice([2, 3, 4, 22, 23]))  # Always off-hours
        
        else:
            # Normal transaction (tighter variance for more consistent patterns)
            amount = round(pattern["base_amount"] * random.uniform(0.85, 1.15), 2)
            quantity = max(1, int(np.random.normal(pattern["quantity"], 1)))
        
        # Ensure valid ranges (skip for injected nulls/negatives)
        if amount is not None and dq_issue_type != "negative_amount":
            amount = max(10, min(10000, amount))
        if quantity is not None:
            quantity = max(1, min(150, quantity))  # Allow bulk orders up to 150
        
        data.append((transaction_id, date, amount, quantity, category, region))
    
    return data

# Generate data
print("🔄 Generating sales transaction data...\n")
anomaly_rate = 0.02
dq_null_amount_rate = 0.01
dq_null_quantity_rate = 0.005
dq_negative_amount_rate = 0.005
dq_issue_rate = dq_null_amount_rate + dq_null_quantity_rate + dq_negative_amount_rate

sales_data = generate_sales_data(
    num_rows=1000,
    anomaly_rate=anomaly_rate,
    dq_null_amount_rate=dq_null_amount_rate,
    dq_null_quantity_rate=dq_null_quantity_rate,
    dq_negative_amount_rate=dq_negative_amount_rate,
)

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("date", TimestampType(), False),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("category", StringType(), False),
    StructField("region", StringType(), False),
])

df_sales = spark.createDataFrame(sales_data, schema)

print("📊 Sample of sales transactions:")
display(df_sales.orderBy("date"))

total_rows = df_sales.count()
print(f"\n✅ Generated {total_rows} sales transactions")
print(f"   Expected anomalies: ~{int(total_rows * anomaly_rate)} ({anomaly_rate*100:.0f}%)")
print(f"   Expected rule-based issues: ~{int(total_rows * dq_issue_rate)} ({dq_issue_rate*100:.1f}%)")
print(f"\n💡 Data includes:")
print(f"   • Normal patterns: Business hours, typical amounts (170-230), reasonable quantities (4-6)")
print(f"   • Injected anomalies: unusual multi-column patterns (e.g., timing + amount + quantity)")
print(f"   • Injected DQ issues: a few nulls and negative amounts (caught by rules)")
print(f"\n🎯 Anomaly detection will identify patterns that deviate significantly from normal behavior")
print(f"\n📌 Note: 2% anomaly rate matches the model's default 'expected_anomaly_rate' parameter")


# COMMAND ----------

# Get catalog and schema from widgets
catalog = dbutils.widgets.get("demo_catalog")
schema_name = dbutils.widgets.get("demo_schema")

print(f"📂 Using catalog: {catalog}")
print(f"📂 Using schema: {schema_name}\n")

# Save data to table
table_name = f"{catalog}.{schema_name}.sales_transactions"
df_sales.write.mode("overwrite").saveAsTable(table_name)

print(f"✅ Data saved to: {table_name}")


# COMMAND ----------

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
# MAGIC ## Section 2: Combined Quality Checks: Rule-Based + ML Anomaly Detection
# MAGIC
# MAGIC Let's build a comprehensive quality pipeline that combines:
# MAGIC 1. **Rule-based checks** (known unknowns): nulls, ranges, formats
# MAGIC 2. **ML anomaly detection** (unknown unknowns): unusual patterns
# MAGIC
# MAGIC With **ZERO configuration**, the system will:
# MAGIC - Automatically select relevant columns for anomaly detection
# MAGIC - Auto-detect if segmentation is needed (e.g., separate models per region)
# MAGIC - Train ensemble models (2 models by default for robustness)
# MAGIC - Score all transactions with both rule-based AND anomaly checks
# MAGIC - Provide feature contributions to explain WHY records are flagged as anomalous
# MAGIC
# MAGIC **You provide**: Just the data and rules of your choice 
# MAGIC **DQX provides**: Everything else, optimized for performance!
# MAGIC

# COMMAND ----------

# Train anomaly detection model with zero configuration
print("🎯 Training anomaly detection model...")
print("   DQX will automatically discover patterns in your data\n")

model_name_auto = "sales_auto"
model_uri_auto = anomaly_engine.train(
    df=spark.table(table_name),
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

print("\n💡 Understanding the Results:")
print("   • DQX automatically found patterns in your data")
print("   • If 'segment_by' has values, DQX created separate models for different groups")
print("   • Each row is a trained model ready to score new data")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Viewing Models in Databricks UI
# MAGIC
# MAGIC Your trained models are automatically registered in **Unity Catalog Model Registry**. Here's how to view them:
# MAGIC
# MAGIC **Option 1: Catalog Explorer**
# MAGIC 1. Click **Catalog** in the left sidebar
# MAGIC 2. Navigate to your catalog → schema
# MAGIC 3. Look for models named `sales_auto` (or `sales_auto_ensemble_0`, `sales_auto_ensemble_1` for ensemble models)
# MAGIC 4. Click on a model to see:
# MAGIC    - Model versions
# MAGIC    - MLflow run details (parameters, metrics)
# MAGIC    - Model lineage and schema
# MAGIC
# MAGIC **Option 2: MLflow Experiments**
# MAGIC 1. Click **Experiments** in the left sidebar
# MAGIC 2. Find your notebook's experiment (automatically created per notebook)
# MAGIC 3. View all training runs with:
# MAGIC    - Hyperparameters (contamination, num_trees, etc.)
# MAGIC    - Validation metrics (precision, recall, F1)
# MAGIC    - Model artifacts and signatures
# MAGIC
# MAGIC **What DQX Logs Automatically:**
# MAGIC - ✅ **Parameters**: contamination, num_trees, subsampling_rate, random_seed
# MAGIC - ✅ **Metrics**: precision, recall, F1 score, validation accuracy
# MAGIC - ✅ **Model Signature**: Input/output schemas for Unity Catalog
# MAGIC - ✅ **Model Artifacts**: Serialized sklearn model + feature metadata
# MAGIC
# MAGIC **Model URI Format:**
# MAGIC ```
# MAGIC models:/<catalog>.<schema>.<model_name>/<version>
# MAGIC ```
# MAGIC Example: `models:/main.dqx_demo.sales_auto/1`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying Quality Checks
# MAGIC
# MAGIC Now that we have our anomaly detection model trained, let's apply it alongside traditional rule-based checks to score all transactions.
# MAGIC The anomalies will be reported in the _warn and _error columns similar to other DQX checks.
# MAGIC In addition, _dq_info column will contain anomaly score (0-1) and feature contributions explaining WHY it was flagged.
# MAGIC For analysis purposes, the score will be present in all records, not just anomalies.
# MAGIC This allows you to understand the "unusualness" of every record, and tune the threshold as needed.
# MAGIC

# COMMAND ----------

# Apply quality checks: combine rule-based + ML anomaly detection
print("🔍 Applying quality checks to all transactions...\n")

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
            "merge_columns": ["transaction_id"],
            "model": model_name_auto,
            "registry_table": registry_table
            # Default: 2 models for confidence, explains why data is anomalous, threshold 0.60
        }
    )
]

df_scored = dq_engine.apply_checks(df_sales, checks_combined)

display(df_scored)

df_anomalies = dq_engine.get_invalid(df_scored)
score_col = F.col("_dq_info.anomaly.score")

score_df = df_scored.select(score_col.alias("score")).where(score_col.isNotNull())
p90, p95, p99 = score_df.stat.approxQuantile("score", [0.9, 0.95, 0.99], 0.0)
percentile_band = (
    F.when(score_col >= p99, F.lit("p99+ (top 1%)"))
    .when(score_col >= p95, F.lit("p95-99 (top 5%)"))
    .when(score_col >= p90, F.lit("p90-95 (top 10%)"))
    .otherwise(F.lit("<p90 (bottom 90%)"))
)
total_scored = df_scored.count()
anomalies_count = df_anomalies.count()

print(f"✅ Quality checks complete!")
print(f"\n📊 Results:")
print(f"   Total transactions: {total_rows}")
print(f"   Anomalies found: {anomalies_count} ({(anomalies_count / total_rows) * 100:.1f}%)")
print("ℹ️  Each record receives an anomaly score.")
print("   The score threshold decides whether a record is flagged as anomalous.")
print("   Even records that are not flagged still get a score for analysis.")
print("   Think of the score as a 0-1 unusualness rating: higher = more likely anomalous.")
print(f"\n🔝 Top 10 anomalies:\n")

display(df_anomalies.orderBy(F.col("_dq_info.anomaly.score").desc()).select(
    "transaction_id", "date", "amount", "quantity", "category", "region",
    F.round("_dq_info.anomaly.score", 3).alias("anomaly_score"),
    percentile_band.alias("score_percentile"),
    F.col("_dq_info.anomaly.contributions").alias("why_anomalous")
).limit(10))

print("   Example pattern: high amount + off-hours timing + unusual region/category")

print("\n💡 What Just Happened:")
print("   • Rule-based checks caught known issues (nulls, out-of-range values)")
print("   • Anomaly detection found unusual patterns you didn't explicitly define")
print("   • The 'why_anomalous' column explains what made each record unusual")
print("   • Percentile band shows how unusual a record is within this dataset")
print("   • Threshold of 0.60 balances finding issues vs false alarms")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 3: Understanding Your Results
# MAGIC
# MAGIC Let's explore the anomalies we found and learn how to interpret anomaly scores.
# MAGIC
# MAGIC **What you'll learn:**
# MAGIC - How anomaly scores work (0 to 1 unusualness scale)
# MAGIC - What makes a score "high" vs "normal"  
# MAGIC - Why certain records were flagged as unusual
# MAGIC
# MAGIC **Important**: Anomaly scores are NOT probabilities or confidence levels! Think of the score as how
# MAGIC easy it is to separate a record from the rest of your data. Easier to separate = more unusual.
# MAGIC

# COMMAND ----------

# Analyze score distribution
print("📊 Anomaly Score Distribution:\n")

score_stats = df_scored.select("_dq_info.anomaly.score").describe()
display(score_stats)

# Show score ranges (aligned with 0.60 threshold)
print("📈 Score Range Breakdown (raw scores):\n")

score_ranges = df_scored.select(
    F.count(F.when(F.col("_dq_info.anomaly.score") < 0.5, 1)).alias("normal_0.0_0.5"),
    F.count(F.when((F.col("_dq_info.anomaly.score") >= 0.5) & (F.col("_dq_info.anomaly.score") < 0.6), 1)).alias("borderline_0.5_0.6"),
    F.count(F.when((F.col("_dq_info.anomaly.score") >= 0.6) & (F.col("_dq_info.anomaly.score") < 0.75), 1)).alias("flagged_0.6_0.75"),
    F.count(F.when(F.col("_dq_info.anomaly.score") >= 0.75, 1)).alias("highly_anomalous_0.75_1.0"),
).first()

total = total_scored
print(f"Likely Normal (0.0-0.5):      {score_ranges['normal_0.0_0.5']:4d} ({score_ranges['normal_0.0_0.5']/total*100:5.1f}%) ← Not flagged")
print(f"Near Threshold (0.5-<0.6):    {score_ranges['borderline_0.5_0.6']:4d} ({score_ranges['borderline_0.5_0.6']/total*100:5.1f}%) ← Not flagged")
print(f"Flagged (0.6-0.75):           {score_ranges['flagged_0.6_0.75']:4d} ({score_ranges['flagged_0.6_0.75']/total*100:5.1f}%) ← ANOMALIES (flagged)")
print(f"Highly Anomalous (0.75-1.0):  {score_ranges['highly_anomalous_0.75_1.0']:4d} ({score_ranges['highly_anomalous_0.75_1.0']/total*100:5.1f}%) ← ANOMALIES (extreme)")

print("\n📊 Percentile-Based Breakdown (easy to interpret):\n")
percentile_ranges = df_scored.select(
    F.count(F.when(score_col < p90, 1)).alias("normal_p90"),
    F.count(F.when((score_col >= p90) & (score_col < p95), 1)).alias("borderline_p90_p95"),
    F.count(F.when((score_col >= p95) & (score_col < p99), 1)).alias("flagged_p95_p99"),
    F.count(F.when(score_col >= p99, 1)).alias("highly_anomalous_p99"),
).first()

print(f"Normal (<p90):                {percentile_ranges['normal_p90']:4d} ({percentile_ranges['normal_p90']/total*100:5.1f}%) ← Typical")
print(f"Upper tail (p90-p95):         {percentile_ranges['borderline_p90_p95']:4d} ({percentile_ranges['borderline_p90_p95']/total*100:5.1f}%) ← Unusual")
print(f"Top tail (p95-p99):           {percentile_ranges['flagged_p95_p99']:4d} ({percentile_ranges['flagged_p95_p99']/total*100:5.1f}%) ← Very unusual")
print(f"Extreme (p99+):               {percentile_ranges['highly_anomalous_p99']:4d} ({percentile_ranges['highly_anomalous_p99']/total*100:5.1f}%) ← Extremely unusual")

print(f"\nℹ️ Percentiles compare records within this dataset.")
print(f"   p90~{p90:.3f}, p95~{p95:.3f}, p99~{p99:.3f} (top 10%, 5%, 1%)")
print("   Percentile bands are for interpretation only; the threshold is the actual rule.")

print(f"\n💡 What Do These Scores Mean?")
print(f"   • Scores are based on how 'isolated' a record is from normal patterns")
print(f"   • Low scores (0.0-0.5): Blend in with normal data (NOT flagged)")
print(f"   • Near-threshold (0.5-<0.6): Close to 0.60 (NOT flagged)")
print(f"   • High scores (≥0.6): Stands out as different (FLAGGED as anomalies)")
print(f"   • This is NOT a probability - it is a relative unusualness score")
print(f"   • The threshold (0.60) is tuned empirically, not a statistical significance level")


# COMMAND ----------

# Compare normal vs anomalous transactions (using 0.60 threshold)
print("🔍 Normal vs Anomalous Transaction Comparison:\n")

normal_stats = df_scored.filter(F.col("_dq_info.anomaly.score") < 0.6).agg(
    F.avg("amount").alias("avg_amount"),
    F.avg("quantity").alias("avg_quantity"),
    F.count("*").alias("count")
).first()

anomaly_stats = df_scored.filter(F.col("_dq_info.anomaly.score") >= 0.6).agg(
    F.avg("amount").alias("avg_amount"),
    F.avg("quantity").alias("avg_quantity"),
    F.count("*").alias("count")
).first()

print("Normal Transactions (score < 0.60):")
print(f"   Count: {normal_stats['count']} ({normal_stats['count']/total_scored*100:.1f}%)")
print(f"   Avg Amount: ${normal_stats['avg_amount']:.2f}")
print(f"   Avg Quantity: {normal_stats['avg_quantity']:.1f}")

print("\nFlagged Anomalies (score ≥ 0.60):")
print(f"   Count: {anomaly_stats['count']} ({anomaly_stats['count']/total_scored*100:.1f}%)")
print(f"   Avg Amount: ${anomaly_stats['avg_amount']:.2f}")
print(f"   Avg Quantity: {anomaly_stats['avg_quantity']:.1f}")

print("\n💡 Expected Results:")
print("   • Normal transactions should be ~95% of data with typical amounts/quantities")
print("   • Anomalies should be ~5% with extreme or unusual patterns")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 4: Tuning the Threshold
# MAGIC
# MAGIC The threshold controls which records get flagged as anomalies. It's like setting a "sensitivity dial":
# MAGIC
# MAGIC - **Lower threshold** (e.g., 0.50-0.55): More sensitive, flags more records as unusual
# MAGIC - **Higher threshold** (e.g., 0.65-0.75): Less sensitive, flags only very unusual records
# MAGIC
# MAGIC **The default of 0.60** was chosen through testing across various datasets. It balances:
# MAGIC - Finding real issues (recall)
# MAGIC - Avoiding false alarms (precision)
# MAGIC
# MAGIC **Remember**: This is NOT a statistical confidence level! It's a cutoff on the "isolation score" that determines what's unusual enough to investigate.
# MAGIC
# MAGIC **Important**: We already computed scores for every record. You can change the threshold and re-filter the
# MAGIC results without re-running the anomaly check.
# MAGIC
# MAGIC Let's see how changing the threshold affects results!
# MAGIC

# COMMAND ----------

# Try different thresholds
print("🎚️  Testing Different Thresholds:\n")
print("Threshold | Anomalies | % of Data | Interpretation")
print("-" * 70)

thresholds = [0.5, 0.6, 0.7]
total_count = total_scored

for threshold in thresholds:
    anomaly_count = df_scored.filter(F.col("_dq_info.anomaly.score") >= threshold).count()
    percentage = (anomaly_count / total_count) * 100
    
    if threshold <= 0.5:
        interpretation = "Sensitive (more alerts)"
    elif threshold <= 0.6:
        interpretation = "Balanced (recommended)"
    else:
        interpretation = "Strict (fewer alerts)"
    
    print(f"   {threshold:.1f}   |   {anomaly_count:4d}    |  {percentage:5.1f}%  | {interpretation}")

print("\n💡 How to Choose Your Threshold:")
print("   • Start with default 0.60 (balanced for most use cases)")
print("   • Too many alerts to investigate? → Increase to 0.65 or 0.70")
print("   • Missing real issues? → Decrease to 0.50 or 0.55")
print("   • The 'right' threshold depends on:")
print("     - Your investigation capacity (how many alerts can you handle?)")
print("     - Your risk tolerance (cost of missing an issue vs false alarm)")


# COMMAND ----------

# Let's look at borderline cases
print("🔍 Examining Borderline Cases (scores 0.50-<0.60):\n")

borderline = df_scored.filter(
    (F.col("_dq_info.anomaly.score") >= 0.50) & 
    (F.col("_dq_info.anomaly.score") < 0.60)
).orderBy(F.col("_dq_info.anomaly.score").desc())

print(f"Found {borderline.count()} borderline transactions:\n")
display(borderline.select(
    "transaction_id", "amount", "quantity", "category", "region",
    F.round("_dq_info.anomaly.score", 3).alias("score")
).limit(10))

print("\n💡 These are on the edge - slight threshold changes will include/exclude them")
print("   Review these to calibrate your threshold for your use case")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 5: Manual Column Selection (Optional - Advanced)
# MAGIC
# MAGIC **Note**: This section shows optional advanced features. Feel free to skip to Section 7 for production patterns!
# MAGIC
# MAGIC Auto-discovery is great for exploration, but for production you might want explicit control over which features the model uses.
# MAGIC
# MAGIC Let's train a model with **manually selected columns**.
# MAGIC

# COMMAND ----------

# Train with manual column selection
print("🎯 Training model with manual column selection...\n")

model_name_manual = "sales_manual"
model_uri_manual = anomaly_engine.train(
    df=spark.table(table_name),
    columns=["amount", "quantity"],  # Explicitly specify numeric columns only
    model_name=model_name_manual,
    registry_table=registry_table
)

print(f"✅ Manual model trained!")
print(f"   Model URI: {model_uri_manual}")
print(f"\n💡 Note: Manual column selection works best with numeric columns")
print(f"   Datetime/categorical columns are auto-engineered when using auto-discovery")

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

print(f"\n💡 Key Differences:")
print(f"   • Auto model: Discovered columns automatically + may have segmentation")
print(f"   • Manual model: You explicitly chose 3 columns (amount, quantity)")
print(f"\n💡 When to use each approach:")
print(f"   • Auto-discovery: Exploration, quick start, don't know what matters")
print(f"   • Manual selection: Production, control features, domain knowledge")
print(f"   • Both are valid! Start with auto, refine with manual")


# COMMAND ----------

# Score with manual model
print("🔍 Scoring with manual model...\n")

checks_manual = [
    DQDatasetRule(
        check_func=has_no_anomalies,
        check_func_kwargs={
            "merge_columns": ["transaction_id"],
            "model": model_name_manual,
            "score_threshold": 0.5,
            "registry_table": registry_table
        }
    )
]

df_valid, df_anomalies_manual = dq_engine.apply_checks_and_split(df_sales, checks_manual)

print(f"⚠️  Manual model found {df_anomalies_manual.count()} anomalies")
print(f"   (Auto model found {df_anomalies.count()} anomalies)")
print(f"\n🔝 Top 5 anomalies from manual model:\n")


display(df_anomalies_manual.orderBy(F.col("_dq_info.anomaly.score").desc()).select(
    "transaction_id", "amount", "quantity", "date",
    F.round("_dq_info.anomaly.score", 3).alias("score")
).limit(5))

print("\n💡 Results may differ slightly because we're using different features")
print("   This is normal and expected!")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Section 6: Deep Dive - Feature Contributions (Optional - Advanced)
# MAGIC
# MAGIC **Note**: This section shows detailed analysis of contributions. Skip to Section 7 for production patterns!
# MAGIC
# MAGIC **Reminder**: Contributions are now enabled by default (you already saw them in Section 2), but this section shows how to analyze them in depth.
# MAGIC
# MAGIC Finding anomalies is great, but **understanding WHY** they're anomalous is crucial for investigation. Feature contributions show which columns drove each anomaly score.
# MAGIC

# COMMAND ----------

# Score with feature contributions
print("🔍 Scoring with feature contributions (explainability)...\n")

checks_with_contrib = [
    DQDatasetRule(
        check_func=has_no_anomalies,
        check_func_kwargs={
            "merge_columns": ["transaction_id"],
            "model": model_name_manual,
            "score_threshold": 0.5,
            "include_contributions": True,  # Add this to get explanations!
            "registry_table": registry_table
        }
    )
]

df_with_contrib = dq_engine.apply_checks(df_sales, checks_with_contrib)

print("✅ Scored with feature contributions!")
print("\n🎯 Top Anomalies with Explanations:\n")

# Filter by _errors column (standard DQX pattern) to get flagged anomalies
anomalies_explained = df_with_contrib.filter(
    F.size(F.col("_errors")) > 0
).orderBy(F.col("_dq_info.anomaly.score").desc()).limit(5)

display(anomalies_explained.select(
    "transaction_id",
    "amount",
    "quantity",
    F.date_format("date", "yyyy-MM-dd HH:mm").alias("date"),
    F.round("_dq_info.anomaly.score", 3).alias("score"),
    F.col("_dq_info.anomaly.contributions").alias("contributions")
))

print("\n💡 How to Read Contributions:")
print("   • Contributions show which features made this transaction unusual")
print("   • Higher contribution = that feature is more responsible for the anomaly")
print("   • Use this to triage and investigate efficiently!")
print("\n   Example: If 'amount' has high contribution → pricing issue")
print("            If 'quantity' has high contribution → bulk order anomaly")
print("            If 'date' has high contribution → timing anomaly")
print("\n💡 About Category Contributions:")
print("   • You'll see contributions from ALL category features (one-hot encoded)")
print("   • Non-matching categories (e.g., 'category_Electronics' when item is Clothing)")
print("     show small contributions representing the feature's ABSENCE")
print("   • Focus on features with >5% contribution for investigation")


# COMMAND ----------

# Show one detailed example
print("🔎 Detailed Example - Top Anomaly:\n")

# Extract flat columns for easier access
anomalies_flattened = anomalies_explained.select(
    "transaction_id",
    "amount",
    "quantity",
    "date",
    F.col("_dq_info.anomaly.score").alias("score"),
    F.col("_dq_info.anomaly.contributions").alias("contributions")
)

top_anomaly = anomalies_flattened.first()

print(f"Transaction ID: {top_anomaly['transaction_id']}")
print(f"Anomaly Score: {top_anomaly['score']:.3f}")
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
        print(f"   {feature}: {abs(value)*100:.1f}% contribution")
    
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
# MAGIC ## Section 7: Using in Production
# MAGIC
# MAGIC Ready to use anomaly detection in real pipelines? This section shows you how.
# MAGIC
# MAGIC **What you'll learn:**
# MAGIC - How to automatically separate good data from bad data
# MAGIC - How to route anomalies to a quarantine table for investigation
# MAGIC - Best practices for production data quality pipelines
# MAGIC
# MAGIC **Workflow tip**: If you train via YAML workflow and omit `model_name`, it defaults to
# MAGIC `dqx_anomaly_<run_config.name>` (e.g., `dqx_anomaly_orders`).
# MAGIC

# COMMAND ----------

# Automatically separate clean data from anomalies
print("📦 Separating clean data from anomalies...\n")

# Save clean and quarantined records using the built-in helper
clean_table = f"{catalog}.{schema_name}.sales_transactions_clean"
quarantine_table = f"{catalog}.{schema_name}.sales_anomalies_quarantine"

dq_engine.apply_checks_and_save_in_table(
    checks=checks_combined,
    input_config=InputConfig(location=table_name),
    output_config=OutputConfig(location=clean_table, mode="overwrite"),
    quarantine_config=OutputConfig(location=quarantine_table, mode="overwrite"),
)

good_df = spark.table(clean_table)
bad_df = spark.table(quarantine_table)

print(f"✅ Automatically saved clean and quarantined data using apply_checks_and_save_in_table():")
print(f"   Clean records: {good_df.count()}")
print(f"   Quarantined: {bad_df.count()}")
print(f"\n💡 Benefits of apply_checks_and_save_in_table():")
print(f"   • Automatically routes failed checks to quarantine")
print(f"   • Handles both anomalies AND rule violations")
print(f"   • No manual filtering needed - just specify the checks!")
print(f"\nTables created:")
print(f"   Input: {table_name}")
print(f"   Clean: {clean_table}")
print(f"   Quarantine: {quarantine_table}")
print(f"\n📋 Access quarantined records (includes DQX _errors/_warnings metadata):")
print(f"   spark.table('{quarantine_table}')")


# COMMAND ----------

# Pattern 2: Use in downstream pipelines
print("🔄 Pattern 2: Integrate with Downstream Pipelines\n")

# Use the clean data (good_df) for downstream processing
print("💡 Best Practices:")
print("   ✅ Use good_df for downstream analytics, ML training, reporting")
print("   ✅ Route bad_df to investigation/remediation workflows")
print("   ✅ Monitor quarantine table for trends and retraining signals")
print("   ✅ Combine rule-based + anomaly checks (shown in Section 2)")
print(f"\n📊 Production Flow:")
print(f"   1. Apply checks (Section 2) → rule-based + anomaly detection")
print(f"   2. Split data (this section) → good vs bad records")
print(f"   3. Process good_df → downstream systems")
print(f"   4. Investigate bad_df → manual review or auto-remediation")
print(f"\n✨ With new defaults, you get:")
print(f"   • Ensemble models (confidence scores)")
print(f"   • Feature contributions (explainability)")
print(f"   • Optimized performance (10-15x faster than baseline)")
print(f"   • Production-ready out-of-the-box!")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Summary & Next Steps
# MAGIC
# MAGIC ### 💡 Key Takeaways
# MAGIC
# MAGIC - **Start simple**: Use auto-discovery first, then refine with manual selection
# MAGIC - **Threshold matters**: Adjust based on your tolerance for false positives
# MAGIC - **Contributions are crucial**: Use them to triage and investigate efficiently
# MAGIC - **Complement, don't replace**: Use both rule-based checks and anomaly detection
# MAGIC - **Unity Catalog + DQX**: Together they provide comprehensive data quality coverage
# MAGIC
# MAGIC ### 🚀 Next Steps
# MAGIC
# MAGIC #### 1. Apply to Your Data
# MAGIC ```python
# MAGIC # Replace with your table
# MAGIC model = anomaly_engine.train(
# MAGIC     df=spark.table("your_catalog.your_schema.your_table"),
# MAGIC     model_name="your_model_name"
# MAGIC )
# MAGIC
# MAGIC checks = [
# MAGIC     has_no_anomalies(
# MAGIC         merge_columns=["your_id_column"],
# MAGIC         model="your_model_name"
# MAGIC     )
# MAGIC ]
# MAGIC df_scored = dq_engine.apply_checks(your_df, checks)
# MAGIC ```
# MAGIC
# MAGIC #### 2. Explore Advanced Features
# MAGIC - **Segmented models**: Train separate models per region, category, etc.
# MAGIC - **Drift detection**: Monitor when models become stale
# MAGIC - **Ensemble models**: Get confidence intervals on scores
# MAGIC - See the pharma and investment banking demos for examples!
# MAGIC
# MAGIC #### 3. Set Up Production Workflows
# MAGIC - Automate model training (weekly/monthly)
# MAGIC - Schedule scoring (hourly/daily)
# MAGIC - Build investigation workflow around quarantine table
# MAGIC - Integrate with alerting (Slack, PagerDuty, etc.)
# MAGIC
# MAGIC #### 4. Monitor & Iterate
# MAGIC - Review flagged anomalies regularly
# MAGIC - Adjust thresholds based on false positive rate
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