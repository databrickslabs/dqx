# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 E2E Single Table Validation (DQX-Optimized)
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Demonstrates a best-practice single-table DQX pipeline aligned with official Databricks Labs DQX methodology.  
# MAGIC
# MAGIC - **Input:** `wgu_poc.wgu_bronze.students_data_workflow`  
# MAGIC - **Outputs:**  
# MAGIC   - Valid records → `wgu_poc.dqx_output.students_valid_workflow`  
# MAGIC   - Quarantined (errors only) → `wgu_poc.dqx_output.students_quarantined_workflow`  
# MAGIC   - Clean audit summary → printed and stored in-memory  
# MAGIC
# MAGIC **DQX Lifecycle Steps**
# MAGIC
# MAGIC 1️⃣ Load data (incremental & valid timestamp range)  
# MAGIC 2️⃣ Profile dataset (using DQProfiler)  
# MAGIC 3️⃣ Generate rules (auto + manual\custom)  
# MAGIC 4️⃣ Apply checks via DQEngine  
# MAGIC 5️⃣ Upsert valid/quarantined records  
# MAGIC 6️⃣ Create null-safe audit summary

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx pyyaml

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, current_timestamp, max as spark_max, size
from delta.tables import DeltaTable
from datetime import datetime
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from pyspark.sql import functions as F

# -------------------------------------------
# ENVIRONMENT SETUP
# -------------------------------------------
spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# ============================================
# CONFIGURATION
# ============================================
source_table  = "wgu_poc.wgu_bronze.students_data_workflow"
valid_table   = "wgu_poc.wgu_silver.students_valid_workflow"
invalid_table = "wgu_poc.dqx_output.students_quarantined_workflow"

print(f"📘 Source Table: {source_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Load Data (Incremental with Timestamp Validation)
# MAGIC
# MAGIC - Filters out already-processed rows (based on latest `load_timestamp`)  
# MAGIC - Applies year sanity checks to prevent corrupt timestamps

# COMMAND ----------

students_df = spark.table(source_table)

# Incremental filter — only new records since last valid DQ run
if spark.catalog.tableExists(valid_table):
    last_ts = spark.table(valid_table).agg(spark_max("load_timestamp")).collect()[0][0]
    if last_ts:
        students_df = students_df.filter(col("load_timestamp") > last_ts)
        print(f"Incremental mode — filtering records newer than {last_ts}")
else:
    print("Full run — no prior valid data detected.")

if students_df.count() == 0:
    print("⚠️ No new rows detected. Exiting gracefully.")
    dbutils.notebook.exit("No new rows detected.")

# Sanity filter: remove invalid timestamps
students_df_filtered = students_df.filter(
    (year(col("load_timestamp")).between(1900, 9999)) &
    (year(col("month_end_date")).between(1900, 9999))
)
print(f"✅ Rows ready for profiling: {students_df_filtered.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ DQX Profiling + Auto Rule Generation
# MAGIC
# MAGIC Profiles the dataset to infer distributions, null counts, and data types.  
# MAGIC Then automatically generates DQX rules per field.

# COMMAND ----------

profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(students_df_filtered, options={"sample_fraction": 0.7, "limit": 10000})
auto_rules = DQGenerator(ws).generate_dq_rules(profiles)
print(f"✅ Profiling complete and {len(auto_rules)} auto rules generated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Custom & Manual Rules (with Custom Function Registration)
# MAGIC
# MAGIC - Manual YAML rules represent consistent validation logic  
# MAGIC - Custom rule demonstrates DQX extensibility

# COMMAND ----------

@register_rule("row")
def paid_status_consistency(paid_col: str, status_col: str):
    """
    Ensure that if a student is marked as 'paid', their status is logically consistent.
    """
    invalid_condition = (
        (F.col(paid_col) == True) &
        (~F.col(status_col).isin("graduated", "dropped"))
    )
    return make_condition(
        invalid_condition,
        "Paid is TRUE but student_status is not graduated or dropped",
        "paid_status_consistency"
    )

manual_rules = yaml.safe_load("""
- check:
    function: is_not_null_and_not_empty
    for_each_column:
      - student_id
      - load_timestamp
      - paid
  criticality: error
  name: not_null_core_fields

- check:
    function: is_unique
    arguments:
      columns: [student_id]
  criticality: error
  name: unique_student_id

- check:
    function: regex_match
    for_each_column: [student_id]
    arguments:
      regex: '^(?i)(?!INVALID|_|NULL)[A-Z]{3}[0-9]{8}$'
      negate: false
  criticality: error
  name: valid_student_id_pattern_strict

- check:
    function: regex_match
    for_each_column: [email]
    arguments:
      regex: '^(?i)(?!.*\\.\\.)(?!.*\\.@)(?!.*@\\.)[A-Z0-9](?:[A-Z0-9._%+-]{0,63}[A-Z0-9])?@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\\.[A-Z]{2,})+$'
      negate: false
  criticality: warn
  name: valid_email_format_strict

- check:
    function: is_in_list
    for_each_column: [student_status]
    arguments:
      allowed: [graduated, current, break, dropped]
  criticality: error
  name: valid_student_status

- check:
    function: paid_status_consistency
    arguments:
      paid_col: paid
      status_col: student_status
  criticality: warn
  name: paid_status_inconsistent_with_status

- check:
    function: regex_match
    for_each_column: [name]
    arguments:
      regex: '.*[0-9].*'
      negate: true
  criticality: warn
  name: name_no_numeric_chars
""")

print("✅ Manual and custom rules registered successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Apply DQX Rules + Split Valid vs Quarantined
# MAGIC
# MAGIC Uses **DQEngine** to apply all active rules to the dataset,  
# MAGIC generating `_errors` and `_warnings` metadata columns automatically.

# COMMAND ----------

dq_engine = DQEngine(ws)
all_rules = manual_rules + auto_rules

# Validate all rule structures
status = dq_engine.validate_checks(all_rules, custom_check_functions=globals())
if status.has_errors:
    print("⚠️ Rule validation issues detected:")
    print(status.errors)
else:
    print("✅ All rules validated successfully.")

# Apply rules and split valid/quarantined records
valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
    students_df_filtered,
    all_rules,
    custom_check_functions=globals()
)

# Ensure unique quarantine by student_id if present
if "student_id" in invalid_df.columns:
    invalid_df = invalid_df.dropDuplicates(["student_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Upsert Valid + Quarantined Records (Delta Upsert)
# MAGIC
# MAGIC Maintains incremental history via merge/upsert semantics.

# COMMAND ----------

def upsert_to_table(df, table_name, key="student_id"):
    """Upsert (merge) a dataframe into a Delta table by primary key."""
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"💾 Created new table {table_name} with {df.count()} records.")
        return
    delta = DeltaTable.forName(spark, table_name)
    join_key = key if key in df.columns else df.columns[0]
    (
        delta.alias("tgt")
        .merge(df.alias("src"), f"tgt.{join_key} = src.{join_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"💾 Upserted {df.count()} → {table_name}")

valid_df = valid_df.withColumn("dq_run_timestamp", current_timestamp())
invalid_df = invalid_df.withColumn("dq_run_timestamp", current_timestamp())

upsert_to_table(valid_df, valid_table)
upsert_to_table(invalid_df, invalid_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Simplified Audit Summary (DQX Metadata + Null-Safe Filtering)
# MAGIC
# MAGIC Leverages `_errors` and `_warnings` arrays produced by DQX to compute counts  
# MAGIC without assuming fixed schema or null behavior.

# COMMAND ----------

total_rows = students_df_filtered.count()
total_warnings = invalid_df.filter(size(col("_warnings")) > 0).count()
total_errors = invalid_df.filter(size(col("_errors")) > 0).count()

clean_df = valid_df.filter(
    ((col("_warnings").isNull()) | (size(col("_warnings")) == 0)) &
    ((col("_errors").isNull())   | (size(col("_errors")) == 0))
)
clean_count = clean_df.count()

summary_df = spark.createDataFrame(
    [
        ("Total Rows Profiled", total_rows),
        ("Total Warnings Found", total_warnings),
        ("Total Errors Found", total_errors),
        ("Total Fully Clean Records", clean_count),
    ],
    ["Metric", "Count"]
)

print(f"\n{'='*80}")
print("📊 FINAL DQX SUMMARY (Aligned with DQX Methodology)")
print(f"🏁 Run Completed: {datetime.now()}")
print(f"📦 Total Rows Profiled: {total_rows}")
print(f"⚠️ Total Warnings Found: {total_warnings}")
print(f"❌ Total Errors Found: {total_errors}")
print(f"💎 Fully Clean Records (no warns/errors): {clean_count}")
print(f"{'='*80}")

display(summary_df)
