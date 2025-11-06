# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 DQX Silver Table Re-Validation (No Profiling)
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Re-apply all pre-defined DQX rules on Silver-layer data to confirm ongoing integrity.  
# MAGIC
# MAGIC - **Input:** `wgu_poc.wgu_silver.students_valid_workflow`  
# MAGIC - **Outputs:**  
# MAGIC   - Revalidated valid records → `wgu_poc.wgu_gold.students_gold_valid`  
# MAGIC   - Revalidated quarantined (errors) → `wgu_poc.dqx_output.students_silver_quarantined`  
# MAGIC   - Audit summary → printed and displayed  
# MAGIC
# MAGIC **Steps**
# MAGIC 1️⃣ Load Silver data  
# MAGIC 2️⃣ Apply same DQX rules (manual + custom)  
# MAGIC 3️⃣ Split valid vs. quarantined  
# MAGIC 4️⃣ Upsert results and print audit summary

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx pyyaml
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, size
from delta.tables import DeltaTable
from datetime import datetime
import yaml
from databricks.sdk import WorkspaceClient
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
silver_table      = "wgu_poc.wgu_silver.students_valid_workflow"
valid_output_tbl  = "wgu_poc.wgu_gold.students_gold_valid"
invalid_output_tbl = "wgu_poc.dqx_output.students_silver_quarantined"

print(f"📘 Input Silver Table: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Load Silver Data

# COMMAND ----------

if not spark.catalog.tableExists(silver_table):
    raise Exception(f"❌ Silver table not found: {silver_table}")

silver_df = spark.table(silver_table)
print(f"✅ Loaded {silver_df.count()} records from Silver layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Manual + Custom Rules (Re-use same definitions)

# COMMAND ----------

@register_rule("row")
def paid_status_consistency(paid_col: str, status_col: str):
    """Ensure that if a student is marked as 'paid', their status is logically consistent."""
    invalid_condition = (
        (F.col(paid_col) == True) &
        (~F.col(status_col).isin("graduated", "dropped"))
    )
    return make_condition(
        invalid_condition,
        "Paid is TRUE but student_status is not graduated or dropped",
        "paid_status_consistency"
    )

# --- manual rules reused from original workflow
manual_rules = yaml.safe_load(r"""
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
      regex: "^(?i)(?!INVALID|_|NULL)[A-Z]{3}[0-9]{8}$"
      negate: false
  criticality: error
  name: valid_student_id_pattern_strict

- check:
    function: regex_match
    for_each_column: [email]
    arguments:
      regex: "^(?i)(?!.*\\.\\.)(?!.*\\.@)(?!.*@\\.)[A-Z0-9](?:[A-Z0-9._%+-]{0,63}[A-Z0-9])?@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\\.[A-Z]{2,})+$"
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
      regex: "^[A-Za-z][A-Za-z\\s'\\-]*$"
      negate: false
  criticality: error
  name: valid_name_format
""")

print("✅ Manual and custom rules loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Apply DQX Checks + Split Valid vs Quarantined

# COMMAND ----------

dq_engine = DQEngine(ws)

# Validate rule structure
status = dq_engine.validate_checks(manual_rules, custom_check_functions=globals())
if status.has_errors:
    print("⚠️ Rule validation issues detected:")
    print(status.errors)
else:
    print("✅ All rules validated successfully.")

# Apply rules and split valid/quarantined
valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
    silver_df,
    manual_rules,
    custom_check_functions=globals()
)

# Ensure uniqueness by student_id
if "student_id" in invalid_df.columns:
    invalid_df = invalid_df.dropDuplicates(["student_id"])

print(f"✅ Validation complete. Valid: {valid_df.count()}, Invalid: {invalid_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Upsert Results to Output Delta Tables

# COMMAND ----------

def upsert_to_table(df, table_name, key="student_id"):
    """Upsert dataframe into a Delta table by key."""
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

valid_df = valid_df.withColumn("recheck_run_timestamp", current_timestamp())
invalid_df = invalid_df.withColumn("recheck_run_timestamp", current_timestamp())

upsert_to_table(valid_df, valid_output_tbl)
upsert_to_table(invalid_df, invalid_output_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Generate Audit Summary

# COMMAND ----------

total_rows = silver_df.count()
total_warnings = invalid_df.filter(size(col("_warnings")) > 0).count()
total_errors = invalid_df.filter(size(col("_errors")) > 0).count()

clean_df = valid_df.filter(
    ((col("_warnings").isNull()) | (size(col("_warnings")) == 0)) &
    ((col("_errors").isNull())   | (size(col("_errors")) == 0))
)
clean_count = clean_df.count()

summary_df = spark.createDataFrame(
    [
        ("Total Rows Rechecked", total_rows),
        ("Total Warnings Found", total_warnings),
        ("Total Errors Found", total_errors),
        ("Total Fully Clean Records", clean_count),
    ],
    ["Metric", "Count"]
)

print(f"\n{'='*80}")
print("📊 FINAL SILVER REVALIDATION SUMMARY")
print(f"🏁 Run Completed: {datetime.now()}")
print(f"📦 Rows Checked: {total_rows}")
print(f"⚠️ Warnings: {total_warnings}")
print(f"❌ Errors: {total_errors}")
print(f"💎 Fully Clean Records: {clean_count}")
print(f"{'='*80}")

display(summary_df)
