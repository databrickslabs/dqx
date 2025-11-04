# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 E2E Multi-Table Validation (DQX-Optimized)
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Apply unified DQX validation rules to multiple tables of identical schema.  
# MAGIC Based on your working single-table pipeline, extended for multi-table orchestration.  
# MAGIC
# MAGIC **Tables:**  
# MAGIC - `wgu_poc.wgu_bronze.students_data_workflow`  
# MAGIC - `wgu_poc.wgu_bronze.students_data_clean_workflow`
# MAGIC
# MAGIC **Outputs (isolated)**  
# MAGIC - Valid → `<table>_checked_multitable`  
# MAGIC - Quarantined → `<table>_quarantine_multitable`  
# MAGIC - Audit summary printed per table

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install --quiet databricks-labs-dqx pyyaml
# MAGIC
# MAGIC # Restart only if first-time install
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, current_timestamp, year, max as spark_max
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

# ============================================
# ENVIRONMENT + CONFIGURATION
# ============================================
spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()
dq_engine = DQEngine(ws, spark)

catalog = "wgu_poc"
schema = "wgu_bronze"
checks_table = f"{catalog}.{schema}.checks_multitable"

target_tables = [
    f"{catalog}.{schema}.students_data_workflow",
    f"{catalog}.{schema}.students_data_clean_workflow",
]

print("📘 Target tables:")
for t in target_tables:
    print(f" - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Register Custom Rule: `paid_status_consistency`

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

print("✅ Custom rule registered successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Manual Rules (Shared Across Tables)

# COMMAND ----------

manual_rules = yaml.safe_load("""
- check:
    function: is_not_null_and_not_empty
    for_each_column:
      - student_id
      - name
      - email
      - student_status
      - load_timestamp
      - month_end_date
      - paid
      - stays_on_campus
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
      regex: '^[A-Za-z]{3}[0-9]{8}$'
      negate: false
  criticality: warn
  name: valid_student_id_pattern

- check:
    function: regex_match
    for_each_column: [email]
    arguments:
      regex: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      negate: false
  criticality: warn
  name: valid_email_format

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Function to Apply DQX Checks to Each Table

# COMMAND ----------

def apply_dqx_to_table(table_name: str):
    print(f"\n{'='*80}")
    print(f"🚀 Starting DQX validation for {table_name}")

    # 1. Load data
    df = spark.table(table_name)
    if df.count() == 0:
        print(f"⚠️ Table {table_name} is empty. Skipping.")
        return

    # 2. Basic timestamp sanity
    df = df.filter(
        (year(col("load_timestamp")).between(1900, 9999)) &
        (year(col("month_end_date")).between(1900, 9999))
    )

    # 3. Optional: profile & auto-rule gen (for completeness)
    profiler = DQProfiler(ws)
    summary_stats, profiles = profiler.profile(df, options={"sample_fraction": 0.7, "limit": 5000})
    auto_rules = DQGenerator(ws).generate_dq_rules(profiles)

    # 4. Combine rules
    all_rules = manual_rules + auto_rules

    # 5. Validate rules
    status = dq_engine.validate_checks(all_rules, custom_check_functions=globals())
    if status.has_errors:
        print(f"❌ Validation issues for {table_name}:")
        print(status.errors)
        return
    print("✅ Rules validated successfully.")

    # 6. Apply rules
    valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
        df,
        all_rules,
        custom_check_functions=globals()
    )

    valid_df = valid_df.withColumn("dq_run_timestamp", current_timestamp())
    invalid_df = invalid_df.withColumn("dq_run_timestamp", current_timestamp())

    # 7. Write results
    base_name = table_name.split(".")[-1]
    valid_target = f"{catalog}.{'dqx_output'}.{'students_valid'}_multitable"
    invalid_target = f"{catalog}.{'dqx_output'}.{'students_quarantine'}_multitable"

    valid_df.write.format("delta").mode("overwrite").saveAsTable(valid_target)
    invalid_df.write.format("delta").mode("overwrite").saveAsTable(invalid_target)

    # 8. Audit summary
    total_rows = df.count()
    total_warnings = invalid_df.filter(size(col("_warnings")) > 0).count()
    total_errors = invalid_df.filter(size(col("_errors")) > 0).count()
    clean_df = valid_df.filter(
        ((col("_warnings").isNull()) | (size(col("_warnings")) == 0)) &
        ((col("_errors").isNull()) | (size(col("_errors")) == 0))
    )
    clean_count = clean_df.count()

    print(f"📊 SUMMARY for {base_name}")
    print(f" - Total Rows: {total_rows}")
    print(f" - Errors: {total_errors}")
    print(f" - Warnings: {total_warnings}")
    print(f" - Clean Records: {clean_count}")
    print(f"💾 Valid Output: {valid_target}")
    print(f"💾 Quarantine Output: {invalid_target}")
    print(f"{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Run Across All Tables

# COMMAND ----------

for tbl in target_tables:
    apply_dqx_to_table(tbl)

print("🏁 Multi-table DQX run complete.")
