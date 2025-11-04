# Databricks notebook source
# MAGIC %md
# MAGIC # E2E DQX Workflow (Programmatic)
# MAGIC
# MAGIC - Input: `wgu_poc.wgu_bronze.students_data_workflow`
# MAGIC - Outputs:
# MAGIC   - Valid records + warn ONLY records → `wgu_poc.dqx_output.students_valid_workflow`
# MAGIC   - Quarantined
# MAGIC       - error level records are failed and will not show in dqx valid table → `wgu_poc.dqx_output.students_quarantined_workflow`
# MAGIC       - warn only records are captured but will still be passed to dqx valid table, warns recorded for auditing/monitoring purposes → `wgu_poc.dqx_output.students_quarantined_workflow`
# MAGIC - Clean audit summary with only key metrics

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

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# ============================================
# CONFIGURATION
# ============================================
source_table  = "wgu_poc.wgu_bronze.students_data_workflow"
valid_table   = "wgu_poc.dqx_output.students_valid_workflow"
invalid_table = "wgu_poc.dqx_output.students_quarantined_workflow"

print(f"Source: {source_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1️⃣ Load Data (Incremental Filter)

# COMMAND ----------

students_df = spark.table(source_table)

if spark.catalog.tableExists(valid_table):
    last_ts = spark.table(valid_table).agg(spark_max("load_timestamp")).collect()[0][0]
    if last_ts:
        students_df = students_df.filter(col("load_timestamp") > last_ts)
        print(f"Incremental mode — filtering records newer than {last_ts}")
else:
    print("Full run — no prior valid data detected.")

if students_df.count() == 0:
    print("No new rows to process.")
    dbutils.notebook.exit("No new rows detected.")

# Filter timestamp sanity
students_df_filtered = students_df.filter(
    (year(col("load_timestamp")).between(1900, 9999))
    & (year(col("month_end_date")).between(1900, 9999))
)
print(f"Rows profiled: {students_df_filtered.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2️⃣ DQX Profiling + Auto Rules

# COMMAND ----------

profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(students_df_filtered, options={"sample_fraction": 0.7})
auto_rules = DQGenerator(ws).generate_dq_rules(profiles)
print("Auto rules generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3️⃣ Custom & Manual Rules

# COMMAND ----------

@register_rule("row")
def paid_status_consistency(paid_col: str, status_col: str):
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

print("Manual rules loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4️⃣ Apply DQX Rules + Split

# COMMAND ----------

dq_engine = DQEngine(ws)
all_rules = manual_rules + auto_rules

status = dq_engine.validate_checks(all_rules, custom_check_functions=globals())
if status.has_errors:
    print("Rule validation issues:")
    print(status.errors)
else:
    print("Rules validated successfully")

valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
    students_df_filtered,
    all_rules,
    custom_check_functions=globals()
)

invalid_df_unique = invalid_df.dropDuplicates(["student_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5️⃣ Upsert Valid + Quarantined Data

# COMMAND ----------

def upsert_to_table(df, table_name, key="student_id"):
    if spark.catalog.tableExists(table_name):
        delta = DeltaTable.forName(spark, table_name)
        delta.alias("tgt").merge(df.alias("src"), f"tgt.{key}=src.{key}") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Upserted {df.count()} → {table_name}")

valid_df = valid_df.withColumn("dq_run_timestamp", current_timestamp())
invalid_df_unique = invalid_df_unique.withColumn("dq_run_timestamp", current_timestamp())

upsert_to_table(valid_df, valid_table)
upsert_to_table(invalid_df_unique, invalid_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6️⃣ Audit Summary (Warnings / Errors / Fully Clean)

# COMMAND ----------

total_rows = students_df_filtered.count()
total_warnings = invalid_df.filter(size(col("_warnings")) > 0).count()
total_errors = invalid_df.filter(size(col("_errors")) > 0).count()

# Fully clean = records with no warns or errors in valid table
clean_df = valid_df.filter(
    (size(col("_warnings")) == 0) & (size(col("_errors")) == 0)
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
print("📊 FINAL DQX SUMMARY")
print(f"🏁 Run Completed: {datetime.now()}")
print(f"📦 Total Rows Profiled: {total_rows}")
print(f"⚠️ Total Warnings Found: {total_warnings}")
print(f"❌ Total Errors Found: {total_errors}")
print(f"💎 Fully Clean Records (no warns/errors): {clean_count}")
print(f"{'='*80}")

display(summary_df)
