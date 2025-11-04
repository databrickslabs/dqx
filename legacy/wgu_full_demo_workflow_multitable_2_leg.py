# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 DQX Native Multi-Table Workflow (Per Docs)
# MAGIC
# MAGIC **Purpose:** Demonstrate a DQX-native implementation of profiling and quality checks  
# MAGIC across all tables matching the wildcard `wgu_poc.wgu_bronze.*`, following official docs:
# MAGIC
# MAGIC - [Data Profiling — multiple tables](https://databrickslabs.github.io/dqx/docs/guide/data_profiling/#profiling-multiple-tables)
# MAGIC - [Quality Checks — multiple tables](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply/#applying-checks-on-multiple-tables)
# MAGIC
# MAGIC **Outputs:**  
# MAGIC - Validated tables (auto-created by DQX)  
# MAGIC - Quarantined data (per-table)  
# MAGIC - Summary metrics (DQX engine metadata)

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # Install DQX + dependencies
# MAGIC %pip install --quiet databricks-labs-dqx pyyaml
# MAGIC
# MAGIC # Restart Python kernel (required after new package installs)
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from databricks.sdk import WorkspaceClient
import yaml

ws = WorkspaceClient()

# --------------------------------------------
# CONFIGURATION
# --------------------------------------------
input_pattern = "wgu_poc.wgu_bronze.*"
output_valid_prefix = "wgu_poc.dqx_output"
output_quarantine_prefix = "wgu_poc.dqx_output"

profiler = DQProfiler(ws)
generator = DQGenerator(ws)
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Profile All Tables Matching Pattern (DQX-native)
# MAGIC
# MAGIC Uses official `profile_tables_for_patterns()` API to collect stats for all bronze tables.

# COMMAND ----------

results = profiler.profile_tables_for_patterns(
    patterns=[input_pattern],
    options=[{"table": input_pattern, "sample_fraction": 0.7, "limit": 10000}]
)

if not results:
    raise ValueError(f"No tables found matching pattern: {input_pattern}")

print("✅ Profiling completed. Tables profiled:")
for table_name, profile_data in results:
    col_count = len(profile_data.get("summary_stats", []))
    print(f" - {table_name} ({col_count} columns profiled)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Generate Rules for Each Table (Auto + Manual)

# COMMAND ----------

# Auto-generate rules from profiling metadata
auto_rules_by_table = generator.generate_dq_rules_for_profiles(results)
print(f"Generated auto rules for {len(auto_rules_by_table)} tables")

# Example: define one global manual rule
@register_rule("row")
def paid_status_consistency(paid_col: str, status_col: str):
    from pyspark.sql import functions as F
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
    function: paid_status_consistency
    arguments:
      paid_col: paid
      status_col: student_status
  criticality: warn
  name: paid_status_inconsistent_with_status
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Apply Checks on All Tables (Official Multi-Table Method)
# MAGIC
# MAGIC Using DQX's documented `apply_checks_on_multiple_tables()` for bulk validation.

# COMMAND ----------

multi_table_results = dq_engine.apply_checks_on_multiple_tables(
    patterns=[input_pattern],
    dq_rules=manual_rules,     # shared manual rules
    auto_rules=auto_rules_by_table,  # generated per table
    output_config={
        "base_path": output_valid_prefix,
        "quarantine_path": output_quarantine_prefix,
        "mode": "append"
    },
    custom_check_functions=globals(),
)

print("✅ Multi-table checks applied successfully.")
for result in multi_table_results:
    print(f" - {result['table_name']} → {result['summary']['total_errors']} errors, {result['summary']['total_warnings']} warnings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Summarize Quality Results Across All Tables
# MAGIC
# MAGIC The DQEngine multi-table API already produces per-table summaries;  
# MAGIC this cell aggregates them for convenience.

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

summary_data = [
    (
        r["table_name"],
        r["summary"]["total_rows"],
        r["summary"]["total_warnings"],
        r["summary"]["total_errors"],
        r["summary"]["total_clean_records"]
    )
    for r in multi_table_results
]

summary_df = spark.createDataFrame(
    summary_data,
    ["table_name", "total_rows", "total_warnings", "total_errors", "clean_records"]
)

summary_table = f"{output_valid_prefix}.audit_summary_multitable"
summary_df.write.format("delta").mode("overwrite").saveAsTable(summary_table)

print(f"✅ DQX summary written to {summary_table}")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **References:**  
# MAGIC - [Profiling Multiple Tables](https://databrickslabs.github.io/dqx/docs/guide/data_profiling/#profiling-multiple-tables)  
# MAGIC - [Applying Checks on Multiple Tables](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply/#applying-checks-on-multiple-tables)
