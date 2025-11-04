# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 DQX Multi-Table Profiling – WGU POC
# MAGIC
# MAGIC This notebook:
# MAGIC - Profiles multiple student tables using **Databricks Labs DQX**
# MAGIC - Generates inferred DQ rules for each
# MAGIC - Stores rules in Delta (schema merge–safe for Unity Catalog)
# MAGIC - Exports readable YAML copies to the current directory
# MAGIC
# MAGIC **Workflow Steps**
# MAGIC 1️⃣ Define target table patterns  
# MAGIC 2️⃣ Profile tables using `DQProfiler`  
# MAGIC 3️⃣ Generate and store DQ rules using `DQGenerator` + `DQEngine`  
# MAGIC 4️⃣ Export YAML copies to the working directory for review/version control

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩 0️⃣ Install Dependencies (Run Once)
# MAGIC Installs DQX and YAML library, then restarts Python environment.

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install --upgrade databricks-labs-dqx pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 1️⃣ Environment Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import TableChecksStorageConfig
import yaml, os

# Initialize DQX components
ws = WorkspaceClient()
profiler = DQProfiler(ws)
generator = DQGenerator(ws)
dq_engine = DQEngine(ws, spark)

# For Unity Catalog: handle schema evolution per write
merge_opts = {"mergeSchema": "true"}

# Target locations
catalog = "wgu_poc"
schema = "wgu_bronze"
output_schema = "dqx_output"
checks_table = f"{catalog}.{output_schema}.checks_profiles_multitable"

# Table selection pattern (supports wildcards)
patterns = [f"{catalog}.{schema}.students_*"]

print(f"📘 Profiling pattern(s): {patterns}")
print(f"🗂️ Output rules table: {checks_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 2️⃣ Profile Multiple Tables
# MAGIC
# MAGIC Profiles each table that matches the pattern using `DQProfiler.profile_tables_for_patterns`.
# MAGIC Returns both table-level summary stats and column-level profiles.

# COMMAND ----------

results = profiler.profile_tables_for_patterns(patterns=patterns)

for table, (summary, profiles) in results.items():
    print(f"\n{'='*80}")
    print(f"📘 Table: {table}")
    display(summary)
    print("🧩 Field-Level Profiles:")
    if isinstance(profiles, list):
        for idx, p in enumerate(profiles):
            print(f"  → Column profile {idx+1}/{len(profiles)}")
            if hasattr(p, "to_spark_df"):
                display(p.to_spark_df())
            else:
                display(p)
    else:
        display(profiles)
    print(f"{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 3️⃣ Generate and Store Inferred Rules
# MAGIC
# MAGIC Uses `DQGenerator.generate_dq_rules()` to infer rules from profiles.  
# MAGIC Each rule set is stored as YAML in Delta (schema merge–safe).  
# MAGIC YAML copies are also written locally for inspection/version control.

# COMMAND ----------

for table, (summary, profiles) in results.items():
    dq_rules = generator.generate_dq_rules(profiles)
    print(f"✅ Generated {len(dq_rules)} inferred rules for {table}")

    # Save to DQX checks table (schema merge enabled)
    try:
        config = TableChecksStorageConfig(
            location=checks_table,
            run_config_name=table,
            mode="overwrite"
        )
        dq_engine.save_checks(dq_rules, config=config)
        print(f"💾 Stored {len(dq_rules)} rules in {checks_table}")
    except Exception as e:
        print(f"⚠️ DQEngine write failed ({e}); using direct writer fallback.")
        df = spark.createDataFrame(dq_rules)
        (
            df.write
              .format("delta")
              .mode("append")
              .options(**merge_opts)
              .saveAsTable(checks_table)
        )
        print(f"💾 Fallback write successful → {checks_table}")

    # Export YAML copy
    yaml_str = yaml.dump(dq_rules, sort_keys=False)
    filename = f"{table.replace('.', '_')}_rules.yaml"
    local_path = f"./profiler_checks/{filename}"

    # Save locally
    with open(local_path, "w", encoding="utf-8") as f:
        f.write(yaml_str)
    print(f"🧾 Exported YAML → {local_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 4️⃣ Verify Stored Rules
# MAGIC
# MAGIC Displays the Delta table containing all generated rule definitions.

# COMMAND ----------

if spark.catalog.tableExists(checks_table):
    print(f"\n✅ Table created: {checks_table}")
    display(spark.table(checks_table))
else:
    print(f"⚠️ Table {checks_table} not found. Verify permissions or location.")
