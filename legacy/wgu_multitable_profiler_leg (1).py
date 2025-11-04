# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 DQX Multi-Table Profiling (Aligned with Databricks Labs Pattern)
# MAGIC
# MAGIC **Goal:** Profile, generate, and store DQX rules for multiple tables.
# MAGIC
# MAGIC ---
# MAGIC ### DQX Steps
# MAGIC 1️⃣ Define table patterns  
# MAGIC 2️⃣ Profile with `DQProfiler`  
# MAGIC 3️⃣ Generate rules with `DQGenerator`  
# MAGIC 4️⃣ Store inferred YAMLs for downstream validation
# MAGIC ---

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install --upgrade databricks-labs-dqx pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Imports (only DQX + SDK, no low-level PySpark logic)
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import TableChecksStorageConfig
import yaml

# Initialize DQX environment
ws = WorkspaceClient()
profiler = DQProfiler(ws)
generator = DQGenerator(ws)

catalog = "wgu_poc"
schema = "wgu_bronze"
checks_table = f"{catalog}.dqx_output.checks_profiles_multitable"

# Define tables/patterns (can be wildcard or explicit)
patterns = [
    f"{catalog}.{schema}.students_data_*",
    # f"{catalog}.{schema}.students_data_clean_workflow"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Profile Tables (DQX-native)
# MAGIC
# MAGIC Uses DQX’s built-in profiler to analyze structure and value distributions.

# COMMAND ----------

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1️⃣ Profile Tables (DQX-native)
# MAGIC
# MAGIC Uses DQX’s built-in profiler to analyze structure and value distributions.

# COMMAND ----------

results = profiler.profile_tables_for_patterns(patterns=patterns)

for table, (summary, profiles) in results.items():
    print(f"\n{'='*80}")
    print(f"📘 Table: {table}")
    display(summary)

    print("\n🧩 Field-Level Profiles:")
    # Version-safe visualization (works for all DQX releases)
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
# MAGIC ## 2️⃣ Generate & Store Inferred DQ Rules (Aligned with TableChecksStorageConfig)

# COMMAND ----------

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2️⃣ Generate & Store Inferred DQ Rules (Official DQX Method)
# MAGIC
# MAGIC Uses DQEngine.save_checks() with TableChecksStorageConfig
# MAGIC to persist inferred rules for each profiled table.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import TableChecksStorageConfig

dq_engine = DQEngine(ws, spark)
checks_table = f"{catalog}.{'dqx_output'}.checks_profiles_multitable"

for table, (summary, profiles) in results.items():
    dq_rules = generator.generate_dq_rules(profiles)
    print(f"✅ Generated {len(dq_rules)} inferred rules for {table}")

    config = TableChecksStorageConfig(
        location=checks_table,
        run_config_name=table,
        mode="append"
    )

    dq_engine.save_checks(dq_rules, config=config)
    print(f"💾 Saved {len(dq_rules)} rules to {checks_table}")
