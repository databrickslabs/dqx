# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 DQX Multi-Table Profiling – WGU POC (All Versions Compatible)
# MAGIC
# MAGIC Profiles multiple WGU tables with Databricks Labs DQX.
# MAGIC Handles schema normalization, Boolean/Timestamp merge issues,
# MAGIC and compatibility between older and new DQX profiling object models.

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install --upgrade databricks-labs-dqx pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, TimestampType
import yaml, inspect

# Initialize workspace + DQX
ws = WorkspaceClient()
profiler = DQProfiler(ws)
generator = DQGenerator(ws)
print("✅ DQX initialized successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Normalize Table Schemas

# COMMAND ----------

tables_to_profile = [
    "wgu_poc.wgu_bronze.students_data_workflow",
    "wgu_poc.wgu_bronze.students_data_clean_workflow"
]

expected_schema = {
    "student_id": StringType(),
    "student_status": StringType(),
    "month_end_date": TimestampType(),
    "paid": BooleanType(),
    "stays_on_campus": BooleanType(),
    "load_timestamp": TimestampType(),
}

def safe_cast(df, column, target_type):
    try:
        return df.withColumn(column, F.col(column).cast(target_type))
    except Exception:
        return df.withColumn(column, F.lit(None).cast(target_type))

def normalize_table_strict(table_name: str):
    df = spark.table(table_name)
    for c, t in expected_schema.items():
        if c in df.columns:
            df = safe_cast(df, c, t)
    return df

normalized_tables = []
for table in tables_to_profile:
    df_norm = normalize_table_strict(table)
    norm_table = f"{table}_normalized"
    df_norm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(norm_table)
    normalized_tables.append(norm_table)
    print(f"💾 Created normalized table: {norm_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Profile Tables Safely (All DQX Versions)

# COMMAND ----------

results = profiler.profile_tables_for_patterns(patterns=normalized_tables)

for table, (summary_stats, profiles) in results.items():
    print(f"\n{'='*100}")
    print(f"📘 Table: {table}")
    display(summary_stats)
    print("\n🧩 Field-Level Profiles:")

    # New DQX: profiles may be a list of DQProfile objects
    if isinstance(profiles, list):
        for idx, p in enumerate(profiles):
            print(f"  → Column profile {idx+1}/{len(profiles)}")
            # detect if p is a DQProfile object
            if hasattr(p, "to_spark_df") and callable(getattr(p, "to_spark_df")):
                pdf = p.to_spark_df()
            elif hasattr(p, "as_spark_df") and callable(getattr(p, "as_spark_df")):
                pdf = p.as_spark_df()
            else:
                pdf = p  # fallback if it's already a DataFrame/dict
            if hasattr(pdf, "columns"):
                display(pdf.select([F.col(c).cast("string").alias(c) for c in pdf.columns]).limit(50))
            else:
                print(pdf)
    else:
        # Older DQX: single Spark DF
        if hasattr(profiles, "columns"):
            display(profiles.select([F.col(c).cast("string").alias(c) for c in profiles.columns]).limit(50))
        else:
            print(profiles)

    print(f"{'='*100}\n")

print("✅ Profiling completed successfully for all tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Generate + Store Inferred Rules

# COMMAND ----------

checks_table = "wgu_poc.wgu_bronze.checks_profiles_multitable"

for table, (summary_stats, profiles) in results.items():
    # Support list of DQProfiles or single DataFrame
    if isinstance(profiles, list):
        dq_rules = generator.generate_dq_rules(
            [p.to_spark_df() if hasattr(p, "to_spark_df") else p for p in profiles]
        )
    else:
        dq_rules = generator.generate_dq_rules(profiles)
    yaml_rules = yaml.dump(dq_rules, sort_keys=False)

    print(f"✅ Generated {len(dq_rules)} inferred rules for {table}")
    print(yaml_rules[:400])

    spark.createDataFrame(
        [(table, yaml_rules)],
        ["table_name", "generated_rules_yaml"]
    ).write.mode("append").saveAsTable(checks_table)

print(f"💾 Stored all inferred DQ rules in: {checks_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Verify Stored Rules

# COMMAND ----------

display(spark.table("wgu_poc.wgu_bronze.checks_profiles_multitable"))
