# Databricks notebook source
# MAGIC %md
# MAGIC # Student Location Generator + Enrichment + Gold View (Workflow-Optimized, Cleaned & Normalized)
# MAGIC
# MAGIC **Pipeline Flow**
# MAGIC
# MAGIC 1️⃣ Bronze → Raw student data (`students_data_workflow`)  
# MAGIC 2️⃣ Bronze → Generate & append location data (`students_location_workflow`)  
# MAGIC 3️⃣ Silver → Enrich valid DQX results + clean & normalize names (`student_data_enriched_workflow`)  
# MAGIC 4️⃣ Gold → Business filter for current students (`student_data_current_workflow`)
# MAGIC
# MAGIC - Incremental updates for locations  
# MAGIC - Cleans numeric characters from names  
# MAGIC - Normalizes capitalization (Firstname Lastname / Firstname Lastname Jr)  
# MAGIC - Idempotent enrichment for Silver layer  
# MAGIC - Clean Gold subset for “current” students only

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, udf
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from faker import Faker
import pandas as pd
import random
from datetime import datetime

# Initialize Spark + Faker
spark = SparkSession.builder.getOrCreate()
fake = Faker("en_US")
random.seed(datetime.now().timestamp())

# ============================================
# CONFIGURATION
# ============================================
SOURCE_TABLE = "wgu_poc.wgu_bronze.students_data_workflow"
TARGET_TABLE = "wgu_poc.wgu_bronze.students_location_workflow"
VALID_STUDENTS_TABLE = "wgu_poc.wgu_silver.students_valid_workflow"
ENRICHED_TABLE = "wgu_poc.wgu_silver.student_data_enriched_workflow"
GOLD_TABLE = "wgu_poc.wgu_gold.student_data_current_workflow"

print(f"Source Table: {SOURCE_TABLE}")
print(f"Location Table: {TARGET_TABLE}")
print(f"Valid Students Table: {VALID_STUDENTS_TABLE}")
print(f"Silver Target Table: {ENRICHED_TABLE}")
print(f"Gold Target Table: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1️⃣ Load Source + Identify New Student IDs

# COMMAND ----------

src_students_df = spark.table(SOURCE_TABLE).select("student_id").distinct()
src_count = src_students_df.count()

if spark.catalog.tableExists(TARGET_TABLE):
    tgt_students_df = spark.table(TARGET_TABLE).select("student_id").distinct()
    new_students_df = src_students_df.join(
        tgt_students_df, on="student_id", how="left_anti"
    )
    new_count = new_students_df.count()
    print(f"Found {new_count} new students not yet in location table.")
else:
    new_students_df = src_students_df
    new_count = new_students_df.count()
    print(f"No target table yet — processing all {src_count} students.")

if new_count == 0:
    print("No new students to process. Exiting gracefully.")
    dbutils.notebook.exit("No new rows detected.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2️⃣ Generate Location Data (City, State, Zip)

# COMMAND ----------

new_student_ids = [row["student_id"] for row in new_students_df.collect()]

location_data = []
for sid in new_student_ids:
    city = fake.city()
    state = fake.state_abbr()
    zipcode = fake.zipcode()[:5]
    location_data.append({
        "student_id": sid,
        "city": city,
        "state": state,
        "zipcode": zipcode
    })

df_location = pd.DataFrame(location_data)
spark_df_location = spark.createDataFrame(df_location)

print(f"Generated {spark_df_location.count()} new location records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3️⃣ Append to Location Table (Delta Append)

# COMMAND ----------

if spark.catalog.tableExists(TARGET_TABLE):
    spark_df_location.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
    print(f"Appended {spark_df_location.count()} records into {TARGET_TABLE}")
else:
    spark_df_location.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"Created new Delta table and inserted {spark_df_location.count()} records into {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4️⃣ Enrich Valid DQX Students + Clean and Normalize Names (Silver Layer)

# COMMAND ----------

valid_students_df = spark.table(VALID_STUDENTS_TABLE)
location_df = spark.table(TARGET_TABLE)

# Safe join — prevent duplicate column conflicts
enriched_df = (
    valid_students_df.alias("v")
    .join(location_df.alias("l"), on="student_id", how="left")
    .select(
        "v.student_id",
        "v.name",
        "v.email",
        "v.student_status",
        "v.month_end_date",
        "v.paid",
        "v.stays_on_campus",
        "v.load_timestamp",
        "v.dq_run_timestamp",
        "l.city",
        "l.state",
        "l.zipcode"
    )
)

# --- Step 1: Remove digits & extra whitespace ---
enriched_df_cleaned = (
    enriched_df
    .withColumn("name", regexp_replace(col("name"), "[0-9]", ""))
    .withColumn("name", trim(col("name")))
)

# --- Step 2: Normalize capitalization and suffixes ---
def normalize_name(name: str) -> str:
    if not name or not isinstance(name, str):
        return name
    name = " ".join(name.split())
    parts = name.split(" ")
    normalized = [p.capitalize() for p in parts]
    suffixes = {"Jr", "Sr", "Ii", "Iii", "Iv"}
    if normalized[-1] in suffixes:
        normalized[-1] = normalized[-1].upper()
    return " ".join(normalized)

normalize_name_udf = udf(normalize_name, StringType())
enriched_df_normalized = enriched_df_cleaned.withColumn("name", normalize_name_udf(col("name")))

# Write to Silver table (overwrite for freshness)
silver_before = spark.table(ENRICHED_TABLE).count() if spark.catalog.tableExists(ENRICHED_TABLE) else 0
enriched_df_normalized.write.format("delta").mode("overwrite").saveAsTable(ENRICHED_TABLE)
silver_after = enriched_df_normalized.count()
silver_added = silver_after - silver_before

print(f"Enriched & cleaned dataset written to {ENRICHED_TABLE} ({silver_added} new or updated records).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5️⃣ Create Gold Table (Current Students Only)

# COMMAND ----------

gold_df = enriched_df_normalized.filter(col("student_status") == "current")

gold_before = spark.table(GOLD_TABLE).count() if spark.catalog.tableExists(GOLD_TABLE) else 0
gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
gold_after = gold_df.count()
gold_added = gold_after - gold_before

print(f"Gold dataset created: {GOLD_TABLE} ({gold_added} new or updated current students).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6️⃣ Final Summary

# COMMAND ----------

print(f"\n{'='*70}")
print(f"📊 STUDENT LOCATION + ENRICHMENT + GOLD SUMMARY")
print(f"🏁 Run Completed: {datetime.now()}")
print(f"💎 Records Added to Silver: {silver_added}")
print(f"🏆 Records Added to Gold: {gold_added}")
print(f"{'='*70}")

display(spark.table(GOLD_TABLE).limit(10))
