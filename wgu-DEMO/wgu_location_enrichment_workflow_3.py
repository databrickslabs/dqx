# Databricks notebook source
# MAGIC %md
# MAGIC # 🗺️ Student Location Generator + Enrichment + Gold View (Workflow-Optimized, with Name Cleanup)
# MAGIC
# MAGIC **Pipeline Flow**
# MAGIC 1️⃣ Bronze → Raw student data (`students_data_workflow`)  
# MAGIC 2️⃣ Bronze → Generate & append location data (`students_location_workflow`)  
# MAGIC 3️⃣ Silver → Enrich valid DQX results + clean student names (`student_data_enriched_workflow`)  
# MAGIC 4️⃣ Gold → Business filter for current students (`student_data_current_workflow`)
# MAGIC
# MAGIC ✅ Incremental updates for locations  
# MAGIC ✅ Name cleanup (removes digits and trims whitespace)  
# MAGIC ✅ Idempotent Silver enrichment  
# MAGIC ✅ Clean Gold subset (current students only)

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
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
VALID_STUDENTS_TABLE = "wgu_poc.dqx_output.students_valid_workflow"
ENRICHED_TABLE = "wgu_poc.wgu_silver.student_data_enriched_workflow"
GOLD_TABLE = "wgu_poc.wgu_gold.student_data_current_workflow"

print(f"📘 Source Table: {SOURCE_TABLE}")
print(f"📦 Location Table: {TARGET_TABLE}")
print(f"✅ Valid Students Table: {VALID_STUDENTS_TABLE}")
print(f"💎 Silver Target Table: {ENRICHED_TABLE}")
print(f"🏆 Gold Target Table: {GOLD_TABLE}")

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
    print(f"🧮 Found {new_count} new students not yet in location table.")
else:
    new_students_df = src_students_df
    new_count = new_students_df.count()
    print(f"🆕 No target table yet — processing all {src_count} students.")

if new_count == 0:
    print("✅ No new students to process. Exiting gracefully.")
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

# Convert to Spark DataFrame
df_location = pd.DataFrame(location_data)
spark_df_location = spark.createDataFrame(df_location)

print(f"🏙️ Generated {spark_df_location.count()} new location records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3️⃣ Append to Location Table (Delta Append)

# COMMAND ----------

if spark.catalog.tableExists(TARGET_TABLE):
    spark_df_location.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
    print(f"✅ Appended {spark_df_location.count()} records into {TARGET_TABLE}")
else:
    spark_df_location.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"✅ Created new Delta table and inserted {spark_df_location.count()} records into {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4️⃣ Enrich Valid DQX Students with Location Data + Clean Names (Silver Layer)

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

# ✅ Name cleanup transformation — remove digits and extra spaces
enriched_df_cleaned = (
    enriched_df
    .withColumn("name", regexp_replace(col("name"), "[0-9]", ""))  # remove numbers
    .withColumn("name", trim(col("name")))                         # remove extra spaces
)

# Write to Silver table (overwrite for freshness)
enriched_df_cleaned.write.format("delta").mode("overwrite").saveAsTable(ENRICHED_TABLE)

print(f"💎 Enriched & cleaned dataset written to {ENRICHED_TABLE} ({enriched_df_cleaned.count()} records).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5️⃣ Create Gold Table (Current Students Only)

# COMMAND ----------

gold_df = enriched_df_cleaned.filter(col("student_status") == "current")

gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)

print(f"🏆 Gold dataset created: {GOLD_TABLE} ({gold_df.count()} current students).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6️⃣ Final Summary

# COMMAND ----------

final_count = spark.table(TARGET_TABLE).count()
enriched_count = spark.table(ENRICHED_TABLE).count()
gold_count = spark.table(GOLD_TABLE).count()

print(f"\n{'='*70}")
print(f"📊 STUDENT LOCATION + ENRICHMENT + GOLD SUMMARY")
print(f"🏁 Run Completed: {datetime.now()}")
print(f"📦 Total in Source: {src_count}")
print(f"🆕 Location Records Added: {spark_df_location.count()}")
print(f"📘 Total in Location Table: {final_count}")
print(f"💎 Total in Silver Table: {enriched_count}")
print(f"🏆 Total in Gold Table: {gold_count}")
print(f"{'='*70}")

display(spark.table(GOLD_TABLE).limit(10))
