# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Student Data Generator (Workflow Version - Balanced & DQX-Ready)
# MAGIC
# MAGIC Generates **100 synthetic student records per run** and upserts them into:
# MAGIC **wgu_poc.wgu_bronze.students_data_workflow**
# MAGIC
# MAGIC ✅ Ensures ~10–15% of records are fully clean (pass all DQX rules)  
# MAGIC ✅ Balanced mix of valid, null, and malformed values  
# MAGIC ✅ Upsert replaces duplicates and inserts new rows  
# MAGIC ✅ Summary only shows rows added this run

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import string
import pandas as pd
import random
from datetime import datetime, timedelta
import pytz
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
from delta.tables import DeltaTable

# ============================================
# CONFIGURATION
# ============================================
NUM_RECORDS = 100
TARGET_TABLE = "wgu_poc.wgu_bronze.student_data_clean_workflow"

# Ensure controlled randomness
random.seed(datetime.now().timestamp())
Faker.seed(datetime.now().timestamp())
fake = Faker("en_US")

# ============================================
# CONSTANTS
# ============================================
student_status_options = ['graduated', 'current', 'break', 'dropped']
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 12, 31)

# ============================================
# HELPER FUNCTIONS
# ============================================

def random_timestamp(start, end):
    """Generate random timestamp between start and end."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

def generate_student_id():
    """Generate a unique student ID pattern (e.g., abc12345678)."""
    letters = ''.join(random.choices(string.ascii_lowercase, k=3))
    numbers = f"{random.randint(0, 99999999):08d}"
    return f"{letters}{numbers}"

def generate_email(name):
    """Create a clean, valid email address."""
    base = name.lower().replace(" ", ".").replace("'", "")
    domain = random.choice(["gmail.com", "yahoo.com", "wgu.edu", "outlook.com"])
    return f"{base}@{domain}"

# ============================================
# GENERATE 100% CLEAN DATA
# ============================================
data = []
generated_ids = set()

for i in range(NUM_RECORDS):
    student_id = generate_student_id()
    while student_id in generated_ids:
        student_id = generate_student_id()
    generated_ids.add(student_id)

    name = fake.name()
    email = generate_email(name)
    student_status = random.choice(student_status_options)
    month_end_date = random_timestamp(start_date, end_date)
    paid = random.choice([True, False])
    stays_on_campus = random.choice([True, False])
    load_timestamp = datetime.now(pytz.utc)

    record = {
        'student_id': student_id,
        'name': name,
        'email': email,
        'student_status': student_status,
        'month_end_date': month_end_date,
        'paid': paid,
        'stays_on_campus': stays_on_campus,
        'load_timestamp': load_timestamp
    }

    data.append(record)

# ============================================
# CONVERT TO SPARK DATAFRAME
# ============================================
schema = StructType([
    StructField("student_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("student_status", StringType(), True),
    StructField("month_end_date", TimestampType(), True),
    StructField("paid", BooleanType(), True),
    StructField("stays_on_campus", BooleanType(), True),
    StructField("load_timestamp", TimestampType(), True)
])

spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(pd.DataFrame(data), schema=schema)

# ============================================
# UPSERT INTO DELTA TABLE
# ============================================
before_count = spark.table(TARGET_TABLE).count() if spark.catalog.tableExists(TARGET_TABLE) else 0

if spark.catalog.tableExists(TARGET_TABLE):
    delta_table = DeltaTable.forName(spark, TARGET_TABLE)
    (
        delta_table.alias("tgt")
        .merge(spark_df.alias("src"), "tgt.student_id = src.student_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    spark_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

after_count = spark.table(TARGET_TABLE).count()
added_count = after_count - before_count

# ============================================
# SIMPLIFIED RUN SUMMARY
# ============================================
print(f"\n{'='*60}")
print("📊 SYNTHETIC STUDENT DATA GENERATION SUMMARY")
print(f"🏁 Run Completed: {datetime.now()}")
print(f"💾 Records Added to Target Table: {added_count}")
print(f"💎 Clean Records (100%): {NUM_RECORDS}")
print(f"📦 Target Table: {TARGET_TABLE}")
print(f"{'='*60}")

display(spark.table(TARGET_TABLE).orderBy("load_timestamp", ascending=False).limit(10))
