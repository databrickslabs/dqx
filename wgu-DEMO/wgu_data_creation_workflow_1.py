# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Student Data Generator (Workflow Version - Email, Balanced)
# MAGIC
# MAGIC Generates 100 records per run and **upserts** them into:
# MAGIC **wgu_poc.wgu_bronze.students_data_workflow**
# MAGIC
# MAGIC ✅ About 90% valid rows in "super-dirty" mode  
# MAGIC ✅ Replaces existing records with same student_id  
# MAGIC ✅ Appends new ones (lightweight for workflows)

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
DATA_QUALITY_SCENARIO = "super-dirty"  # Options: "clean", "nulls", "malformed", "super-dirty"
NUM_RECORDS = 100
TARGET_TABLE = "wgu_poc.wgu_bronze.students_data_workflow"

# ============================================
# RANDOMNESS CONFIG
# ============================================
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
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

def generate_student_id():
    letters = ''.join(random.choices(string.ascii_lowercase, k=3))
    numbers = f"{random.randint(0, 99999999):08d}"
    return f"{letters}{numbers}"

def maybe_null(value, probability=0.05):
    """Introduce nulls at low probability (5%) for super-dirty balance."""
    if random.random() < probability:
        return None
    return value

def malform_student_id(value):
    """Introduce malformed IDs in ~8–10% of records."""
    if random.random() < 0.10:
        malformations = [
            value + "###",
            value.replace('a', '@').replace('e', '3'),
            value[:5],
            value.upper() + "!",
            "INVALID_" + value,
        ]
        return random.choice(malformations)
    return value

def malform_status(value):
    """Introduce malformed statuses in ~8–10% of cases."""
    if random.random() < 0.10:
        malformations = ["UNKNOWN", "grad@uated", "current123", "N/A", "---", "pending??"]
        return random.choice(malformations)
    return value

def malform_name(value):
    """Introduce malformed names (e.g. weird casing or appended digits)."""
    if random.random() < 0.08:
        malformations = [
            value.lower(),
            value.upper(),
            value + "123",
            "???",
            value.replace(" ", "_"),
        ]
        return random.choice(malformations)
    return value

def generate_email(name):
    base = name.lower().replace(" ", ".").replace("'", "")
    domain = random.choice(["gmail.com", "yahoo.com", "wgu.edu", "outlook.com"])
    return f"{base}@{domain}"

def malform_email(value):
    """Introduce malformed email in ~8–10% of cases."""
    if random.random() < 0.08:
        malformations = [
            "notanemail",
            "missing_at_symbol.com",
            "@nouser.com",
            "test@fake",
            "user@@domain.com",
            "email@domain",
            None,
        ]
        return random.choice(malformations)
    return value

# ============================================
# DETERMINE SCENARIOS
# ============================================
apply_nulls = DATA_QUALITY_SCENARIO in ["nulls", "super-dirty"]
apply_malformed = DATA_QUALITY_SCENARIO in ["malformed", "super-dirty"]

# ============================================
# GENERATE DATA
# ============================================
data = []
generated_student_ids = set()

for _ in range(NUM_RECORDS):
    student_id = generate_student_id()
    while student_id in generated_student_ids:
        student_id = generate_student_id()
    generated_student_ids.add(student_id)
    
    name = fake.name()
    email = generate_email(name)
    student_status = random.choice(student_status_options)
    month_end_date = random_timestamp(start_date, end_date)
    paid = random.choice([True, False])
    stays_on_campus = random.choice([True, False])
    load_timestamp = datetime.now(pytz.utc)
    
    # Apply malformed transformations
    if apply_malformed:
        student_id = malform_student_id(student_id)
        student_status = malform_status(student_status)
        name = malform_name(name)
        email = malform_email(email)
    
    # Apply null transformations
    if apply_nulls:
        student_id = maybe_null(student_id)
        student_status = maybe_null(student_status)
        name = maybe_null(name)
        email = maybe_null(email)
        month_end_date = maybe_null(month_end_date)
        paid = maybe_null(paid)
        stays_on_campus = maybe_null(stays_on_campus)
        # load_timestamp always valid

    data.append({
        'student_id': student_id,
        'name': name,
        'email': email,
        'student_status': student_status,
        'month_end_date': month_end_date,
        'paid': paid,
        'stays_on_campus': stays_on_campus,
        'load_timestamp': load_timestamp
    })

# ============================================
# DEFINE SCHEMA & CONVERT
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
df = pd.DataFrame(data)
spark_df = spark.createDataFrame(df, schema=schema)

# ============================================
# UPSERT INTO DELTA TABLE
# ============================================
if spark.catalog.tableExists(TARGET_TABLE):
    delta_table = DeltaTable.forName(spark, TARGET_TABLE)
    (
        delta_table.alias("tgt")
        .merge(
            spark_df.alias("src"),
            "tgt.student_id = src.student_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✓ Upserted {NUM_RECORDS} records into {TARGET_TABLE}")
else:
    spark_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"✓ Created and loaded {NUM_RECORDS} records into new table {TARGET_TABLE}")

# ============================================
# VALIDATION SUMMARY
# ============================================
total_rows = spark.table(TARGET_TABLE).count()
print(f"\n{'='*50}")
print(f"✅ Data Quality Scenario: {DATA_QUALITY_SCENARIO.upper()}")
print(f"✅ Approx. 90% valid records generated")
print(f"✅ Total Rows Now in Table: {total_rows}")
print(f"✅ Target Table: {TARGET_TABLE}")
print(f"{'='*50}")

display(spark.table(TARGET_TABLE).orderBy("load_timestamp", ascending=False).limit(10))
