# Databricks notebook source
# MAGIC %md
# MAGIC ## DAIS 2025 DQX Demo Session 
# MAGIC This notebook illustrates example usage of DQX for a fictional Manufacturing Company "Machina Metrics". <br>
# MAGIC Watch **DAIS Demo Session Recording** that showcases this demo: https://www.youtube.com/watch?v=e5Qvx_gnxTE
# MAGIC
# MAGIC
# MAGIC By default, the demo sample dataset gets persisted inside catalog=`main` and schema=`default`. <br> If you want to change the default catalog and schema, specify appropriate catalog and schema in notebook **widgets**.

# COMMAND ----------

# MAGIC %md
# MAGIC <h1>
# MAGIC MachinaMetrics - A Manufacturing Company
# MAGIC </h1>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aspirations - Proactive Machine Maintenance
# MAGIC
# MAGIC MachinaMetrics is a leading-edge manufacturing company whose CTO is spearheading the adoption of an AI-driven predictive maintenance solution. By harnessing real-time machine status data and historical maintenance schedules, this technology will proactively forecast service needs, minimize unplanned downtime, and optimize maintenance cycles. The result is a significant reduction in operational costs, extended equipment lifespan, and maximized production efficiency-positioning MachinaMetrics as an industry innovator in smart manufacturing.

# COMMAND ----------

# MAGIC %md
# MAGIC # DQX - The Data Quality Framework

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQX Deployment and Usage Options

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are different deployment and usage options for DQX:
# MAGIC
# MAGIC | Item | Option 1 | Option 2|
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | Installation| Deploy as a Library | Deploy as a workspace tool |
# MAGIC | Usage | Use with Spark Core or Spark Structure Streaming| Use with Lakeflow Pipelines (formerly DLT) |
# MAGIC | Quality Rules| Define as YAML/JSON | Define as Code |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX as Library <br>
# MAGIC For this demo, we will install DQX as library and define quality rules as YAML.

# COMMAND ----------

# DBTITLE 1,Install DQX Library

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

import os
workspace_root_path = os.getcwd()
quality_rules_path = f"{workspace_root_path}/quality_checks"

# Cleanup existing DQ Rules files, if already exists
if os.path.exists(quality_rules_path):
    for filename in os.listdir(quality_rules_path):
        file_path = os.path.join(quality_rules_path, filename)
        # Only delete files, not subdirectories
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Deleted: {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Generation
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Set Catalog and Schema for Demo Dataset
default_catalog_name = "main"
default_schema_name = "default"

dbutils.widgets.text("demo_catalog", default_catalog_name, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema_name, "Schema Name")

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")

print(f"Selected Catalog for Demo Dataset: {catalog}")
print(f"Selected Schema for Demo Dataset: {schema}")

sensor_table = f"{catalog}.{schema}.sensor_data"
maintenance_table = f"{catalog}.{schema}.maintenance_data"

# COMMAND ----------

# DBTITLE 1,Generate Demo Sensor Datasets
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime


if spark.catalog.tableExists(sensor_table) and spark.table(sensor_table).count() > 0:
    print(
        f"Table {sensor_table} already exists with demo data. Skipping data generation"
    )
else:
    # 1. Enhanced Sensor Data with ingest_date and multiple rows per ingest_date
    sensor_schema = StructType(
        [
            StructField("sensor_id", StringType(), False),
            StructField("machine_id", StringType(), True),  # Allow null values
            StructField("sensor_type", StringType(), False),
            StructField("reading_value", DoubleType(), True),
            StructField("reading_timestamp", TimestampType(), True),
            StructField("calibration_date", DateType(), True),
            StructField("battery_level", IntegerType(), True),
            StructField("facility_zone", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("firmware_version", StringType(), True),
            StructField("ingest_date", DateType(), True),
        ]
    )

    sensor_data = [
        # Ingest date 2025-04-28
        (
            "SEN-001",
            "MCH-001",
            "temperature",
            72.4,
            datetime.strptime("2025-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        (
            "SEN-002",
            "MCH-001",
            "pressure",
            2.1,
            datetime.strptime("2025-04-28 14:32:05", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-03-15", "%Y-%m-%d").date(),
            45,
            "Zone-A",
            True,
            "v1.9.4",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        (
            "SEN-003",
            "MCH-002",
            "vibration",
            0.02,
            datetime.strptime("2025-04-28 14:32:10", "%Y-%m-%d %H:%M:%S"),
            None,
            92,
            "Zone-B",
            False,
            "v3.0.0",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),  # Invalid calibration
        # Ingest date 2025-04-29
        (
            "SEN-001",
            "MCH-001",
            "temperature",
            73.5,
            datetime.strptime("2025-04-29 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            80,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
        ),
        (
            "SEN-002",
            "MCH-001",
            "pressure",
            2.3,
            datetime.strptime("2025-04-29 14:32:05", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-03-15", "%Y-%m-%d").date(),
            50,
            "Zone-A",
            True,
            "v1.9.4",
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
        ),
        (
            "SEN-004",
            None,
            "temperature",
            74.5,
            datetime.strptime("2025-04-29 14:32:15", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-15", "%Y-%m-%d").date(),
            10,
            "Zone-C",
            True,
            "invalid_ver",
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
        ),  # Multiple issues
        # Ingest date 2025-04-30
        (
            "SEN-001",
            "MCH-001",
            "temperature",
            74.0,
            datetime.strptime("2025-04-30 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            75,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
        ),
        (
            "SEN-003",
            "MCH-002",
            "vibration",
            0.03,
            datetime.strptime("2025-04-30 14:32:10", "%Y-%m-%d %H:%M:%S"),
            None,
            90,
            "Zone-B",
            False,
            "v3.0.0",
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
        ),
        # Bad Data Insertion
        # Sensor is empty
        (
            "",
            "MCH-002",
            "vibration",
            0.03,
            datetime.strptime("2025-04-30 14:32:10", "%Y-%m-%d %H:%M:%S"),
            None,
            90,
            "Zone-B",
            False,
            "v3.0.0",
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
        ),
        # Machine_id is empty
        (
            "SEN-001",
            "",
            "temperature",
            72.4,
            datetime.strptime("2025-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        # Invalid Temperature
        (
            "SEN-001",
            "MCH-001",
            "temperature",
            735,
            datetime.strptime("2025-04-29 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            80,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
        ),
        # Sensor regex pattern is wrong
        (
            "SEN002",
            "MCH-002",
            "vibration",
            0.03,
            datetime.strptime("2025-04-30 14:32:10", "%Y-%m-%d %H:%M:%S"),
            None,
            90,
            "Zone-B",
            False,
            "v3.0.0",
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
        ),
        (
            "SEN001",
            "",
            "temperature",
            72.4,
            datetime.strptime("2025-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        # Reading TS in future
        (
            "SEN-001",
            "",
            "temperature",
            72.4,
            datetime.strptime("2026-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        (
            "SEN-002",
            "MCH-001",
            "temperature",
            735,
            datetime.strptime("2026-04-29 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            80,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
        ),
        # Invalid Temperature
        (
            "SEN-003",
            "MCH-001",
            "vibration",
            0.03,
            datetime.strptime("2025-04-30 14:32:10", "%Y-%m-%d %H:%M:%S"),
            None,
            90,
            "Zone-B",
            False,
            "v3.0.0",
            datetime.strptime("2025-05-01", "%Y-%m-%d").date(),
        ),
        (
            "SEN-004",
            "MCH-001",
            "temperature",
            15000.0,
            datetime.strptime("2025-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        # Invalid wrong sensor pattern and ts in future
        (
            "SE003",
            "MCH-001",
            "vibration",
            0.03,
            datetime.strptime("2026-04-30 14:32:10", "%Y-%m-%d %H:%M:%S"),
            None,
            90,
            "Zone-B",
            False,
            "v3.0.0",
            datetime.strptime("2025-05-01", "%Y-%m-%d").date(),
        ),
        # Invalid Temperature and wrong sensor pattern
        (
            "SEN004",
            "MCH-001",
            "temperature",
            724,
            datetime.strptime("2025-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        # Invalid Firmware Version and wrong sensor pattern
        (
            "SEN004",
            "MCH-001",
            "temperature",
            724,
            datetime.strptime("2025-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "b2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        # Machine ID regex pattern is wrong
        (
            "SEN-002",
            "MCH2",
            "vibration",
            0.03,
            datetime.strptime("2025-04-30 14:32:10", "%Y-%m-%d %H:%M:%S"),
            None,
            90,
            "Zone-B",
            False,
            "v3.0.0",
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
        ),
        (
            "",
            "MCH2",
            "temperature",
            72.4,
            datetime.strptime("2025-04-28 14:32:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            85,
            "Zone-A",
            True,
            "v2.3.1",
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
    ]

    sensor_df = spark.createDataFrame(sensor_data, schema=sensor_schema)
    sensor_df.write.mode("overwrite").saveAsTable(sensor_table)

# COMMAND ----------

# DBTITLE 1,Generate Demo Maintenance Datasets
from decimal import Decimal

if (
    spark.catalog.tableExists(maintenance_table)
    and spark.table(maintenance_table).count() > 0
):
    print(
        f"Table {maintenance_table} already exists with demo data. Skipping data generation"
    )
else:
    # 2. Enhanced Maintenance Data with ingest_date and multiple rows per ingest_date
    maintenance_schema = StructType(
        [
            StructField("maintenance_id", StringType(), False),
            StructField("machine_id", StringType(), False),
            StructField("maintenance_type", StringType(), True),
            StructField("maintenance_date", DateType(), True),
            StructField("duration_minutes", IntegerType(), True),
            StructField("cost", DecimalType(10, 2), True),
            StructField("next_scheduled_date", DateType(), True),
            StructField("work_order_id", StringType(), True),
            StructField("safety_check_passed", BooleanType(), True),
            StructField("parts_list", ArrayType(StringType()), True),
            StructField("ingest_date", DateType(), True),
        ]
    )

    maintenance_data = [
        # Ingest date 2025-04-28
        (
            "MTN-001",
            "MCH-001",
            "preventive",
            datetime.strptime("2025-04-01", "%Y-%m-%d").date(),
            120,
            Decimal("450.00"),
            datetime.strptime("2025-07-01", "%Y-%m-%d").date(),
            "WO-001",
            True,
            ["filter", "gasket"],
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),
        (
            "MTN-002",
            "MCH-002",
            "corrective",
            datetime.strptime("2025-04-15", "%Y-%m-%d").date(),
            240,
            Decimal("1200.50"),
            datetime.strptime("2026-04-01", "%Y-%m-%d").date(),
            "WO-002",
            False,
            ["motor"],
            datetime.strptime("2025-04-28", "%Y-%m-%d").date(),
        ),  # Future date issue
        # Ingest date 2025-04-29
        (
            "MTN-003",
            "MCH-003",
            None,
            datetime.strptime("2025-04-20", "%Y-%m-%d").date(),
            -30,
            Decimal("-500.00"),
            datetime.strptime("2024-04-20", "%Y-%m-%d").date(),
            "INVALID",
            None,
            [],
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
        ),  # Multiple issues
        (
            "MTN-004",
            "MCH-001",
            "predictive",
            datetime.strptime("2025-04-25", "%Y-%m-%d").date(),
            180,
            Decimal("800.00"),
            datetime.strptime("2025-10-01", "%Y-%m-%d").date(),
            "WO-003",
            True,
            ["sensor"],
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
        ),
        # Ingest date 2025-04-30
        (
            "MTN-005",
            "MCH-002",
            "preventive",
            datetime.strptime("2025-04-29", "%Y-%m-%d").date(),
            90,
            Decimal("300.00"),
            datetime.strptime("2025-07-15", "%Y-%m-%d").date(),
            "WO-004",
            True,
            ["valve"],
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
        ),
        (
            "MTN-006",
            "MCH-003",
            "corrective",
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
            60,
            Decimal("150.00"),
            datetime.strptime("2025-08-01", "%Y-%m-%d").date(),
            "WO-005",
            False,
            ["pump"],
            datetime.strptime("2025-04-30", "%Y-%m-%d").date(),
        ),
    ]

    maintenance_df = spark.createDataFrame(maintenance_data, schema=maintenance_schema)
    maintenance_df.write.mode("overwrite").saveAsTable(maintenance_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the datasets
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC ### Machine Sensor Readings Dataset
# MAGIC
# MAGIC | Column Name        | Data Type    | Description                                      | Example Value         |
# MAGIC |--------------------|-------------|--------------------------------------------------|----------------------|
# MAGIC | `sensor_id`        | string      | Unique sensor identifier                         | SEN-001              |
# MAGIC | `machine_id`       | string      | Linked machine identifier                        | MCH-001              |
# MAGIC | `sensor_type`      | string      | Type of sensor (temperature, pressure, etc.)     | temperature          |
# MAGIC | `reading_value`    | double      | Value recorded by the sensor                     | 72.4                 |
# MAGIC | `reading_timestamp`| timestamp   | Time the reading was taken                       | 2025-04-28 14:32:00  |
# MAGIC | `calibration_date` | date        | Last calibration date of the sensor              | 2025-04-01           |
# MAGIC | `battery_level`    | int         | Battery percentage (0-100)                       | 85                   |
# MAGIC | `facility_zone`    | string      | Plant zone or location                           | Zone-A               |
# MAGIC | `is_active`        | boolean     | Whether the sensor is active                     | true                 |
# MAGIC | `firmware_version` | string      | Sensor firmware version                          | v2.3.1               |
# MAGIC | `ingest_date`      | date        | Date the record was ingested                     | 2025-04-28           |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Some sample "Sensor Table" Data

# COMMAND ----------

# DBTITLE 1,Sensor Bronze Tables

sensor_bronze_df = spark.read.table(sensor_table)
print("=== Sensor Data Sample ===")
display(sensor_bronze_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Maintenance Records Dataset
# MAGIC
# MAGIC | Column Name           | Data Type        | Description                                   | Example Value           |
# MAGIC |-----------------------|-----------------|-----------------------------------------------|------------------------|
# MAGIC | `maintenance_id`      | string          | Unique maintenance event identifier           | MTN-001                |
# MAGIC | `machine_id`          | string          | Linked machine identifier                     | MCH-001                |
# MAGIC | `maintenance_type`    | string          | Type of maintenance (preventive, corrective)  | preventive             |
# MAGIC | `maintenance_date`    | date            | Date maintenance was performed                | 2025-04-01             |
# MAGIC | `duration_minutes`    | int             | Duration of maintenance in minutes            | 120                    |
# MAGIC | `cost`                | decimal(10,2)   | Cost of maintenance                           | 450.00                 |
# MAGIC | `next_scheduled_date` | date            | Next scheduled maintenance date               | 2025-07-01             |
# MAGIC | `work_order_id`       | string          | Associated work order identifier              | WO-001                 |
# MAGIC | `safety_check_passed` | boolean         | Whether safety check was passed               | true                   |
# MAGIC | `parts_list`          | array   | List of parts replaced or serviced            | ["filter", "gasket"]   |
# MAGIC | `ingest_date`         | date            | Date the record was ingested                  | 2025-04-28             |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Some sample "Maintenance Table" Data

# COMMAND ----------

mntnc_bronze_df = spark.read.table(maintenance_table)
print("=== Maintenance Data Sample ===")
display(mntnc_bronze_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Common Imports
import yaml
from pprint import pprint

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig, TableChecksStorageConfig

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Problem - Team doesn't know the Quality Rules for Maintenance Dataset
# MAGIC ### Feature - Infer the Data Quality Rules using DQX

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step-1**. Read Raw Data and Instantiate DQX 

# COMMAND ----------

# Read Input Data
mntnc_bronze_df = spark.read.table(maintenance_table)

# Instantiate DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step-2**. Run DQX Profiler and **Infer** Quality Rules

# COMMAND ----------

# Profile Inpute Data
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(mntnc_bronze_df)

# Generate DQX quality rules/checks
generator = DQGenerator(ws)
maintenance_checks = generator.generate_dq_rules(profiles)  # with default level "error"

# COMMAND ----------

# DBTITLE 1,Review the Inferred checks
print("=== Inferred DQ Checks ===\n")

for idx, check in enumerate(maintenance_checks):
   print(f"========Check {idx} ==========\n")
   pprint(check)

# COMMAND ----------

# DBTITLE 1,Save the quality rules in a file for review
# save checks in a workspace location
maintenance_dq_rules_yaml = f"{quality_rules_path}/maintenance_dq_rules.yml"

# Save file in a workspace path
dq_engine.save_checks(maintenance_checks, config=WorkspaceFileChecksStorageConfig(location=maintenance_dq_rules_yaml))

# display the link to the saved checks
displayHTML(f'<a href="/#workspace{maintenance_dq_rules_yaml}" target="_blank">Maintenance Data Quality Rules YAML</a>')

# COMMAND ----------

# DBTITLE 1,Save the quality rules in delta table
# or save in delta table
maintenance_quality_rules_table = f"{catalog}.{schema}.maintenance_inferred_quality_rules"
dq_engine.save_checks(maintenance_checks, config=TableChecksStorageConfig(location=maintenance_quality_rules_table, run_config_name="maintenance"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step-3**. Apply Inferred Quality Rules to Input Data

# COMMAND ----------

# Load checks from workspace file
quality_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=maintenance_dq_rules_yaml))
# or Load checks from a table
#quality_checks = dq_engine.load_checks(config=TableChecksStorageConfig(location=maintenance_quality_rules_table, run_config_name="maintenance"))

# Apply checks on input data
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(mntnc_bronze_df, quality_checks)

print("=== Maintenance Bad Data Sample ===")
display(quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data Quality Rules for Machine Sensor Data
# MAGIC
# MAGIC | Rule Type             | Example Rule                                                                                           | Purpose / Impact                                                        |DQ Rule|Quality Error Level|
# MAGIC |-----------------------|-------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|-|--|
# MAGIC | **Completeness**      | Required fields (`sensor_id`, `machine_id`) must not be null    | Ensures all critical data is present and usable     |`is_not_null_and_not_empty`                    |ERROR|
# MAGIC | **Range / Domain**    | `reading_value` (temperature): 0–100 | Detects outliers and sensor faults; ensures physical plausibility       |**FILTER quality Check + `is_in_range`**| WARN|
# MAGIC | **Format Standardization**  |  `machine_id` follows standard format                             | Standardizes data for integration and analysis                          |`regex_match` |WARN|
# MAGIC | **Timeliness**        | `reading_timestamp` is not in the future; beyond 3 days                                     | Prevents erroneous time-series data                            |`is_not_in_future` |ERROR|
# MAGIC | **Correctness**        | `calibration_date` is eariler than `reading_timestamp`| Prevents erroneous sesnor readings data                            |`SQL Expression` |ERROR|
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sensor Dataset Quality Rules YAML

# COMMAND ----------

# DBTITLE 1,Sensor Dataset Quality Rules YAML
sensor_dq_checks = yaml.safe_load("""
# Completeness Check on 2 columns
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
    - sensor_id
    - machine_id
        
# Filter + Range Based Check
- criticality: warn
  filter: sensor_type = 'temperature'
  check:
    function: is_in_range
    arguments:
      column: reading_value
      min_limit: 0
      max_limit: 100

# Regex Based Check
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: machine_id
      regex: '^MCH-\\d{3}$'

# timeliness check
- criticality: error
  check:
    function: is_not_in_future
    arguments:
      column: reading_timestamp
      offset: 259200

# sql_expression check
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: (calibration_date > date(reading_timestamp))
      msg: Sensor calibration_date is later than sensor reading_timestamp
      name: calib_dt_gt_reading_ts
      negate: true
""")

# validate the checks
status = DQEngine.validate_checks(sensor_dq_checks)
print(status)
assert not status.has_errors

# COMMAND  ----------

# save checks in a workspace location
sensor_dq_rules_yaml = f"{quality_rules_path}/sensor_dq_rules.yml"
dq_engine.save_checks(sensor_dq_checks, config=WorkspaceFileChecksStorageConfig(location=sensor_dq_rules_yaml))

# display the link to the saved checks
displayHTML(f'<a href="/#workspace{sensor_dq_rules_yaml}" target="_blank">Sensor Data Quality Rules YAML</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarantine Bad Data & Perform Granular Issue Detection

# COMMAND ----------

# DBTITLE 1,Apply quality checks defined in YAML
# read sensor data
sensor_bronze_df = spark.read.table(sensor_table)

# Load quality rules from YAML file
sensor_dq_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=sensor_dq_rules_yaml))

# Apply checks on input data
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(sensor_bronze_df, sensor_dq_checks)

print("=== Bad Data DF ===")
display(quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bring / Build Your own Quality Rules (Checks)
# MAGIC This section elaborates how you can extend DQX to implement your own quality rule. 
# MAGIC 3 Steps - 
# MAGIC 1. Define the new rules
# MAGIC 2. Add the rules to YAML definition
# MAGIC 3. Apply the DQ Rules on input data 
# MAGIC
# MAGIC For this demo, we need to add a new rule to quarantine the rows where firmware version doesn't start with 'v':
# MAGIC
# MAGIC |Dataset| Rule Type             | Example Rule                                                                                           | Purpose / Impact                                                        |DQ Rule|
# MAGIC |-|-----------------------|-------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|--|
# MAGIC |Sensor Data| **Standardization**          | `firmware_version` starts with "v"  | Ensures firmware version value is a standard value | Custom Rule Development| 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Define the custom rule function
import pyspark.sql.functions as F
from pyspark.sql import Column as col
from databricks.labs.dqx.check_funcs import make_condition


def firmware_version_start_with_v(column: str) -> col:
    column_expr = F.expr(column)
    
    quality_rule_expr = ~(column_expr.startswith("v"))
    quality_rule_err_msg = f"firmware_version doesn't starts with 'v'"
    quality_rule_err_col_name = f"firmware_version_not_starts_with_v"

    return make_condition(quality_rule_expr, quality_rule_err_msg, quality_rule_err_col_name)

# COMMAND ----------

# DBTITLE 1,Add custom DQ rule in YAML
# Define Custom Check in YAML
byor_quality_rule = {
    'criticality': 'error',
    'check': {
        'function': 'firmware_version_start_with_v',
        'arguments': {
            'column': 'firmware_version'
        }
    }
}

sensor_dq_checks.append(byor_quality_rule)

# Save the YAML file with the new custom DQ rule
sensor_custom_dq_rules_yaml = f"{quality_rules_path}/sensor_custom_dq_rules.yml"
dq_engine.save_checks(sensor_dq_checks, config=WorkspaceFileChecksStorageConfig(location=sensor_custom_dq_rules_yaml))

# display the link to the saved checks
displayHTML(f'<a href="/#workspace{sensor_custom_dq_rules_yaml}" target="_blank">Sensor Custom Data Quality Rules YAML</a>')


# COMMAND ----------

# DBTITLE 1,Apply the DQ Rules on Input Data
dq_engine = DQEngine(WorkspaceClient())

sensor_quality_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=sensor_custom_dq_rules_yaml))

# Define the custom check 
custom_check_functions = {"firmware_version_start_with_v": firmware_version_start_with_v}  # list of custom check functions

# Apply the custom check on the bronze data
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(sensor_bronze_df, sensor_quality_checks, custom_check_functions)

print("=== Quarantined Data Sample ===")
display(quarantined_df)

sensor_quarantine_table = f"{catalog}.{schema}.sensor_quarantine"
quarantined_df.write.mode("overwrite").saveAsTable(sensor_quarantine_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Quality on Pre-Configured Dashboard
# MAGIC
# MAGIC When you deploy DQX as [`workspace tool`](https://databrickslabs.github.io/dqx/docs/installation/#dqx-installation-as-a-tool-in-a-databricks-workspace), it automatically generates a Quality Dashboard. <br> You can open the dashboard using Databricks CLI: `databricks labs dqx open-dashboards`
# MAGIC
# MAGIC