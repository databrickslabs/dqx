# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Demo Notebook
# MAGIC This notebook illustrates example usage of DQX for **MachinaMetrics**, a fictional manufacturing company.
# MAGIC
# MAGIC MachinaMetrics is a leading-edge manufacturing company whose CTO is spearheading the adoption of an AI-driven predictive maintenance solution. By harnessing real-time machine status data and historical maintenance schedules, this technology will proactively forecast service needs, minimize unplanned downtime, and optimize maintenance cycles. The result is a significant reduction in operational costs, extended equipment lifespan, and maximized production efficiency-positioning MachinaMetrics as an industry innovator in smart manufacturing.
# MAGIC
# MAGIC ***NOTE:***
# MAGIC - This notebook is intended to be run as a task in a Databricks Job deployed using Databricks Asset Bundles. We include DQX as a cluster-scoped library in our `databricks.yml` configuration file.
# MAGIC - By default, the example datasets are persisted in `main.default`. To change the default catalog and schema, specify appropriate `demo_catalog` and `demo_schema` in the task parameters configured in `databricks.yml`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating Example Data

# COMMAND ----------

# DBTITLE 1,Set Catalog and Schema for Demo Dataset
default_database = "main"
default_schema = "default"

dbutils.widgets.text("demo_database", default_database, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema, "Schema Name")

database = dbutils.widgets.get("demo_database")
schema = dbutils.widgets.get("demo_schema")

print(f"Selected Catalog for Demo Dataset: {database}")
print(f"Selected Schema for Demo Dataset: {schema}")

sensor_table = f"{database}.{schema}.sensor_data"
maintenance_table = f"{database}.{schema}.maintenance_data"

# COMMAND ----------

# DBTITLE 1,Set Rules File Paths
import os

dbutils.widgets.text("maintenance_rules_file", "maintenance_dq_rules.yml", "Rules file for the maintenance dataset")
dbutils.widgets.text("sensor_rules_file", "sensor_dq_rules.yml", "Rules file for the sensor dataset")

maintenance_rules_file = dbutils.widgets.get("maintenance_rules_file")
sensor_rules_file = dbutils.widgets.get("sensor_rules_file")

maintenance_rules_file_path = f"{os.getcwd()}/{maintenance_rules_file}"
sensor_rules_file_path = f"{os.getcwd()}/{sensor_rules_file}"

print(f"Rules File for Maintenance Dataset: {maintenance_rules_file_path}")
print(f"Rules File for Sensor Dataset: {sensor_rules_file_path}")

assert os.path.exists(sensor_rules_file_path), "Quality rules file not found for sensor dataset"
assert os.path.exists(maintenance_rules_file_path), "Quality rules file not found for maintenance dataset"

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
            735.0,
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
            735.0,
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
            724.0,
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
            724.0,
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
# MAGIC ## Exploring the data

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

mntnc_bronze_df = spark.read.table(maintenance_table)
print("=== Maintenance Data Sample ===")
display(mntnc_bronze_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying DQX checks

# COMMAND ----------

# DBTITLE 1,Common Imports
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import InputConfig, OutputConfig, WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine

# COMMAND ----------

# DBTITLE 1,Checking the Sensor Dataset
# Instantiate DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

displayHTML(f'<a href="/#workspace{sensor_rules_file_path}" target="_blank">Quality rules file for sensor dataset</a>')

# Load the checks
maintenance_quality_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=sensor_rules_file_path))

# Apply the checks and write the output data
dq_engine.apply_checks_by_metadata_and_save_in_table(
  checks=maintenance_quality_checks,
  input_config=InputConfig(sensor_table),
  output_config=OutputConfig(f"{sensor_table}_valid", mode="overwrite"),
  quarantine_config=OutputConfig(f"{sensor_table}_quarantine", mode="overwrite")
)

# COMMAND ----------

# DBTITLE 1,Checking the Maintenance Dataset
# Instantiate DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

displayHTML(f'<a href="/#workspace{maintenance_rules_file_path}" target="_blank">Quality rules file for maintenance dataset</a>')

# Load the checks
maintenance_quality_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=maintenance_rules_file_path))

# Apply the checks and write the output data
dq_engine.apply_checks_by_metadata_and_save_in_table(
  checks=maintenance_quality_checks,
  input_config=InputConfig(maintenance_table),
  output_config=OutputConfig(f"{maintenance_table}_valid", mode="overwrite"),
  quarantine_config=OutputConfig(f"{maintenance_table}_quarantine", mode="overwrite")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizing checked data

# COMMAND ----------

quarantine_sensor_data = spark.table(f"{sensor_table}_quarantine")
print("=== Quarantined Sensor Data Sample ===")
display(quarantine_sensor_data.limit(10))

# COMMAND ----------

quarantine_maintenance_data = spark.table(f"{maintenance_table}_quarantine")
print("=== Quarantined Maintenance Data Sample ===")
display(quarantine_maintenance_data.limit(10))