# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - Customer 360 Industry Accelerator Demo
# MAGIC ## Customer Identity, Activity, and Revenue Quality Checks
# MAGIC
# MAGIC This demo shows how DQX can be used to validate a Customer 360 dataset before it is consumed by dashboards, analytics, AI/BI, or downstream reporting.
# MAGIC
# MAGIC Customer 360 data often combines CRM, orders, support tickets, marketing engagement, and product usage. The pipeline may run successfully, but if customer identity, revenue, dates, or activity flags are wrong, downstream reports can still become unreliable.
# MAGIC
# MAGIC ### Use Case: Customer 360 Data Quality Validation
# MAGIC
# MAGIC This demo validates a synthetic Customer 360 dataset and checks for:
# MAGIC
# MAGIC * `customer_id` not null
# MAGIC * `customer_id` uniqueness
# MAGIC * valid email format
# MAGIC * non-negative `total_revenue`
# MAGIC * non-negative `open_ticket_count`
# MAGIC * `last_purchase_date` not in the future
# MAGIC * `active_customer_flag` consistency
# MAGIC
# MAGIC ### Why this matters
# MAGIC
# MAGIC In Customer 360 pipelines, the issue is not always pipeline failure. Many times the data loads fine, but downstream users still see mismatches because customer identity, metrics, or derived flags are not validated properly.
# MAGIC
# MAGIC This example shows how data quality rules can be centralized and applied before Customer 360 data is trusted for reporting or AI-driven analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX

# COMMAND ----------

# DBTITLE 1,Install DQX Library

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Catalog and Schema

# COMMAND ----------

default_catalog_name = "main"
default_schema_name = "default"

dbutils.widgets.text("demo_catalog", default_catalog_name, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema_name, "Schema Name")

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")

print(f"Selected Catalog: {catalog}")
print(f"Selected Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Synthetic Customer 360 Data
# MAGIC
# MAGIC The dataset below is fully synthetic. It includes both valid and intentionally invalid records to demonstrate DQX validation behavior.

# COMMAND ----------

from pyspark.sql import Row
from datetime import date

customer_360_data = [
    # Valid active customer
    Row(
        customer_id="CUST-001",
        customer_name="Aarav Mehta",
        email="aarav.mehta@example.com",
        customer_status="ACTIVE",
        total_revenue=1250.75,
        last_purchase_date=date(2026, 7, 10),
        last_campaign_engagement_date=date(2026, 7, 15),
        open_ticket_count=1,
        active_customer_flag=True
    ),

    # Valid inactive customer
    Row(
        customer_id="CUST-002",
        customer_name="Maya Sharma",
        email="maya.sharma@example.com",
        customer_status="INACTIVE",
        total_revenue=0.00,
        last_purchase_date=date(2024, 1, 20),
        last_campaign_engagement_date=date(2024, 2, 1),
        open_ticket_count=0,
        active_customer_flag=False
    ),

    # Invalid: missing customer_id
    Row(
        customer_id=None,
        customer_name="Missing Customer Id",
        email="missing.id@example.com",
        customer_status="ACTIVE",
        total_revenue=500.00,
        last_purchase_date=date(2026, 6, 5),
        last_campaign_engagement_date=date(2026, 6, 8),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Invalid: bad email format
    Row(
        customer_id="CUST-003",
        customer_name="Invalid Email Customer",
        email="invalid-email",
        customer_status="ACTIVE",
        total_revenue=900.00,
        last_purchase_date=date(2026, 5, 12),
        last_campaign_engagement_date=date(2026, 5, 14),
        open_ticket_count=2,
        active_customer_flag=True
    ),

    # Invalid: negative revenue
    Row(
        customer_id="CUST-004",
        customer_name="Negative Revenue Customer",
        email="negative.revenue@example.com",
        customer_status="ACTIVE",
        total_revenue=-25.00,
        last_purchase_date=date(2026, 4, 10),
        last_campaign_engagement_date=date(2026, 4, 12),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Invalid: future purchase date
    Row(
        customer_id="CUST-005",
        customer_name="Future Date Customer",
        email="future.date@example.com",
        customer_status="ACTIVE",
        total_revenue=300.00,
        last_purchase_date=date(2099, 1, 1),
        last_campaign_engagement_date=date(2026, 3, 10),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Invalid: active flag is false even though recent activity exists
    Row(
        customer_id="CUST-006",
        customer_name="Flag Mismatch Customer",
        email="flag.mismatch@example.com",
        customer_status="ACTIVE",
        total_revenue=750.00,
        last_purchase_date=date(2026, 7, 1),
        last_campaign_engagement_date=date(2026, 7, 2),
        open_ticket_count=0,
        active_customer_flag=False
    ),

    # Invalid: duplicate customer_id
    Row(
        customer_id="CUST-001",
        customer_name="Duplicate Customer",
        email="duplicate.customer@example.com",
        customer_status="ACTIVE",
        total_revenue=100.00,
        last_purchase_date=date(2026, 7, 11),
        last_campaign_engagement_date=date(2026, 7, 12),
        open_ticket_count=0,
        active_customer_flag=True
    ),
]

customer_360_df = spark.createDataFrame(customer_360_data)

display(customer_360_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Customer 360 Quality Checks
# MAGIC
# MAGIC We define quality checks in YAML format. This keeps the rules readable and easier to manage as metadata.

# COMMAND ----------

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

customer_360_checks_yaml = """
# 1. Customer ID must be present
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
    - customer_id
    name: customer_id_not_null
  user_metadata:
    domain: customer_360
    rule_type: identity

# 2. Customer ID should be unique
- criticality: error
  check:
    function: is_unique
    arguments:
      column: customer_id
    name: customer_id_unique
  user_metadata:
    domain: customer_360
    rule_type: identity

# 3. Email should follow a basic email pattern
- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'"
    name: valid_email_format
  user_metadata:
    domain: customer_360
    rule_type: contact_quality

# 4. Revenue should not be negative
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: total_revenue
      min_limit: 0
      max_limit: 999999999.99
    name: total_revenue_non_negative
  user_metadata:
    domain: customer_360
    rule_type: revenue_quality

# 5. Open ticket count should not be negative
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: open_ticket_count
      min_limit: 0
      max_limit: 999999
    name: open_ticket_count_non_negative
  user_metadata:
    domain: customer_360
    rule_type: operational_quality

# 6. Last purchase date should not be in the future
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: "last_purchase_date <= current_date() OR last_purchase_date IS NULL"
    name: last_purchase_date_not_in_future
  user_metadata:
    domain: customer_360
    rule_type: date_quality

# 7. Active customer flag should align with recent customer activity
- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: >
        NOT (
          active_customer_flag = false
          AND (
            last_purchase_date >= add_months(current_date(), -12)
            OR last_campaign_engagement_date >= add_months(current_date(), -12)
          )
        )
    name: active_customer_flag_consistency
  user_metadata:
    domain: customer_360
    rule_type: business_logic
"""

checks = yaml.safe_load(customer_360_checks_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Checks

# COMMAND ----------

status = DQEngine.validate_checks(checks)
print(status)
assert not status.has_errors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Checks and Split Valid vs Invalid Records
# MAGIC
# MAGIC DQX can split records into valid and invalid DataFrames. Error-level violations are quarantined. Warning-level violations can be monitored based on the selected DQX behavior.

# COMMAND ----------

ws = WorkspaceClient()
dq_engine = DQEngine(ws)

valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
    customer_360_df,
    checks
)

# COMMAND ----------

# DBTITLE 1,Valid Customer 360 Records
display(valid_df)

# COMMAND ----------

# DBTITLE 1,Invalid / Quarantined Customer 360 Records
display(invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist Invalid Records for Review
# MAGIC
# MAGIC In a real pipeline, invalid records can be written to a quarantine table for review and remediation.

# COMMAND ----------

quarantine_table = f"{catalog}.{schema}.customer_360_quarantine"

invalid_df.write.mode("overwrite").saveAsTable(quarantine_table)

print(f"Customer 360 invalid records saved to {quarantine_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practical Takeaway
# MAGIC
# MAGIC Customer 360 pipelines are often used by dashboards, analytics teams, AI/BI tools, and operational users.
# MAGIC
# MAGIC A pipeline can be technically successful but still produce unreliable business output if identity, revenue, activity, and derived flags are not validated.
# MAGIC
# MAGIC DQX can help centralize these rules and apply them consistently before Customer 360 data becomes a trusted consumption layer.