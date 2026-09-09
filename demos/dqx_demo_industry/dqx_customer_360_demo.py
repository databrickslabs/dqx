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
# MAGIC * required customer name, email, and status
# MAGIC * valid email format
# MAGIC * supported customer status values
# MAGIC * non-negative `total_revenue`
# MAGIC * non-negative `open_ticket_count`
# MAGIC * `last_purchase_date` not in the future
# MAGIC * `last_campaign_engagement_date` not in the future
# MAGIC * `active_customer_flag` consistency with `customer_status`
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
from datetime import date, timedelta

today = date.today()

customer_360_data = [
    # Valid active customer
    Row(
        customer_id="CUST-001",
        customer_name="Aarav Mehta",
        email="aarav.mehta@example.com",
        customer_status="ACTIVE",
        total_revenue=1250.75,
        last_purchase_date=today - timedelta(days=30),
        last_campaign_engagement_date=today - timedelta(days=25),
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
        last_purchase_date=today - timedelta(days=730),
        last_campaign_engagement_date=today - timedelta(days=718),
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
        last_purchase_date=today - timedelta(days=96),
        last_campaign_engagement_date=today - timedelta(days=93),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Warning-level issue: bad email format.
    # Warning-only records remain eligible for valid_df, but also appear in
    # invalid_df with warning metadata and are included in the quarantine output.
    Row(
        customer_id="CUST-003",
        customer_name="Invalid Email Customer",
        email="invalid-email",
        customer_status="ACTIVE",
        total_revenue=900.00,
        last_purchase_date=today - timedelta(days=120),
        last_campaign_engagement_date=today - timedelta(days=118),
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
        last_purchase_date=today - timedelta(days=150),
        last_campaign_engagement_date=today - timedelta(days=148),
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
        last_campaign_engagement_date=today - timedelta(days=180),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Warning-level issue: ACTIVE status conflicts with active_customer_flag=False.
    # Warning-only records remain eligible for valid_df, but also appear in
    # invalid_df with warning metadata and are included in the quarantine output.
    Row(
        customer_id="CUST-006",
        customer_name="Flag Mismatch Customer",
        email="flag.mismatch@example.com",
        customer_status="ACTIVE",
        total_revenue=750.00,
        last_purchase_date=today - timedelta(days=60),
        last_campaign_engagement_date=today - timedelta(days=59),
        open_ticket_count=0,
        active_customer_flag=False
    ),

    # Invalid: missing required customer name
    Row(
        customer_id="CUST-007",
        customer_name=None,
        email="missing.name@example.com",
        customer_status="ACTIVE",
        total_revenue=425.00,
        last_purchase_date=today - timedelta(days=80),
        last_campaign_engagement_date=today - timedelta(days=78),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Invalid: missing required email
    Row(
        customer_id="CUST-008",
        customer_name="Missing Email Customer",
        email=None,
        customer_status="ACTIVE",
        total_revenue=610.00,
        last_purchase_date=today - timedelta(days=82),
        last_campaign_engagement_date=today - timedelta(days=79),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Invalid: unsupported customer status
    Row(
        customer_id="CUST-009",
        customer_name="Invalid Status Customer",
        email="invalid.status@example.com",
        customer_status="UNKNOWN",
        total_revenue=275.00,
        last_purchase_date=today - timedelta(days=100),
        last_campaign_engagement_date=today - timedelta(days=97),
        open_ticket_count=0,
        active_customer_flag=False
    ),

    # Invalid: negative open ticket count
    Row(
        customer_id="CUST-010",
        customer_name="Negative Ticket Customer",
        email="negative.ticket@example.com",
        customer_status="ACTIVE",
        total_revenue=825.00,
        last_purchase_date=today - timedelta(days=85),
        last_campaign_engagement_date=today - timedelta(days=83),
        open_ticket_count=-1,
        active_customer_flag=True
    ),

    # Warning-level issue: future campaign engagement date.
    # Warning-only records remain eligible for valid_df, but also appear in
    # invalid_df with warning metadata and are included in the quarantine output.
    Row(
        customer_id="CUST-011",
        customer_name="Future Engagement Customer",
        email="future.engagement@example.com",
        customer_status="ACTIVE",
        total_revenue=530.00,
        last_purchase_date=today - timedelta(days=90),
        last_campaign_engagement_date=date(2099, 1, 1),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Invalid: first record in a duplicate customer_id pair
    Row(
        customer_id="CUST-DUP-001",
        customer_name="Duplicate Customer One",
        email="duplicate.customer@example.com",
        customer_status="ACTIVE",
        total_revenue=100.00,
        last_purchase_date=today - timedelta(days=55),
        last_campaign_engagement_date=today - timedelta(days=54),
        open_ticket_count=0,
        active_customer_flag=True
    ),

    # Invalid: second record in a duplicate customer_id pair
    Row(
        customer_id="CUST-DUP-001",
        customer_name="Duplicate Customer Two",
        email="duplicate.customer.two@example.com",
        customer_status="ACTIVE",
        total_revenue=200.00,
        last_purchase_date=today - timedelta(days=54),
        last_campaign_engagement_date=today - timedelta(days=53),
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
  name: customer_id_not_null
  check:
    function: is_not_null_and_not_empty
    for_each_column:
    - customer_id
  user_metadata:
    domain: customer_360
    rule_type: identity

# 2. Customer ID should be unique
- criticality: error
  name: customer_id_unique
  check:
    function: is_unique
    arguments:
      columns:
      - customer_id
  user_metadata:
    domain: customer_360
    rule_type: identity

# 3. Customer name must be present
- criticality: error
  name: customer_name_not_null
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: customer_name
  user_metadata:
    domain: customer_360
    rule_type: identity

# 4. Email must be present
- criticality: error
  name: email_not_null
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: email
  user_metadata:
    domain: customer_360
    rule_type: contact_quality

# 5. Email should have a valid format
- criticality: warn
  name: valid_email_format
  check:
    function: is_valid_email
    arguments:
      column: email
  user_metadata:
    domain: customer_360
    rule_type: contact_quality

# 6. Customer status must be present
- criticality: error
  name: customer_status_not_null
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: customer_status
  user_metadata:
    domain: customer_360
    rule_type: status_quality

# 7. Customer status must use a supported value
- criticality: error
  name: customer_status_allowed
  check:
    function: is_in_list
    arguments:
      column: customer_status
      allowed:
      - "'ACTIVE'"
      - "'INACTIVE'"
  user_metadata:
    domain: customer_360
    rule_type: status_quality

# 8. Revenue should not be negative
- criticality: error
  name: total_revenue_non_negative
  check:
    function: is_not_less_than
    arguments:
      column: total_revenue
      limit: 0
  user_metadata:
    domain: customer_360
    rule_type: revenue_quality

# 9. Open ticket count should not be negative
- criticality: error
  name: open_ticket_count_non_negative
  check:
    function: is_not_less_than
    arguments:
      column: open_ticket_count
      limit: 0
  user_metadata:
    domain: customer_360
    rule_type: operational_quality

# 10. Last purchase date should not be in the future
- criticality: error
  name: last_purchase_date_not_in_future
  check:
    function: is_not_in_future
    arguments:
      column: last_purchase_date
  user_metadata:
    domain: customer_360
    rule_type: date_quality

# 11. Last campaign engagement date should not be in the future
- criticality: warn
  name: last_campaign_engagement_date_not_in_future
  check:
    function: is_not_in_future
    arguments:
      column: last_campaign_engagement_date
  user_metadata:
    domain: customer_360
    rule_type: date_quality

# 12. Active customer flag should align with customer status
- criticality: warn
  name: active_customer_flag_consistency
  check:
    function: sql_expression
    arguments:
      expression: >-
        (customer_status = 'ACTIVE' AND active_customer_flag = true)
        OR (customer_status = 'INACTIVE' AND active_customer_flag = false)
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
# MAGIC DQX returns error-level and warning-level violations in `invalid_df`. Warning-only records also remain eligible for `valid_df`, but `get_valid()` removes the warning and error metadata columns. In this example, `invalid_df` is persisted as the quarantine output so reviewers can inspect both errors and warnings.

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
