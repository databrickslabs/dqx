# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - Banking / FSI Industry Accelerator Demo
# MAGIC ## Anti-Money Laundering (AML) & Transaction Monitoring
# MAGIC
# MAGIC This demo showcases DQX data quality rules tailored for the **Banking and Financial Services industry**, with a focus on **fraud detection, transaction structuring prevention, and regulatory compliance**.
# MAGIC
# MAGIC ### Use Case: Real-Time Transaction Quality & Fraud Monitoring
# MAGIC
# MAGIC Process incoming transaction batches and automatically:
# MAGIC * **Quarantine invalid transactions** (errors) for immediate investigation
# MAGIC * **Flag suspicious patterns** (warnings) for AML review while allowing transactions to process
# MAGIC * **Enforce data integrity** to ensure downstream systems receive clean, compliant data
# MAGIC
# MAGIC ### Implemented Data Quality Checks
# MAGIC
# MAGIC **1. Custom Fraud Detection (AML Red Flags)**
# MAGIC * **Structuring Detection**: Perfectly round amounts (\$5K, \$10K, \$15K) often indicate structuring to avoid reporting
# MAGIC * **Threshold Avoidance**: Amounts just below \$10K reporting threshold (\$9,950-\$9,999)
# MAGIC * **Anomalous Patterns**: Repeated digit patterns (\$7,777.77, \$3,333.33) statistically unlikely in legitimate transactions
# MAGIC
# MAGIC **2. Critical Field Validation (Errors)**
# MAGIC * Required fields: `transaction_id`, `currency` must not be null
# MAGIC * Amount constraints: Not null, not zero, within system bounds (-999M to 999M)
# MAGIC
# MAGIC **3. Business Logic Validation (Errors)**
# MAGIC * **Transaction Sign Consistency**: CREDIT/DEPOSIT/INCOMING_TRANSFER must be positive; DEBIT/WITHDRAWAL/FEE/CHARGEBACK must be negative
# MAGIC * Prevents accounting errors and reconciliation failures
# MAGIC
# MAGIC **4. Data Quality Standards (Warnings)**
# MAGIC * **Currency Validation**: ISO-4217 compliance (USD, EUR, GBP, etc.)
# MAGIC * **Precision Standards**: Maximum 2 decimal places for monetary amounts
# MAGIC
# MAGIC ### Key DQX Features Demonstrated
# MAGIC
# MAGIC * **Dual-Routing Split Behavior**: Warnings go to BOTH valid and invalid DataFrames (monitoring without blocking)
# MAGIC * **Custom Check Functions**: Industry-specific fraud detection logic integrated with built-in checks
# MAGIC * **Centralized Rule Management**: Rules stored in Delta tables for versioning and governance
# MAGIC * **Single-Pass Execution**: All checks applied in one data scan for optimal performance

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
# MAGIC ### Production Best Practice: Pin DQX Version
# MAGIC
# MAGIC To ensure consistent behavior and avoid unexpected issues from automatic upgrades, it is highly recommended to always pin DQX to a specific version in your production pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Catalog and Schema
# MAGIC
# MAGIC Specify the catalog and schema where the quality rules table will be stored.

# COMMAND ----------

# DBTITLE 1,Set Catalog and Schema for Demo
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
# MAGIC ### Setup Sample Financial Data
# MAGIC
# MAGIC We'll create a DataFrame simulating a typical transaction stream or daily batch (`transactions_df`).

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from datetime import datetime

# Create a comprehensive sample DataFrame simulating international banking transactions
transactions = [
    # Valid transactions
    Row(transaction_id="TRX-101", account_id="ACC-US-001", country="USA", transaction_type="CREDIT", currency="USD", amount=1247.89, timestamp="2024-03-15 10:30:00"),
    Row(transaction_id="TRX-102", account_id="ACC-UK-002", country="GBR", transaction_type="DEPOSIT", currency="GBP", amount=876.45, timestamp="2024-03-15 14:20:00"),
    Row(transaction_id="TRX-103", account_id="ACC-JP-003", country="JPN", transaction_type="WITHDRAWAL", currency="JPY", amount=-23456.00, timestamp="2024-03-15 09:15:00"),
    Row(transaction_id="TRX-104", account_id="ACC-FR-004", country="FRA", transaction_type="CREDIT", currency="EUR", amount=543.21, timestamp="2024-03-15 11:45:00"),
    
    # Suspicious Pattern: Perfectly round amounts (structuring)
    Row(transaction_id="TRX-201", account_id="ACC-US-005", country="USA", transaction_type="DEPOSIT", currency="USD", amount=10000.00, timestamp="2024-03-15 10:00:00"),  # Warn: Perfectly round 10K
    Row(transaction_id="TRX-202", account_id="ACC-CA-006", country="CAN", transaction_type="CREDIT", currency="CAD", amount=5000.00, timestamp="2024-03-15 15:30:00"),   # Warn: Perfectly round 5K
    Row(transaction_id="TRX-203", account_id="ACC-AU-007", country="AUS", transaction_type="DEPOSIT", currency="AUD", amount=15000.00, timestamp="2024-03-16 08:20:00"),  # Warn: Perfectly round 15K
    
    # Suspicious Pattern: Just below reporting thresholds
    Row(transaction_id="TRX-301", account_id="ACC-US-008", country="USA", transaction_type="DEPOSIT", currency="USD", amount=9995.00, timestamp="2024-03-16 11:00:00"),  # Warn: Just below 10K threshold
    Row(transaction_id="TRX-302", account_id="ACC-US-009", country="USA", transaction_type="CREDIT", currency="USD", amount=4975.50, timestamp="2024-03-16 13:45:00"),  # Warn: Just below 5K threshold
    Row(transaction_id="TRX-303", account_id="ACC-CH-010", country="CHE", transaction_type="DEPOSIT", currency="CHF", amount=2980.00, timestamp="2024-03-16 16:20:00"),  # Warn: Just below 3K threshold
    
    # Suspicious Pattern: Repeated digit patterns
    Row(transaction_id="TRX-401", account_id="ACC-BR-011", country="BRA", transaction_type="CREDIT", currency="BRL", amount=7777.77, timestamp="2024-03-17 09:30:00"),  # Warn: Repeated 7s
    Row(transaction_id="TRX-402", account_id="ACC-MX-012", country="MEX", transaction_type="DEPOSIT", currency="MXN", amount=3333.33, timestamp="2024-03-17 10:15:00"),  # Warn: Repeated 3s
    Row(transaction_id="TRX-403", account_id="ACC-IN-013", country="IND", transaction_type="CREDIT", currency="INR", amount=8888.88, timestamp="2024-03-17 14:00:00"),  # Warn: Repeated 8s
    
    # Data quality errors: Sign inconsistency
    Row(transaction_id="TRX-501", account_id="ACC-DE-014", country="DEU", transaction_type="CREDIT", currency="EUR", amount=-500.00, timestamp="2024-03-18 10:00:00"),     # Error: Credit must be positive
    Row(transaction_id="TRX-502", account_id="ACC-ES-015", country="ESP", transaction_type="WITHDRAWAL", currency="EUR", amount=300.00, timestamp="2024-03-18 11:30:00"),  # Error: Withdrawal must be negative
    Row(transaction_id="TRX-503", account_id="ACC-IT-016", country="ITA", transaction_type="CHARGEBACK", currency="EUR", amount=125.50, timestamp="2024-03-18 13:15:00"),  # Error: Chargeback must be negative
    
    # Data quality errors: Precision issues
    Row(transaction_id="TRX-601", account_id="ACC-SG-017", country="SGP", transaction_type="FEE", currency="USD", amount=-1.234, timestamp="2024-03-19 09:00:00"),        # Warn: Too many decimals
    Row(transaction_id="TRX-602", account_id="ACC-CN-018", country="CHN", transaction_type="CREDIT", currency="CNY", amount=567.8901, timestamp="2024-03-19 10:30:00"),    # Warn: Too many decimals
    
    # Data quality errors: Null values
    Row(transaction_id="TRX-701", account_id="ACC-NL-019", country="NLD", transaction_type="DEBIT", currency=None, amount=-150.00, timestamp="2024-03-20 08:45:00"),       # Error: Null currency
    Row(transaction_id=None, account_id="ACC-SE-020", country="SWE", transaction_type="CREDIT", currency="EUR", amount=200.00, timestamp="2024-03-20 09:30:00"),           # Error: Null transaction_id
    
    # More valid international transactions
    Row(transaction_id="TRX-801", account_id="ACC-KR-021", country="KOR", transaction_type="INCOMING_TRANSFER", currency="USD", amount=2345.67, timestamp="2024-03-20 11:00:00"),
    Row(transaction_id="TRX-802", account_id="ACC-ZA-022", country="ZAF", transaction_type="FEE", currency="USD", amount=-5.50, timestamp="2024-03-20 12:15:00"),
    Row(transaction_id="TRX-803", account_id="ACC-AR-023", country="ARG", transaction_type="WITHDRAWAL", currency="USD", amount=-789.25, timestamp="2024-03-20 14:30:00"),
    Row(transaction_id="TRX-804", account_id="ACC-PL-024", country="POL", transaction_type="DEPOSIT", currency="EUR", amount=1456.88, timestamp="2024-03-20 15:45:00"),
]

transactions_table = f"{catalog}.{schema}.banking_transactions"

if spark.catalog.tableExists(transactions_table) and spark.table(transactions_table).count() > 0:
    print(f"Table {transactions_table} already exists with demo data. Skipping data generation")
else:
    transactions_df = spark.createDataFrame(transactions)
    transactions_df.write.mode("overwrite").saveAsTable(transactions_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Dataset
# MAGIC
# MAGIC ### Banking Transactions Dataset
# MAGIC
# MAGIC | Column Name        | Data Type | Description                                          | Example Value          |
# MAGIC |--------------------|-----------|------------------------------------------------------|------------------------|
# MAGIC | `transaction_id`   | string    | Unique transaction identifier                        | TRX-101                |
# MAGIC | `account_id`       | string    | Account identifier                                   | ACC-US-001             |
# MAGIC | `country`          | string    | Country code (ISO 3166-1 alpha-3)                    | USA                    |
# MAGIC | `transaction_type` | string    | Type of transaction (CREDIT, DEBIT, DEPOSIT, etc.)   | CREDIT                 |
# MAGIC | `currency`         | string    | Currency code (ISO 4217)                             | USD                    |
# MAGIC | `amount`           | double    | Transaction amount (positive for credits, negative for debits) | 1247.89     |
# MAGIC | `timestamp`        | string    | Transaction timestamp                                | 2024-03-15 10:30:00    |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Some Sample Transaction Data

# COMMAND ----------

# DBTITLE 1,Transactions Bronze Table
transactions_df = spark.read.table(transactions_table)
print("=== Transactions Data Sample ===")
display(transactions_df.limit(10))

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
# MAGIC ### Auto-Infer Quality Rules with DQProfiler
# MAGIC
# MAGIC Before defining rules manually, DQX can **automatically infer data quality rules** from the data profile. This is useful for bootstrapping a quality rule set for a new dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1**: Read raw data and instantiate DQX

# COMMAND ----------

# Read Input Data
transactions_df = spark.read.table(transactions_table)

# Instantiate DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2**: Run DQProfiler to infer quality rules

# COMMAND ----------

# DBTITLE 1,Profile Data and Infer Quality Rules
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(transactions_df)

generator = DQGenerator(ws)
inferred_checks = generator.generate_dq_rules(profiles)

print("=== Inferred DQ Checks ===\n")
for idx, check in enumerate(inferred_checks):
    print(f"========Check {idx} ==========\n")
    pprint(check)

# COMMAND ----------

# DBTITLE 1,Save Inferred Rules to Workspace File
banking_inferred_rules_yaml = f"{quality_rules_path}/banking_inferred_dq_rules.yml"
dq_engine.save_checks(inferred_checks, config=WorkspaceFileChecksStorageConfig(location=banking_inferred_rules_yaml))
displayHTML(f'<a href="/#workspace{banking_inferred_rules_yaml}" target="_blank">Banking Inferred Quality Rules YAML</a>')

# COMMAND ----------

# DBTITLE 1,Save Inferred Rules to Delta Table
banking_inferred_rules_table = f"{catalog}.{schema}.banking_inferred_quality_rules"
dq_engine.save_checks(inferred_checks, config=TableChecksStorageConfig(location=banking_inferred_rules_table, run_config_name="banking_inferred"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3**: Apply Inferred Quality Rules to Input Data

# COMMAND ----------

# Load checks from workspace file
inferred_quality_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=banking_inferred_rules_yaml))

# Apply checks on input data
valid_inferred_df, quarantined_inferred_df = dq_engine.apply_checks_by_metadata_and_split(transactions_df, inferred_quality_checks)

print("=== Transactions Quarantined by Inferred Rules ===")
display(quarantined_inferred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bring Your Own Rule: Suspicious Amount Pattern Detection
# MAGIC
# MAGIC This section demonstrates how to extend DQX with a **custom quality rule**. The workflow follows 3 steps:
# MAGIC 1. **Define** the custom rule function
# MAGIC 2. **Add** the rule to the YAML definition
# MAGIC 3. **Apply** the DQ rules on input data
# MAGIC
# MAGIC For this demo, we define a custom fraud detection rule that flags suspicious amount patterns commonly associated with financial crime and structuring:
# MAGIC * **Perfectly round amounts** above threshold (e.g., exactly \$5,000, \$10,000) which may indicate attempts to structure transactions
# MAGIC * **Just below reporting thresholds** (e.g., \$9,950-\$9,999) to avoid regulatory reporting
# MAGIC * **Repeated digit patterns** (e.g., \$7,777.77, \$3,333.33) which are statistically unlikely in legitimate transactions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Define the Custom Rule Function

# COMMAND ----------

# DBTITLE 1,Custom Industry Check: Suspicious Amount Pattern Detection
import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.check_funcs import make_condition, get_normalized_column_and_expr
from databricks.labs.dqx.rule import register_rule

@register_rule("row")
def is_suspicious_amount_pattern(
    column: str | Column, 
    threshold: float = 5000.0,
    reporting_thresholds: float | list[float] = 10000.0,
    margin: float = 50.0
) -> Column:
    """
    Detects suspicious amount patterns commonly associated with structuring or financial crime:
    1. Perfectly round amounts above threshold (e.g., exactly 5000.00, 10000.00)
    2. Amounts just below configurable reporting thresholds (e.g., 9950-9999 for a 10K threshold)
    3. Repeated digit patterns (e.g., 7777.77, 3333.33)
    
    These patterns may indicate attempts to avoid regulatory reporting requirements.
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    
    # Take absolute value for the checks
    abs_amount = F.abs(col_expr)
    
    # Check 1: Perfectly round amounts above threshold (e.g., 5000.00, 10000.00)
    # Amount equals its floor and is divisible by 1000 or 5000
    is_round = (
        (abs_amount >= threshold) & 
        (abs_amount == F.floor(abs_amount)) & 
        ((abs_amount % 1000 == 0) | (abs_amount % 5000 == 0))
    )
    
    # Check 2: Just below the reporting thresholds
    if isinstance(reporting_thresholds, (int, float)):
        reporting_thresholds = [float(reporting_thresholds)]
    
    near_threshold = F.lit(False)
    for t in reporting_thresholds:
        near_threshold = near_threshold | ((abs_amount >= t - margin) & (abs_amount < t))
    
    # Check 3: Repeated digit patterns (e.g., 7777.77, 3333.33)
    amount_str = F.format_number(abs_amount, 2)
    # Remove decimal point and check if all digits are the same
    digits_only = F.regexp_replace(amount_str, "[^0-9]", "")
    first_digit = F.substring(digits_only, 1, 1)
    all_same_digit = (
        (F.length(digits_only) >= 4) & 
        (F.length(F.regexp_replace(digits_only, first_digit, "")) == 0)
    )
    
    # Flag as suspicious if any pattern is detected
    suspicious = is_round | near_threshold | all_same_digit
    
    return make_condition(
        suspicious,
        F.concat(
            F.lit(f"Column '{col_expr_str}' value '"), 
            col_expr.cast("string"), 
            F.lit("' matches suspicious amount pattern (possible structuring/fraud indicator)")
        ),
        f"{col_str_norm}_suspicious_amount_pattern"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Add the Custom Rule to YAML Definition
# MAGIC
# MAGIC We define all quality checks — both built-in and custom — in a single YAML configuration.

# COMMAND ----------

# Define industry-specific checks in YAML format
banking_checks_yaml = f"""
# 1. Critical fields must not be null
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
    - transaction_id
    - currency
  user_metadata:
    version: v1
    location: {catalog}.{schema}.banking_quality_rules

# 2. Suspicious Amount Pattern Detection (Custom Fraud Check)
- name: suspicious_amount_pattern
  criticality: warn
  check:
    function: is_suspicious_amount_pattern
    arguments:
      column: amount
      threshold: 5000.0
      reporting_thresholds: [10000.0, 5000.0, 3000.0]
      margin: 50.0
  user_metadata:
    version: v1
    location: {catalog}.{schema}.banking_quality_rules

# 3. Currency validation
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: currency
      allowed: ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY", "INR", "BRL", "MXN"]
    name: valid_iso_currency
  user_metadata:
    version: v1

# 4. Amount Basic Checks
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: amount
    name: amount_not_null
  user_metadata:
    version: v1

- criticality: error
  check:
    function: is_not_equal_to
    arguments:
      column: amount
      value: 0
    name: amount_not_zero
  user_metadata:
    version: v1

# 5. Business Boundary Checks
- criticality: error
  check:
    function: is_in_range
    arguments:
      column: amount
      min_limit: -999999999.99
      max_limit: 999999999.99
    name: amount_within_system_bounds
  user_metadata:
    version: v1

# 6. Transaction Sign Consistency
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: >
        NOT (
          transaction_type IN ('CREDIT', 'DEPOSIT', 'INCOMING_TRANSFER')
          AND amount < 0
        )
    name: credit_transaction_must_be_positive
  user_metadata:
    version: v1

- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: >
        NOT (
          transaction_type IN ('DEBIT', 'WITHDRAWAL', 'FEE', 'CHARGEBACK')
          AND amount > 0
        )
    name: debit_transaction_must_be_negative
  user_metadata:
    version: v1

# 7. Banking Precision (max 2 decimals)
- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: "amount = ROUND(amount, 2)"
    name: amount_max_two_decimal_places
  user_metadata:
    version: v1
"""

checks = yaml.safe_load(banking_checks_yaml)

# COMMAND ----------

# DBTITLE 1,Validate Checks
status = DQEngine.validate_checks(
    checks,
    custom_check_functions={'is_suspicious_amount_pattern': is_suspicious_amount_pattern}
)
print(status)
assert not status.has_errors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Best Practice: Centralized Rule Management
# MAGIC
# MAGIC Instead of hardcoding rules in every pipeline, DQX recommends maintaining them in a **centralized Delta table**. This enables versioning, governance, sharing across teams, and easier discoverability.
# MAGIC
# MAGIC **Recommended Practices:**
# MAGIC * **Access Control**: Grant read-only access to users; restrict write access to service principals.
# MAGIC * **Granularity**: Prioritize row-level checks for distributed performance.
# MAGIC * **One-Pass Execution**: Apply all checks (row, dataset, anomaly) in a single pass to minimize redundant data scans.

# COMMAND ----------

# DBTITLE 1,Centralize Rules in Delta Table
banking_rules_table = f"{catalog}.{schema}.banking_quality_rules"

# Note: Custom check functions (like is_suspicious_amount_pattern) cannot be saved to Delta tables.
# Only built-in checks are persisted. Custom checks must be applied directly from the YAML list.

# Filter out custom checks for table storage (keep only built-in checks)
built_in_checks = [check for check in checks if check.get('check', {}).get('function') != 'is_suspicious_amount_pattern']

# 1. Save built-in checks to a Delta table
dq_engine.save_checks(
    built_in_checks, 
    config=TableChecksStorageConfig(location=banking_rules_table, run_config_name="banking_prod_v1")
)

# 2. Load checks from the Delta table
quality_checks = dq_engine.load_checks(
    config=TableChecksStorageConfig(location=banking_rules_table, run_config_name="banking_prod_v1")
)

# 3. Add the custom check back to the loaded checks for runtime application
custom_amount_check = next(check for check in checks if check.get('check', {}).get('function') == 'is_suspicious_amount_pattern')
quality_checks.append(custom_amount_check)

# COMMAND ----------

# DBTITLE 1,Save Rules to Workspace File (Alternative Storage)
# DQX also supports storing rules as workspace YAML files
banking_rules_yaml = f"{quality_rules_path}/banking_quality_rules.yml"
dq_engine.save_checks(built_in_checks, config=WorkspaceFileChecksStorageConfig(location=banking_rules_yaml))

# Display a link to the saved checks file
displayHTML(f'<a href="/#workspace{banking_rules_yaml}" target="_blank">Banking Quality Rules YAML</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Apply Checks & Quarantine Invalid Records
# MAGIC
# MAGIC We'll use `apply_checks_by_metadata_and_split` to process the **loaded** checks.
# MAGIC
# MAGIC **Custom Function Handling**: 
# MAGIC * **Better practice**: Pass only specific functions: `{'is_suspicious_amount_pattern': is_suspicious_amount_pattern}`
# MAGIC * **Production best practice**: Package custom checks as an importable module instead of notebook-defined functions
# MAGIC
# MAGIC **Split Behavior**:
# MAGIC * Records with **`error`** criticality violations → go **only** to `invalid_df` (quarantined)
# MAGIC * Records with **`warn`** criticality violations → go to **BOTH** `valid_df` and `invalid_df` (allows processing while flagging for review)
# MAGIC * This dual-routing for warnings enables fraud monitoring without blocking legitimate transactions
# MAGIC * For different behavior, use `apply_checks_by_metadata()` and filter manually

# COMMAND ----------

# DQX Split Behavior:
# - Records with 'error' criticality violations → ONLY in invalid_df (quarantined)
# - Records with 'warn' criticality violations → BOTH valid_df AND invalid_df (dual-routing)
#
# This design allows suspicious patterns (warnings) to be monitored in the quarantine dataset
# while still flowing through the pipeline for processing.

valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
    transactions_df, 
    quality_checks, 
    custom_check_functions={'is_suspicious_amount_pattern': is_suspicious_amount_pattern}
)

# COMMAND ----------

display(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Invalid / Quarantined Transactions
# MAGIC Data quality issues are captured in the invalid DataFrame via the `_errors` and `_warnings` array columns mapping directly to our named checks.

# COMMAND ----------

display(invalid_df)

# COMMAND ----------

# DBTITLE 1,Persist Quarantine Table
quarantine_table = f"{catalog}.{schema}.banking_transactions_quarantine"
invalid_df.write.mode("overwrite").saveAsTable(quarantine_table)
print(f"Quarantined transactions saved to {quarantine_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Quality on Pre-Configured Dashboard
# MAGIC
# MAGIC When you deploy DQX as [`workspace tool`](https://databrickslabs.github.io/dqx/docs/installation/#dqx-installation-as-a-tool-in-a-databricks-workspace), it automatically generates a Quality Dashboard. <br> You can open the dashboard using Databricks CLI: `databricks labs dqx open-dashboards`
