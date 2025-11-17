# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Data Contract Integration Demo (ODCS Compatible)
# MAGIC
# MAGIC This notebook demonstrates how to use DQX with data contracts to automatically generate
# MAGIC and apply data quality rules. DQX supports the datacontract-cli specification, which is
# MAGIC compatible with the Open Data Contract Standard (ODCS).
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC
# MAGIC 1. Loading a data contract from a YAML file
# MAGIC 2. Automatically generating DQX quality rules from field constraints
# MAGIC 3. Understanding implicit rule generation
# MAGIC 4. Applying generated rules to your data
# MAGIC 5. Viewing quality check results with contract metadata
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Run the following cell to install DQX with data contract support:

# COMMAND ----------

%pip install databricks-labs-dqx[datacontract]

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import tempfile
import yaml
from pyspark.sql import types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine

# Initialize workspace client (spark is available as a built-in global)
ws = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a Data Contract
# MAGIC
# MAGIC Data contracts define your data schema, constraints, and quality expectations.
# MAGIC This example uses a simple e-commerce orders contract.

# COMMAND ----------

# Example data contract (datacontract-cli format, ODCS compatible)
contract_yaml = """
dataContractSpecification: 0.9.3
id: urn:datacontract:ecommerce:orders
info:
  title: E-Commerce Orders
  version: 1.0.0
  description: Customer order data
  owner: Data Engineering Team

models:
  orders:
    type: table
    description: Customer orders table
    fields:
      order_id:
        type: string
        description: Unique order identifier
        required: true
        unique: true
        pattern: '^ORD-[0-9]{8}$'
      
      customer_email:
        type: string
        description: Customer email address
        required: true
        pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
      
      order_date:
        type: date
        description: Order placement date
        required: true
        format: '%Y-%m-%d'
      
      order_total:
        type: decimal
        description: Total order amount
        required: true
        minimum: 0.01
        maximum: 100000.00
      
      order_status:
        type: string
        description: Current order status
        required: true
        enum:
          - pending
          - confirmed
          - shipped
          - delivered
          - cancelled
      
      quantity:
        type: integer
        description: Number of items
        required: true
        minimum: 1
        maximum: 1000
      
      discount_percentage:
        type: decimal
        description: Discount applied
        required: false
        minimum: 0.0
        maximum: 100.0
"""

# Parse contract
contract = yaml.safe_load(contract_yaml)
print(f"Contract: {contract['info']['title']} v{contract['info']['version']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate DQX Rules from Contract
# MAGIC
# MAGIC DQX automatically generates quality rules from field constraints like:
# MAGIC - `required` → `is_not_null`
# MAGIC - `unique` → `is_unique`
# MAGIC - `pattern` → `regex_match`
# MAGIC - `minimum`/`maximum` → `is_in_range`
# MAGIC - `enum` → `is_in_list`
# MAGIC - `format` → `is_valid_date`/`is_valid_timestamp`

# COMMAND ----------

# Write contract to temporary file
with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
    yaml.dump(contract, f)
    contract_file = f.name

# Generate DQX rules
generator = DQGenerator(workspace_client=ws)
rules = generator.generate_rules_from_contract(
    contract_file=contract_file,
    generate_implicit_rules=True,
    process_text_rules=False,
    default_criticality="error"
)

print(f"Generated {len(rules)} quality rules from contract")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. View Generated Rules
# MAGIC
# MAGIC Each generated rule includes:
# MAGIC - Check function and arguments
# MAGIC - Criticality level
# MAGIC - Contract metadata (contract ID, version, model, field)

# COMMAND ----------

# Display first few rules
import json

for i, rule in enumerate(rules[:5], 1):
    print(f"\n--- Rule {i} ---")
    print(f"Name: {rule['name']}")
    print(f"Function: {rule['check']['function']}")
    print(f"Arguments: {json.dumps(rule['check']['arguments'], indent=2)}")
    print(f"Criticality: {rule['criticality']}")
    print(f"Metadata: {json.dumps(rule['user_metadata'], indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Sample Data
# MAGIC
# MAGIC Let's create sample data that matches the contract schema.
# MAGIC We'll include both valid and invalid records to demonstrate rule application.

# COMMAND ----------

# Define schema
schema = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("customer_email", T.StringType(), True),
    T.StructField("order_date", T.DateType(), True),
    T.StructField("order_total", T.DecimalType(10, 2), True),
    T.StructField("order_status", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("discount_percentage", T.DecimalType(5, 2), True),
])

# Sample data with mix of valid and invalid records
data = [
    # Valid record
    ("ORD-12345678", "customer@example.com", "2024-01-15", 99.99, "confirmed", 2, 10.0),
    # Valid record
    ("ORD-87654321", "another@test.com", "2024-01-16", 150.50, "shipped", 1, 5.0),
    # Invalid: null required field
    (None, "test@example.com", "2024-01-17", 75.00, "pending", 1, 0.0),
    # Invalid: pattern mismatch
    ("INVALID", "bad@example.com", "2024-01-18", 200.00, "delivered", 3, 15.0),
    # Invalid: out of range
    ("ORD-99999999", "valid@test.com", "2024-01-19", 150000.00, "confirmed", 2, 10.0),
    # Invalid: status not in enum
    ("ORD-11111111", "test@mail.com", "2024-01-20", 50.00, "unknown", 1, 5.0),
]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate Generated Rules
# MAGIC
# MAGIC Before applying rules, let's validate them to ensure they're properly formed.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine

validation_status = DQEngine.validate_checks(rules)

if validation_status.has_errors:
    print("⚠️  Validation errors found:")
    for error in validation_status.errors:
        print(f"  - {error}")
else:
    print(f"✅ All {len(rules)} rules validated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply Rules to Data
# MAGIC
# MAGIC DQX applies all rules and adds result columns to your DataFrame.

# COMMAND ----------

engine = DQEngine(ws)
result_df = engine.apply_checks_by_metadata(df, rules)

# Show results (added columns prefixed with dq_)
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Split Good and Bad Records
# MAGIC
# MAGIC DQX can automatically split your data into passing and failing records.

# COMMAND ----------

good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, rules)

print(f"Good records: {good_df.count()}")
print(f"Bad records: {bad_df.count()}")

print("\n=== Good Records ===")
display(good_df)

print("\n=== Bad Records ===")
display(bad_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Custom Criticality Levels
# MAGIC
# MAGIC You can set default criticality for all implicit rules:
# MAGIC - `error`: Critical issues that should block data (default)
# MAGIC - `warn`: Warnings that log but don't block

# COMMAND ----------

# Generate rules with 'warn' criticality
warn_rules = generator.generate_rules_from_contract(
    contract_file=contract_file,
    default_criticality="warn"
)

print(f"Generated {len(warn_rules)} rules with 'warn' criticality")

# Apply warn rules
warn_result_df = engine.apply_checks_by_metadata(df, warn_rules)

# With 'warn' criticality, no records are filtered out
print(f"Result count: {warn_result_df.count()} (same as input: {df.count()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Using DataContract Object
# MAGIC
# MAGIC You can also pass a pre-loaded DataContract object instead of a file path.

# COMMAND ----------

from datacontract.data_contract import DataContract

# Load contract as object
dc = DataContract(data_contract_file=contract_file)

# Generate rules from object
rules_from_object = generator.generate_rules_from_contract(
    contract=dc,
    generate_implicit_rules=True,
    process_text_rules=False
)

print(f"Generated {len(rules_from_object)} rules from DataContract object")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Save and Load Generated Rules
# MAGIC
# MAGIC Rules can be saved to JSON and loaded later for reuse.

# COMMAND ----------

import json

# Get current username
current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]

# Save rules to workspace file
rules_path = f"/Workspace/Users/{current_user}/dqx_generated_rules.json"

with open(rules_path.replace("/Workspace", "/dbfs"), "w") as f:
    json.dump(rules, f, indent=2)

print(f"Saved {len(rules)} rules to {rules_path}")

# Load rules back
with open(rules_path.replace("/Workspace", "/dbfs"), "r") as f:
    loaded_rules = json.load(f)

print(f"Loaded {len(loaded_rules)} rules from file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC 1. ✅ Created a data contract with field constraints
# MAGIC 2. ✅ Generated DQX rules automatically from the contract
# MAGIC 3. ✅ Validated and applied rules to data
# MAGIC 4. ✅ Split good and bad records
# MAGIC 5. ✅ Customized criticality levels
# MAGIC 6. ✅ Saved and loaded generated rules
# MAGIC
# MAGIC ### Supported Field Constraints
# MAGIC
# MAGIC | Constraint | DQX Rule | Example |
# MAGIC |------------|----------|---------|
# MAGIC | `required: true` | `is_not_null` | All required fields |
# MAGIC | `unique: true` | `is_unique` | Primary keys |
# MAGIC | `pattern: regex` | `regex_match` | Email, phone formats |
# MAGIC | `minimum`/`maximum` | `is_in_range` | Age, amount limits |
# MAGIC | `enum: [...]` | `is_in_list` | Status codes |
# MAGIC | `format: date/time` | `is_valid_date`/`is_valid_timestamp` | Dates, timestamps |
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Store contracts in version control
# MAGIC - Integrate with CI/CD pipelines
# MAGIC - Add custom quality checks to contracts
# MAGIC - Monitor quality metrics over time

# COMMAND ----------

# Cleanup
import os
os.unlink(contract_file)
print("✅ Demo complete!")


