# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Data Contract Integration Demo (ODCS v3.x)
# MAGIC
# MAGIC This notebook demonstrates how to use DQX with Open Data Contract Standard (ODCS) v3.x
# MAGIC contracts to automatically generate and apply data quality rules. DQX natively processes
# MAGIC ODCS v3.x contracts, extracting constraints from `logicalTypeOptions` and quality sections.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC
# MAGIC 1. Creating ODCS v3.x data contracts
# MAGIC 2. Automatically generating DQX quality rules from property constraints
# MAGIC 3. Understanding predefined vs explicit rule generation
# MAGIC 4. Applying generated rules to your data
# MAGIC 5. Using AI-assisted rule generation from text expectations
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Run the following cell to install DQX with data contract support:

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[datacontract,llm] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[datacontract,llm]

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import os
import tempfile
import yaml
from datetime import date
from decimal import Decimal
from pyspark.sql import types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine

ws = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a Data Contract
# MAGIC
# MAGIC Data contracts define your data schema, constraints, and quality expectations.
# MAGIC This example uses a simple e-commerce orders contract.

# COMMAND ----------

# Example data contract (ODCS v3.x format)
contract_yaml = """
kind: DataContract
apiVersion: v3.0.2
id: urn:datacontract:ecommerce:orders
name: E-Commerce Orders
version: 1.0.0
status: active
domain: ecommerce
dataProduct: orders_data_product
tenant: Data Engineering Team

description:
  purpose: Customer order data for e-commerce platform
  usage: Demonstrate DQX rule generation from ODCS v3.x contracts

tags:
  - ecommerce
  - orders
  - demo

schema:
  - name: orders
    physicalName: orders_table
    physicalType: table
    description: Customer orders table
    
    properties:
      - name: order_id
        logicalType: string
        physicalType: varchar(12)
        description: Unique order identifier
        required: true
        unique: true
        primaryKey: true
        logicalTypeOptions:
          pattern: '^ORD-[0-9]{8}$'
      
      - name: customer_email
        logicalType: string
        physicalType: varchar(100)
        description: Customer email address
        required: true
        logicalTypeOptions:
          pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
      
      - name: order_date
        logicalType: date
        physicalType: date
        description: Order placement date
        required: true
        logicalTypeOptions:
          format: 'yyyy-MM-dd'
      
      - name: order_total
        logicalType: number
        physicalType: decimal(10,2)
        description: Total order amount
        required: true
        logicalTypeOptions:
          minimum: 0.01
          maximum: 100000.00
        quality:
          # Field-level quality check (explicit DQX rule)
          - type: custom
            engine: dqx
            description: Warn on unusually high order totals
            implementation:
              criticality: warn
              name: order_total_reasonable_check
              check:
                function: is_not_greater_than
                arguments:
                  column: order_total
                  limit: 50000
      
      - name: order_status
        logicalType: string
        physicalType: varchar(20)
        description: Current order status
        required: true
        logicalTypeOptions:
          pattern: '^(pending|confirmed|shipped|delivered|cancelled)$'
      
      - name: quantity
        logicalType: integer
        physicalType: int
        description: Number of items
        required: true
        logicalTypeOptions:
          minimum: 1
          maximum: 1000
      
      - name: discount_percentage
        logicalType: number
        physicalType: decimal(5,2)
        description: Discount applied
        required: false
        logicalTypeOptions:
          minimum: 0.0
          maximum: 100.0
    
    # Dataset-level quality checks (explicit DQX rules and text-based expectations)
    quality:
      # Example 1: Custom DQX rule - Check dataset is not empty
      - type: custom
        engine: dqx
        description: Ensure orders dataset contains data
        implementation:
          criticality: error
          name: orders_dataset_not_empty
          check:
            function: is_aggr_not_less_than
            arguments:
              column: order_id
              limit: 1
              aggr_type: count
      
      # Example 2: Text-based expectation (AI-assisted rule generation)
      - type: text
        description: "For rows where order_total > 10000, the order_status column must be either 'confirmed' or 'shipped'"
      
      # Example 3: Another text-based expectation for cross-field validation
      - type: text
        description: "For rows where discount_percentage > 50, the discount_percentage must be less than or equal to 75"
"""

# Parse contract and write to temporary file
contract = yaml.safe_load(contract_yaml)
print(f"Contract: {contract['name']} v{contract['version']}")

with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
    yaml.dump(contract, f)
    contract_file = f.name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate DQX Rules from Contract
# MAGIC
# MAGIC The ODCS v3.x contract above demonstrates three types of quality rules:
# MAGIC
# MAGIC ### 1. Predefined Rules (from property constraints in logicalTypeOptions)
# MAGIC - `required: true` → `is_not_null`
# MAGIC - `unique: true` → `is_unique`
# MAGIC - `pattern: regex` → `regex_match`
# MAGIC - `minimum`/`maximum` → `is_in_range` or `sql_expression` (for floats)
# MAGIC - `minLength`/`maxLength` → `sql_expression` for LENGTH checks
# MAGIC - `format` → `is_valid_date` or `is_valid_timestamp` for date/timestamp fields
# MAGIC
# MAGIC ### 2. Explicit DQX Rules (type: custom, engine: dqx, implementation: {...})
# MAGIC - Dataset-level: `is_aggr_not_less_than` to check dataset is not empty
# MAGIC - Property-level: `is_not_greater_than` for warning on high order totals
# MAGIC
# MAGIC ### 3. Text-based Rules (type: text - AI-assisted via LLM)
# MAGIC - Natural language business logic converted to executable rules by AI
# MAGIC - Useful for complex conditional logic and cross-field validations
# MAGIC - Example: "For rows where order_total > 10000, order_status must be 'confirmed' or 'shipped'"
# MAGIC - Example: "For rows where discount_percentage > 50, discount must not exceed 75"

# COMMAND ----------

# Generate DQX rules (including AI-assisted text rules)
generator = DQGenerator(workspace_client=ws)
rules = generator.generate_rules_from_contract(
    contract_file=contract_file,
    process_text_rules=True,  # Enable AI-assisted rule generation from text expectations
    default_criticality="error"  # or "warn" for warnings instead of errors
)

print(f"Generated {len(rules)} quality rules from contract")

# Show rule breakdown by type
predefined_count = len([r for r in rules if r.get("user_metadata", {}).get("rule_type") == "predefined"])
explicit_count = len([r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"])
text_llm_count = len([r for r in rules if r.get("user_metadata", {}).get("rule_type") == "text_llm"])

print(f"  - {predefined_count} predefined rules (from property constraints)")
print(f"  - {explicit_count} explicit DQX rules")
print(f"  - {text_llm_count} AI-generated rules (from text expectations)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. View Generated Rules
# MAGIC
# MAGIC Each generated rule includes:
# MAGIC - Check function and arguments
# MAGIC - Criticality level
# MAGIC - Contract metadata (contract ID, version, schema, field)
# MAGIC - Rule type (predefined, explicit, or text_llm)

# COMMAND ----------

# Display sample rules
import json

print("========== Generated Rules (first 3) ==========")
print(json.dumps(rules[:3], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Sample Data
# MAGIC
# MAGIC Let's create sample data that matches the contract schema.
# MAGIC We'll include both valid and invalid records to demonstrate rule application.

# COMMAND ----------

from datetime import date
from decimal import Decimal
from pyspark.sql import types as T

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
    ("ORD-12345678", "customer@example.com", date(2024, 1, 15), Decimal("99.99"), "confirmed", 2, Decimal("10.0")),
    # Valid record
    ("ORD-87654321", "another@test.com", date(2024, 1, 16), Decimal("150.50"), "shipped", 1, Decimal("5.0")),
    # Invalid: null required field
    (None, "test@example.com", date(2024, 1, 17), Decimal("75.00"), "pending", 1, Decimal("0.0")),
    # Invalid: pattern mismatch
    ("INVALID", "bad@example.com", date(2024, 1, 18), Decimal("200.00"), "delivered", 3, Decimal("15.0")),
    # Invalid: out of range
    ("ORD-99999999", "valid@test.com", date(2024, 1, 19), Decimal("150000.00"), "confirmed", 2, Decimal("10.0")),
    # Invalid: status not in enum
    ("ORD-11111111", "test@mail.com", date(2024, 1, 20), Decimal("50.00"), "unknown", 1, Decimal("5.0")),
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
# MAGIC ## 8. Using DataContract Object
# MAGIC
# MAGIC You can also pass a pre-loaded DataContract object instead of a file path.

# COMMAND ----------

from datacontract.data_contract import DataContract

# Load contract as object
dc = DataContract(data_contract_file=contract_file)

# Generate rules from object
rules_from_object = generator.generate_rules_from_contract(
    contract=dc,
    process_text_rules=False
)

print(f"Generated {len(rules_from_object)} rules from DataContract object")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Save and Load Generated Rules
# MAGIC
# MAGIC Rules can be saved and loaded using DQEngine's built-in methods.

# COMMAND ----------

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig

# Get current username
current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]

# Save rules to workspace file
rules_path = f"/Workspace/Users/{current_user}/dqx_generated_rules.yml"
storage_config = WorkspaceFileChecksStorageConfig(location=rules_path)

engine.save_checks(rules, storage_config)
print(f"Saved {len(rules)} rules to {rules_path}")

# Load rules back
loaded_rules = engine.load_checks(storage_config)
print(f"Loaded {len(loaded_rules)} rules from file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC 1. ✅ Created an ODCS v3.x data contract with field constraints and text-based expectations
# MAGIC 2. ✅ Generated DQX rules automatically from the contract (predefined, explicit, and AI-assisted)
# MAGIC 3. ✅ Validated and applied rules to data
# MAGIC 4. ✅ Split good and bad records for quarantine workflows
# MAGIC 5. ✅ Saved and loaded generated rules for reuse
# MAGIC
# MAGIC ### Supported ODCS v3.x Property Constraints
# MAGIC
# MAGIC | Constraint | Location | DQX Rule | Example |
# MAGIC |------------|----------|----------|---------|
# MAGIC | `required: true` | Direct property attribute | `is_not_null` | All required properties |
# MAGIC | `unique: true` | Direct property attribute | `is_unique` | Primary keys |
# MAGIC | `pattern` | `logicalTypeOptions` | `regex_match` | Email, phone formats |
# MAGIC | `minimum`/`maximum` | `logicalTypeOptions` | `is_in_range` or `sql_expression` | Age, amount limits |
# MAGIC | `minLength`/`maxLength` | `logicalTypeOptions` | `sql_expression` | String length validation |
# MAGIC | `format` | `logicalTypeOptions` | `is_valid_date`, `is_valid_timestamp` | Date/timestamp formats |
# MAGIC
# MAGIC ### Three Types of Rules
# MAGIC
# MAGIC | Rule Type | Source | Example |
# MAGIC |-----------|--------|---------|
# MAGIC | **Predefined** | Property constraints (required, unique, logicalTypeOptions) | `is_not_null`, `is_unique`, `regex_match` |
# MAGIC | **Explicit DQX** | Custom DQX rules in quality section with `implementation` | `is_aggr_not_less_than`, `is_not_greater_than` |
# MAGIC | **Text-based (LLM)** | Natural language expectations (`type: text`) | Complex business logic converted by AI |
# MAGIC
# MAGIC ### How AI-Assisted Rule Generation Works
# MAGIC
# MAGIC 1. **Text Expectations**: Define business rules in natural language in your contract (`type: text`)
# MAGIC 2. **LLM Processing**: DQX uses an LLM to understand the intent and convert to executable rules
# MAGIC 3. **Validation**: Generated rules are validated for correctness
# MAGIC 4. **Metadata**: Rules are tagged with `rule_type: "text_llm"` for tracking
# MAGIC
# MAGIC **When to Use Text Rules:**
# MAGIC - Complex business logic that's hard to express as simple constraints
# MAGIC - Conditional rules (e.g., "if X then Y")
# MAGIC - Cross-field validations
# MAGIC - Domain-specific rules that benefit from natural language
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Store contracts in version control
# MAGIC - Integrate with CI/CD pipelines
# MAGIC - Add custom quality checks to contracts
# MAGIC - Monitor quality metrics over time
# MAGIC - Explore more complex text-based expectations for AI-assisted rule generation

# COMMAND ----------

# Cleanup
os.unlink(contract_file)
print("✅ Demo complete!")


