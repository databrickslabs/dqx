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
                  limit: 50000.00
      
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
    
    # Dataset-level quality checks (explicit DQX rules)
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
      
      # Example 2: Text-based expectation (requires LLM processing)
      # Uncomment to use with process_text_rules=True
      # - type: text
      #   description: "Orders with total amount over $10000 must have order_status of 'confirmed' or 'shipped'"
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
# MAGIC
# MAGIC ### 2. Explicit DQX Rules (type: custom, engine: dqx, implementation: {...})
# MAGIC - Dataset-level: `is_aggr_not_less_than` to check dataset is not empty
# MAGIC - Property-level: `is_not_greater_than` for warning on high order totals
# MAGIC
# MAGIC ### 3. Text-based Rules (type: text - requires LLM)
# MAGIC - Natural language business logic converted to executable rules
# MAGIC - Useful for complex conditional logic and cross-field validations

# COMMAND ----------

# Generate DQX rules
generator = DQGenerator(workspace_client=ws)
rules = generator.generate_rules_from_contract(
    contract_file=contract_file,
    process_text_rules=False,
    default_criticality="error"  # or "warn" for warnings instead of errors
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

print(json.dumps(rules[:5], indent=2))

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
# MAGIC ## 10. AI-Assisted Rule Generation from Text Expectations
# MAGIC
# MAGIC DQX can automatically convert text-based expectations into executable rules using LLM.
# MAGIC This is useful when business rules are too complex to express as simple constraints.
# MAGIC
# MAGIC **Prerequisites:** Requires DQX with LLM extras: `pip install databricks-labs-dqx[llm]`

# COMMAND ----------

# Create a contract with text-based quality expectations (ODCS v3.x)
contract_with_text = {
    "kind": "DataContract",
    "apiVersion": "v3.0.2",
    "id": "orders-with-text",
    "name": "Orders with Text Rules",
    "version": "1.0.0",
    "status": "active",
    "domain": "ecommerce",
    "dataProduct": "orders_with_text",
    "schema": [
        {
            "name": "orders",
            "physicalType": "table",
            "description": "Order data with business logic rules",
            "properties": [
                {"name": "order_id", "logicalType": "string", "required": True},
                {"name": "customer_id", "logicalType": "string", "required": True},
                {"name": "order_amount", "logicalType": "number", "required": True},
                {"name": "approval_id", "logicalType": "string", "required": False},
                {"name": "order_date", "logicalType": "date", "required": True}
            ],
            "quality": [
                {
                    "type": "text",
                    "description": "Orders over $1000 must have a manager approval_id. Orders under $1000 can have null approval_id."
                },
                {
                    "type": "text",
                    "description": "Customer orders should not have duplicate order_id for the same customer_id within the same day."
                }
            ]
        }
    ]
}

# Save contract to file
contract_text_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False).name
with open(contract_text_file, 'w') as f:
    yaml.dump(contract_with_text, f)

# COMMAND ----------

# Generate rules with text processing enabled
# This will use LLM to convert text expectations into executable DQX rules
rules_with_llm = generator.generate_rules_from_contract(
    contract_file=contract_text_file,
    process_text_rules=True  # Enable LLM-based text rule processing
)

print(f"Generated {len(rules_with_llm)} total rules")

# COMMAND ----------

# Examine the text-based rules generated by LLM
text_llm_rules = [r for r in rules_with_llm if r["user_metadata"]["rule_type"] == "text_llm"]
predefined_rules = [r for r in rules_with_llm if r["user_metadata"]["rule_type"] == "predefined"]

print(f"\n📝 Predefined rules (from field constraints): {len(predefined_rules)}")
print(f"🤖 Text-based rules (generated by LLM): {len(text_llm_rules)}")

print("\n=== LLM-Generated Rules ===")
for rule in text_llm_rules:
    print(f"\nRule: {rule['check']['function']}")
    print(f"Arguments: {rule['check']['arguments']}")
    print(f"Filter: {rule.get('filter', 'None')}")
    print(f"Criticality: {rule['criticality']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Text Rule Generation Works
# MAGIC
# MAGIC 1. **Text Expectations**: Define business rules in natural language in your contract
# MAGIC 2. **LLM Processing**: DQX uses an LLM to understand the intent and convert to executable rules
# MAGIC 3. **Validation**: Generated rules are validated for correctness
# MAGIC 4. **Metadata**: Rules are tagged with `rule_type: "text_llm"` for tracking
# MAGIC
# MAGIC ### When to Use Text Rules
# MAGIC
# MAGIC - ✅ Complex business logic that's hard to express as simple constraints
# MAGIC - ✅ Conditional rules (e.g., "if X then Y")
# MAGIC - ✅ Cross-field validations
# MAGIC - ✅ Domain-specific rules that benefit from natural language
# MAGIC
# MAGIC For more details, see: [AI-Assisted Quality Checks Generation](https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_quality_checks_generation/)

# COMMAND ----------

# Cleanup text contract file
os.unlink(contract_text_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Save and Load Generated Rules
# MAGIC
# MAGIC Rules can be saved and loaded using DQEngine's built-in methods.

# COMMAND ----------

# Get current username
current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]

# Save rules to workspace file
rules_path = f"/Workspace/Users/{current_user}/dqx_generated_rules.yml"

engine.save_checks(rules, rules_path)
print(f"Saved {len(rules)} rules to {rules_path}")

# Load rules back
loaded_rules = engine.load_checks(rules_path)
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
# MAGIC 5. ✅ Saved and loaded generated rules
# MAGIC 6. ✅ Explored AI-assisted rule generation from text
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
# MAGIC | `format` | `logicalTypeOptions` | Logged (not yet implemented) | Date/timestamp formats |
# MAGIC
# MAGIC ### Three Types of Rules
# MAGIC
# MAGIC | Rule Type | Source | Example |
# MAGIC |-----------|--------|---------|
# MAGIC | **Predefined** | Property constraints (required, unique, logicalTypeOptions) | `is_not_null`, `is_unique`, `regex_match` |
# MAGIC | **Explicit DQX** | Custom DQX rules in quality section with `implementation` | `is_data_fresh_per_time_window`, custom checks |
# MAGIC | **Text-based (LLM)** | Natural language expectations (`type: text`) | Complex business logic converted by AI |
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Store contracts in version control
# MAGIC - Integrate with CI/CD pipelines
# MAGIC - Add custom quality checks to contracts
# MAGIC - Explore AI-assisted rule generation
# MAGIC - Monitor quality metrics over time

# COMMAND ----------

# Cleanup
import os
os.unlink(contract_file)
print("✅ Demo complete!")


