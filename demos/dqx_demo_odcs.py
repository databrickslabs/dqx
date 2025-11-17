# Databricks notebook source
# MAGIC %md
# MAGIC # DQX ODCS Data Contract Integration Demo
# MAGIC
# MAGIC This notebook demonstrates how to use DQX with ODCS (Open Data Contract Standard) v3.0.x
# MAGIC data contracts to automatically generate and apply data quality rules.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC
# MAGIC 1. Loading an ODCS data contract from YAML
# MAGIC 2. Automatically generating DQX quality rules from the contract
# MAGIC 3. Understanding implicit vs explicit rule generation
# MAGIC 4. Applying generated rules to your data
# MAGIC 5. Viewing quality check results with contract metadata
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Run the following cell to install DQX and dependencies:

# COMMAND ----------

%pip install databricks-labs-dqx pyyaml

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import yaml
from pyspark.sql import types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine

# Initialize workspace client (spark is available as a built-in global)
ws = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load an ODCS Data Contract
# MAGIC
# MAGIC ODCS contracts can be loaded from YAML files, version control, or defined inline.
# MAGIC This example uses an inline ODCS v3.0.2 contract for IoT sensor readings.

# COMMAND ----------

# Example ODCS v3.0.2 contract (in practice, load from file or version control)
odcs_contract_yaml = """
apiVersion: v3.0.2
kind: DataContract

id: demo-sensor-data-001
name: iot_sensor_readings
version: 1.0.0
status: active
domain: manufacturing
dataProduct: sensor_analytics

description:
  purpose: Real-time sensor data from manufacturing floor
  usage: Use for equipment monitoring and predictive maintenance

# Schema is an ARRAY of schema objects (ODCS v3.0.x format)
schema:
  - name: sensor_readings_table
    type: table
    # Properties is an ARRAY of property objects
    properties:
      - name: sensor_id
        logicalType: string
        description: Unique identifier for the sensor device
        required: true
        pattern: '^SENSOR-[A-Z]{2}-[0-9]{4}$'
        
      - name: machine_id
        logicalType: string
        description: Machine being monitored
        required: true
        # Quality is an ARRAY of quality check objects using native DQX format
        quality:
          - type: custom
            engine: dqx
            implementation:
              criticality: error
              check:
                function: is_not_null_and_not_empty
                arguments:
                  column: machine_id
                  trim_strings: true
          
      - name: reading_timestamp
        logicalType: date  # ODCS uses 'date' for both dates and timestamps
        format: yyyy-MM-dd HH:mm:ss
        description: When the sensor reading was recorded
        required: true
        
      - name: temperature
        logicalType: number  # ODCS uses 'number' not 'numeric'
        description: Temperature reading in Celsius
        required: true
        minValue: -50.0
        maxValue: 150.0
        
      - name: pressure
        logicalType: number
        description: Pressure reading in PSI
        required: true
        minValue: 0.0
        maxValue: 500.0
        
      - name: sensor_status
        logicalType: string
        description: Operational status of the sensor
        required: true
        validValues:
          - active
          - inactive
          - maintenance
          - faulty
          
      - name: location
        logicalType: string
        description: Physical location of the sensor
        required: true
        quality:
          - type: custom
            engine: dqx
            implementation:
              criticality: error
              check:
                function: is_not_null_and_not_empty
                arguments:
                  column: location
                  trim_strings: true
"""

# Parse the YAML contract
contract = yaml.safe_load(odcs_contract_yaml)

# Alternative: Load from a file
# with open('/Workspace/Users/your_email/odcs_contract.yaml', 'r') as f:
#     contract = yaml.safe_load(f)

print(f"📋 Loaded contract: {contract['name']} v{contract['version']}")
print(f"   Domain: {contract['domain']}")
print(f"   Properties: {len(contract['schema'][0]['properties'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate DQX Rules from ODCS Contract
# MAGIC
# MAGIC The `DQGenerator` can automatically generate quality rules based on the contract's schema properties.

# COMMAND ----------

# Create generator
generator = DQGenerator(ws)

# Generate rules from the contract
dqx_rules = generator.generate_rules_from_contract(
    contract=contract,
    format="odcs",  # Currently only ODCS v3.0.x is supported
    generate_implicit_rules=True,  # Generate rules from schema constraints
    process_text_rules=False,  # Skip text-based rules (requires LLM)
    default_criticality="error",  # Default severity level
)

print(f"\n✅ Generated {len(dqx_rules)} DQX quality rules")

# Validate the generated rules
status = DQEngine.validate_checks(dqx_rules)
print(f"\n🔍 Validation: {'✅ All rules valid' if not status.has_errors else '❌ Errors found'}")
if status.has_errors:
    print("Validation errors:")
    for error in status.errors:
        print(f"  - {error}")

print("\nRule Functions Used:")
for rule in dqx_rules:
    func = rule['check']['function']
    col = rule['check']['arguments'].get('column', 'N/A')
    print(f"  - {func}: {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect Generated Rules
# MAGIC
# MAGIC Let's examine a few generated rules to understand what was created.

# COMMAND ----------

import json

print("📊 Sample Generated Rules:\n")

# Show first 3 rules with details
for i, rule in enumerate(dqx_rules[:3], 1):
    print(f"\nRule {i}: {rule['name']}")
    print(f"  Function: {rule['check']['function']}")
    print(f"  Arguments: {rule['check']['arguments']}")
    print(f"  Criticality: {rule['criticality']}")
    print(f"  Metadata:")
    for key, value in rule['user_metadata'].items():
        print(f"    - {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Sample Data
# MAGIC
# MAGIC Let's create a DataFrame with both valid and invalid data to demonstrate the rules.

# COMMAND ----------

# Define schema
schema = T.StructType([
    T.StructField("sensor_id", T.StringType(), True),
    T.StructField("machine_id", T.StringType(), True),
    T.StructField("reading_timestamp", T.TimestampType(), True),
    T.StructField("temperature", T.DoubleType(), True),
    T.StructField("pressure", T.DoubleType(), True),
    T.StructField("sensor_status", T.StringType(), True),
    T.StructField("location", T.StringType(), True),
])

# Create sample data (mix of valid and invalid)
data = [
    # Valid record
    ("SENSOR-MF-0001", "MACHINE-001", "2024-01-15 10:30:00", 75.5, 120.0, "active", "Building A"),
    
    # Invalid: temperature out of range
    ("SENSOR-MF-0002", "MACHINE-002", "2024-01-15 10:31:00", 200.0, 130.0, "active", "Building B"),
    
    # Invalid: invalid sensor_id pattern
    ("INVALID-ID", "MACHINE-003", "2024-01-15 10:32:00", 68.0, 115.0, "active", "Building C"),
    
    # Invalid: status not in valid values
    ("SENSOR-MF-0004", "MACHINE-004", "2024-01-15 10:33:00", 72.0, 125.0, "broken", "Building D"),
    
    # Invalid: null machine_id
    ("SENSOR-MF-0005", None, "2024-01-15 10:34:00", 70.0, 118.0, "active", "Building E"),
    
    # Valid record
    ("SENSOR-MF-0006", "MACHINE-006", "2024-01-15 10:35:00", 78.0, 122.0, "maintenance", "Building F"),
]

df = spark.createDataFrame(data, schema)

print(f"📊 Created DataFrame with {df.count()} records")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Apply Generated Rules to Data
# MAGIC
# MAGIC Now let's apply the generated quality rules to our DataFrame.

# COMMAND ----------

# Apply the generated rules
engine = DQEngine(WorkspaceClient())
result_df = engine.apply_checks_by_metadata(df, dqx_rules)

print(f"✅ Applied {len(dqx_rules)} quality rules")
print(f"📊 Result DataFrame has {result_df.count()} rows")

# Display results with quality check columns
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Split Valid and Invalid Data
# MAGIC
# MAGIC Separate the data into "good" (valid) and "bad" (invalid) DataFrames.

# COMMAND ----------

# Split into good and bad data
good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, dqx_rules)

print(f"✅ Good records: {good_df.count()}")
print(f"❌ Bad records: {bad_df.count()}")

print("\n📗 Valid Data:")
display(good_df)

print("\n📕 Invalid Data (with error details):")
display(bad_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analyze Quality Issues by Contract Metadata
# MAGIC
# MAGIC Each rule includes metadata about the ODCS contract, allowing us to trace quality issues back to the source.

# COMMAND ----------

from pyspark.sql.functions import col, explode, collect_list

# Get error columns
error_cols = [c for c in result_df.columns if c.startswith("_dqx_error_")]

if error_cols:
    # Analyze errors by contract property
    print("📊 Quality Issues by ODCS Property:\n")
    
    for error_col in error_cols:
        errors = result_df.filter(col(error_col).isNotNull()).select(error_col).collect()
        if errors:
            # Find the corresponding rule
            rule_name = error_col.replace("_dqx_error_", "")
            matching_rules = [r for r in dqx_rules if r.get('name') == rule_name]
            
            if matching_rules:
                rule = matching_rules[0]
                metadata = rule['user_metadata']
                
                print(f"  Property: {metadata.get('odcs_property', 'N/A')}")
                print(f"    - Rule: {rule['check']['function']}")
                print(f"    - Dimension: {metadata.get('odcs_dimension', 'N/A')}")
                print(f"    - Affected rows: {len(errors)}")
                print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Custom Criticality Level
# MAGIC
# MAGIC You can customize the default severity level for all generated implicit rules.

# COMMAND ----------

# Generate rules with custom criticality level
warn_rules = generator.generate_rules_from_contract(
    contract=contract,
    format="odcs",
    generate_implicit_rules=True,
    process_text_rules=False,
    default_criticality="warn",  # All implicit rules = warning
)

print(f"📊 Generated {len(warn_rules)} rules with 'warn' criticality")

# Show that all rules have the specified criticality
from collections import Counter
criticality_counts = Counter(r['criticality'] for r in warn_rules)
print(f"\nCriticality Distribution:")
for level, count in criticality_counts.items():
    print(f"  {level}: {count} rules")

# Note: To set different criticalities per rule, use explicit custom rules in the contract

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Save Generated Rules for Reuse
# MAGIC
# MAGIC Save the generated rules to a file or Delta table for later use or version control.

# COMMAND ----------

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig
import json

# Option 1: Preview rules as JSON
rules_json = json.dumps(dqx_rules, indent=2)
print("💾 Generated rules (JSON format preview):")
print(rules_json[:500] + "..." if len(rules_json) > 500 else rules_json)

# Option 2: Preview rules as YAML
rules_yaml = yaml.dump(dqx_rules, default_flow_style=False, sort_keys=False)
print("\n💾 Generated rules (YAML format preview):")
print(rules_yaml[:500] + "..." if len(rules_yaml) > 500 else rules_yaml)

# Option 3: Save to workspace file
user_name = spark.sql("select current_user() as user").collect()[0]["user"]
checks_file = f"/Workspace/Users/{user_name}/dqx_odcs_generated_rules.yml"

dq_engine = DQEngine(WorkspaceClient())
dq_engine.save_checks(
    checks=dqx_rules,
    config=WorkspaceFileChecksStorageConfig(location=checks_file)
)
print(f"\n✅ Saved generated rules to: {checks_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9b. Load and Reuse Saved Rules
# MAGIC
# MAGIC Demonstrate loading previously saved rules from a file.

# COMMAND ----------

# Load the saved rules
loaded_rules = dq_engine.load_checks(
    config=WorkspaceFileChecksStorageConfig(location=checks_file)
)

print(f"📥 Loaded {len(loaded_rules)} rules from: {checks_file}")

# Apply loaded rules to new data
new_data = [
    ("SENSOR-MF-0007", "MACHINE-007", "2024-01-15 11:00:00", 80.0, 125.0, "active", "Building G"),
]
new_df = spark.createDataFrame(new_data, schema)

result_df = dq_engine.apply_checks_by_metadata(new_df, loaded_rules)
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Advanced: Dataset-Level Custom Rules
# MAGIC
# MAGIC ODCS supports embedding DQX custom rules for dataset-level checks (e.g., data freshness, aggregate validation).

# COMMAND ----------

# Contract with dataset-level rules
contract_with_dataset_rules = yaml.safe_load("""
apiVersion: v3.0.2
kind: DataContract
id: demo-with-dataset-rules
name: sensor_data_with_freshness
version: 1.0.0
status: active

schema:
  - name: sensor_data
    properties:
      - name: sensor_id
        logicalType: string
        required: true
        
      - name: reading_timestamp
        logicalType: date
        format: yyyy-MM-dd HH:mm:ss
        required: true
        # Dataset-level freshness check using DQX custom format
        quality:
          - type: custom
            engine: dqx
            implementation:
              criticality: error
              name: data_freshness_check
              check:
                function: is_data_fresh_per_time_window
                arguments:
                  column: reading_timestamp
                  window_minutes: 60
                  min_records_per_window: 1
                  lookback_windows: 24
              user_metadata:
                dimension: timeliness
                description: Ensure data is updated at least once per hour
                
      - name: reading_value
        logicalType: number
        required: true
        minValue: 0.0
        maxValue: 1000.0
""")

# Generate rules including dataset-level checks
dataset_rules = generator.generate_rules_from_contract(
    contract=contract_with_dataset_rules,
    generate_implicit_rules=True,
    process_text_rules=False
)

print(f"✅ Generated {len(dataset_rules)} rules (including dataset-level)")
print("\nDataset-Level Rules:")
for rule in dataset_rules:
    if rule['check']['function'] in ['is_data_fresh_per_time_window', 'is_aggr_not_greater_than']:
        print(f"  - {rule['name']}: {rule['check']['function']}")
        print(f"    Dimension: {rule['user_metadata'].get('dimension', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Automatic Rule Generation**: ODCS contracts enable automatic DQX rule generation from schema constraints
# MAGIC 2. **Contract Lineage**: Every rule includes metadata linking it back to the source contract
# MAGIC 3. **Flexible Configuration**: Control rule generation with flags and criticality mappings
# MAGIC 4. **Validation Integration**: Generated rules work seamlessly with DQX validation workflows
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC 1. **Version Control**: Store ODCS contracts and generated rules in version control
# MAGIC 2. **Contract Evolution**: Update contracts and regenerate rules when schema changes
# MAGIC 3. **Metadata Tracking**: Use contract metadata to trace quality issues to their source
# MAGIC 4. **Criticality Tuning**: Adjust criticality mappings based on business requirements
# MAGIC 5. **Incremental Adoption**: Start with implicit rules, add explicit rules as needed
# MAGIC
# MAGIC ### What's Supported
# MAGIC
# MAGIC - ✅ Implicit rules from schema properties (required, unique, pattern, range, validValues, format)
# MAGIC - ✅ Explicit custom rules using DQX native format (including dataset-level checks)
# MAGIC - ✅ Full ODCS v3.0.x compliance with JSON Schema validation
# MAGIC - ✅ Contract metadata and lineage tracking
# MAGIC
# MAGIC ### What's Not Supported (Yet)
# MAGIC
# MAGIC - Text-based quality expectations with LLM (optional, requires LLM setup)
# MAGIC - ODCS Library format rules (use DQX custom format instead)
# MAGIC
# MAGIC ### Learn More
# MAGIC
# MAGIC - ODCS Specification: https://bitol-io.github.io/open-data-contract-standard/v3.0.2/
# MAGIC - DQX Documentation: https://databrickslabs.github.io/dqx/
# MAGIC - Sample Contracts: See `demos/sample_odcs_contract.yaml`

# COMMAND ----------

print("✅ Demo Complete!")
print("\n📚 Next Steps:")
print("  1. Try with your own ODCS contracts")
print("  2. Integrate into your data pipelines")
print("  3. Track quality metrics over time")
print("  4. Use contract metadata for governance reporting")

