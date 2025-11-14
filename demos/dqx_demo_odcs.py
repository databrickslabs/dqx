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
# MAGIC ```python
# MAGIC %pip install databricks-labs-dqx pyyaml
# MAGIC dbutils.library.restartPython()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import yaml
from pyspark.sql import types as T
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine

# Initialize clients
w = WorkspaceClient()
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define an ODCS Data Contract
# MAGIC
# MAGIC Here's a sample ODCS v3.0.2 data contract for IoT sensor readings.
# MAGIC In practice, you'd load this from a YAML file or version control.

# COMMAND ----------

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

schema:
  type: table
  properties:
    sensor_id:
      logicalType: string
      description: Unique identifier for the sensor device
      required: true
      pattern: '^SENSOR-[A-Z]{2}-[0-9]{4}$'
      
    machine_id:
      logicalType: string
      description: Machine being monitored
      required: true
      quality:
        notNull: true
        notEmpty: true
        
    reading_timestamp:
      logicalType: timestamp
      description: When the sensor reading was recorded
      required: true
      format: yyyy-MM-dd HH:mm:ss
      
    temperature:
      logicalType: numeric
      description: Temperature reading in Celsius
      required: true
      minValue: -50.0
      maxValue: 150.0
      
    pressure:
      logicalType: numeric
      description: Pressure reading in PSI
      required: true
      minValue: 0.0
      maxValue: 500.0
      
    sensor_status:
      logicalType: string
      description: Operational status of the sensor
      required: true
      validValues:
        - active
        - inactive
        - maintenance
        - faulty
        
    location:
      logicalType: string
      description: Physical location of the sensor
      required: true
      quality:
        notNull: true
        notEmpty: true
"""

# Parse the YAML contract
contract = yaml.safe_load(odcs_contract_yaml)
print(f"📋 Loaded contract: {contract['name']} v{contract['version']}")
print(f"   Domain: {contract['domain']}")
print(f"   Properties: {len(contract['schema']['properties'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate DQX Rules from ODCS Contract
# MAGIC
# MAGIC The `DQGenerator` can automatically generate quality rules based on the contract's schema properties.

# COMMAND ----------

# Create generator
generator = DQGenerator(workspace_client=w, spark=spark)

# Generate rules from the contract
dqx_rules = generator.generate_rules_from_contract(
    contract=contract,
    format="odcs",  # Currently only ODCS v3.0.x is supported
    generate_implicit_rules=True,  # Generate rules from schema constraints
    process_text_rules=False,  # Skip text-based rules (requires LLM)
    default_criticality="error",  # Default severity level
)

print(f"\n✅ Generated {len(dqx_rules)} DQX quality rules")
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
engine = DQEngine(spark)
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
# MAGIC Save the generated rules to a file for later use or version control.

# COMMAND ----------

import json

# Save rules to JSON
rules_json = json.dumps(dqx_rules, indent=2)

# In practice, you'd save to a file or Delta table
print("💾 Generated rules (JSON format):")
print(rules_json[:500] + "..." if len(rules_json) > 500 else rules_json)

# You can also convert to YAML
import yaml
rules_yaml = yaml.dump(dqx_rules, default_flow_style=False, sort_keys=False)
print("\n💾 Generated rules (YAML format):")
print(rules_yaml[:500] + "..." if len(rules_yaml) > 500 else rules_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary and Best Practices
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
# MAGIC ### What's Not Supported (Yet)
# MAGIC
# MAGIC - ODCS Library format rules (use DQX custom format instead)
# MAGIC - SQL-based checks from ODCS (coming in future release)
# MAGIC - Dataset-level quality rules (e.g., row counts)
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

