# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX for IPV6 Address Validation
# MAGIC With the rapid adoption of IPv6, Databricks customers increasingly rely on accurate network metadata for security, compliance, and analytics use cases. Companies need to validate IPv6 addresses in-flight and at-rest to ensure data integrity, support fraud detection, maintain compliance, and prevent downstream errors. Proactive IPv6 validation enables organizations to quarantine or correct malformed addresses before persisting them in Unity Catalog or downstream analytical systems.
# MAGIC
# MAGIC DQX provides in-flight data quality monitoring for Spark `DataFrames`. You can apply checks, get row-level metadata, and quarantine failing records. Workloads can also use DQX's built-in functions to check `DataFrames` for IPV6 addresses.

# COMMAND ----------

# MAGIC %md
# MAGIC IPv6 validation is powered by Python’s built-in `ipaddress` library, ensuring that workloads can reliably detect malformed addresses before persisting them. To install DQX, run:

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.ipaddress.ipaddress_funcs import is_valid_ipv6_address, is_ipv6_address_in_cidr

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecting IPV6 address with DQX
# MAGIC DQX supports built-in IPV6 address detection using the `ipaddress` library to define a function that checks values for valid IPV6 addresses. For any invalid addresses detected, the `entity_mapping` will contain the type of error identified and a confidence score.

# COMMAND ----------

# Define the DQX rule for IPV6 address validation:
checks = [
  DQRowRule(
    criticality="error",
    check_func=is_valid_ipv6_address,
    column="val",
    name="is_valid_ipv6_address",
  )
]

# Initialize the DQX engine:
dq_engine = DQEngine(WorkspaceClient())

# Create some sample data:
data = [
  ["2001:0db8:85a3:08d3:0000:0000:0000:0001"],
  ["2001:0db8:85a3:08d3:0000:0000:0000:1"],
  ["2001:0db8:85a3:08d3:0000::2"],
  ["fe80::1234:5678:9abc:def0"],
  ["::1"],
  ["2001:0db8:85a3:1234:0000:0000:0000:abcd"],
  ["2001:db8:85a3:8d3:ffff:ffff:ffff:ffff"],
  ["this-is-not-an-ipv6-address"],
  [None],
]
df = spark.createDataFrame(data, "val string")

# Run the checks and display the output:
checked_df = dq_engine.apply_checks(df, checks)
display(checked_df)


# COMMAND ----------

# Define the DQX rule for IPV6 address in CIDR block validation:
checks = [
  DQRowRule(
    criticality="error",
    check_func=is_ipv6_address_in_cidr,
    column="val",
    check_func_kwargs={
      "cidr_block": "2001:0db8:85a3:08d3:0000:0000:0000:0000/64",
    },
    name="is_ipv6_address_in_cidr",
  )
]

# Initialize the DQX engine:
dq_engine = DQEngine(WorkspaceClient())

# Create some sample data:
data = [
  ["2001:0db8:85a3:08d3:0000:0000:0000:0001"],
  ["2001:0db8:85a3:08d3:0000:0000:0000:1"],
  ["2001:0db8:85a3:08d3:0000::2"],
  ["this-is-not-an-ipv6-address"],
  ["10.9.2.1.0"],
  ["2001:0db8:85a3:1234:0000:0000:0000:abcd"],
  ["2001:0db8:ffff:ffff:1111:2222:3333:4444"],
  [None],
]
df = spark.createDataFrame(data, "val string")

# Run the checks and display the output:
checked_df = dq_engine.apply_checks(df, checks)
display(checked_df)
