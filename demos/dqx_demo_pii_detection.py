# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX for PII Detection
# MAGIC Increased regulation makes Databricks customers responsible for any Personally Identifiable Information (PII) stored in Unity Catalog. While [Lakehouse Monitoring](https://docs.databricks.com/aws/en/lakehouse-monitoring/data-classification#discover-sensitive-data) can identify sensitive data in-place, many customers need to proactively quarantine or anonymize PII before writing the data to Delta.
# MAGIC
# MAGIC [Databricks Labs' DQX project](https://databrickslabs.github.io/dqx/) provides in-flight data quality monitoring for Spark `DataFrames`. Customers can apply checks, get row-level metadata, and quarantine failing records. Workloads can use DQX's built-in checks or custom user-defined functions.
# MAGIC
# MAGIC In this notebook, we'll use DQX with a custom function to detect PII in JSON strings.

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[pii]

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.pii.nlp_engine_config import NLPEngineConfig
from databricks.labs.dqx.pii import contains_pii

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII Detection with DQX
# MAGIC DQX supports built-in PII detection using Presidio's `AnalyzerEngine` to define a function that checks values for PII. For any PII detected, the `entity_mapping` will contain the type of PII identified and a confidence score.

# COMMAND ----------

# Use a built-in NLP configuration for detecting PII:
nlp_engine_config = NLPEngineConfig.SPACY_MEDIUM

# Define the DQX rule:
checks = [
  DQRowRule(
    criticality='error',
    check_func=contains_pii,
    column='val',
    check_func_kwargs={"nlp_engine_config": nlp_engine_config}
  )
]

# Initialize the DQX engine:
dq_engine = DQEngine(WorkspaceClient())

# Create some sample data:
data = [
  ['My name is John Smith'],
  ['The sky is blue, road runner'],
  ['Jane Smith sent an email to sara@info.com']
]
df = spark.createDataFrame(data, 'val string')

# Run the checks and display the output:
checked_df = dq_engine.apply_checks(df, checks)
display(checked_df)
