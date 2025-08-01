# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX for PII Detection
# MAGIC Increased regulation makes Databricks customers responsible for any Personally Identifiable Information (PII) stored in Unity Catalog. While [Lakehouse Monitoring](https://docs.databricks.com/aws/en/lakehouse-monitoring/data-classification#discover-sensitive-data) can identify sensitive data in-place, many customers need to proactively quarantine or anonymize PII before writing the data to Delta.
# MAGIC
# MAGIC [Databricks Labs' DQX project](https://databrickslabs.github.io/dqx/) provides in-flight data quality monitoring for Spark `DataFrames`. Customers can apply checks, get row-level metadata, and quarantine failing records. Workloads can use DQX's built-in checks or custom user-defined functions.
# MAGIC
# MAGIC In this notebook, we'll use DQX with a custom function to detect PII in JSON strings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC This notebook uses [Presidio](https://microsoft.github.io/presidio/) to detect PII in strings. To run this notebook:
# MAGIC - Use DBR 15.4LTS
# MAGIC - Install [SpaCy](https://spacy.io/usage/models#download) as a cluster-scoped library

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%pip install presidio_analyzer numpy==1.23.5

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import pandas as pd

from pyspark.sql.functions import concat_ws, col, lit, pandas_udf
from pyspark.sql import Column
from presidio_analyzer import AnalyzerEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import make_condition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the Presidio analyzer
# MAGIC First, we'll use Presidio's `AnalyzerEngine` to define a function that checks values for PII. For any PII detected, the `entity_mapping` will contain the type of PII identified and a confidence score.

# COMMAND ----------

# Create the Presidio analyzer:
analyzer = AnalyzerEngine()

# Get the list of entities to download the model:
entities = analyzer.get_supported_entities()

# Create a wrapper function to generate the entity mapping results:
def get_entity_mapping(data: str) -> str | None:
  if data:
    # Run the Presidio analyzer to detect PII in the string:
    results = analyzer.analyze(
      text=data,
      entities=["PERSON", "EMAIL_ADDRESS"],
      language='en',
      score_threshold=0.5,
    )
    if results != []:
      output = []
      # Validate and return the results:
      for result in results:
        # Ignore if the result is low confidence:
        if result.score < 0.5:
          continue
        # Append the result to the output:
        output.append({
          "entity_type": result.entity_type,
          "start": int(result.start),
          "end": int(result.end),
          "score": float(result.score),
        })
      if output != []:
        # Return the results as JSON:
        return json.dumps(output)
  return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Pandas UDF
# MAGIC We can call `get_entity_mapping` on DataFrame rows using a Pandas user-defined function. This provides good performance with batched execution over the arriving records.

# COMMAND ----------

# Register a pandas UDF to run the analyzer:
@pandas_udf('string')
def contains_pii(batch: pd.Series) -> pd.Series:
    # Apply `get_entity_mapping` to each value:
    return batch.map(get_entity_mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Making and applying a DQX condition
# MAGIC Once our Presidio algorithm can be called as a Spark UDF, we can use DQX's `make_condition` to implement a custom check that identifies PII and generates row-level metadata about the `json` keys and types of PII identified.

# COMMAND ----------

def does_not_contain_pii(column: str) -> Column:
  # Define a PII detection expression calling the pandas UDF:
  pii_info = contains_pii(col(column))

  # Return the DQX condition that uses the PII detection expression:
  return make_condition(
    pii_info.isNotNull(),
    concat_ws(
      ' ',
      lit(column),
      lit('contains pii with the following info:'),
      pii_info
    ),
    f'{column}_contains_pii'
  )

# Define the DQX rule:
checks = [
  DQRowRule(criticality='error', check_func=does_not_contain_pii, column='val')
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