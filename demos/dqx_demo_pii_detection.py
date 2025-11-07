# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX for PII Detection
# MAGIC Increased regulation makes Databricks customers responsible for any Personally Identifiable Information (PII) stored in Unity Catalog. Companies need to be able to perform PII detection for data at-rest and in-transit to proactively quarantine or anonymize PII before persisting the data.
# MAGIC
# MAGIC DQX provides in-transit data quality monitoring for Spark `DataFrames`. You can apply checks, get row-level metadata, and quarantine failing records. Workloads can also use DQX's built-in functions to check `DataFrames` for PII.

# COMMAND ----------

# MAGIC %md
# MAGIC # Install DQX with PII extras
# MAGIC
# MAGIC To enable PII detection quality checking, DQX has to be installed with `pii` extras: 
# MAGIC
# MAGIC `%pip install databricks-labs-dqx[pii]`

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[pii] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[pii]

# All NLP model dependencies must be pre-installed for use with DQX's built-in PII detection checks.
# By default, DQX uses spaCy's small English model, which is installed automatically when running PII checks.
# Other spaCy models are also auto-installed if missing. However, due to Databricks Connect memory limitations,
# it is recommended to pre-install them via pip before execution to avoid OOM issues.
# It is generally more efficient to pre-install the models you need rather than relying on DQX to install them at runtime.

# Installing spaCy's small English model (default for DQX PII checks):
%pip install "en_core_web_sm @ https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl"
# Installing spaCy's medium English model:
%pip install "en_core_web_md @ https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.8.0/en_core_web_md-3.8.0-py3-none-any.whl"
# Installing spaCy's large English model (not used in this demo):
#%pip install "en_core_web_lg @ https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.pii.nlp_engine_config import NLPEngineConfig
from databricks.labs.dqx.pii.pii_detection_funcs import does_not_contain_pii

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecting PII with DQX
# MAGIC DQX supports built-in PII detection using Presidio's `AnalyzerEngine` to define a function that checks values for PII. For any PII detected, the `entity_mapping` will contain the type of PII identified and a confidence score.

# COMMAND ----------

# Define the DQX rule with default nlp model:
checks = [
  DQRowRule(
    criticality="error",
    check_func=does_not_contain_pii,
    column="val",
    name="does_not_contain_pii",
  )
]

# Initialize the DQX engine:
dq_engine = DQEngine(WorkspaceClient())

# Create some sample data:
data = [
  ["My name is John Smith"],
  ["The sky is blue, road runner"],
  ["Jane Smith sent an email to sara@info.com"],
  [None],
]
df = spark.createDataFrame(data, "val string")

# Run the checks and display the output:
checked_df = dq_engine.apply_checks(df, checks)
display(checked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuring the PII detection settings
# MAGIC DQX supports several configurable settings which control PII detection:
# MAGIC - `threshold` controls the specificity of the PII detection (higher values give more specificity with less sensitivity)
# MAGIC - `entities` specifies which [entity types](https://microsoft.github.io/presidio/supported_entities/) are marked as PII
# MAGIC - `language` sets the detection language
# MAGIC - `nlp_engine_config` sets various properties of the Presidio analyzer's [named entity recognition model](https://microsoft.github.io/presidio/samples/python/ner_model_configuration/)

# COMMAND ----------

checks = [
  # Define a PII check with a lower threshold (more sensitivity):
  DQRowRule(
    criticality="error",
    check_func=does_not_contain_pii,
    check_func_kwargs={"threshold": 0.5},
    column="val",
    name="does_not_contain_pii_lower_threshold",
  ),
  # Define a PII check with a subset of named entities:
  DQRowRule(
    criticality="error",
    check_func=does_not_contain_pii,
    check_func_kwargs={
      "entities": ["EMAIL_ADDRESS"],
    },
    column="val",
    name="contains_email_address_data",
  ),
  # Define a PII check with a built-in named-entity recognizer (SpaCy medium):
  DQRowRule(
    criticality="error",
    check_func=does_not_contain_pii,
    check_func_kwargs={
      "entities": ["PERSON", "LOCATION"],
      "nlp_engine_config": NLPEngineConfig.SPACY_MEDIUM
    },
    column="val",
    name="contains_person_or_address_data",
  ),
  # Define a PII check with a built-in named-entity recognizer (SpaCy medium):
  DQRowRule(
    criticality="error",
    check_func=does_not_contain_pii,
    check_func_kwargs={
      "entities": ["PERSON", "LOCATION"],
      "nlp_engine_config": NLPEngineConfig.SPACY_MEDIUM
    },
    column="val",
    name="contains_person_or_address_data",
  ),
]

# Initialize the DQX engine:
dq_engine = DQEngine(WorkspaceClient())

# Create some sample data:
data = [
  ["My name is John Smith and I live at 123 Main St New York, NY 07008"],
  ["The sky is blue, road runner"],
  ["Jane Smith sent an email to sara@info.com"],
  [None],
]
df = spark.createDataFrame(data, "val string")

# Run the checks and display the output:
checked_df = dq_engine.apply_checks(df, checks)
display(checked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using a custom named entity recognizer
# MAGIC DQX supports custom named entity recognizers passed as Python dictionaries. All dependencies must be pre-loaded for use with DQX's built-in PII detection checks.
# MAGIC
# MAGIC ***WARNING:** Using custom named entity recognizers can significantly degrade performance of quality checking at scale. Sample data or use smaller models for best performance. Run checks on non-serverless compute when using large named entity recognizers.*

# COMMAND ----------

# Define the NLP configuration:
nlp_engine_config = {
  "nlp_engine_name": "spacy",
  "models": [{"lang_code": "en", "model_name": "en_core_web_md"}]
}

checks = [
  # Define a PII check with a custom named-entity recognizer (Stanford De-Identifier Base):
  DQRowRule(
    criticality="error",
    check_func=does_not_contain_pii,
    check_func_kwargs={"nlp_engine_config": nlp_engine_config},
    column="val",
    name="contains_pii_custom_recognizer",
  ),
]

# Initialize the DQX engine:
dq_engine = DQEngine(WorkspaceClient())

# Create some sample data:
data = [
  ["My name is John Smith and I live at 123 Main St New York, NY 07008"],
  ["The sky is blue, road runner"],
  ["Jane Smith sent an email to sara@info.com"],
  [None],
]
df = spark.createDataFrame(data, "val string")

# Run the checks and display the output:
checked_df = dq_engine.apply_checks(df, checks)
display(checked_df)