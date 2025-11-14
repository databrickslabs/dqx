# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - AI Assisted Checks Generation Demo
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Install DQX with LLM extras
# MAGIC
# MAGIC To use DQX AI Assisted features, DQX has to be installed with `llm` extras: 
# MAGIC
# MAGIC `%pip install databricks-labs-dqx[llm]`

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[llm] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[llm]

%restart_python

# COMMAND ----------

model_name = "databricks/databricks-claude-sonnet-4-5"
default_user_input = "customername should not start with s and account balance should be positive"
default_table_name = "samples.tpch.customer"

dbutils.widgets.text("model_name", model_name, "Model Name")
dbutils.widgets.text("user_requirement", default_user_input, "User Requirement")
dbutils.widgets.text("table_name", default_table_name, "Table Name")

model_name = dbutils.widgets.get("model_name")
user_requirement = dbutils.widgets.get("user_requirement")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

import os, yaml
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import LLMModelConfig, InputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Instantiate DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws, spark)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating DQX Rules with AI Assistance
# MAGIC
# MAGIC DQX supports AI-assisted rule generation based on user requirements. The following configurations are available:
# MAGIC
# MAGIC - **Model Serving Endpoint**:  
# MAGIC   By default, DQX uses the `databricks/databricks-claude-sonnet-4-5` model serving endpoint to generate rules. However, users can specify a different model endpoint if they prefer to use another one.
# MAGIC
# MAGIC - **Table Name**:  
# MAGIC   Users can optionally provide the fully qualified name of a table. DQX will use the table's schema to generate rules. If no table name is provided, the schema will be inferred based on the user's input.
# MAGIC

# COMMAND ----------

# Creating model config with optional model name (default Databricks Foundational model endpoint is used if not provided)
llm_model_config = LLMModelConfig(model_name=model_name)
generator = DQGenerator(ws, llm_model_config=llm_model_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate DQ rules using AI-Assisted approach using user requirement
# MAGIC
# MAGIC Schema of the data will be guessed.

# COMMAND ----------

checks = generator.generate_dq_rules_ai_assisted(user_input=user_requirement)
print("======== Generated checks =========")
print(checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate DQ rules using AI-Assisted approach using user requirement and user specified table name
# MAGIC
# MAGIC Schema will be fetched from the table specified by the user.
# MAGIC

# COMMAND ----------

checks = generator.generate_dq_rules_ai_assisted(user_input=user_requirement, input_config=InputConfig(location=table_name))
print("======== Generated checks =========")
print(checks)
