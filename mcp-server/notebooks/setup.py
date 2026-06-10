# Databricks notebook source

# COMMAND ----------

"""
One-time setup notebook for the DQX MCP server.

Run after `databricks bundle deploy` to create the temp view schema and
grant all required UC permissions. Safe to re-run (all statements are idempotent).

Grants:
  Catalog level:
    - users          → USE CATALOG
    - app SP         → USE CATALOG
  Schema level (catalog.tmp):
    - users          → USE SCHEMA, CREATE TABLE (so OBO token can create views)
    - app SP         → USE SCHEMA, SELECT (so SP job can read through views)

Parameters:
  - catalog_name: UC catalog for temp views (e.g. 'dqx_mcp')
  - app_name: Databricks App name (e.g. 'mcp-dqx') — used to look up the app SP
  - users_group: Group name for all users (default: 'account users')
"""

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dqx-mcp-setup")

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("app_name", "mcp-dqx")
dbutils.widgets.text("users_group", "account users")

catalog_name = dbutils.widgets.get("catalog_name")
app_name = dbutils.widgets.get("app_name")
users_group = dbutils.widgets.get("users_group")

if not catalog_name:
    raise ValueError("catalog_name parameter is required")

logger.info(f"Setting up DQX MCP: catalog={catalog_name}, app={app_name}, users_group={users_group}")

# COMMAND ----------

# Look up the app's service principal
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
app = ws.apps.get(app_name)
sp_id = app.service_principal_id

# Resolve SP application_id — UC GRANTs use application_id, not display_name
sp = ws.service_principals.get(sp_id)
sp_principal = sp.application_id
logger.info(f"App SP: display_name={sp.display_name}, application_id={sp_principal} (id={sp_id})")

# COMMAND ----------

schema_name = "tmp"

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
logger.info(f"Schema `{catalog_name}`.`{schema_name}` ready")

# COMMAND ----------

# All grants — single source of truth
grants = [
    # Catalog-level
    f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{users_group}`",
    f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{sp_principal}`",
    # Schema-level: users can create views via OBO token
    f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{users_group}`",
    f"GRANT CREATE TABLE ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{users_group}`",
    # Schema-level: app SP can read through views
    f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{sp_principal}`",
    f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{sp_principal}`",
]

for sql in grants:
    logger.info(f"Executing: {sql}")
    spark.sql(sql)

logger.info("Setup complete — all grants applied.")
