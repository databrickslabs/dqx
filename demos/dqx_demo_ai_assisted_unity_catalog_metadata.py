# Databricks notebook source
# MAGIC %md
# MAGIC # AI-assisted rule generation — with and without Unity Catalog metadata
# MAGIC
# MAGIC Builds a synthetic bronze → silver → gold invoice pipeline (same data layer as
# MAGIC `demos/dqx_demo_lineage_action.py`), then calls
# MAGIC `DQGenerator.generate_dq_rules_ai_assisted` **twice** against the same table:
# MAGIC
# MAGIC 1. **Baseline** — `unity_catalog_metadata_config=None`; the LLM sees only column names
# MAGIC    and types.
# MAGIC 2. **Enriched** — `UnityCatalogMetadataConfig()` with defaults; the LLM additionally
# MAGIC    sees UC table / column comments, column tags, bounded column upstream lineage, and
# MAGIC    an external upstream relationship from a SAP-origin `ExternalMetadata` object.
# MAGIC
# MAGIC The two generated rule sets are printed side-by-side at the end so you can see how
# MAGIC the Unity Catalog metadata shifts the model's choices. Comments / tags are deliberately
# MAGIC brief — the goal is to add semantics the LLM would otherwise have to guess, not to
# MAGIC write encyclopaedia entries.
# MAGIC
# MAGIC Disable a specific enrichment by setting its sub-model to `None`, e.g.
# MAGIC `UnityCatalogMetadataConfig(column_upstream_lineage=None, external_lineage=None)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install DQX with LLM extras + dbldatagen

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}' dbldatagen
else:
    %pip install 'databricks-labs-dqx[llm]' dbldatagen

%restart_python

# COMMAND ----------

dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")
dbutils.widgets.text("model_name", "databricks/databricks-claude-sonnet-4-5", "Model Name")

# COMMAND ----------

demo_catalog_name = dbutils.widgets.get("demo_catalog")
demo_schema_name = dbutils.widgets.get("demo_schema")
model_name = dbutils.widgets.get("model_name")

qualified = f"{demo_catalog_name}.{demo_schema_name}"
bronze_table = f"{qualified}.invoices_bronze"
silver_table = f"{qualified}.invoices_silver"
dim_customer_table = f"{qualified}.dim_customer"
dim_service_table = f"{qualified}.dim_service"
fact_invoice_table = f"{qualified}.fact_invoice"

print(
    "Tables:\n"
    f"  bronze:      {bronze_table}\n"
    f"  silver:      {silver_table}\n"
    f"  dim_customer:{dim_customer_table}\n"
    f"  dim_service: {dim_service_table}\n"
    f"  fact_invoice:{fact_invoice_table}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — synthetic invoices via `dbldatagen`

# COMMAND ----------

import dbldatagen as dg
from datetime import datetime, timedelta

ROW_COUNT = 10_000

bronze_generator = (
    dg.DataGenerator(sparkSession=spark, name="invoices_bronze", rowCount=ROW_COUNT, partitions=4)
    .withColumn("invoice_id", "string", template=r"INV-\d{8}", random=True)
    .withColumn(
        "invoice_timestamp",
        "timestamp",
        begin=datetime.utcnow() - timedelta(days=30),
        end=datetime.utcnow() + timedelta(days=2),
        random=True,
    )
    .withColumn("client_id", "string", values=[f"C{n:04d}" for n in range(1, 250)] + [None], random=True)
    .withColumn("client_name", "string", values=[" alice corp ", "Beta LLC", "gamma inc", None], random=True)
    .withColumn("service_id", "string", values=[f"S{n:03d}" for n in range(1, 40)], random=True)
    .withColumn("service_name", "string", values=[" widgets ", "Support", "consulting"], random=True)
    .withColumn("amount", "integer", minValue=-50, maxValue=5000, random=True)
    .withColumn("currency_code", "string", values=["USD", "EUR", "GBP", "CHF", "JPY", "ZZZ"], random=True)
    .withColumn("signed", "boolean", random=True)
)

bronze_generator.build().write.mode("overwrite").format("delta").saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver + gold layers

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {silver_table} USING DELTA AS
    SELECT DISTINCT invoice_id, invoice_timestamp, client_id,
           INITCAP(TRIM(client_name)) AS client_name,
           service_id, INITCAP(TRIM(service_name)) AS service_name,
           amount, currency_code, signed
    FROM {bronze_table}
    WHERE invoice_id IS NOT NULL
    """
)
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {dim_customer_table} USING DELTA AS
    SELECT DISTINCT client_id, client_name FROM {silver_table} WHERE client_id IS NOT NULL
    """
)
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {dim_service_table} USING DELTA AS
    SELECT DISTINCT service_id, service_name FROM {silver_table} WHERE service_id IS NOT NULL
    """
)
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fact_invoice_table} USING DELTA AS
    SELECT invoice_id, invoice_timestamp, client_id, service_id, amount, currency_code, signed
    FROM {silver_table}
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline — generate rules without Unity Catalog metadata
# MAGIC
# MAGIC `unity_catalog_metadata_config=None` is the default. At this point the tables have no
# MAGIC comments, no tags, and no external lineage, so the LLM has only column names + types
# MAGIC to work with.

# COMMAND ----------

import yaml
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import InputConfig, LLMModelConfig, UnityCatalogMetadataConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator

ws = WorkspaceClient()
engine = DQEngine(ws, spark)
generator = DQGenerator(ws, spark, llm_model_config=LLMModelConfig(model_name=model_name))

USER_INPUT = "generate quality checks for an invoice fact table"

baseline_checks = generator.generate_dq_rules_ai_assisted(
    user_input=USER_INPUT,
    input_config=InputConfig(location=fact_invoice_table),
    unity_catalog_metadata_config=None,
)
print(yaml.safe_dump(baseline_checks, sort_keys=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach concise UC comments on every layer

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {bronze_table} IS 'Raw SAP SD invoice events, pre-dedup'")
spark.sql(f"COMMENT ON TABLE {silver_table} IS 'Deduplicated invoices with normalised names'")
spark.sql(f"COMMENT ON TABLE {fact_invoice_table} IS 'One row per confirmed invoice line'")

for tbl in (bronze_table, silver_table, fact_invoice_table):
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN invoice_id COMMENT 'Invoice primary key'")
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN invoice_timestamp COMMENT 'Invoice issuance time, UTC'")
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN client_id COMMENT 'Client identifier (SAP KUNAG)'")
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN amount COMMENT 'Positive integer amount in minor units'")
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN currency_code COMMENT 'ISO-4217 three-letter currency'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach a handful of UC tags
# MAGIC
# MAGIC Tags land in `system.information_schema.table_tags` / `column_tags`, which the Spark
# MAGIC tag collector reads. Requires the appropriate UC privileges — the demo swallows the
# MAGIC error and continues so the rest of the flow still runs.

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {fact_invoice_table} SET TAGS ('domain' = 'finance')")
    spark.sql(f"ALTER TABLE {fact_invoice_table} ALTER COLUMN client_id SET TAGS ('pii' = 'client')")
    spark.sql(f"ALTER TABLE {fact_invoice_table} ALTER COLUMN amount SET TAGS ('financial' = 'true')")
    spark.sql(f"ALTER TABLE {fact_invoice_table} ALTER COLUMN invoice_id SET TAGS ('source' = 'sap_sd')")
except Exception as exc:
    print(f"Skipping tag DDL (requires UC privileges): {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## External lineage: register the SAP source
# MAGIC
# MAGIC Create an `ExternalMetadata` object representing the upstream SAP SD invoice table
# MAGIC (VBRK) and an `ExternalLineageRelationship` from it into the bronze UC table. This
# MAGIC enrichment is invisible to the recursive-CTE walker (external lineage is not in
# MAGIC `system.access.*_lineage`) and is only reachable via the SDK — see
# MAGIC https://docs.databricks.com/aws/en/data-governance/unity-catalog/external-lineage.

# COMMAND ----------

from databricks.sdk.service.catalog import (
    ColumnRelationship,
    CreateRequestExternalLineage,
    ExternalLineageExternalMetadata,
    ExternalLineageObject,
    ExternalLineageTableInfo,
    ExternalMetadata,
    SystemType,
)

EXTERNAL_METADATA_NAME = "dqx_demo_sap_sd_invoices"
external_metadata_created = False
external_relationship_created = False

try:
    ws.external_metadata.create_external_metadata(
        ExternalMetadata(
            name=EXTERNAL_METADATA_NAME,
            system_type=SystemType.SAP,
            entity_type="TABLE",
            description="SAP SD invoice headers (table VBRK)",
        )
    )
    external_metadata_created = True

    bronze_cat, bronze_sch, bronze_name = bronze_table.split(".")
    ws.external_lineage.create_external_lineage_relationship(
        CreateRequestExternalLineage(
            source=ExternalLineageObject(
                external_metadata=ExternalLineageExternalMetadata(name=EXTERNAL_METADATA_NAME),
            ),
            target=ExternalLineageObject(
                table=ExternalLineageTableInfo(
                    catalog_name=bronze_cat, schema_name=bronze_sch, name=bronze_name
                ),
            ),
            columns=[
                ColumnRelationship(source="VBELN", target="invoice_id"),
                ColumnRelationship(source="KUNAG", target="client_id"),
                ColumnRelationship(source="NETWR", target="amount"),
                ColumnRelationship(source="WAERK", target="currency_code"),
            ],
        )
    )
    external_relationship_created = True
    print("External lineage SAP → bronze created.")
except Exception as exc:
    print(f"Skipping external lineage setup (API unavailable or missing permission): {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriched — generate rules with Unity Catalog metadata
# MAGIC
# MAGIC `UnityCatalogMetadataConfig()` enables every enrichment with sensible defaults
# MAGIC (`column_upstream_lineage=ColumnUpstreamLineageConfig(depth=2, lookback_days=30,
# MAGIC max_nodes=50)` and `external_lineage=ExternalLineageConfig(max_relationships=50)`).

# COMMAND ----------

enriched_checks = generator.generate_dq_rules_ai_assisted(
    user_input=USER_INPUT,
    input_config=InputConfig(location=fact_invoice_table),
    unity_catalog_metadata_config=UnityCatalogMetadataConfig(),
)
print(yaml.safe_dump(enriched_checks, sort_keys=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Side-by-side comparison

# COMMAND ----------

print("--- baseline (no UC metadata) ---")
print(yaml.safe_dump(baseline_checks, sort_keys=False))
print("--- enriched (UnityCatalogMetadataConfig defaults) ---")
print(yaml.safe_dump(enriched_checks, sort_keys=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply the enriched rules

# COMMAND ----------

valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(
    spark.table(fact_invoice_table), enriched_checks
)
print(f"valid rows:     {valid_df.count()}")
print(f"invalid rows:   {invalid_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup — drop the external metadata registration

# COMMAND ----------

if external_relationship_created:
    try:
        from databricks.sdk.service.catalog import DeleteRequestExternalLineage

        bronze_cat, bronze_sch, bronze_name = bronze_table.split(".")
        ws.external_lineage.delete_external_lineage_relationship(
            DeleteRequestExternalLineage(
                source=ExternalLineageObject(
                    external_metadata=ExternalLineageExternalMetadata(name=EXTERNAL_METADATA_NAME),
                ),
                target=ExternalLineageObject(
                    table=ExternalLineageTableInfo(
                        catalog_name=bronze_cat, schema_name=bronze_sch, name=bronze_name
                    ),
                ),
            )
        )
    except Exception as exc:
        print(f"Could not delete external lineage relationship: {exc}")

if external_metadata_created:
    try:
        ws.external_metadata.delete_external_metadata(name=EXTERNAL_METADATA_NAME)
    except Exception as exc:
        print(f"Could not delete external metadata: {exc}")
