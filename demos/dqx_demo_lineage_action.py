# Databricks notebook source
# MAGIC %md
# MAGIC # Lineage Collection with `CollectLineageAction`
# MAGIC
# MAGIC This demo walks through a compact **bronze → silver → gold** invoice pipeline (star schema)
# MAGIC and shows how `CollectLineageAction` persists upstream and downstream table lineage from
# MAGIC `system.access.table_lineage`, plus recursive per-failed-column lineage.
# MAGIC
# MAGIC The lineage sink is a plain Delta table with a stable, documented schema
# MAGIC (`LINEAGE_TABLE_SCHEMA`), so it can be queried, joined, or dashboarded like any other DQX
# MAGIC output. The action is attached at the bronze layer *and* the gold layer so we can watch both
# MAGIC upstream propagation (gold → silver → bronze) and downstream propagation (bronze → silver →
# MAGIC gold) in a single run of the notebook.
# MAGIC
# MAGIC A deliberate bug is planted at the gold layer (duplicate `client_id` rows) so the
# MAGIC `is_unique` check fires — and, together with the log-alert destination, produces a
# MAGIC visible alert in the driver log alongside the persisted lineage rows.
# MAGIC
# MAGIC See the [Actions and Alerting guide](https://databrickslabs.github.io/dqx/docs/guide/actions_and_alerts)
# MAGIC for the full `CollectLineageAction` reference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install DQX + `dbldatagen`

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}' dbldatagen
else:
    %pip install databricks-labs-dqx dbldatagen

%restart_python

# COMMAND ----------

dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure the demo
# MAGIC
# MAGIC All widgets are optional — defaults land in `main.default`. Change them to any catalog /
# MAGIC schema you have `CREATE TABLE` on. This demo is idempotent: it uses `CREATE OR REPLACE
# MAGIC TABLE` for every layer and appends to the lineage sink.

# COMMAND ----------

demo_catalog_name = dbutils.widgets.get("demo_catalog")
demo_schema_name = dbutils.widgets.get("demo_schema")

qualified = f"{demo_catalog_name}.{demo_schema_name}"
bronze_table = f"{qualified}.invoices_bronze"
silver_table = f"{qualified}.invoices_silver"
dim_customer_table = f"{qualified}.dim_customer"
dim_service_table = f"{qualified}.dim_service"
fact_invoice_table = f"{qualified}.fact_invoice"
lineage_sink_table = f"{qualified}.dqx_lineage"

print(
    "Tables:\n"
    f"  bronze:      {bronze_table}\n"
    f"  silver:      {silver_table}\n"
    f"  dim_customer:{dim_customer_table}\n"
    f"  dim_service: {dim_service_table}\n"
    f"  fact_invoice:{fact_invoice_table}\n"
    f"  lineage_sink:{lineage_sink_table}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — synthetic invoices with `dbldatagen`
# MAGIC
# MAGIC Ten thousand rows on the schema
# MAGIC `(invoice_id STRING, invoice_timestamp TIMESTAMP, client_id STRING, client_name STRING,
# MAGIC service_id STRING, service_name STRING, amount DECIMAL(18,2), currency_code STRING,
# MAGIC signed BOOLEAN)`. Defects are injected so every bronze DQX check has at least one row to
# MAGIC flag: future timestamps, null client IDs, negative amounts, and invalid ISO-4217 codes
# MAGIC (e.g. `"ZZZ"`).

# COMMAND ----------

import dbldatagen as dg
from decimal import Decimal
from datetime import datetime, timedelta
from pyspark.sql import functions as F

ROW_COUNT = 10_000

generator = (
    dg.DataGenerator(sparkSession=spark, name="invoices_bronze", rowCount=ROW_COUNT, partitions=4)
    .withColumn("invoice_id", "string", template=r"INV-\\d{8}", random=True)
    .withColumn(
        "invoice_timestamp",
        "timestamp",
        begin=datetime.utcnow() - timedelta(days=30),
        end=datetime.utcnow() + timedelta(days=2),  # ← some future timestamps by construction
        random=True,
    )
    .withColumn("client_id", "string", values=[f"C{n:04d}" for n in range(1, 250)] + [None], random=True)
    .withColumn("client_name", "string", values=[" alice corp ", "Beta LLC", "gamma inc", None], random=True)
    .withColumn("service_id", "string", values=[f"S{n:03d}" for n in range(1, 40)], random=True)
    .withColumn("service_name", "string", values=[" widgets ", "Support", "consulting"], random=True)
    .withColumn(
        "amount",
        "decimal(18,2)",
        minValue=Decimal("-50.00"),  # ← some negative amounts
        maxValue=Decimal("5000.00"),
        random=True,
    )
    .withColumn(
        "currency_code",
        "string",
        values=["USD", "EUR", "GBP", "CHF", "JPY", "ZZZ"],  # ← "ZZZ" is invalid ISO-4217
        random=True,
    )
    .withColumn("signed", "boolean", random=True)
)

bronze_df = generator.build()
bronze_df.write.mode("overwrite").format("delta").saveAsTable(bronze_table)
print(f"bronze rows: {spark.table(bronze_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — dedup + normalise names
# MAGIC
# MAGIC `SELECT DISTINCT` on `invoice_id`, `INITCAP(TRIM(...))` on the two name columns, drop rows
# MAGIC missing `invoice_id`.

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {silver_table}
    USING DELTA AS
    SELECT DISTINCT
      invoice_id,
      invoice_timestamp,
      client_id,
      INITCAP(TRIM(client_name)) AS client_name,
      service_id,
      INITCAP(TRIM(service_name)) AS service_name,
      amount,
      currency_code,
      signed
    FROM {bronze_table}
    WHERE invoice_id IS NOT NULL
    """
)
print(f"silver rows: {spark.table(silver_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — star schema (`dim_customer`, `dim_service`, `fact_invoice`)
# MAGIC
# MAGIC A deliberate bug: `fact_invoice` is UNION-ed with a resampled copy so `client_id` is
# MAGIC intentionally *not* unique. The gold `is_unique` check will fire on that column.

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {dim_customer_table}
    USING DELTA AS
    SELECT DISTINCT client_id, client_name
    FROM {silver_table}
    WHERE client_id IS NOT NULL
    """
)
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {dim_service_table}
    USING DELTA AS
    SELECT DISTINCT service_id, service_name
    FROM {silver_table}
    WHERE service_id IS NOT NULL
    """
)
# Deliberate bug: resample a small subset and UNION so duplicate client_ids appear in the fact
# table. The gold `is_unique` check should catch this.
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fact_invoice_table}
    USING DELTA AS
    SELECT invoice_id, invoice_timestamp, client_id, service_id, amount, currency_code, signed
    FROM {silver_table}
    UNION ALL
    SELECT invoice_id, invoice_timestamp, client_id, service_id, amount, currency_code, signed
    FROM {silver_table}
    WHERE client_id IN (
        SELECT client_id FROM {silver_table} WHERE client_id IS NOT NULL LIMIT 20
    )
    """
)
print(f"fact_invoice rows: {spark.table(fact_invoice_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define quality checks
# MAGIC
# MAGIC * **Bronze** — `invoice_timestamp <= current_timestamp()`, `is_not_null(client_id)`,
# MAGIC   `amount > 0`, and — for ISO-4217 validation — the built-in **`is_valid_currency_code`**
# MAGIC   check (do not hand-roll a regex allowlist; the dedicated check accepts every ISO-assigned
# MAGIC   code and is the recommended way).
# MAGIC * **Gold** — `is_unique(client_id)` on `fact_invoice`, which fails because of the
# MAGIC   duplicate-injection above.

# COMMAND ----------

import yaml

bronze_checks = yaml.safe_load(
    """
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: invoice_timestamp <= current_timestamp()
      name: invoice_timestamp_not_in_future
      msg: invoice_timestamp is in the future

- criticality: error
  check:
    function: is_not_null
    arguments:
      column: client_id

- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: amount > 0
      name: amount_is_positive
      msg: amount must be strictly positive

- criticality: error
  check:
    function: is_valid_currency_code
    arguments:
      column: currency_code
      code_format: alphabetic
"""
)

gold_checks = yaml.safe_load(
    f"""
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
        - client_id
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wire actions
# MAGIC
# MAGIC A single `DQAction` list carries two behaviours on every run:
# MAGIC
# MAGIC * `CollectLineageAction` — persists upstream / downstream / column / entity lineage rows to
# MAGIC   the shared `dqx_lineage` sink.
# MAGIC * `DQAlert` (`DQLogAlertDestination`) — writes an alert to the driver log whenever
# MAGIC   `error_row_count > 0`.

# COMMAND ----------

from databricks.labs.dqx.actions import (
    CollectLineageAction,
    DQAction,
    DQAlert,
    DQLogAlertDestination,
)
from databricks.labs.dqx.actions.lineage import (
    LineageActionConfig,
    LineageSearchConfig,
)
from databricks.labs.dqx.config import OutputConfig

lineage_action = CollectLineageAction(
    output_config=OutputConfig(location=lineage_sink_table, mode="append"),
    config=LineageActionConfig(
        upstream=LineageSearchConfig(depth=3, lookback_days=7),
        downstream=LineageSearchConfig(depth=3, lookback_days=7),
        columns=LineageSearchConfig(depth=1, lookback_days=7),
    ),
)

log_alert = DQAlert(
    name="alert_on_errors",
    destinations=[DQLogAlertDestination(name="driver-log", level="warning")],
)

actions = [
    DQAction(action=lineage_action, condition=None, name="collect_lineage"),
    DQAction(action=log_alert, condition="error_row_count > 0", name="alert_on_errors"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run bronze and gold layers
# MAGIC
# MAGIC The bronze run flags the injected defects (future timestamps, null client IDs, negative
# MAGIC amounts, invalid currency codes) and appends bronze-anchored lineage edges to the sink.
# MAGIC The gold run fires the `is_unique` failure and appends gold-anchored lineage edges. We use
# MAGIC the end-to-end `apply_checks_by_metadata_and_save_in_table` so the actions fire
# MAGIC automatically after each save.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver

engine = DQEngine(
    WorkspaceClient(),
    observer=DQMetricsObserver(name="lineage_demo"),
    actions=actions,
)

engine.apply_checks_by_metadata_and_save_in_table(
    checks=bronze_checks,
    input_config=InputConfig(location=bronze_table),
    output_config=OutputConfig(location=f"{bronze_table}_checked", mode="overwrite"),
)

engine.apply_checks_by_metadata_and_save_in_table(
    checks=gold_checks,
    input_config=InputConfig(location=fact_invoice_table),
    output_config=OutputConfig(location=f"{fact_invoice_table}_checked", mode="overwrite"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the collected lineage
# MAGIC
# MAGIC Two slices:
# MAGIC
# MAGIC * **Upstream from gold** — `fact_invoice ← silver ← bronze`.
# MAGIC * **Downstream from bronze** — `bronze → silver → {fact, dim_customer, dim_service}`.
# MAGIC
# MAGIC Depending on `system.access.table_lineage` propagation lag, edges may take a few minutes to
# MAGIC surface on a fresh workspace — re-run this cell if the output looks empty. The `is_unique`
# MAGIC failure and the log alert should already be visible in the driver log above.

# COMMAND ----------

lineage_df = spark.table(lineage_sink_table)

print("Upstream slice (source_table = fact_invoice):")
display(
    lineage_df.where(
        (F.col("edge_type") == "upstream") & (F.col("source_table") == fact_invoice_table)
    ).orderBy("depth", "target_table")
)

print("Downstream slice (source_table = invoices_bronze):")
display(
    lineage_df.where(
        (F.col("edge_type") == "downstream") & (F.col("source_table") == bronze_table)
    ).orderBy("depth", "target_table")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)
# MAGIC
# MAGIC Uncomment to drop every table created by this demo. Re-run the notebook to recreate them —
# MAGIC everything above uses `CREATE OR REPLACE` so it is safe to rerun with or without cleanup.

# COMMAND ----------

# for t in [
#     bronze_table,
#     f"{bronze_table}_checked",
#     silver_table,
#     dim_customer_table,
#     dim_service_table,
#     fact_invoice_table,
#     f"{fact_invoice_table}_checked",
#     lineage_sink_table,
# ]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC * [Actions and Alerting guide](https://databrickslabs.github.io/dqx/docs/guide/actions_and_alerts) — full reference for `CollectLineageAction`, alert destinations, suppression, and `NotifyOn` modes.
# MAGIC * [Quality checks reference](https://databrickslabs.github.io/dqx/docs/reference/quality_checks) — every built-in check function, including `is_valid_currency_code` used above.
