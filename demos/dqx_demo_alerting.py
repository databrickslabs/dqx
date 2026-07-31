# Databricks notebook source
# MAGIC %md
# MAGIC # Alerting with DQX Actions
# MAGIC
# MAGIC This short demo shows how to react automatically to data quality problems using DQX **actions**. After checks run, DQX evaluates each action's optional *condition* against the observed summary metrics and, when it matches, fires the action — for example sending an alert.
# MAGIC
# MAGIC We use a `DQLogAlertDestination`, which writes the alert to the driver log and contacts no external system, so the demo runs end-to-end anywhere. We also show how to add a Slack destination: supply a Slack incoming-webhook URL in the `slack_webhook_url` widget and the demo will additionally deliver to Slack. Finally, we show how to register and fire a **custom action** of your own.
# MAGIC
# MAGIC See the [Actions and Alerting guide](https://databrickslabs.github.io/dqx/docs/guide/actions_and_alerts) for the full reference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install DQX

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

# Optional: provide a Slack incoming-webhook URL to also deliver alerts to Slack.
# Leave empty to log alerts only (no external calls).
dbutils.widgets.text("slack_webhook_url", "", "Slack Webhook URL")
slack_webhook_url = dbutils.widgets.get("slack_webhook_url")

# Catalog and schema used by the end-to-end (save-in-table) example near the end of this demo.
dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")
demo_catalog_name = dbutils.widgets.get("demo_catalog")
demo_schema_name = dbutils.widgets.get("demo_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a small dataset with some invalid rows
# MAGIC
# MAGIC Two rows have a null `name` or `age`, so an error-level check will flag them and drive our alert.

# COMMAND ----------

df = spark.createDataFrame(
    [
        (1, "Alice", 30),
        (2, None, 25),  # null name -> error
        (3, "Bob", None),  # null age -> error
        (4, "Carol", 40),
    ],
    "id int, name string, age int",
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the quality checks
# MAGIC
# MAGIC We require `name` and `age` to be non-null at the error level.

# COMMAND ----------

import yaml

checks = yaml.safe_load(
    """
- criticality: error
  check:
    function: is_not_null
    for_each_column:
      - name
      - age
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure the alerting action
# MAGIC
# MAGIC Actions require a `DQMetricsObserver` because they evaluate the metrics it collects. Our action fires whenever at least one error row is observed (`error_row_count > 0`) and delivers the alert to the driver log — plus Slack if a webhook URL was provided.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.actions import (
    DQAction,
    DQAlert,
    DQLogAlertDestination,
    DQSlackAlertDestination,
)

# Always log alerts; add Slack only when a webhook URL is supplied.
destinations = [DQLogAlertDestination(name="driver-log", level="warning")]
if slack_webhook_url:
    destinations.append(DQSlackAlertDestination(name="slack", webhook_url=slack_webhook_url))

actions = [
    DQAction(
        condition="error_row_count > 0",
        action=DQAlert(name="alert_on_errors", destinations=destinations),
    )
]

observer = DQMetricsObserver(name="alerting_demo")
engine = DQEngine(WorkspaceClient(), observer=observer, actions=actions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply the checks and evaluate the actions
# MAGIC
# MAGIC After applying checks we trigger a Spark action (`count()`) to materialize the observed metrics, then call `evaluate_actions`. The returned results describe which actions fired; the alert itself is written to the driver log (look for the `[DQX alert]` line above the cell output).
# MAGIC
# MAGIC We call `evaluate_actions` manually here because this demo works on an in-memory DataFrame that is never saved. In most pipelines you don't need to: the **end-to-end save methods** `apply_checks_and_save_in_table` and `apply_checks_by_metadata_and_save_in_table` (batch and streaming) fire the configured actions automatically after writing the results — the recommended approach. Reach for `evaluate_actions` only when you are not using those save methods (e.g. Lakeflow Pipelines or custom `writeStream` code). See the [guide](https://databrickslabs.github.io/dqx/docs/guide/actions_and_alerts) for details.

# COMMAND ----------

checked_df, observation = engine.apply_checks_by_metadata(df, checks)

# Materialize the observation so metrics are populated.
checked_df.count()

results = engine.evaluate_actions(observation.get, input_location="in-memory demo dataframe")

for result in results:
    print(f"action={result.action_name} fired={result.fired} status={result.status.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registering a custom action
# MAGIC
# MAGIC The built-in actions (`DQAlert`, `FailPipeline`, `NoOpAction`) cover the common cases, but you can add your own. A custom action is any subclass of `Action` that declares a unique literal `type`, implements `execute`, and is registered with the `@register_action` decorator. Every action — built-in or custom — always receives the observed summary metrics via `context.metrics`, so your action can react to `error_row_count`, `warning_row_count`, etc. regardless of its gating `condition`.
# MAGIC
# MAGIC The action below simply logs the observed error-row count. Note that a custom action is *code*, not configuration: the process running DQX (this notebook here, or the job cluster in a workflow) must be able to import the class before the action is constructed or loaded from metadata.

# COMMAND ----------

import logging
from typing import Literal

from databricks.labs.dqx.actions import (
    Action,
    ActionContext,
    ActionResult,
    ActionServices,
    ActionStatus,
    register_action,
)

logger = logging.getLogger(__name__)


@register_action
class LogErrorCount(Action):
    # The literal 'type' is the discriminator used in metadata; it must be unique across actions.
    type: Literal["log_error_count"] = "log_error_count"

    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        # context.metrics always holds the observed summary metrics, keyed by metric name.
        errors = context.metrics.get("error_row_count", 0)
        logger.warning(f"DQX-demo: {errors} error rows observed")
        return ActionResult(action_name=self.name or self.type, fired=True, status=ActionStatus.UNHEALTHY)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fire the custom action
# MAGIC
# MAGIC The custom action is used exactly like a built-in one — attach it to a `DQEngine` (programmatically here, but it can equally be authored as metadata) and evaluate. Look for the `DQX-demo:` line it writes to the driver log.

# COMMAND ----------

custom_actions = [
    DQAction(
        condition="error_row_count > 0",
        action=LogErrorCount(),
    )
]

custom_observer = DQMetricsObserver(name="custom_action_demo")
custom_engine = DQEngine(WorkspaceClient(), observer=custom_observer, actions=custom_actions)

custom_checked_df, custom_observation = custom_engine.apply_checks_by_metadata(df, checks)
custom_checked_df.count()  # materialize the observation so metrics are populated

custom_results = custom_engine.evaluate_actions(custom_observation.get, input_location="in-memory demo dataframe")

for result in custom_results:
    print(f"action={result.action_name} fired={result.fired} status={result.status.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end: let the save method fire the actions automatically
# MAGIC
# MAGIC The examples above call `evaluate_actions` by hand because they work on an in-memory DataFrame. In a real pipeline you normally read from and write to Unity Catalog tables, and the **save methods** fire the configured actions for you — no manual `evaluate_actions` call. Below we write the demo data to an input table, then let `apply_checks_by_metadata_and_save_in_table` apply the checks, save the results, and fire the actions automatically.
# MAGIC
# MAGIC We also pass an `action_events_config` so DQX keeps a durable **alert history**: one row per action evaluation (including evaluations that did *not* fire) is appended to an events table, giving a complete audit log of what was checked, what fired, where it was delivered, and what failed.
# MAGIC
# MAGIC The alert here uses `notify_on=NotifyOn.STATUS_CHANGE`, so it notifies only on the transition *into* an unhealthy state. On this first run against a fresh events table that transition happens, so the alert fires; because state is persisted to the events table, re-running the cell while the data stays unhealthy suppresses the repeat notification (the evaluation is still recorded in history with `fired=false`).
# MAGIC
# MAGIC This step writes to `{demo_catalog}.{demo_schema}` (see the widgets at the top). Look again for the `[DQX alert]` and `DQX-demo:` lines in the driver log — this time DQX fired them without an explicit call.

# COMMAND ----------

from databricks.labs.dqx.config import ActionEventsConfig, InputConfig, OutputConfig
from databricks.labs.dqx.actions import NotifyOn

# Persist the demo data to an input table so the save method can read it.
input_table = f"{demo_catalog_name}.{demo_schema_name}.dqx_alerting_demo_input"
output_table = f"{demo_catalog_name}.{demo_schema_name}.dqx_alerting_demo_output"
events_table = f"{demo_catalog_name}.{demo_schema_name}.dqx_alerting_demo_events"

df.write.mode("overwrite").saveAsTable(input_table)

# Reuse both the built-in alert and the custom action; a fresh observer is required for the run.
# action_events_config records a durable, auditable history of every action evaluation.
e2e_observer = DQMetricsObserver(name="alerting_e2e_demo")
e2e_engine = DQEngine(
    WorkspaceClient(),
    observer=e2e_observer,
    actions=[
        DQAction(
            condition="error_row_count > 0",
            action=DQAlert(name="alert_on_errors", destinations=destinations, notify_on=NotifyOn.STATUS_CHANGE),
        ),
        DQAction(
            condition="error_row_count > 0",
            action=LogErrorCount(name="log_error_count"), # notify_on not configured, so it will notify on every evaluation
        ),
    ],
    action_events_config=ActionEventsConfig(location=events_table),
)

# Checks are applied, results are saved to the output table, and the actions fire automatically
# after the observation is materialized — no evaluate_actions call needed.
e2e_engine.apply_checks_by_metadata_and_save_in_table(
    checks=checks,
    input_config=InputConfig(location=input_table),
    output_config=OutputConfig(location=output_table, mode="overwrite"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the outputs
# MAGIC
# MAGIC Display the checked output table (with the `_errors` / `_warnings` columns DQX appends) and the alert history table. Each history row records one action evaluation: `fired` tells you whether it executed, `status` is the outcome, `observed_metrics` is the metrics snapshot, and `destinations` / `delivery_errors` show where it was sent and any failures.

# COMMAND ----------

# Checked output data, including the appended _errors and _warnings columns.
print("Checked output:")
display(spark.table(output_table))

# Durable alert history — one row per action evaluation, most recent first.
print("Alert history:")
display(spark.table(events_table).orderBy("run_time", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wiring a real Slack alert
# MAGIC
# MAGIC To deliver to Slack, create a [Slack incoming webhook](https://api.slack.com/messaging/webhooks) and pass its URL to the `slack_webhook_url` widget above (delivery is restricted to `hooks.slack.com`). In production, store the URL in a Databricks secret scope and reference it with `DQSecret` rather than passing a plaintext string:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.labs.dqx.config import DQSecret
# MAGIC from databricks.labs.dqx.actions import DQSlackAlertDestination
# MAGIC
# MAGIC DQSlackAlertDestination(
# MAGIC     name="slack",
# MAGIC     webhook_url=DQSecret(scope="dq-secrets", key="slack-webhook-url"),
# MAGIC )
# MAGIC ```
