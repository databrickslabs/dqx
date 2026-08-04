# Databricks notebook source
# MAGIC %md
# MAGIC # Alerting with DQX Actions
# MAGIC
# MAGIC This short demo shows how to react automatically to data quality problems using DQX **actions**. After checks run, DQX evaluates each action's optional *condition* against the observed summary metrics and, when it matches, fires the action — for example sending an alert.
# MAGIC
# MAGIC We use a `DQLogAlertDestination`, which writes the alert to the driver log and contacts no external system, so the demo runs end-to-end anywhere. We also show how to add Slack and Microsoft Teams destinations: supply a Slack incoming-webhook URL in the `slack_webhook_url` widget and/or a Teams Workflows webhook URL in the `teams_webhook_url` widget, and the demo will additionally deliver to those. Finally, we show how to register and fire a **custom action** of your own.
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

# Create the widgets.
dbutils.widgets.text("slack_webhook_url_secret", "", "Slack Webhook Secret (scope/key)")
dbutils.widgets.text("slack_webhook_url", "", "Slack Webhook URL (plaintext, dev only)")
dbutils.widgets.text("teams_webhook_url_secret", "", "Teams Webhook Secret (scope/key)")
dbutils.widgets.text("teams_webhook_url", "", "Teams Webhook URL (plaintext, dev only)")
dbutils.widgets.text("demo_catalog", "main", "Catalog Name")
dbutils.widgets.text("demo_schema", "default", "Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure the demo
# MAGIC
# MAGIC The cell above created the input widgets at the top of the notebook. Fill them in now, then run
# MAGIC the next cell to read the values. All widgets are optional:
# MAGIC
# MAGIC - **Catalog / Schema** — used by the end-to-end (save-in-table) example near the end of this demo.
# MAGIC - **Slack / Teams** — provide EITHER a secret reference (recommended: `scope/key` pointing at a
# MAGIC   Databricks secret holding the webhook URL) OR a plaintext webhook URL (dev only). Leave all four
# MAGIC   empty to skip Slack/Teams entirely — the demo still logs alerts to the driver log. The
# MAGIC   configuration cell later in this demo wires both destinations and disables each one when no
# MAGIC   webhook is supplied — no code changes needed, just fill in the widgets:
# MAGIC   - **Slack** — create a [Slack incoming webhook](https://api.slack.com/messaging/webhooks) (delivery is restricted to `hooks.slack.com`).
# MAGIC   - **Teams** — create a **Power Automate Workflows** webhook (the legacy Office 365 Connector webhooks were retired): in Teams, use the *Workflows* app → template **"Post to a channel when a webhook request is received"** (delivery is restricted to `logic.azure.com` and `environment.api.powerplatform.com`).
# MAGIC
# MAGIC   In production, store each webhook URL in a Databricks secret and pass its `scope/key` reference to
# MAGIC   the `*_webhook_url_secret` widget — the demo resolves it via `DQSecret` so the URL is never held in
# MAGIC   plaintext. The `*_webhook_url` widgets accept a plaintext URL for local development only.

# COMMAND ----------

# Read the widget values (run this after filling in the widgets above).
slack_webhook_url_secret = dbutils.widgets.get("slack_webhook_url_secret")
slack_webhook_url = dbutils.widgets.get("slack_webhook_url")
teams_webhook_url_secret = dbutils.widgets.get("teams_webhook_url_secret")
teams_webhook_url = dbutils.widgets.get("teams_webhook_url")
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
# MAGIC Our action fires whenever at least one error row is observed (`error_row_count > 0`) and delivers the alert to the driver log — plus Slack and/or Teams if their webhook URLs were provided.

# COMMAND ----------

from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.actions import (
    DQAction,
    DQAlert,
    DQLogAlertDestination,
    DQSlackAlertDestination,
    DQTeamsAlertDestination,
)


def _resolve_webhook(secret_ref: str, plaintext_url: str):
    """Prefer a secret reference ('scope/key' -> DQSecret, resolved at delivery time), fall back to a
    plaintext URL (dev only), or return None to disable the destination when neither is provided."""
    if secret_ref:
        return DQSecret.from_reference(secret_ref)
    if plaintext_url:
        return plaintext_url
    return None


# Always log alerts; add Slack and/or Teams only when a webhook (secret or plaintext URL) is supplied.
destinations = [DQLogAlertDestination(name="driver-log", level="warning")]

slack_webhook = _resolve_webhook(slack_webhook_url_secret, slack_webhook_url)
if slack_webhook is not None:
    destinations.append(DQSlackAlertDestination(name="slack", webhook_url=slack_webhook))

teams_webhook = _resolve_webhook(teams_webhook_url_secret, teams_webhook_url)
if teams_webhook is not None:
    destinations.append(DQTeamsAlertDestination(name="teams", webhook_url=teams_webhook))

actions = [
    DQAction(
        condition="error_row_count > 0",
        action=DQAlert(name="alert_on_errors", destinations=destinations),
    )
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply the checks and evaluate the actions
# MAGIC
# MAGIC Actions require a `DQMetricsObserver` because they evaluate the metrics it collects, so we pass one along with the `actions` defined above. We also attach engine-level `user_metadata` via `ExtraParams`: it is included in every alert payload (a *Metadata* section in Slack, facts in Teams, `user_metadata.*` fields in the webhook body, and the driver-log line), so run-level context like the pipeline or owning team travels with the alert.
# MAGIC
# MAGIC After applying checks we trigger a Spark action (`count()`) to materialize the observed metrics, then call `evaluate_actions`. The returned results describe which actions fired; the alert itself is written to the driver log (look for the `[DQX alert]` line above the cell output).
# MAGIC
# MAGIC We call `evaluate_actions` manually here because this demo works on an in-memory DataFrame that is never saved. In most pipelines you don't need to: the **end-to-end save methods** `apply_checks_and_save_in_table` and `apply_checks_by_metadata_and_save_in_table` (batch and streaming) fire the configured actions automatically after writing the results — the recommended approach. Reach for `evaluate_actions` only when you are not using those save methods (e.g. Lakeflow Pipelines or custom `writeStream` code). See the [guide](https://databrickslabs.github.io/dqx/docs/guide/actions_and_alerts) for details.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver

# Engine-level user_metadata is attached to every alert payload (Slack "Metadata" section, Teams
# facts, webhook fields, and the driver-log line), so run-level context reaches the notification.
extra_params = ExtraParams(user_metadata={"pipeline": "alerting_demo", "team": "data-quality"})

observer = DQMetricsObserver(name="alerting_demo")
engine = DQEngine(WorkspaceClient(), observer=observer, actions=actions, extra_params=extra_params)

checked_df, observation = engine.apply_checks_by_metadata(df, checks)

# Materialize the observation so metrics are populated.
checked_df.count()

results = engine.evaluate_actions(observation.get, input_location="in-memory demo dataframe")

for result in results:
    print(f"action={result.action_name} fired={result.fired} status={result.status.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end: let the save method fire the actions automatically
# MAGIC
# MAGIC The example above calls `evaluate_actions` by hand because it works on an in-memory DataFrame. In a real pipeline you normally read from and write to Unity Catalog tables, and the **save methods** fire the configured actions for you — no manual `evaluate_actions` call. Below we write the demo data to an input table, then let `apply_checks_by_metadata_and_save_in_table` apply the checks, save the results, and fire the alert automatically.
# MAGIC
# MAGIC We also pass an `action_events_config` pointing at an events table. This does two things: DQX **records every action evaluation** there (one row per evaluation, including ones that did *not* fire) as a durable audit log, and it **persists the alert-suppression state** so frequency (`HOURLY`/`DAILY`) and `STATUS_CHANGE` gating survive across engine restarts (e.g. each scheduled job run seeds its state from this table).
# MAGIC
# MAGIC The alert here uses `notify_on=NotifyOn.STATUS_CHANGE`, so it notifies only on the transition *into* an unhealthy state. On this first run against a fresh events table that transition happens, so the alert fires; because state is persisted, re-running the cell while the data stays unhealthy suppresses the repeat notification (the evaluation is still recorded in history with `fired=false`).
# MAGIC
# MAGIC `action_events_config` is optional. Without it, `STATUS_CHANGE` (and the frequency windows) still work — the suppression state is just kept **in memory** for the engine's lifetime, so it does not survive restarts: a fresh engine treats the first evaluation of each run as a new transition and fires. Configuring the events table is what makes that suppression durable across runs.
# MAGIC
# MAGIC This step writes to `{demo_catalog}.{demo_schema}` (see the widgets at the top). Look again for the `[DQX alert]` line in the driver log — this time DQX fired it without an explicit call.

# COMMAND ----------

from databricks.labs.dqx.config import ActionEventsConfig, InputConfig, OutputConfig
from databricks.labs.dqx.actions import NotifyOn

# Persist the demo data to an input table so the save method can read it.
input_table = f"{demo_catalog_name}.{demo_schema_name}.dqx_alerting_demo_input"
output_table = f"{demo_catalog_name}.{demo_schema_name}.dqx_alerting_demo_output"
events_table = f"{demo_catalog_name}.{demo_schema_name}.dqx_alerting_demo_events"

df.write.mode("overwrite").saveAsTable(input_table)

e2e_observer = DQMetricsObserver(name="alerting_e2e_demo")
e2e_engine = DQEngine(
    WorkspaceClient(),
    observer=e2e_observer,
    actions=[
        DQAction(
            condition="error_row_count > 0",
            action=DQAlert(name="alert_on_errors", destinations=destinations, notify_on=NotifyOn.STATUS_CHANGE),
        ),
    ],
    action_events_config=ActionEventsConfig(location=events_table),
    extra_params=extra_params,  # reuse the engine-level user_metadata so it appears in the e2e alerts too
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
