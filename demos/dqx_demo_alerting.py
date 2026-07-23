# Databricks notebook source
# MAGIC %md
# MAGIC # Alerting with DQX Actions
# MAGIC
# MAGIC This short demo shows how to react automatically to data quality problems using DQX **actions**. After checks run, DQX evaluates each action's optional *condition* against the observed summary metrics and, when it matches, fires the action — for example sending an alert.
# MAGIC
# MAGIC We use a `LogDQAlertDestination`, which writes the alert to the driver log and contacts no external system, so the demo runs end-to-end anywhere. We also show how to add a Slack destination: supply a Slack incoming-webhook URL in the `slack_webhook_url` widget and the demo will additionally deliver to Slack.
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
    LogDQAlertDestination,
    SlackDQAlertDestination,
)

# Always log alerts; add Slack only when a webhook URL is supplied.
destinations = [LogDQAlertDestination(name="driver-log", level="warning")]
if slack_webhook_url:
    destinations.append(SlackDQAlertDestination(name="slack", webhook_url=slack_webhook_url))

actions = [
    DQAction(
        condition="error_row_count > 0",
        action=DQAlert(destinations=destinations),
    )
]

observer = DQMetricsObserver(name="alerting_demo")
engine = DQEngine(WorkspaceClient(), observer=observer, actions=actions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply the checks and evaluate the actions
# MAGIC
# MAGIC After applying checks we trigger a Spark action (`count()`) to materialize the observed metrics, then call `evaluate_actions`. The returned results describe which actions fired; the alert itself is written to the driver log (look for the `[DQX alert]` line above the cell output).

# COMMAND ----------

checked_df, observation = engine.apply_checks_by_metadata(df, checks)

# Materialize the observation so metrics are populated.
checked_df.count()

results = engine.evaluate_actions(observation.get, input_location="in-memory demo dataframe")

for result in results:
    print(f"action={result.action_name} fired={result.fired} status={result.status.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wiring a real Slack alert
# MAGIC
# MAGIC To deliver to Slack, create a [Slack incoming webhook](https://api.slack.com/messaging/webhooks) and pass its URL to the `slack_webhook_url` widget above (delivery is restricted to `hooks.slack.com`). In production, store the URL in a Databricks secret scope and reference it with `DQSecret` rather than passing a plaintext string:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.labs.dqx.config import DQSecret
# MAGIC from databricks.labs.dqx.actions import SlackDQAlertDestination
# MAGIC
# MAGIC SlackDQAlertDestination(
# MAGIC     name="slack",
# MAGIC     webhook_url=DQSecret(scope="dq-secrets", key="slack-webhook-url"),
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC You can also attach actions directly to `apply_checks_and_save_in_table` so they fire automatically after each save (batch or streaming) — see the guide for details.
