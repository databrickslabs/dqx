"""Integration tests for per-run-config action auto-loading in the multi-run-config runner.

Verifies that *DQEngine.apply_checks_and_save_in_tables* auto-loads action definitions from a run
config's *actions_location*, fires them, and persists action events to its *action_events_location* —
the workflow path for RunConfig-driven actions.

Prerequisites:
- A Databricks workspace accessible via *WorkspaceClient* (*ws* fixture).
- An active *SparkSession* (*spark* fixture from pytester).
- A UC catalog named by *TEST_CATALOG*.
"""

from __future__ import annotations

from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.destinations import LogDQAlertDestination
from databricks.labs.dqx.actions.manager import DQActionManager
from databricks.labs.dqx.config import (
    InputConfig,
    OutputConfig,
    RunConfig,
    TableActionsStorageConfig,
    TableChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine

from tests.constants import TEST_CATALOG


def test_run_config_auto_loads_fires_and_persists_action_events(ws, spark, make_schema, make_random) -> None:
    """A run config with *actions_location* auto-loads its actions, fires them, and records events."""
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    base = f"{catalog}.{schema}"
    input_table = f"{base}.{make_random(8).lower()}"
    output_table = f"{base}.{make_random(8).lower()}"
    checks_table = f"{base}.{make_random(8).lower()}"
    actions_table = f"{base}.{make_random(8).lower()}"
    events_table = f"{base}.{make_random(8).lower()}"
    run_config_name = "rc"

    # Input data with one NULL row so error_row_count > 0 and the alert condition fires.
    spark.createDataFrame([[1, "alice"], [None, "bob"]], "id: int, name: string").write.format("delta").mode(
        "overwrite"
    ).saveAsTable(input_table)

    engine = DQEngine(ws, spark)

    # Persist checks to a UC table scoped to the run config.
    checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
    engine.save_checks(
        config=TableChecksStorageConfig(location=checks_table, run_config_name=run_config_name),
        checks=checks,
    )

    # Persist an alert action (serializable log destination) to a UC actions table.
    action = DQAction(
        action=DQAlert(destinations=[LogDQAlertDestination(name="driver-log")]),
        condition="error_row_count > 0",
        name="alert_on_errors",
    )
    DQActionManager(ws=ws, spark=spark).save_actions(
        [action],
        TableActionsStorageConfig(location=actions_table, run_config_name=run_config_name),
    )

    run_config = RunConfig(
        name=run_config_name,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
        checks_location=checks_table,
        actions_location=actions_table,
        action_events_location=events_table,
    )

    engine.apply_checks_and_save_in_tables([run_config])

    # The action fired and its event was persisted to the events table.
    events = spark.read.table(events_table)
    fired = [row for row in events.collect() if row["fired"]]
    assert len(fired) == 1, f"Expected exactly one fired action event, got {len(fired)}"
    assert fired[0]["action_name"] == "alert_on_errors"


def test_run_config_without_actions_location_runs_without_actions(ws, spark, make_schema, make_random) -> None:
    """A run config without *actions_location* applies checks normally and fires no actions."""
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    base = f"{catalog}.{schema}"
    input_table = f"{base}.{make_random(8).lower()}"
    output_table = f"{base}.{make_random(8).lower()}"
    checks_table = f"{base}.{make_random(8).lower()}"
    run_config_name = "rc"

    spark.createDataFrame([[1, "alice"], [None, "bob"]], "id: int, name: string").write.format("delta").mode(
        "overwrite"
    ).saveAsTable(input_table)

    engine = DQEngine(ws, spark)
    checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
    engine.save_checks(
        config=TableChecksStorageConfig(location=checks_table, run_config_name=run_config_name),
        checks=checks,
    )

    run_config = RunConfig(
        name=run_config_name,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
        checks_location=checks_table,
    )

    # Should complete without error and produce the output table.
    engine.apply_checks_and_save_in_tables([run_config])
    assert spark.read.table(output_table).count() == 2
