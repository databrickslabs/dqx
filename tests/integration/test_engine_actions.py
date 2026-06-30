"""Integration tests for Task 14: actions wired into DQEngine batch flow.

These tests verify that *evaluate_actions* is called correctly by
*apply_checks_and_save_in_table* after a batch run completes.

Prerequisites:
- A Databricks workspace accessible via *WorkspaceClient* (*ws* fixture).
- An active *SparkSession* (*spark* fixture from pytester).
- A UC catalog named by *TEST_CATALOG*.

Run with::

    .venv/bin/pytest tests/integration/test_engine_actions.py -v
"""

from __future__ import annotations

import pytest

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.base import ActionContext, DQAction
from databricks.labs.dqx.actions.destinations import CallbackDQAlertDestination
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import PipelineFailedError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQRowRule

from tests.constants import TEST_CATALOG


# ---------------------------------------------------------------------------
# Test: FailPipeline action raises PipelineFailedError on condition match
# ---------------------------------------------------------------------------


def test_apply_checks_and_save_raises_pipeline_failed_error(ws, spark, make_schema, make_random):
    """*FailPipeline* with a matching condition aborts the run with *PipelineFailedError*."""
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create a table with one NULL row so error_row_count > 0.
    test_schema = "id: int, name: string"
    test_df = spark.createDataFrame([[1, "alice"], [None, "bob"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    checks = [
        DQRowRule(
            name="id_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="id",
        )
    ]

    observer = DQMetricsObserver()
    fail_action = DQAction(
        action=FailPipeline(message="Data quality failure detected"),
        condition="error_row_count > 0",
        name="fail_on_errors",
    )

    engine = DQEngine(ws, spark=spark, observer=observer, actions=[fail_action])

    with pytest.raises(PipelineFailedError, match="Data quality failure detected"):
        engine.apply_checks_and_save_in_table(
            checks=checks,
            input_config=InputConfig(location=input_table),
            output_config=OutputConfig(location=output_table, mode="overwrite"),
        )


# ---------------------------------------------------------------------------
# Test: CallbackDQAlertDestination fires once with populated metrics
# ---------------------------------------------------------------------------


def test_apply_checks_and_save_fires_callback_with_metrics(ws, spark, make_schema, make_random):
    """*CallbackDQAlertDestination* receives a populated *ActionContext* with metrics."""
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data: one null row so error_row_count > 0.
    test_schema = "id: int, name: string"
    test_df = spark.createDataFrame([[1, "alice"], [None, "bob"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    checks = [
        DQRowRule(
            name="id_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="id",
        )
    ]

    received_contexts: list[ActionContext] = []

    def capture_callback(_message: AlertMessage, context: ActionContext) -> None:
        received_contexts.append(context)

    callback_dest = CallbackDQAlertDestination(name="capture", callback=capture_callback)
    alert = DQAlert(destinations=[callback_dest])
    action = DQAction(
        action=alert,
        condition="error_row_count > 0",
        name="alert_on_errors",
    )

    observer = DQMetricsObserver()
    engine = DQEngine(ws, spark=spark, observer=observer, actions=[action])

    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
    )

    assert len(received_contexts) == 1, "Callback must fire exactly once"
    ctx = received_contexts[0]
    # error_row_count should be > 0 since one row has a NULL id
    error_count = ctx.metrics.get("error_row_count")
    assert isinstance(error_count, int) and error_count > 0, f"Expected error_row_count > 0, got {error_count!r}"
    assert ctx.input_location == input_table
