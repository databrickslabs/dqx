"""Integration tests for actions wired into DQEngine batch and streaming flows.

These tests verify that *evaluate_actions* is called correctly by
*apply_checks_and_save_in_table* after a batch run completes and that actions
are evaluated per streaming micro-batch via *StreamingMetricsListener*.

Prerequisites:
- A Databricks workspace accessible via *WorkspaceClient* (*ws* fixture).
- An active *SparkSession* (*spark* fixture from pytester).
- A UC catalog named by *TEST_CATALOG*.

Run with::

    .venv/bin/pytest tests/integration/test_engine_actions.py -v
"""

from datetime import timedelta

import pytest

from databricks.sdk.retries import retried

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.base import ActionContext
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.destinations import CallbackDQAlertDestination, SlackDQAlertDestination
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.manager import DQActionManager
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.actions.secret_field import DQSecret
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import PipelineFailedError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQRowRule

from tests.constants import TEST_CATALOG


# ---------------------------------------------------------------------------
# Test: FailPipeline action raises PipelineFailedError on condition match
# ---------------------------------------------------------------------------


def test_apply_checks_and_save_raises_pipeline_failed_error(ws, spark, make_schema, make_random) -> None:
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


def test_apply_checks_and_save_fires_callback_with_metrics(ws, spark, make_schema, make_random) -> None:
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


# ---------------------------------------------------------------------------
# Test: streaming run fires callback destination per micro-batch
# ---------------------------------------------------------------------------


def _make_streaming_action(received_contexts: list[ActionContext]) -> DQAction:
    """Build a *DQAction* with a callback that appends contexts to *received_contexts*."""

    def capture_callback(_message: AlertMessage, context: ActionContext) -> None:
        received_contexts.append(context)

    callback_dest = CallbackDQAlertDestination(name="capture_streaming", callback=capture_callback)
    alert = DQAlert(destinations=[callback_dest])
    return DQAction(action=alert, condition="error_row_count > 0", name="alert_on_errors_streaming")


def test_streaming_apply_checks_fires_callback_per_microbatch(ws, spark, make_schema, make_volume, make_random) -> None:
    """*CallbackDQAlertDestination* receives an *ActionContext* from each streaming micro-batch.

    This test uses *availableNow=True* so the stream processes all available
    data as a single micro-batch and terminates, allowing a synchronous assert.
    """
    schema = make_schema(catalog_name=TEST_CATALOG)
    volume_name = make_volume(catalog_name=TEST_CATALOG, schema_name=schema.name).name

    input_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}"
    metrics_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}"
    checkpoint_location = f"/Volumes/{TEST_CATALOG}/{schema.name}/{volume_name}/{make_random(8).lower()}"

    # One null row so error_row_count > 0 and the callback condition is met.
    test_df = spark.createDataFrame([[1, "alice"], [None, "bob"]], "id: int, name: string")
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    checks = [DQRowRule(name="id_not_null", criticality="error", check_func=check_funcs.is_not_null, column="id")]

    received_contexts: list[ActionContext] = []
    action = _make_streaming_action(received_contexts)

    observer = DQMetricsObserver()
    engine = DQEngine(ws, spark=spark, observer=observer, actions=[action])

    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table, is_streaming=True),
        output_config=OutputConfig(
            location=output_table,
            options={"checkPointLocation": checkpoint_location},
            trigger={"availableNow": True},
        ),
        metrics_config=OutputConfig(location=metrics_table),
    )

    # The listener bus flushes onQueryProgress callbacks asynchronously after the (availableNow) query
    # terminates, so poll with a bounded deadline rather than a fixed sleep (deterministic: returns as
    # soon as the callback fires, fails fast if it never does).
    @retried(on=[AssertionError], timeout=timedelta(minutes=2))
    def _wait_for_callback() -> None:
        assert received_contexts, "Callback must fire at least once for the streaming micro-batch"

    _wait_for_callback()

    ctx = received_contexts[0]
    error_count = ctx.metrics.get("error_row_count")
    assert isinstance(error_count, int) and error_count > 0, f"Expected error_row_count > 0, got {error_count!r}"
    assert ctx.input_location == input_table


# ---------------------------------------------------------------------------
# Test: actions defined as declarative metadata dicts fire end-to-end
# ---------------------------------------------------------------------------


def test_apply_checks_and_save_fires_action_from_metadata(ws, spark, make_schema, make_random) -> None:
    """A *FailPipeline* action defined as a metadata dict aborts the run against real metrics.

    Exercises the *DQEngine* dict-normalization path (*actions* passed as
    ``list[dict]`` rather than *DQAction* instances) end-to-end: the dict is
    deserialized at construction time and evaluated against the observed
    ``error_row_count`` after the batch save completes.
    """
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    test_df = spark.createDataFrame([[1, "alice"], [None, "bob"]], "id: int, name: string")
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    checks = [
        DQRowRule(name="id_not_null", criticality="error", check_func=check_funcs.is_not_null, column="id"),
    ]

    action_dicts: list[DQAction | dict[str, object]] = [
        {
            "action": {"type": "fail_pipeline", "message": "Metadata-defined failure"},
            "condition": "error_row_count > 0",
            "name": "fail_from_metadata",
        }
    ]

    observer = DQMetricsObserver()
    engine = DQEngine(ws, spark=spark, observer=observer, actions=action_dicts)

    with pytest.raises(PipelineFailedError, match="Metadata-defined failure"):
        engine.apply_checks_and_save_in_table(
            checks=checks,
            input_config=InputConfig(location=input_table),
            output_config=OutputConfig(location=output_table, mode="overwrite"),
        )


# ---------------------------------------------------------------------------
# Test: local-file save / load round-trip reconstructs actions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("filename", ["actions.yml", "actions.json"])
def test_local_file_action_round_trip(tmp_path, filename) -> None:
    """*save_actions_in_local_file* / *load_actions_from_local_file* round-trip for YAML and JSON."""
    actions = [
        DQAction(
            action=DQAlert(
                destinations=[SlackDQAlertDestination(name="ops", webhook_url=DQSecret(scope="s", key="k"))]
            ),
            condition="error_row_count > 0",
            name="alert_from_file",
        ),
        DQAction(action=FailPipeline(message="boom"), condition="error_row_count > 10", name="fail_from_file"),
    ]

    filepath = str(tmp_path / filename)
    DQActionManager.save_actions_in_local_file(actions, filepath)
    loaded = DQActionManager.load_actions_from_local_file(filepath)

    assert len(loaded) == 2
    alert = loaded[0].action
    assert isinstance(alert, DQAlert)
    destination = alert.destinations[0]
    assert isinstance(destination, SlackDQAlertDestination)
    assert isinstance(destination.webhook_url, DQSecret)
    fail = loaded[1].action
    assert isinstance(fail, FailPipeline)
    assert fail.message == "boom"
    assert loaded[1].condition == "error_row_count > 10"
    assert loaded[1].name == "fail_from_file"
