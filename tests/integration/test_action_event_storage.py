"""Integration tests for *TableActionEventStore*.

These tests require a live Databricks workspace with Unity Catalog access.
They are NOT meant to be run locally; they execute against a real Spark
session and UC table.

Prerequisites:
- *ws*, *spark*, *make_random*, *make_schema* fixtures from *databricks-labs-pytester*.
- The *TEST_CATALOG* Unity Catalog must be accessible.

Run with:
    .venv/bin/pytest tests/integration/test_action_event_storage.py -v

Note: these tests create and drop a UC Delta table per test using the
*factory* cleanup pattern so that failures still clean up.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from databricks.labs.pytester.fixtures.baseline import factory

from databricks.labs.dqx.actions.base import ActionStatus
from databricks.labs.dqx.actions.event_storage import TableActionEventStore
from databricks.labs.dqx.actions.state import AlertEvent
from databricks.labs.dqx.config import ActionEventsConfig

from tests.constants import TEST_CATALOG


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def make_events_table(ws, spark, make_schema, make_random):
    """Fixture that creates a UC events table and ensures cleanup on test exit.

    Yields a callable *create(table_name) -> str* that returns the fully
    qualified table name.  The factory pattern guarantees the table is dropped
    even when the test fails.
    """

    def create() -> str:
        schema = make_schema(catalog_name=TEST_CATALOG)
        table_name = f"events_{make_random(6).lower()}"
        full_name = f"{TEST_CATALOG}.{schema.name}.{table_name}"
        return full_name

    def delete(full_name: str) -> None:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")

    yield from factory("events_table", create, delete)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_append_and_load_latest_per_action(ws, spark, make_events_table) -> None:
    """append() persists events; load_latest_per_action() returns most recent per action."""
    full_name = make_events_table()
    config = ActionEventsConfig(location=full_name)
    store = TableActionEventStore(spark=spark, ws=ws, config=config)

    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    later_time = datetime(2024, 6, 1, 13, 0, 0, tzinfo=timezone.utc)

    event_a_old = AlertEvent(
        action_name="action-a",
        condition="errors > 0",
        fired=True,
        status=ActionStatus.UNHEALTHY,
        observed_metrics={"errors": 5},
        run_id="run-001",
        run_time=base_time,
        input_location="catalog.schema.table",
        destinations=["slack-dest"],
        delivery_errors=[],
    )
    event_a_new = AlertEvent(
        action_name="action-a",
        condition="errors > 0",
        fired=True,
        status=ActionStatus.UNHEALTHY,
        observed_metrics={"errors": 3},
        run_id="run-002",
        run_time=later_time,
        input_location="catalog.schema.table",
        destinations=["slack-dest"],
        delivery_errors=[],
    )
    event_b = AlertEvent(
        action_name="action-b",
        condition=None,
        fired=False,
        status=ActionStatus.HEALTHY,
        observed_metrics={},
        run_id="run-001",
        run_time=base_time,
        input_location=None,
        destinations=[],
        delivery_errors=[],
    )

    store.append([event_a_old, event_b])
    store.append([event_a_new])

    latest = store.load_latest_per_action()

    assert set(latest.keys()) == {"action-a", "action-b"}

    # action-a: should return the newer event (run-002).
    assert latest["action-a"].run_id == "run-002"
    assert latest["action-a"].status == ActionStatus.UNHEALTHY
    assert latest["action-a"].fired is True

    # action-b: only one event.
    assert latest["action-b"].run_id == "run-001"
    assert latest["action-b"].status == ActionStatus.HEALTHY
    assert latest["action-b"].fired is False


def test_load_latest_returns_empty_when_table_missing(ws, spark, make_schema, make_random) -> None:
    """load_latest_per_action() returns an empty dict when the table does not exist."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    full_name = f"{TEST_CATALOG}.{schema.name}.nonexistent_{make_random(6).lower()}"
    config = ActionEventsConfig(location=full_name)
    store = TableActionEventStore(spark=spark, ws=ws, config=config)

    result = store.load_latest_per_action()
    assert not result


def test_append_empty_events_is_noop(ws, spark, make_events_table) -> None:
    """append() with an empty list does not create the table or raise."""
    full_name = make_events_table()
    config = ActionEventsConfig(location=full_name)
    store = TableActionEventStore(spark=spark, ws=ws, config=config)

    store.append([])  # should not raise

    # Table should still not exist (noop).
    result = store.load_latest_per_action()
    assert not result


def test_observed_metrics_round_trip(ws, spark, make_events_table) -> None:
    """observed_metrics values are preserved as dict[str, object] after a round-trip."""
    full_name = make_events_table()
    config = ActionEventsConfig(location=full_name)
    store = TableActionEventStore(spark=spark, ws=ws, config=config)

    event = AlertEvent(
        action_name="metrics-action",
        condition=None,
        fired=True,
        status=ActionStatus.UNHEALTHY,
        observed_metrics={"error_count": 42, "warn_count": 7},
        run_id="run-xyz",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        input_location=None,
        destinations=[],
        delivery_errors=[],
    )
    store.append([event])

    latest = store.load_latest_per_action()
    assert "metrics-action" in latest
    # Values are serialized as str in the MAP<STRING,STRING> schema.
    assert latest["metrics-action"].observed_metrics["error_count"] == "42"
    assert latest["metrics-action"].observed_metrics["warn_count"] == "7"
