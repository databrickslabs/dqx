"""Integration tests for *LakebaseActionEventStore* against a real Lakebase (PostgreSQL) instance.

These tests require a live Databricks workspace with a Lakebase instance and are NOT meant to run
locally. They exercise the real Postgres ``DISTINCT ON`` deduplication path in
*load_latest_per_action* / *load_last_fired_per_action* — which the unit tests can only mock — so
that a wrong ``ORDER BY`` (which still compiles) is caught here against an actual database.

Run with:
    .venv/bin/pytest tests/integration/test_action_event_storage_lakebase.py -v
"""

from datetime import datetime, timezone

from databricks.labs.dqx.actions.base import ActionStatus
from databricks.labs.dqx.actions.event_storage import LakebaseActionEventStore
from databricks.labs.dqx.actions.state import AlertEvent
from databricks.labs.dqx.config import LakebaseActionsStorageConfig


def _create_lakebase_location(database_name: str, make_random) -> str:
    schema_name = f"dqx_events_{make_random(10).lower()}"
    table_name = f"events_{make_random(10).lower()}"
    return f"{database_name}.{schema_name}.{table_name}"


def _event(action_name: str, run_time: datetime, *, fired: bool, status: ActionStatus, run_id: str) -> AlertEvent:
    return AlertEvent(
        action_name=action_name,
        condition="error_row_count > 0",
        fired=fired,
        status=status,
        observed_metrics={"error_row_count": 5},
        run_id=run_id,
        run_time=run_time,
        input_location="catalog.schema.table",
        destinations=["slack-dest"],
        delivery_errors=[],
    )


def test_lakebase_load_latest_and_last_fired_dedup_via_distinct_on(
    ws, spark, make_lakebase_instance, lakebase_client_id, make_random
) -> None:
    """The real Postgres DISTINCT ON must return the most-recent row per action, not an arbitrary one.

    Appends an older event AFTER a newer one (and a suppressed non-fired newest for one action) so a
    query that ordered wrong, or deduped on the insert order, would return the wrong row. Asserts the
    latest-overall and latest-fired rows are selected per action.
    """
    instance = make_lakebase_instance()
    location = _create_lakebase_location(instance.database_name, make_random)
    config = LakebaseActionsStorageConfig(location=location, client_id=lakebase_client_id, instance_name=instance.name)
    store = LakebaseActionEventStore(spark=spark, ws=ws, config=config)

    older = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    newer = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    newest = datetime(2024, 6, 1, 14, 0, 0, tzinfo=timezone.utc)

    # action-a: fired at newer, then a later suppressed (non-fired) evaluation at newest.
    # action-b: an older fired row appended AFTER a newer fired row (insert order != time order).
    store.append([_event("action-a", newer, fired=True, status=ActionStatus.UNHEALTHY, run_id="a-fire")])
    store.append([_event("action-b", newer, fired=True, status=ActionStatus.UNHEALTHY, run_id="b-new")])
    store.append([_event("action-a", newest, fired=False, status=ActionStatus.UNHEALTHY, run_id="a-suppress")])
    store.append([_event("action-b", older, fired=True, status=ActionStatus.UNHEALTHY, run_id="b-old")])

    latest = store.load_latest_per_action()
    last_fired = store.load_last_fired_per_action()

    # Latest overall: action-a -> the newest (suppressed, non-fired) row; action-b -> the newer fired row.
    assert latest["action-a"].run_id == "a-suppress"
    assert latest["action-a"].run_time == newest
    assert latest["action-b"].run_id == "b-new"
    assert latest["action-b"].run_time == newer

    # Latest *fired*: action-a -> the fired row at 'newer' (the newest is non-fired and must be excluded);
    # action-b -> the newer fired row despite the older one being inserted last.
    assert last_fired["action-a"] == newer
    assert last_fired["action-b"] == newer
