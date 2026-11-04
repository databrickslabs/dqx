"""Tests for the lifecycle history of tables, collections and rules (``LifecycleHistoryService``)."""

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.lifecycle_history import (
    LifecycleHistoryService,
    rule_history_action,
)
from databricks_labs_dqx_app.backend.sql_executor import RawSql, SqlExecutor


@pytest.fixture
def sql():
    mock = create_autospec(SqlExecutor, instance=True)
    mock.fqn.side_effect = lambda t: f"cat.sch.{t}"
    mock.json_literal_expr.side_effect = lambda j: f"'{j}'::jsonb"
    return mock


@pytest.fixture
def service(sql):
    return LifecycleHistoryService(sql)


class TestRecord:
    def test_a_table_step_goes_to_the_table_history(self, service, sql):
        service.record(
            "monitored_table",
            "b1",
            "approve",
            "bob@x",
            prev_status="pending_approval",
            new_status="approved",
            version=4,
            rationale="looks right",
        )
        (table,) = sql.insert.call_args.args
        values = sql.insert.call_args.kwargs["values"]
        assert table == "cat.sch.dq_monitored_tables_history"
        assert {k: values[k] for k in ("binding_id", "action", "prev_status", "new_status", "version")} == {
            "binding_id": "b1",
            "action": "approve",
            "prev_status": "pending_approval",
            "new_status": "approved",
            "version": 4,
        }
        assert (values["rationale"], values["changed_by"], values["state"]) == ("looks right", "bob@x", None)
        assert isinstance(values["changed_at"], RawSql)

    def test_a_collection_step_goes_to_the_collection_history_with_its_state(self, service, sql):
        service.record("data_product", "p1", "update", "alice@x", state={"member": "b2"})
        (table,) = sql.insert.call_args.args
        values = sql.insert.call_args.kwargs["values"]
        assert table == "cat.sch.dq_data_products_history"
        assert values["product_id"] == "p1"
        assert isinstance(values["state"], RawSql)

    def test_a_failed_write_does_not_raise(self, service, sql):
        sql.insert.side_effect = RuntimeError("relation does not exist")
        service.record("data_product", "p1", "submit", "alice@x")


class TestListEvents:
    def test_newest_first_with_deletes_left_out(self, service, sql):
        sql.select_rows.return_value = [
            ["create", "alice@x", "2026-10-01 08:00:00+00:00", None, None],
            ["submit", "alice@x", "2026-10-01 09:00:00+00:00", "please review", None],
            ["approve", "bob@x", "2026-10-02 09:00:00+00:00", None, "3"],
            ["update", "alice@x", "2026-10-03 09:00:00+00:00", None, None],
            ["delete", "alice@x", "2026-10-04 09:00:00+00:00", None, None],
        ]
        events = service.list_events("data_product", "p1")
        assert [(e.action, e.actor, e.version) for e in events] == [
            ("edited", "alice@x", None),
            ("approved", "bob@x", 3),
            ("submitted", "alice@x", None),
            ("created", "alice@x", None),
        ]
        assert events[2].rationale == "please review"
        assert sql.select_rows.call_args.args[0] == "cat.sch.dq_data_products_history"
        assert sql.select_rows.call_args.kwargs["where"] == {"product_id": "p1"}

    def test_mixed_timestamps_sort_without_error(self, service, sql):
        sql.select_rows.return_value = [
            ["submit", "a", "2026-10-01T09:00:00", None, None],
            ["reject", "b", "2026-10-02 09:00:00+00:00", "no", None],
            ["withdraw", "c", None, None, None],
        ]
        actions = [e.action for e in service.list_events("monitored_table", "b1")]
        assert actions == ["rejected", "submitted", "withdrawn"]


class TestRuleEvents:
    @pytest.mark.parametrize(
        ("action", "prev", "new", "rationale", "expected"),
        [
            ("create", None, "draft", None, "created"),
            ("update", "draft", "draft", None, "edited"),
            ("approve", "pending_approval", "approved", None, "approved"),
            ("reject", "pending_approval", "approved", None, "rejected"),
            ("revoke", "pending_approval", "approved", None, "withdrawn"),
            ("status:pending_approval", "draft", "pending_approval", "ready", "submitted"),
            ("status:rejected", "pending_approval", "rejected", "no", "rejected"),
            ("status:draft", "pending_approval", "draft", None, "withdrawn"),
            # Older rows recorded both a rejection and a withdrawal of a revision
            # as status:approved; only a rejection carries a note.
            ("status:approved", "pending_approval", "approved", "too broad", "rejected"),
            ("status:approved", "pending_approval", "approved", None, "withdrawn"),
            ("status:approved", "deprecated", "approved", None, None),
            ("seed", None, "approved", None, None),
            ("delete", "approved", None, None, None),
        ],
    )
    def test_history_rows_map_to_steps(self, action, prev, new, rationale, expected):
        assert rule_history_action(action, prev, new, rationale) == expected

    def test_reads_the_rule_history_newest_first(self, service, sql):
        sql.select_rows.return_value = [
            ["create", None, "draft", None, "alice@x", "2026-10-01 08:00:00+00:00", "0"],
            [
                "status:pending_approval",
                "draft",
                "pending_approval",
                "ready",
                "alice@x",
                "2026-10-01 09:00:00+00:00",
                "0",
            ],
            ["approve", "pending_approval", "approved", "ok", "bob@x", "2026-10-02 09:00:00+00:00", "1"],
        ]
        events = service.list_rule_events("r1")
        assert [(e.action, e.actor, e.rationale, e.version) for e in events] == [
            ("approved", "bob@x", "ok", 1),
            ("submitted", "alice@x", "ready", None),
            ("created", "alice@x", None, None),
        ]
        assert sql.select_rows.call_args.args[0] == "cat.sch.dq_rules_history"
