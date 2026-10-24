"""Tests for the review trail of tables, collections and rules (``ReviewEventService``)."""

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.review_events import (
    ReviewEventService,
    rule_history_action,
)
from databricks_labs_dqx_app.backend.sql_executor import RawSql, SqlExecutor


@pytest.fixture
def sql():
    mock = create_autospec(SqlExecutor, instance=True)
    mock.fqn.side_effect = lambda t: f"cat.sch.{t}"
    return mock


@pytest.fixture
def service(sql):
    return ReviewEventService(sql)


class TestRecord:
    def test_records_the_step_with_who_the_note_and_the_version(self, service, sql):
        service.record("monitored_table", "b1", "approved", "bob@x", rationale="looks right", version=4)
        (table,) = sql.insert.call_args.args
        values = sql.insert.call_args.kwargs["values"]
        assert table == "cat.sch.dq_review_events"
        assert {k: values[k] for k in ("object_type", "object_id", "action", "actor", "rationale", "version")} == {
            "object_type": "monitored_table",
            "object_id": "b1",
            "action": "approved",
            "actor": "bob@x",
            "rationale": "looks right",
            "version": 4,
        }
        assert isinstance(values["created_at"], RawSql)

    def test_a_failed_write_does_not_raise(self, service, sql):
        sql.insert.side_effect = RuntimeError("relation does not exist")
        service.record("data_product", "p1", "submitted", "alice@x")


class TestListEvents:
    def test_newest_first_with_unknown_actions_left_out(self, service, sql):
        sql.select_rows.return_value = [
            ["submitted", "alice@x", "2026-10-01 09:00:00+00:00", "please review", None],
            ["approved", "bob@x", "2026-10-02 09:00:00+00:00", None, "3"],
            ["created", "alice@x", "2026-10-03 09:00:00+00:00", None, None],
        ]
        events = service.list_events("data_product", "p1")
        assert [(e.action, e.actor, e.version) for e in events] == [
            ("approved", "bob@x", 3),
            ("submitted", "alice@x", None),
        ]
        assert events[1].rationale == "please review"
        assert sql.select_rows.call_args.kwargs["where"] == {"object_type": "data_product", "object_id": "p1"}

    def test_mixed_timestamps_sort_without_error(self, service, sql):
        sql.select_rows.return_value = [
            ["submitted", "a", "2026-10-01T09:00:00", None, None],
            ["rejected", "b", "2026-10-02 09:00:00+00:00", "no", None],
            ["withdrawn", "c", None, None, None],
        ]
        assert [e.action for e in service.list_events("monitored_table", "b1")] == [
            "rejected",
            "submitted",
            "withdrawn",
        ]


class TestRuleEvents:
    @pytest.mark.parametrize(
        ("action", "prev", "new", "rationale", "expected"),
        [
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
            ("create", None, "draft", None, None),
            ("update", "draft", "draft", None, None),
        ],
    )
    def test_history_rows_map_to_review_steps(self, action, prev, new, rationale, expected):
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
        ]
        assert sql.select_rows.call_args.args[0] == "cat.sch.dq_rules_history"
        assert sql.select_rows.call_args.kwargs["where"] == {"rule_id": "r1"}
