"""Tests for the lifecycle history of rules, tables and collections (``LifecycleHistoryService``)."""

from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.lifecycle_history import LifecycleHistoryService
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
    @pytest.mark.parametrize("object_type", ["registry_rule", "monitored_table", "data_product"])
    def test_every_kind_goes_to_the_one_history_table(self, service, sql, object_type):
        service.record(
            object_type,
            "id1",
            "approve",
            "bob@x",
            prev_status="pending_approval",
            new_status="approved",
            version=4,
            rationale="looks right",
        )
        (table,) = sql.insert.call_args.args
        values = sql.insert.call_args.kwargs["values"]
        assert table == "cat.sch.dq_lifecycle_history"
        assert {
            k: values[k] for k in ("object_type", "object_id", "action", "prev_status", "new_status", "version")
        } == {
            "object_type": object_type,
            "object_id": "id1",
            "action": "approve",
            "prev_status": "pending_approval",
            "new_status": "approved",
            "version": 4,
        }
        assert (values["rationale"], values["changed_by"], values["details"], values["is_automatic"]) == (
            "looks right",
            "bob@x",
            None,
            False,
        )
        assert isinstance(values["changed_at"], RawSql)

    def test_details_are_written_as_json(self, service, sql):
        service.record("data_product", "p1", "update", "alice@x", details={"member": "b2"})
        values = sql.insert.call_args.kwargs["values"]
        assert isinstance(values["details"], RawSql)
        sql.json_literal_expr.assert_called_once_with('{"member": "b2"}')

    def test_an_auto_approval_is_stored_unmarked_and_flagged(self, service, sql):
        service.record("monitored_table", "b1", "approve", "bob@x (auto)", version=2)
        values = sql.insert.call_args.kwargs["values"]
        assert (values["changed_by"], values["is_automatic"]) == ("bob@x", True)

    def test_a_replayed_step_keeps_its_time(self, service, sql):
        at = datetime(2026, 9, 1, 8, 30, tzinfo=timezone.utc)
        service.record("monitored_table", "b1", "submit", "alice@x", changed_at=at)
        assert sql.insert.call_args.kwargs["values"]["changed_at"] == "2026-09-01T08:30:00+00:00"

    def test_a_failed_write_does_not_raise(self, service, sql):
        sql.insert.side_effect = RuntimeError("relation does not exist")
        service.record("data_product", "p1", "submit", "alice@x")


class TestListEvents:
    @staticmethod
    def _rows(sql, steps, without_snapshot=()):
        """The steps read, then the ids of those that recorded no snapshot."""
        sql.select_rows.side_effect = [steps, [[str(i)] for i in without_snapshot]]

    def test_newest_first_with_unshown_steps_left_out(self, service, sql):
        self._rows(
            sql,
            [
                ["create", "alice@x", False, "2026-10-01 08:00:00+00:00", None, None, "1"],
                ["submit", "alice@x", False, "2026-10-01 09:00:00+00:00", "please review", None, "2"],
                ["approve", "bob@x", False, "2026-10-02 09:00:00+00:00", None, "3", "3"],
                ["update", "alice@x", False, "2026-10-03 09:00:00+00:00", None, None, "4"],
                ["deprecate", "bob@x", False, "2026-10-04 09:00:00+00:00", None, None, "5"],
                ["delete", "alice@x", False, "2026-10-05 09:00:00+00:00", None, None, "6"],
            ],
            without_snapshot=(2, 3, 4),
        )
        events = service.list_events("data_product", "p1")
        assert [(e.action, e.actor, e.version, e.history_id) for e in events] == [
            ("edited", "alice@x", None, 4),
            ("approved", "bob@x", 3, 3),
            ("submitted", "alice@x", None, 2),
            ("created", "alice@x", None, 1),
        ]
        assert [e.has_snapshot for e in events] == [False, False, False, True]
        assert events[2].rationale == "please review"
        steps_call, snapshotless_call = sql.select_rows.call_args_list
        assert steps_call.args[0] == "cat.sch.dq_lifecycle_history"
        assert steps_call.kwargs["where"] == {"object_type": "data_product", "object_id": "p1"}
        # The snapshots themselves are never read to list the steps.
        assert "snapshot" not in steps_call.args[1]
        assert snapshotless_call.kwargs["where"] == {"object_type": "data_product", "object_id": "p1", "snapshot": None}

    def test_a_rule_reads_the_same_table(self, service, sql):
        self._rows(sql, [["withdraw", "alice@x", False, "2026-10-01 09:00:00+00:00", None, "1", "7"]])
        events = service.list_events("registry_rule", "r1")
        assert [(e.action, e.version) for e in events] == [("withdrawn", None)]
        assert sql.select_rows.call_args_list[0].kwargs["where"] == {"object_type": "registry_rule", "object_id": "r1"}

    @pytest.mark.parametrize("flag", [True, "true", "t"])
    def test_an_automatic_step_keeps_its_auto_marker(self, service, sql, flag):
        self._rows(sql, [["approve", "bob@x", flag, "2026-10-02 09:00:00+00:00", None, "2", "8"]])
        (event,) = service.list_events("monitored_table", "b1")
        assert event.actor == "bob@x (auto)"

    def test_mixed_timestamps_sort_without_error(self, service, sql):
        self._rows(
            sql,
            [
                ["submit", "a", False, "2026-10-01T09:00:00", None, None, "9"],
                ["reject", "b", False, "2026-10-02 09:00:00+00:00", "no", None, "10"],
                ["withdraw", "c", False, None, None, None, "11"],
            ],
        )
        actions = [e.action for e in service.list_events("monitored_table", "b1")]
        assert actions == ["rejected", "submitted", "withdrawn"]

    def test_steps_recorded_at_the_same_time_keep_their_recorded_order(self, service, sql):
        at = "2026-10-01 09:00:00+00:00"
        self._rows(
            sql,
            [["update", "a", False, at, None, None, "4"], ["submit", "a", False, at, None, None, "5"]],
        )
        assert [e.history_id for e in service.list_events("monitored_table", "b1")] == [5, 4]


class TestSnapshots:
    def test_a_step_snapshots_the_object_as_it_is_now(self, sql):
        service = LifecycleHistoryService(sql, snapshotters={"monitored_table": lambda oid: [{"name": oid}]})
        service.record("monitored_table", "b1", "update", "alice@x")
        sql.json_literal_expr.assert_any_call('[{"name": "b1"}]')
        assert isinstance(sql.insert.call_args.kwargs["values"]["snapshot"], RawSql)

    def test_a_delete_and_a_kind_without_a_snapshotter_record_no_snapshot(self, sql):
        service = LifecycleHistoryService(sql, snapshotters={"monitored_table": lambda oid: [{"name": oid}]})
        service.record("monitored_table", "b1", "delete", "alice@x")
        assert sql.insert.call_args.kwargs["values"]["snapshot"] is None
        service.record("data_product", "p1", "update", "alice@x")
        assert sql.insert.call_args.kwargs["values"]["snapshot"] is None

    def test_an_explicit_snapshot_is_recorded_as_given(self, sql):
        service = LifecycleHistoryService(sql, snapshotters={"monitored_table": lambda oid: ["now"]})
        service.record("monitored_table", "b1", "submit", "alice@x", snapshot=["then"])
        sql.json_literal_expr.assert_any_call('["then"]')

    def test_a_failed_snapshot_still_records_the_step(self, sql):
        def broken(_oid):
            raise RuntimeError("gone")

        service = LifecycleHistoryService(sql, snapshotters={"monitored_table": broken})
        service.record("monitored_table", "b1", "update", "alice@x")
        assert sql.insert.call_args.kwargs["values"]["snapshot"] is None


class TestGetSnapshot:
    def test_reads_the_state_a_step_left(self, service, sql):
        sql.select_rows.return_value = [['["a", "b"]']]
        assert service.get_snapshot("monitored_table", "b1", 7) == (True, ["a", "b"])
        assert sql.select_rows.call_args.kwargs["where"] == {
            "object_type": "monitored_table",
            "object_id": "b1",
            "history_id": 7,
        }

    def test_a_step_without_a_snapshot_and_an_unknown_step(self, service, sql):
        sql.select_rows.return_value = [[None]]
        assert service.get_snapshot("monitored_table", "b1", 7) == (True, None)
        sql.select_rows.return_value = []
        assert service.get_snapshot("monitored_table", "b1", 99) == (False, None)
