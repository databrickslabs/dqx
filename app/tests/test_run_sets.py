"""Tests for :class:`RunSetService` — the Data Products Task 3 run-set tracking service.

Drives the service against two spec-bound ``SqlExecutor`` mocks (the OLTP
executor owning ``dq_run_sets``/``dq_run_set_members``, and the Delta
executor owning ``dq_validation_runs``) with per-test ``query``/
``query_dicts`` dispatchers — no Spark or workspace needed. Covers
mint-and-join, member counts, and the aggregated-status precedence
(running > failed > canceled > success).
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

_RUN_SETS = "dqx_test.dqx_app_test.dq_run_sets"
_MEMBERS = "dqx_test.dqx_app_test.dq_run_set_members"
_RUNS = "dqx_test.dqx_app_test.dq_validation_runs"


def _mock_sql(fqn_map: dict[str, str] | None = None) -> SqlExecutor:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.dialect = "delta"
    mock.fqn.side_effect = lambda t: (fqn_map or {}).get(t, f"dqx_test.dqx_app_test.{t}")
    mock.q.side_effect = lambda i: f"`{i}`"
    mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    mock.query.return_value = []
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def oltp_sql():
    return _mock_sql({"dq_run_sets": _RUN_SETS, "dq_run_set_members": _MEMBERS})


@pytest.fixture
def validation_sql():
    return _mock_sql({"dq_validation_runs": _RUNS})


@pytest.fixture
def service(oltp_sql, validation_sql):
    return RunSetService(oltp_sql=oltp_sql, validation_sql=validation_sql)


class TestCreateAndAddMember:
    def test_create_inserts_run_set_row_and_returns_id(self, service, oltp_sql):
        run_set_id = service.create(
            product_id=None, product_version=None, source="approved", trigger="manual", created_by="alice@x"
        )
        assert run_set_id
        sql = oltp_sql.execute.call_args[0][0]
        assert f"INSERT INTO {_RUN_SETS}" in sql
        assert "'approved'" in sql
        assert "'manual'" in sql
        assert "NULL" in sql  # product_id / product_version

    def test_create_with_product_writes_product_columns(self, service, oltp_sql):
        service.create(
            product_id="prod-1", product_version=3, source="draft", trigger="scheduled", created_by="bot"
        )
        sql = oltp_sql.execute.call_args[0][0]
        assert "'prod-1'" in sql
        assert "3" in sql
        assert "'draft'" in sql
        assert "'scheduled'" in sql

    def test_add_member_inserts_member_row(self, service, oltp_sql):
        service.add_member("rs-1", "run-1", "b1", 2)
        sql = oltp_sql.execute.call_args[0][0]
        assert f"INSERT INTO {_MEMBERS}" in sql
        assert "'rs-1'" in sql
        assert "'run-1'" in sql
        assert "'b1'" in sql
        assert "2" in sql

    def test_add_member_null_binding_version_for_draft(self, service, oltp_sql):
        service.add_member("rs-1", "run-1", "b1", None)
        sql = oltp_sql.execute.call_args[0][0]
        assert "NULL" in sql

    def test_delete_empty_removes_the_run_set_row(self, service, oltp_sql):
        service.delete_empty("rs-1")
        sql = oltp_sql.execute.call_args[0][0]
        assert f"DELETE FROM {_RUN_SETS}" in sql
        assert "'rs-1'" in sql


class TestGet:
    def test_missing_run_set_raises_lookup_error(self, service, oltp_sql):
        oltp_sql.query.return_value = []
        with pytest.raises(LookupError):
            service.get("missing")

    def test_get_resolves_members_and_validation_counts(self, service, oltp_sql, validation_sql):
        def oltp_query(sql: str, **_: object) -> list[list[object]]:
            if _RUN_SETS in sql:
                return [["rs-1", None, None, "approved", "manual", "alice@x", "2026-07-07T00:00:00"]]
            if _MEMBERS in sql:
                return [["rs-1", "run-1", "b1", 2]]
            return []

        oltp_sql.query.side_effect = oltp_query
        validation_sql.query_dicts.return_value = [
            {
                "run_id": "run-1",
                "source_table_fqn": "cat.schema.tbl",
                "status": "SUCCESS",
                "total_rows": "100",
                "valid_rows": "90",
                "invalid_rows": "10",
                "error_rows": "5",
                "warning_rows": "5",
            }
        ]

        detail = service.get("rs-1")

        assert detail.run_set_id == "rs-1"
        assert detail.status == "success"
        assert len(detail.members) == 1
        member = detail.members[0]
        assert member.run_id == "run-1"
        assert member.binding_id == "b1"
        assert member.binding_version == 2
        assert member.table_fqn == "cat.schema.tbl"
        assert member.status == "SUCCESS"
        assert member.total_rows == 100
        assert member.valid_rows == 90

    def test_draft_member_has_null_binding_version(self, service, oltp_sql, validation_sql):
        def oltp_query(sql: str, **_: object) -> list[list[object]]:
            if _RUN_SETS in sql:
                return [["rs-1", None, None, "draft", "manual", "alice@x", "2026-07-07T00:00:00"]]
            if _MEMBERS in sql:
                return [["rs-1", "run-1", "b1", None]]
            return []

        oltp_sql.query.side_effect = oltp_query
        validation_sql.query_dicts.return_value = []

        detail = service.get("rs-1")
        assert detail.members[0].binding_version is None


class TestAggregatedStatus:
    """running > failed > canceled > success (design spec §4.2 / plan Task 3)."""

    @pytest.mark.parametrize(
        "statuses,expected",
        [
            (["SUCCESS", "RUNNING", "FAILED"], "running"),
            (["SUCCESS", "FAILED", "CANCELED"], "failed"),
            (["SUCCESS", "CANCELED"], "canceled"),
            (["SUCCESS", "SUCCESS"], "success"),
            ([], "success"),
            ([None, None], "success"),
        ],
    )
    def test_aggregate_status_precedence(self, statuses, expected):
        assert RunSetService._aggregate_status(statuses) == expected

    def test_list_for_product_aggregates_across_members(self, service, oltp_sql, validation_sql):
        def oltp_query(sql: str, **_: object) -> list[list[object]]:
            if _RUN_SETS in sql:
                return [["rs-1", "prod-1", 1, "approved", "manual", "alice@x", "2026-07-07T00:00:00"]]
            if _MEMBERS in sql:
                return [["rs-1", "run-1", "b1", 1], ["rs-1", "run-2", "b2", 1]]
            return []

        oltp_sql.query.side_effect = oltp_query
        validation_sql.query_dicts.return_value = [
            {"run_id": "run-1", "status": "SUCCESS"},
            {"run_id": "run-2", "status": "RUNNING"},
        ]

        summaries = service.list_for_product("prod-1")
        assert len(summaries) == 1
        assert summaries[0].member_count == 2
        assert summaries[0].status == "running"

    def test_list_for_product_empty_run_sets_returns_empty(self, service, oltp_sql):
        oltp_sql.query.return_value = []
        assert service.list_for_product("prod-1") == []


class TestRunSetIdsByRunId:
    def test_maps_run_ids_to_their_run_set_ids(self, service, oltp_sql):
        oltp_sql.query.return_value = [("r1", "set1"), ("r2", "set1"), ("r3", "set2")]
        result = service.run_set_ids_by_run_id(["r1", "r2", "r3"])
        assert result == {"r1": "set1", "r2": "set1", "r3": "set2"}
        sql = oltp_sql.query.call_args[0][0]
        assert f"FROM {_MEMBERS}" in sql
        assert "'r1'" in sql and "'r2'" in sql and "'r3'" in sql

    def test_empty_input_short_circuits_without_query(self, service, oltp_sql):
        assert service.run_set_ids_by_run_id([]) == {}
        oltp_sql.query.assert_not_called()

    def test_drops_rows_missing_either_key(self, service, oltp_sql):
        oltp_sql.query.return_value = [("r1", "set1"), (None, "set2"), ("r3", None)]
        assert service.run_set_ids_by_run_id(["r1"]) == {"r1": "set1"}
