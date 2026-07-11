"""Tests for :class:`MonitoredTableVersionService` — the Data Products Task 2
version freeze service.

Drives the service against a spec-bound ``SqlExecutor`` mock (dialect
helpers wired like the materializer's ``sql`` fixture, ``query`` given a
per-test dispatcher keyed on the emitted SQL) plus mocked
``MonitoredTableService`` / ``RulesCatalogService`` so no Spark or
workspace is needed. Asserts the freeze/re-freeze SQL and — critically —
that a frozen snapshot mirrors ONLY the binding's own approved rows
(scoped by ``applied_rule_id``), excluding directly-authored non-registry
approved rules on the same ``table_fqn``.
"""

from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    AppliedRuleSummary,
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService

_TABLES = "dqx_test.dqx_app_test.dq_monitored_tables"
_VERSIONS = "dqx_test.dqx_app_test.dq_monitored_table_versions"
_APPLIED = "dqx_test.dqx_app_test.dq_applied_rules"
_QUALITY = "dqx_test.dqx_app_test.dq_quality_rules"


@pytest.fixture
def sql(sql_executor_mock):
    sql_executor_mock.dialect = "delta"
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
    sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
    sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
    sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    sql_executor_mock.query.return_value = []
    return sql_executor_mock


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def rules_catalog():
    return create_autospec(RulesCatalogService, instance=True)


@pytest.fixture
def service(sql, monitored_tables, rules_catalog):
    return MonitoredTableVersionService(sql=sql, monitored_tables=monitored_tables, rules_catalog=rules_catalog)


def _check(applied_rule_id: str | None, column: str) -> dict:
    """A materialized check dict shaped like ``get_approved_checks_for_table`` output."""
    user_metadata: dict[str, str] = {"registry_rule_id": "r1", "registry_version": "1"}
    if applied_rule_id is not None:
        user_metadata["applied_rule_id"] = applied_rule_id
    return {
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": column}},
        "user_metadata": user_metadata,
    }


def _detail(applied_ids: list[str], table_fqn: str = "cat.schema.customers", status: str = "approved") -> MonitoredTableDetail:
    table = MonitoredTable(binding_id="b1", table_fqn=table_fqn, status=status)
    summaries = [
        AppliedRuleSummary(
            applied_rule=AppliedRule(id=aid, binding_id="b1", rule_id="r1", column_mapping=[{"column": aid}]),
            rule_name="Not Null",
            rule_dimension="Completeness",
            rule_severity="High",
        )
        for aid in applied_ids
    ]
    return MonitoredTableDetail(table=table, applied_rules=summaries)


def _dispatch(sql, mapping: dict[str, list]):
    """Wire ``sql.query`` to return the first mapping entry whose key is a SQL substring."""

    def _fn(query: str, **_kwargs):
        for needle, result in mapping.items():
            if needle in query:
                return result
        return []

    sql.query.side_effect = _fn


# ---------------------------------------------------------------------------
# freeze_new_version
# ---------------------------------------------------------------------------


class TestFreezeNewVersion:
    def test_bumps_version_and_freezes_scoped_checks(self, service, sql, monitored_tables, rules_catalog):
        monitored_tables.get.return_value = _detail(["ar1", "ar2"])
        # Table's approved rows include a directly-authored (non-registry) rule
        # on the same table_fqn — it must NOT be frozen into the binding snapshot.
        rules_catalog.get_approved_checks_for_table.return_value = [
            _check("ar1", "id"),
            _check("ar2", "name"),
            _check(None, "hand_authored"),  # direct rule, no applied_rule_id
        ]
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["1"]]})

        new_version = service.freeze_new_version("b1", "alice@x")

        assert new_version == 2
        # version bumped on dq_monitored_tables
        exec_sqls = [c.args[0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_TABLES} SET version = 2" in s for s in exec_sqls)
        assert any(f"INSERT INTO {_VERSIONS}" in s and "version = 2" not in s for s in exec_sqls)
        # frozen checks_json = the first json_literal_expr payload
        frozen_checks = json.loads(sql.json_literal_expr.call_args_list[0].args[0])
        frozen_ids = {c["user_metadata"].get("applied_rule_id") for c in frozen_checks}
        assert frozen_ids == {"ar1", "ar2"}
        assert all("applied_rule_id" in c["user_metadata"] for c in frozen_checks)

    def test_state_json_carries_applied_rule_display_metadata(self, service, sql, monitored_tables, rules_catalog):
        monitored_tables.get.return_value = _detail(["ar1"])
        rules_catalog.get_approved_checks_for_table.return_value = [_check("ar1", "id")]
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["0"]]})

        service.freeze_new_version("b1", "alice@x")

        state = json.loads(sql.json_literal_expr.call_args_list[1].args[0])
        assert len(state["applied_rules"]) == 1
        entry = state["applied_rules"][0]
        assert entry["applied_rule_id"] == "ar1"
        assert entry["rule_id"] == "r1"
        assert entry["rule_name"] == "Not Null"
        assert entry["column_mapping"] == [{"column": "ar1"}]

    def test_first_approval_freezes_v1(self, service, sql, monitored_tables, rules_catalog):
        monitored_tables.get.return_value = _detail(["ar1"])
        rules_catalog.get_approved_checks_for_table.return_value = [_check("ar1", "id")]
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["0"]]})
        assert service.freeze_new_version("b1", "alice@x") == 1

    def test_missing_binding_raises_lookup_error(self, service, monitored_tables):
        monitored_tables.get.return_value = None
        with pytest.raises(LookupError):
            service.freeze_new_version("missing", "alice@x")


# ---------------------------------------------------------------------------
# refreeze_current
# ---------------------------------------------------------------------------


class TestRefreezeCurrent:
    def test_rewrites_in_place_and_stamps_refrozen_at(self, service, sql, monitored_tables, rules_catalog):
        monitored_tables.get.return_value = _detail(["ar1"])
        rules_catalog.get_approved_checks_for_table.return_value = [_check("ar1", "id")]
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["3"]]})

        service.refreeze_current("b1")

        exec_sqls = [c.args[0] for c in sql.execute.call_args_list]
        # version stays 3, refrozen_at stamped, no INSERT / no version bump on the table
        assert any(
            f"UPDATE {_VERSIONS} SET" in s and "refrozen_at = now()" in s and "version = 3" in s for s in exec_sqls
        )
        assert not any(f"INSERT INTO {_VERSIONS}" in s for s in exec_sqls)
        assert not any(f"UPDATE {_TABLES} SET version" in s for s in exec_sqls)

    def test_version_zero_is_a_noop(self, service, sql, monitored_tables):
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["0"]]})
        service.refreeze_current("b1")
        sql.execute.assert_not_called()
        monitored_tables.get.assert_not_called()


# ---------------------------------------------------------------------------
# refreeze_for_quality_rule (per-rule approve/reject hook)
# ---------------------------------------------------------------------------


class TestRefreezeForQualityRule:
    def test_resolves_binding_and_refreezes(self, service, sql, monitored_tables, rules_catalog):
        monitored_tables.get.return_value = _detail(["ar1"])
        rules_catalog.get_approved_checks_for_table.return_value = [_check("ar1", "id")]
        _dispatch(
            sql,
            {
                f"SELECT applied_rule_id FROM {_QUALITY}": [["ar1"]],
                f"SELECT binding_id FROM {_APPLIED}": [["b1"]],
                f"SELECT version FROM {_TABLES}": [["2"]],
            },
        )

        assert service.refreeze_for_quality_rule("ar1-0") == "b1"
        exec_sqls = [c.args[0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_VERSIONS} SET" in s and "refrozen_at = now()" in s for s in exec_sqls)

    def test_direct_rule_without_applied_id_is_skipped(self, service, sql):
        _dispatch(sql, {f"SELECT applied_rule_id FROM {_QUALITY}": [[None]]})
        assert service.refreeze_for_quality_rule("hand-authored") is None
        sql.execute.assert_not_called()

    def test_unresolvable_binding_is_skipped(self, service, sql):
        _dispatch(
            sql,
            {
                f"SELECT applied_rule_id FROM {_QUALITY}": [["ar1"]],
                f"SELECT binding_id FROM {_APPLIED}": [],
            },
        )
        assert service.refreeze_for_quality_rule("ar1-0") is None
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# get_checks / list_versions
# ---------------------------------------------------------------------------


class TestGetChecks:
    def test_returns_frozen_checks(self, service, sql):
        frozen = [_check("ar1", "id")]
        _dispatch(sql, {f"FROM {_VERSIONS}": [[json.dumps(frozen)]]})
        result = service.get_checks("b1", 1)
        assert result == frozen

    def test_missing_snapshot_raises_lookup_error(self, service, sql):
        _dispatch(sql, {f"FROM {_VERSIONS}": []})
        with pytest.raises(LookupError):
            service.get_checks("b1", 9)


class TestListVersions:
    def test_maps_rows_newest_first_without_checks(self, service, sql):
        _dispatch(
            sql,
            {
                f"FROM {_VERSIONS}": [
                    ["v2", "b1", "2", json.dumps({"applied_rules": []}), "alice@x", "2026-07-07T00:00:00", "2026-07-07T01:00:00"],
                    ["v1", "b1", "1", json.dumps({"applied_rules": []}), "alice@x", "2026-07-06T00:00:00", None],
                ]
            },
        )
        versions = service.list_versions("b1")
        assert [v.version for v in versions] == [2, 1]
        assert versions[0].checks_json == []  # omitted by the listing
        assert versions[0].refrozen_at is not None
        assert versions[1].refrozen_at is None


class TestSnapshotCountsMany:
    def test_resolves_all_pins_in_one_query(self, service, sql):
        sql.query.return_value = [
            ["b1", "1", json.dumps([_check("ar1", "id"), _check("ar2", "name")]),
             json.dumps({"applied_rules": [{"applied_rule_id": "ar1"}]})],
            ["b2", "3", json.dumps([_check("ar9", "id")]),
             json.dumps({"applied_rules": [{"applied_rule_id": "ar9"}, {"applied_rule_id": "ar10"}]})],
        ]
        result = service.snapshot_counts_many([("b1", 1), ("b2", 3)])
        assert result == {("b1", 1): (1, 2), ("b2", 3): (2, 1)}
        assert sql.query.call_count == 1
        stmt = sql.query.call_args[0][0]
        assert f"FROM {_VERSIONS}" in stmt
        assert "(binding_id = 'b1' AND version = 1)" in stmt
        assert "(binding_id = 'b2' AND version = 3)" in stmt

    def test_missing_snapshot_is_absent_from_result(self, service, sql):
        sql.query.return_value = []
        assert service.snapshot_counts_many([("b1", 9)]) == {}

    def test_empty_pins_short_circuits_without_sql(self, service, sql):
        assert service.snapshot_counts_many([]) == {}
        sql.query.assert_not_called()

    def test_duplicate_pins_are_deduplicated_in_the_predicate(self, service, sql):
        sql.query.return_value = []
        service.snapshot_counts_many([("b1", 1), ("b1", 1)])
        stmt = sql.query.call_args[0][0]
        assert stmt.count("binding_id = 'b1'") == 1
