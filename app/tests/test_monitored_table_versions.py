"""Tests for :class:`MonitoredTableVersionService` — the Data Products Task 2
version freeze service.

Drives the service against a spec-bound ``SqlExecutor`` mock (dialect
helpers wired like the materializer's ``sql`` fixture, ``query`` given a
per-test dispatcher keyed on the emitted SQL) plus mocked
``MonitoredTableService`` / ``RulesCatalogService`` / ``Materializer`` so no
Spark or workspace is needed. Asserts the freeze/re-freeze SQL and —
critically — that a frozen snapshot references ONLY the binding's own
approved rows (scoped by ``applied_rule_id``), excluding directly-authored
non-registry approved rules on the same ``table_fqn``.

The snapshot is REFERENCE-based: freeze stores ``state_json.rule_refs``
(applied-rule id + RESOLVED registry version + mapping) rather than a copy
of the rendered rule set, and :meth:`get_checks` reconstructs the runner
payload by re-rendering those references through the ``Materializer``.
"""

from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable
from databricks_labs_dqx_app.backend.services.materializer import Materializer
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
def materializer():
    return create_autospec(Materializer, instance=True)


@pytest.fixture
def service(sql, monitored_tables, rules_catalog, materializer):
    return MonitoredTableVersionService(
        sql=sql,
        monitored_tables=monitored_tables,
        rules_catalog=rules_catalog,
        materializer=materializer,
    )


def _check(applied_rule_id: str | None, column: str, registry_version: str = "1") -> dict:
    """A materialized check dict shaped like ``get_approved_checks_for_table`` output."""
    user_metadata: dict[str, str] = {"registry_rule_id": "r1", "registry_version": registry_version}
    if applied_rule_id is not None:
        user_metadata["applied_rule_id"] = applied_rule_id
    return {
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": column}},
        "user_metadata": user_metadata,
    }


def _detail(
    applied_ids: list[str], table_fqn: str = "cat.schema.customers", status: str = "approved"
) -> MonitoredTableDetail:
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
    def test_bumps_version_and_freezes_scoped_refs(self, service, sql, monitored_tables, rules_catalog):
        monitored_tables.get.return_value = _detail(["ar1", "ar2"])
        # Table's approved rows include a directly-authored (non-registry) rule
        # on the same table_fqn — it must NOT be referenced by the snapshot.
        rules_catalog.get_approved_checks_for_table.return_value = [
            _check("ar1", "id"),
            _check("ar2", "name"),
            _check(None, "hand_authored"),  # direct rule, no applied_rule_id
        ]
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["1"]]})

        new_version = service.freeze_new_version("b1", "alice@x")

        assert new_version == 2
        exec_sqls = [c.args[0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_TABLES} SET version = 2" in s for s in exec_sqls)
        assert any(f"INSERT INTO {_VERSIONS}" in s and "checks_json" not in s for s in exec_sqls)
        # frozen state carries references to ONLY the binding's own applied rules
        state = json.loads(sql.json_literal_expr.call_args_list[0].args[0])
        ref_ids = {r["applied_rule_id"] for r in state["rule_refs"]}
        assert ref_ids == {"ar1", "ar2"}
        assert all(r["registry_version"] == 1 for r in state["rule_refs"])
        # cached rendered count = the scoped approved checks (excludes the direct rule)
        assert state["check_count"] == 2

    def test_ref_carries_per_rule_pass_threshold(self, service, sql, monitored_tables, rules_catalog):
        # The frozen ref must record pass_threshold (incl. a literal 0) so an
        # approved-version run re-renders with the intended per-rule threshold
        # rather than the admin default. Pair with
        # TestGetChecks.test_pass_threshold_zero_round_trips_through_ref.
        detail = _detail(["ar1"])
        detail.applied_rules[0].applied_rule.pass_threshold = 0
        monitored_tables.get.return_value = detail
        rules_catalog.get_approved_checks_for_table.return_value = [_check("ar1", "id")]
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["0"]]})

        service.freeze_new_version("b1", "alice@x")

        state = json.loads(sql.json_literal_expr.call_args_list[0].args[0])
        assert state["rule_refs"][0]["pass_threshold"] == 0

    def test_state_json_carries_applied_rule_display_metadata(self, service, sql, monitored_tables, rules_catalog):
        monitored_tables.get.return_value = _detail(["ar1"])
        rules_catalog.get_approved_checks_for_table.return_value = [_check("ar1", "id")]
        _dispatch(sql, {f"SELECT version FROM {_TABLES}": [["0"]]})

        service.freeze_new_version("b1", "alice@x")

        state = json.loads(sql.json_literal_expr.call_args_list[0].args[0])
        assert len(state["applied_rules"]) == 1
        entry = state["applied_rules"][0]
        assert entry["applied_rule_id"] == "ar1"
        assert entry["rule_id"] == "r1"
        assert entry["rule_name"] == "Not Null"
        assert entry["registry_version"] == 1
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

    def test_refuses_to_overwrite_nonempty_snapshot_with_empty(self, service, sql, monitored_tables, rules_catalog):
        # DATA-LOSS GUARD (B2-27): the binding's approved rows momentarily
        # resolve to nothing (e.g. an upstream re-materialization couldn't
        # re-render a rule against a new version). The re-freeze must KEEP the
        # previous non-empty snapshot rather than emptying its references.
        monitored_tables.get.return_value = _detail(["ar1"])
        rules_catalog.get_approved_checks_for_table.return_value = []  # newly-computed set is empty
        _dispatch(
            sql,
            {
                f"SELECT version FROM {_TABLES}": [["3"]],
                # previous snapshot has NON-empty references
                f"FROM {_VERSIONS}": [[json.dumps({"rule_refs": [{"applied_rule_id": "ar1"}]})]],
            },
        )

        service.refreeze_current("b1")

        exec_sqls = [c.args[0] for c in sql.execute.call_args_list]
        assert not any(f"UPDATE {_VERSIONS} SET" in s for s in exec_sqls)

    def test_overwrites_when_previous_snapshot_also_empty(self, service, sql, monitored_tables, rules_catalog):
        # The guard is narrow: an empty->empty re-freeze is harmless and must
        # still run (it stamps refrozen_at and refreshes state_json).
        monitored_tables.get.return_value = _detail(["ar1"])
        rules_catalog.get_approved_checks_for_table.return_value = []
        _dispatch(
            sql,
            {
                f"SELECT version FROM {_TABLES}": [["3"]],
                f"FROM {_VERSIONS}": [[json.dumps({"rule_refs": []})]],  # previous snapshot already empty
            },
        )

        service.refreeze_current("b1")

        exec_sqls = [c.args[0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_VERSIONS} SET" in s and "refrozen_at = now()" in s for s in exec_sqls)


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
    def test_reconstructs_checks_from_refs(self, service, sql, materializer):
        state = {
            "rule_refs": [
                {
                    "applied_rule_id": "ar1",
                    "rule_id": "r1",
                    "registry_version": 2,
                    "column_mapping": [{"column": "id"}],
                }
            ],
            "check_count": 1,
        }
        _dispatch(
            sql,
            {
                f"FROM {_VERSIONS}": [[json.dumps(state)]],
                f"SELECT table_fqn FROM {_TABLES}": [["cat.schema.customers"]],
            },
        )
        rendered = [_check("ar1", "id", registry_version="2")]
        materializer.render_applied_checks.return_value = rendered

        result = service.get_checks("b1", 1)

        assert result == rendered
        # re-rendered from the frozen ref, pinned to the RESOLVED registry version
        args = materializer.render_applied_checks.call_args
        assert args.args[0] == "cat.schema.customers"
        applied = args.args[1]
        assert applied[0].id == "ar1"
        assert applied[0].pinned_version == 2

    def test_pass_threshold_zero_round_trips_through_ref(self, service, sql, materializer):
        # Regression: a per-rule pass_threshold of 0 ("never warn") must survive
        # the freeze->reconstruct round-trip. Previously rule_refs omitted
        # pass_threshold and _ref_to_applied never restored it, so an
        # approved-version run re-rendered with pass_threshold=None and the
        # resolver fell through to the admin default (e.g. 70) — silently
        # re-freezing 0% rules at the default on every approved run.
        state = {
            "rule_refs": [
                {
                    "applied_rule_id": "ar1",
                    "rule_id": "r1",
                    "registry_version": 2,
                    "pass_threshold": 0,
                    "column_mapping": [{"column": "id"}],
                }
            ],
            "check_count": 1,
        }
        _dispatch(
            sql,
            {
                f"FROM {_VERSIONS}": [[json.dumps(state)]],
                f"SELECT table_fqn FROM {_TABLES}": [["cat.schema.customers"]],
            },
        )
        materializer.render_applied_checks.return_value = []

        service.get_checks("b1", 1)

        applied = materializer.render_applied_checks.call_args.args[1]
        # literal 0 preserved, not coerced to None (which would resolve to admin default)
        assert applied[0].pass_threshold == 0

    def test_missing_pass_threshold_in_legacy_ref_is_none(self, service, sql, materializer):
        # Snapshots frozen before pass_threshold was recorded carry no such key;
        # reconstruct as None so the resolver falls back to registry/admin default.
        state = {
            "rule_refs": [
                {
                    "applied_rule_id": "ar1",
                    "rule_id": "r1",
                    "registry_version": 2,
                    "column_mapping": [{"column": "id"}],
                }
            ],
        }
        _dispatch(
            sql,
            {
                f"FROM {_VERSIONS}": [[json.dumps(state)]],
                f"SELECT table_fqn FROM {_TABLES}": [["cat.schema.customers"]],
            },
        )
        materializer.render_applied_checks.return_value = []

        service.get_checks("b1", 1)

        applied = materializer.render_applied_checks.call_args.args[1]
        assert applied[0].pass_threshold is None

    def test_missing_snapshot_raises_lookup_error(self, service, sql):
        _dispatch(sql, {f"FROM {_VERSIONS}": []})
        with pytest.raises(LookupError):
            service.get_checks("b1", 9)

    def test_empty_refs_returns_empty_without_rendering(self, service, sql, materializer):
        _dispatch(sql, {f"FROM {_VERSIONS}": [[json.dumps({"rule_refs": []})]]})
        assert service.get_checks("b1", 1) == []
        materializer.render_applied_checks.assert_not_called()


class TestListVersions:
    def test_maps_rows_newest_first_without_checks(self, service, sql):
        _dispatch(
            sql,
            {
                f"FROM {_VERSIONS}": [
                    [
                        "v2",
                        "b1",
                        "2",
                        json.dumps({"applied_rules": []}),
                        "alice@x",
                        "2026-07-07T00:00:00",
                        "2026-07-07T01:00:00",
                    ],
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
            [
                "b1",
                "1",
                json.dumps({"rule_refs": [{"applied_rule_id": "ar1"}], "check_count": 2}),
            ],
            [
                "b2",
                "3",
                json.dumps(
                    {"rule_refs": [{"applied_rule_id": "ar9"}, {"applied_rule_id": "ar10"}], "check_count": 1}
                ),
            ],
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
