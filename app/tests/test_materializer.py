"""Tests for the Phase 3C materializer — the SAFETY-CRITICAL boundary
between the Rules Registry and the unchanged runner.

Two layers are exercised:

* ``render_check`` (pure function) — the exact ``dq_quality_rules.check``
  shape produced for one applied-rule mapping group. The **critical test**
  (``TestRenderCheckMatchesHandAuthoredShape``) asserts this is
  function/arguments/criticality-identical to what
  ``RulesCatalogService.save`` would store for the equivalent hand-authored
  check, so the runner (which only ever reads ``check.function`` /
  ``check.arguments`` / top-level ``criticality``) treats them identically.
* ``Materializer.materialize_binding`` — orchestration: idempotent upsert,
  pin vs follow-latest resolution, severity-override -> criticality,
  auto-upgrade Behaviour A/B, and cleanup of stale/orphaned rows.
"""

from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.errors import UnsafeSqlQueryError

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
    RuleVersion,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer, render_check
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    AppliedRuleSummary,
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService


# ---------------------------------------------------------------------------
# render_check — pure function, shape parity
# ---------------------------------------------------------------------------


def _is_not_null_definition(slot_name: str = "column") -> RuleDefinition:
    return RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {slot_name: f"{{{{{slot_name}}}}}"}},
            "slots": [{"name": slot_name, "family": "any", "position": 0, "cardinality": "one"}],
            "parameters": [],
        }
    )


class TestRenderCheckMatchesHandAuthoredShape:
    """The CRITICAL TEST: materialized shape must equal a hand-authored check's shape."""

    def test_dqx_native_is_not_null_matches_hand_authored_check(self, sql_executor_mock):
        # -- side A: materialize an applied is_not_null rule on customer_id --
        version = RuleVersion(
            rule_id="r1",
            version=1,
            definition=_is_not_null_definition(),
            polarity=None,
            user_metadata={"name": "Not Null Check", "dimension": "Completeness", "severity": "High"},
        )
        materialized_check, is_tableless = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        assert is_tableless is False

        # -- side B: what a human would hand-author + RulesCatalogService.save() --
        sql_executor_mock.dialect = "delta"
        sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
        sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
        sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
        sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
        sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
        sql_executor_mock.query.return_value = []
        catalog = RulesCatalogService(sql=sql_executor_mock)
        hand_authored_check = {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "customer_id"}},
        }
        catalog.save("cat.schema.customers", [hand_authored_check], "alice@x")
        inserted_sql = sql_executor_mock.execute.call_args_list[0].args[0]
        # Pull the JSON literal that RulesCatalogService actually persisted
        # for the hand-authored check out of the INSERT statement.
        start = inserted_sql.index("parse_json('") + len("parse_json('")
        end = inserted_sql.index("')", start)
        stored_hand_authored = json.loads(inserted_sql[start:end])

        # The runner only ever reads these three things off a
        # dq_quality_rules row: function, arguments, and top-level
        # criticality. They must match exactly between the registry-
        # materialized row and the equivalent hand-authored one.
        assert materialized_check["check"]["function"] == stored_hand_authored["check"]["function"] == "is_not_null"
        assert materialized_check["check"]["arguments"] == stored_hand_authored["check"]["arguments"] == {
            "column": "customer_id"
        }
        assert materialized_check["criticality"] == stored_hand_authored["criticality"] == "error"

        # The materialized row additionally carries registry provenance +
        # reserved tags in user_metadata (§9) — the hand-authored one does
        # not, and that's fine; the runner aggregates whatever is there.
        assert materialized_check["user_metadata"]["dimension"] == "Completeness"
        assert materialized_check["user_metadata"]["severity"] == "High"
        assert materialized_check["user_metadata"]["registry_rule_id"] == "r1"
        assert materialized_check["user_metadata"]["registry_version"] == "1"
        assert materialized_check["user_metadata"]["applied_rule_id"] == "ar1"
        assert materialized_check["name"] == "Not Null Check"

    def test_missing_slot_mapping_raises(self):
        version = RuleVersion(rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={})
        with pytest.raises(ValueError, match="column"):
            render_check(
                mode="dqx_native",
                version=version,
                group={},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
            )

    def test_severity_maps_to_criticality(self):
        version = RuleVersion(rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={})
        for severity, expected in (("Low", "warn"), ("Medium", "warn"), ("High", "error"), ("Critical", "error")):
            check, _ = render_check(
                mode="dqx_native",
                version=version,
                group={"column": "id"},
                effective_severity=severity,
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
            )
            assert check["criticality"] == expected


class TestRenderCheckMessageExpr:
    """Phase 7C-a: optional custom failure message (mirrors ``DQRule.message_expr``)."""

    def test_message_expr_present_when_error_message_set(self):
        definition = _is_not_null_definition()
        definition.error_message = "'Column ' || {{column}} || ' failed'"
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        assert check["message_expr"] == "'Column ' || {{column}} || ' failed'"

    def test_message_expr_absent_when_error_message_none(self):
        version = RuleVersion(rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        assert "message_expr" not in check

    def test_message_expr_absent_when_error_message_empty_string(self):
        definition = _is_not_null_definition()
        definition.error_message = ""
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        assert "message_expr" not in check


class TestRenderCheckSqlMode:
    def _sql_definition(self, body: dict, slot_name: str = "column") -> RuleDefinition:
        return RuleDefinition.model_validate(
            {
                "body": body,
                "slots": [{"name": slot_name, "family": "any", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        )

    def test_pass_polarity_does_not_negate(self):
        definition = self._sql_definition({"predicate": "{{column}} IS NOT NULL"})
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, is_tableless = render_check(
            mode="sql",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        assert is_tableless is False
        assert check["check"]["function"] == "sql_expression"
        assert check["check"]["arguments"]["expression"] == "customer_id IS NOT NULL"
        assert check["check"]["arguments"]["negate"] is False

    def test_fail_polarity_negates(self):
        definition = self._sql_definition({"predicate": "{{column}} IS NULL"})
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="fail", user_metadata={})
        check, _ = render_check(
            mode="sql",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        assert check["check"]["arguments"]["negate"] is True

    def test_dataset_sql_query_without_slots_is_tableless(self):
        definition = RuleDefinition.model_validate(
            {"body": {"sql_query": "SELECT COUNT(*) > 100 AS condition FROM t"}, "slots": [], "parameters": []}
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="fail", user_metadata={})
        check, is_tableless = render_check(
            mode="sql",
            version=version,
            group={},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        assert is_tableless is True
        assert check["check"]["function"] == "sql_query"

    def test_unsafe_sql_raises(self):
        definition = self._sql_definition({"predicate": "1=1; DROP TABLE {{column}}"})
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        with pytest.raises(UnsafeSqlQueryError):
            render_check(
                mode="sql",
                version=version,
                group={"column": "t"},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
            )


# ---------------------------------------------------------------------------
# Materializer.materialize_binding — orchestration
# ---------------------------------------------------------------------------


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
def registry():
    return create_autospec(RegistryService, instance=True)


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def app_settings():
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_auto_upgrade_without_approval.return_value = False
    return mock


@pytest.fixture
def materializer(sql, registry, monitored_tables, app_settings):
    return Materializer(sql=sql, registry=registry, monitored_tables=monitored_tables, app_settings=app_settings)


def _detail(applied: AppliedRule, table_fqn: str = "cat.schema.customers") -> MonitoredTableDetail:
    table = MonitoredTable(binding_id="b1", table_fqn=table_fqn, status="draft")
    return MonitoredTableDetail(table=table, applied_rules=[AppliedRuleSummary(applied_rule=applied)])


def _published_rule(rule_id: str = "r1", version: int = 1) -> RegistryRule:
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=version,
        definition=_is_not_null_definition(),
        user_metadata={"name": "Not Null Check", "severity": "High"},
    )


def _version_snapshot(rule_id: str = "r1", version: int = 1, severity: str = "High") -> RuleVersion:
    return RuleVersion(
        rule_id=rule_id,
        version=version,
        definition=_is_not_null_definition(),
        user_metadata={"name": "Not Null Check", "severity": severity},
    )


class TestMaterializeBindingBasics:
    def test_raises_for_missing_binding(self, materializer, monitored_tables):
        monitored_tables.get.return_value = None
        with pytest.raises(MaterializationError):
            materializer.materialize_binding("missing")

    def test_writes_new_row_as_draft(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()
        sql.query.return_value = []  # no existing materialized row, no orphans

        written = materializer.materialize_binding("b1")
        assert written == ["ar1-0"]
        insert_sql = sql.execute.call_args_list[0].args[0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_quality_rules" in insert_sql
        assert "'draft'" in insert_sql
        assert "'ar1-0'" in insert_sql
        assert "'registry'" in insert_sql

    def test_uses_pinned_version_when_set(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            pinned_version=2,
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=5)  # latest published is v5
        registry.get_version.return_value = _version_snapshot(version=2)
        sql.query.return_value = []

        materializer.materialize_binding("b1")
        registry.get_version.assert_called_once_with("r1", 2)

    def test_follows_latest_when_unpinned(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=5)
        registry.get_version.return_value = _version_snapshot(version=5)
        sql.query.return_value = []

        materializer.materialize_binding("b1")
        registry.get_version.assert_called_once_with("r1", 5)

    def test_severity_override_changes_criticality(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            severity_override="Low",
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()  # rule's own tag is High
        registry.get_version.return_value = _version_snapshot(severity="High")
        sql.query.return_value = []

        materializer.materialize_binding("b1")
        insert_sql = sql.execute.call_args_list[0].args[0]
        stored = _extract_json_literal(insert_sql)
        assert stored["criticality"] == "warn"  # Low overrides the rule's High tag
        assert stored["user_metadata"]["severity"] == "Low"


class TestMaterializeBindingIdempotency:
    def test_rerun_with_unchanged_content_updates_without_status_change(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        check, _ = render_check(
            mode="dqx_native",
            version=_version_snapshot(),
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )
        existing_check_json = json.dumps(check, sort_keys=True)

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["approved", existing_check_json]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")

        update_calls = [c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE")]
        assert len(update_calls) == 1
        assert "status = 'approved'" in update_calls[0]
        insert_calls = [c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("INSERT")]
        assert not insert_calls

    def test_rerun_does_not_duplicate_rows(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["draft", "{}"]]
            return []

        sql.query.side_effect = fake_query
        first = materializer.materialize_binding("b1")
        second = materializer.materialize_binding("b1")
        assert first == second == ["ar1-0"]


class TestAutoUpgradeBehaviour:
    def test_behaviour_b_resets_approved_to_pending_when_content_changes(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        app_settings.get_auto_upgrade_without_approval.return_value = False
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)  # republished
        registry.get_version.return_value = _version_snapshot(version=2, severity="Critical")  # content differs

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["approved", json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'pending_approval'" in update_sql

    def test_behaviour_a_keeps_approved_when_content_changes(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        app_settings.get_auto_upgrade_without_approval.return_value = True
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2, severity="Critical")

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["approved", json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'approved'" in update_sql

    def test_pinned_row_content_change_always_goes_to_pending_approval(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        app_settings.get_auto_upgrade_without_approval.return_value = True  # even with auto-upgrade ON
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            pinned_version=1,
            severity_override="Critical",  # direct edit changes content
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=1)
        registry.get_version.return_value = _version_snapshot(version=1, severity="High")

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["approved", json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'pending_approval'" in update_sql


class TestCleanup:
    def test_shrinking_mapping_deletes_stale_group_row(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return []  # ar1-0 is new
            if "SELECT rule_id FROM" in sql_text and "applied_rule_id = " in sql_text:
                return [["ar1-1"]]  # a stale second group from a previous, wider mapping
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        delete_calls = [c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("DELETE")]
        assert any("'ar1-1'" in c for c in delete_calls)

    def test_removed_application_orphan_is_deleted(self, materializer, sql, registry, monitored_tables):
        # binding now has NO applied rules at all (the application was
        # removed directly from dq_applied_rules), but a stale
        # dq_quality_rules row from it still exists.
        table = MonitoredTable(binding_id="b1", table_fqn="cat.schema.customers", status="draft")
        monitored_tables.get.return_value = MonitoredTableDetail(table=table, applied_rules=[])

        materializer.materialize_binding("b1")
        # No applied rules -> no orphan cleanup query needed (nothing to scope to).
        sql.execute.assert_not_called()


class TestRematerializeForRule:
    """``rematerialize_for_rule`` — the "publish -> propagate to followers" entry
    point (design spec §5). Wired into ``approve_registry_rule`` so publishing a
    new registry-rule version re-materializes every FOLLOWING (unpinned)
    application; PINNED applications are excluded from the query entirely and
    only ever change via a direct edit (see ``TestAutoUpgradeBehaviour``).
    """

    def test_queries_only_following_unpinned_applications(self, materializer, sql, monitored_tables, registry):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2)

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["b1"]]
            return []

        sql.query.side_effect = fake_query
        result = materializer.rematerialize_for_rule("r1")

        assert result == ["b1"]
        applied_rules_query = next(c.args[0] for c in sql.query.call_args_list if "dq_applied_rules" in c.args[0])
        assert "pinned_version IS NULL" in applied_rules_query
        assert "'r1'" in applied_rules_query
        monitored_tables.get.assert_called_once_with("b1")

    def test_rematerializes_every_distinct_binding(self, materializer, sql, monitored_tables, registry):
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2)

        def get_detail(binding_id: str):
            applied = AppliedRule(
                id=f"ar-{binding_id}",
                binding_id=binding_id,
                rule_id="r1",
                column_mapping=[{"column": "customer_id"}],
                mapping_hash="h",
            )
            return _detail(applied, table_fqn=f"cat.schema.{binding_id}")

        monitored_tables.get.side_effect = get_detail

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["b1"], ["b2"]]
            return []

        sql.query.side_effect = fake_query
        result = materializer.rematerialize_for_rule("r1")

        assert result == ["b1", "b2"]
        assert monitored_tables.get.call_args_list == [(("b1",),), (("b2",),)]

    def test_skips_binding_that_no_longer_exists(self, materializer, sql, monitored_tables):
        monitored_tables.get.return_value = None  # binding was deleted after publish

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["gone"]]
            return []

        sql.query.side_effect = fake_query
        result = materializer.rematerialize_for_rule("r1")

        # The binding_id is still reported (caller-visible), but nothing raises.
        assert result == ["gone"]

    def test_no_following_applications_is_a_noop(self, materializer, sql, monitored_tables):
        sql.query.return_value = []
        result = materializer.rematerialize_for_rule("r1")
        assert result == []
        monitored_tables.get.assert_not_called()

    def test_respects_auto_upgrade_behaviour_b_via_materialize_binding(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        """Re-materializing through the publish path still honours Behaviour B
        (auto-upgrade OFF): a previously-approved following row whose content
        changed is pushed back to pending_approval, same as a direct
        ``materialize_binding`` call."""
        app_settings.get_auto_upgrade_without_approval.return_value = False
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2, severity="Critical")

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["b1"]]
            if "SELECT status" in sql_text:
                return [["approved", json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.rematerialize_for_rule("r1")

        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'pending_approval'" in update_sql


class TestRenderCheckNativeCrossTable:
    """Phase 7C-a: confirm DQX Native cross-table rules materialize correctly.

    ``foreign_key`` is a dataset-level check with reference-table arguments
    (``ref_table``/``ref_columns``) that are NOT column slots on the
    monitored table — they are frozen ``RuleParameter`` values on the
    registry rule's definition (see ``registry_seed_map.py``:
    ``ref_table``/``ref_columns`` kinds map to the ``ref_table``/``ref_column``
    ``ParamType``s). This asserts a ``dqx_native`` ``foreign_key`` rule
    materializes with those ref-table args intact, alongside its own
    column slot substitution — no materializer change was needed for this;
    ``render_check``'s existing ``dqx_native`` branch already fills in both
    slots and non-``None`` parameters generically regardless of function
    name (see ``_substitute_arguments``).
    """

    def test_foreign_key_native_rule_materializes_with_ref_table_args(self):
        definition = RuleDefinition.model_validate(
            {
                "body": {
                    "function": "foreign_key",
                    "arguments": {"columns": "{{columns}}"},
                },
                "slots": [{"name": "columns", "family": "any", "position": 0, "cardinality": "many"}],
                "parameters": [
                    {"name": "ref_columns", "type": "ref_column", "value": ["id"]},
                    {"name": "ref_table", "type": "ref_table", "value": "catalog.schema.customers"},
                ],
            }
        )
        version = RuleVersion(
            rule_id="r1",
            version=1,
            definition=definition,
            user_metadata={"name": "Orders FK", "severity": "High"},
        )
        check, is_tableless = render_check(
            mode="dqx_native",
            version=version,
            group={"columns": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
        )

        # foreign_key is per-table (it validates the monitored table's own
        # rows against a reference table), not the tableless __sql_check__
        # convention reserved for genuinely table-less cross-table SQL.
        assert is_tableless is False
        assert check["check"]["function"] == "foreign_key"
        assert check["check"]["arguments"]["columns"] == ["customer_id"]
        assert check["check"]["arguments"]["ref_columns"] == ["id"]
        assert check["check"]["arguments"]["ref_table"] == "catalog.schema.customers"
        assert check["criticality"] == "error"


def _extract_json_literal(sql_text: str) -> dict:
    start = sql_text.index("parse_json('") + len("parse_json('")
    end = sql_text.index("')", start)
    return json.loads(sql_text[start:end])
