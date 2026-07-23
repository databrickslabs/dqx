"""Tests for ``ApplyRulesService`` — Phase 3C tier-2 apply/map layer.

Follows the same testing shape as ``test_monitored_table_service.py``:
spec-bound ``create_autospec(SqlExecutor)`` mocks with dialect-helper side
effects wired to their real Delta-flavoured behaviour.
"""

from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import RegistryRule, RuleDefinition, compute_mapping_hash
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    ApplyRulesService,
    DesiredAppliedRule,
    MappingIncompleteError,
    RuleNotPublishedError,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


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
def app_settings():
    mock = create_autospec(AppSettingsService, instance=True)
    # Default matches production default: new applications follow latest
    # unless the caller explicitly pins.
    mock.get_default_auto_upgrade.return_value = True
    mock.resolve_pinned_version_for_new_attachment.side_effect = lambda explicit, current: (
        explicit if explicit is not None else (None if mock.get_default_auto_upgrade() else current)
    )
    return mock


@pytest.fixture
def svc(sql, registry, app_settings):
    return ApplyRulesService(sql=sql, registry=registry, app_settings=app_settings)


def _published_rule(rule_id: str = "r1", slot_names: list[str] | None = None) -> RegistryRule:
    slot_names = slot_names if slot_names is not None else ["column"]
    definition = RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {n: f"{{{{{n}}}}}" for n in slot_names}},
            "slots": [
                {"name": n, "family": "any", "position": i, "cardinality": "one"} for i, n in enumerate(slot_names)
            ],
            "parameters": [],
        }
    )
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=1,
        definition=definition,
        user_metadata={"name": "Not Null Check", "severity": "High"},
    )


def _applied_row(
    id_: str = "ar1",
    binding_id: str = "b1",
    rule_id: str = "r1",
    pinned_version: str | None = None,
    severity_override: str | None = None,
    column_mapping: list[dict[str, str]] | None = None,
    user_metadata: dict | None = None,
    mapping_hash: str = "deadbeef",
) -> list[str]:
    return [
        id_,
        binding_id,
        rule_id,
        pinned_version,
        severity_override,
        json.dumps(column_mapping if column_mapping is not None else [{"column": "customer_id"}]),
        json.dumps(user_metadata or {}),
        mapping_hash,
        "alice@x",
        "2026-07-02T00:00:00+00:00",
    ]


# ---------------------------------------------------------------------------
# apply_rule
# ---------------------------------------------------------------------------


class TestApplyRule:
    def test_applies_published_rule(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        applied = svc.apply_rule(
            "b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=None, severity_override=None
        )
        assert applied.binding_id == "b1"
        assert applied.rule_id == "r1"
        assert applied.column_mapping == [{"column": "customer_id"}]
        assert applied.mapping_hash
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in insert_sql

    def test_rejects_missing_binding(self, svc, sql, registry):
        sql.query.return_value = []  # binding lookup empty
        with pytest.raises(RuntimeError, match="Monitored table not found"):
            svc.apply_rule("missing", "r1", [{"column": "customer_id"}], "alice@x")
        registry.get_rule.assert_not_called()

    def test_rejects_missing_rule(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        registry.get_rule.return_value = None
        with pytest.raises(RuntimeError, match="Registry rule not found"):
            svc.apply_rule("b1", "missing", [{"column": "customer_id"}], "alice@x")

    def test_rejects_unpublished_rule(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        draft_rule = _published_rule()
        draft_rule.status = "draft"
        registry.get_rule.return_value = draft_rule
        with pytest.raises(RuleNotPublishedError):
            svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x")
        sql.execute.assert_not_called()

    def test_allows_empty_mapping_to_stage_application(self, svc, sql, registry):
        # An empty column_mapping stages the rule application without any
        # mapping groups yet — the by-rule card completes the mapping with a
        # follow-up apply_rule() call. materializer.py skips rows with zero
        # groups, so nothing runs until then.
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        applied = svc.apply_rule("b1", "r1", [], "alice@x")
        assert applied.column_mapping == []
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in insert_sql

    def test_rejects_mapping_missing_a_slot(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        registry.get_rule.return_value = _published_rule(slot_names=["column", "reference_column"])
        with pytest.raises(MappingIncompleteError):
            svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x")
        sql.execute.assert_not_called()

    def test_rejects_mapping_with_unknown_slot(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        registry.get_rule.return_value = _published_rule()
        with pytest.raises(MappingIncompleteError):
            svc.apply_rule("b1", "r1", [{"column": "customer_id", "extra": "x"}], "alice@x")

    def test_new_application_unspecified_pin_follows_latest_when_auto_upgrade_on(
        self, svc, sql, registry, app_settings
    ):
        app_settings.get_default_auto_upgrade.return_value = True
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        applied = svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=None)
        assert applied.pinned_version is None

    def test_new_application_unspecified_pin_freezes_current_version_when_auto_upgrade_off(
        self, svc, sql, registry, app_settings
    ):
        app_settings.get_default_auto_upgrade.return_value = False
        registry.get_rule.return_value = _published_rule()  # version=1
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        applied = svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=None)
        assert applied.pinned_version == 1
        insert_sql = sql.execute.call_args[0][0]
        assert ", 1, " in insert_sql

    def test_new_application_explicit_pin_wins_regardless_of_setting(self, svc, sql, registry, app_settings):
        app_settings.get_default_auto_upgrade.return_value = False
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],
            [],
        ]
        applied = svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=7)
        assert applied.pinned_version == 7

    def test_existing_application_explicit_none_unpins_regardless_of_setting(self, svc, sql, registry, app_settings):
        # Attach-time-only: default_auto_upgrade must NOT be consulted on an
        # UPDATE of an existing application — an explicit None here means
        # the steward chose to unpin, not "unspecified".
        app_settings.get_default_auto_upgrade.return_value = False
        registry.get_rule.return_value = _published_rule()
        existing_row = _applied_row(pinned_version="3")
        sql.query.side_effect = [
            [["b1"]],
            [existing_row],
        ]
        applied = svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=None)
        assert applied.pinned_version is None
        app_settings.resolve_pinned_version_for_new_attachment.assert_not_called()

    def test_reapplying_identical_mapping_updates_instead_of_duplicating(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        existing_row = _applied_row(severity_override=None, pinned_version=None)
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [existing_row],  # existing application found by natural key
        ]
        applied = svc.apply_rule(
            "b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=2, severity_override="Critical"
        )
        assert applied.id == "ar1"
        assert applied.pinned_version == 2
        assert applied.severity_override == "Critical"
        update_sql = sql.execute.call_args[0][0]
        assert "UPDATE dqx_test.dqx_app_test.dq_applied_rules" in update_sql
        assert "INSERT INTO" not in update_sql


# ---------------------------------------------------------------------------
# build_applied_rule (in-memory construction, no persistence — profiler staging)
# ---------------------------------------------------------------------------


class TestBuildAppliedRule:
    """build_applied_rule returns a valid AppliedRule WITHOUT inserting to the DB."""

    def test_returns_applied_rule_without_inserting(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists (from _validate_applicable_rule)
        ]
        applied = svc.build_applied_rule("b1", "r1", [{"column": "customer_id"}], "alice@x")

        assert applied.binding_id == "b1"
        assert applied.rule_id == "r1"
        assert applied.column_mapping == [{"column": "customer_id"}]
        assert applied.mapping_hash
        assert applied.id is not None
        assert applied.created_at is not None
        # Critical: no DB insert must happen
        sql.execute.assert_not_called()

    def test_apply_rule_still_inserts(self, svc, sql, registry):
        """Regression: apply_rule's persist behavior is unchanged after extracting build_applied_rule."""
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        applied = svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x")

        assert applied.binding_id == "b1"
        assert applied.rule_id == "r1"
        # Must have issued an INSERT
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in insert_sql

    def test_build_resolves_pinned_version_via_auto_upgrade(self, svc, sql, registry, app_settings):
        app_settings.get_default_auto_upgrade.return_value = False
        registry.get_rule.return_value = _published_rule()  # version=1
        sql.query.side_effect = [
            [["b1"]],  # binding exists
        ]
        applied = svc.build_applied_rule("b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=None)
        assert applied.pinned_version == 1
        sql.execute.assert_not_called()

    def test_build_rejects_unpublished_rule(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        draft_rule = _published_rule()
        draft_rule.status = "draft"
        registry.get_rule.return_value = draft_rule
        with pytest.raises(RuleNotPublishedError):
            svc.build_applied_rule("b1", "r1", [{"column": "customer_id"}], "alice@x")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# attach_auto_mapping (apply-on-tag reconcile — add-only, origin-stamped)
# ---------------------------------------------------------------------------


class TestAttachAutoMapping:
    def test_absent_inserts_origin_stamped_auto_row(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # not suppressed
            [],  # no existing row for the natural key
        ]
        applied = svc.attach_auto_mapping("b1", "r1", [{"column": "customer_id"}], "alice@x")
        assert applied is not None
        assert applied.user_metadata == {"origin": "tag_auto"}
        assert applied.severity_override is None
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in insert_sql
        # The origin marker is persisted in the INSERT payload.
        assert "tag_auto" in insert_sql

    def test_hand_applied_row_is_never_touched(self, svc, sql, registry):
        # A steward hand-applied the SAME rule with the SAME mapping: pinned to
        # v3, a severity override, and NO origin marker. attach_auto_mapping must
        # return it unchanged and issue NO write of any kind.
        registry.get_rule.return_value = _published_rule()
        mapping = [{"column": "customer_id"}]
        hand_row = _applied_row(
            id_="hand1",
            column_mapping=mapping,
            mapping_hash=compute_mapping_hash(mapping),
            pinned_version="3",
            severity_override="Critical",
            user_metadata={},  # no origin marker -> hand-applied
        )
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # not suppressed
            [hand_row],  # existing row found by natural key
        ]
        applied = svc.attach_auto_mapping("b1", "r1", mapping, "alice@x")
        assert applied is not None
        assert applied.id == "hand1"
        assert applied.pinned_version == 3
        assert applied.severity_override == "Critical"
        assert applied.user_metadata == {}
        # Never mutated: no UPDATE/INSERT/DELETE issued at all.
        sql.execute.assert_not_called()

    def test_idempotent_on_its_own_auto_row(self, svc, sql, registry):
        # A prior auto row (origin=tag_auto) with a resolved pin: a second attach
        # is a no-op that returns it unchanged, pin preserved.
        registry.get_rule.return_value = _published_rule()
        mapping = [{"column": "customer_id"}]
        auto_row = _applied_row(
            id_="auto1",
            column_mapping=mapping,
            mapping_hash=compute_mapping_hash(mapping),
            pinned_version="1",
            user_metadata={"origin": "tag_auto"},
        )
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # not suppressed
            [auto_row],  # existing auto row found by natural key
        ]
        applied = svc.attach_auto_mapping("b1", "r1", mapping, "alice@x")
        assert applied is not None
        assert applied.id == "auto1"
        assert applied.pinned_version == 1
        assert applied.user_metadata == {"origin": "tag_auto"}
        sql.execute.assert_not_called()

    def test_skips_suppressed(self, svc, sql, registry):
        # A steward previously removed this auto mapping (tombstone present).
        # attach_auto_mapping must return None and insert nothing.
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [[1]],  # suppression lookup returns a row -> suppressed
        ]
        applied = svc.attach_auto_mapping("b1", "r1", [{"column": "customer_id"}], "alice@x")
        assert applied is None
        sql.execute.assert_not_called()

    def test_not_suppressed_still_inserts(self, svc, sql, registry):
        # Regression: no tombstone + no existing row -> insert as before.
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # not suppressed
            [],  # no existing row for the natural key
        ]
        applied = svc.attach_auto_mapping("b1", "r1", [{"column": "customer_id"}], "alice@x")
        assert applied is not None
        assert applied.user_metadata == {"origin": "tag_auto"}
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in insert_sql

    def test_rejects_missing_binding(self, svc, sql, registry):
        sql.query.return_value = []  # binding lookup empty
        with pytest.raises(RuntimeError, match="Monitored table not found"):
            svc.attach_auto_mapping("missing", "r1", [{"column": "customer_id"}], "alice@x")
        registry.get_rule.assert_not_called()

    def test_rejects_unpublished_rule(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        draft_rule = _published_rule()
        draft_rule.status = "draft"
        registry.get_rule.return_value = draft_rule
        with pytest.raises(RuleNotPublishedError):
            svc.attach_auto_mapping("b1", "r1", [{"column": "customer_id"}], "alice@x")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# save_applied_rules (batch reconcile — staged editor)
# ---------------------------------------------------------------------------


class TestSaveAppliedRules:
    def test_reconciles_new_additions(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists (save_applied_rules)
            [],  # list_applied: no existing rows
            [["b1"]],  # binding exists (apply_rule entry1)
            [],  # apply_rule(entry1): no existing natural-key match
            [["b1"]],  # binding exists (apply_rule entry2)
            [],  # apply_rule(entry2): no existing natural-key match
        ]
        desired = [
            DesiredAppliedRule(rule_id="r1", column_mapping=[{"column": "a"}]),
            DesiredAppliedRule(rule_id="r2", column_mapping=[{"column": "b"}]),
        ]
        results = svc.save_applied_rules("b1", desired, "alice@x")
        assert [r.rule_id for r in results] == ["r1", "r2"]
        calls = [c.args[0] for c in sql.execute.call_args_list]
        insert_calls = [c for c in calls if "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in c]
        assert len(insert_calls) == 2
        # B2-118: the save also bumps the binding's updated_at so an edit forces
        # a fresh draft run before submit.
        assert any("UPDATE dqx_test.dqx_app_test.dq_monitored_tables" in c and "updated_at" in c for c in calls)

    def test_mapping_change_removes_old_row_and_inserts_new(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        old_hash = "oldhash"
        existing_row = _applied_row(id_="ar1", column_mapping=[{"column": "old_col"}], mapping_hash=old_hash)
        new_mapping = [{"column": "new_col"}]
        sql.query.side_effect = [
            [["b1"]],  # binding exists (save_applied_rules)
            [existing_row],  # list_applied
            [existing_row],  # remove_applied -> get_applied lookup
            [["b1"]],  # binding exists (apply_rule)
            [],  # apply_rule -> no natural-key match for the new mapping
        ]
        desired = [DesiredAppliedRule(rule_id="r1", column_mapping=new_mapping)]
        results = svc.save_applied_rules("b1", desired, "alice@x")
        assert len(results) == 1
        assert results[0].column_mapping == new_mapping
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_quality_rules" in c for c in calls)
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_applied_rules" in c for c in calls)
        assert any("INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in c for c in calls)

    def test_removes_rule_no_longer_desired(self, svc, sql, registry):
        existing_row = _applied_row(id_="ar1")
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [existing_row],  # list_applied
            [existing_row],  # remove_applied -> get_applied lookup
        ]
        results = svc.save_applied_rules("b1", [], "alice@x")
        assert results == []
        registry.get_rule.assert_not_called()
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_quality_rules" in c for c in calls)
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_applied_rules" in c for c in calls)
        assert not any("INSERT INTO" in c for c in calls)

    def test_updates_severity_and_pin_in_place_when_mapping_unchanged(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        mapping = [{"column": "customer_id"}]
        unchanged_hash = compute_mapping_hash(mapping)
        existing_row = _applied_row(id_="ar1", column_mapping=mapping, mapping_hash=unchanged_hash)
        sql.query.side_effect = [
            [["b1"]],  # binding exists (save_applied_rules)
            [existing_row],  # list_applied (hash matches desired -> not removed)
            [["b1"]],  # binding exists (apply_rule)
            [existing_row],  # apply_rule -> natural-key match found -> update path
        ]
        desired = [
            DesiredAppliedRule(rule_id="r1", column_mapping=mapping, pinned_version=3, severity_override="Critical")
        ]
        results = svc.save_applied_rules("b1", desired, "alice@x")
        assert results[0].pinned_version == 3
        assert results[0].severity_override == "Critical"
        calls = [c.args[0] for c in sql.execute.call_args_list]
        # Two writes: the in-place applied-rule update, then the B2-118 binding
        # touch (updated_at bump) — no DELETE/INSERT since the mapping is unchanged.
        applied_updates = [c for c in calls if "UPDATE dqx_test.dqx_app_test.dq_applied_rules" in c]
        assert len(applied_updates) == 1
        assert any("UPDATE dqx_test.dqx_app_test.dq_monitored_tables" in c and "updated_at" in c for c in calls)
        assert not any("DELETE" in c for c in calls)
        assert not any("INSERT" in c for c in calls)

    def test_allows_empty_mapping_to_stage(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists (save_applied_rules)
            [],  # list_applied
            [["b1"]],  # binding exists (apply_rule)
            [],  # apply_rule natural-key lookup
        ]
        results = svc.save_applied_rules("b1", [DesiredAppliedRule(rule_id="r1", column_mapping=[])], "alice@x")
        assert results[0].column_mapping == []

    def test_rejects_unpublished_rule_before_mutating(self, svc, sql, registry):
        draft_rule = _published_rule(rule_id="r2")
        draft_rule.status = "draft"

        def get_rule(rule_id: str) -> RegistryRule:
            return _published_rule() if rule_id == "r1" else draft_rule

        registry.get_rule.side_effect = get_rule
        sql.query.side_effect = [[["b1"]]]  # binding exists only
        desired = [
            DesiredAppliedRule(rule_id="r1", column_mapping=[{"column": "a"}]),
            DesiredAppliedRule(rule_id="r2", column_mapping=[{"column": "b"}]),
        ]
        with pytest.raises(RuleNotPublishedError):
            svc.save_applied_rules("b1", desired, "alice@x")
        sql.execute.assert_not_called()

    def test_bad_mapping_leaves_state_untouched(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule(slot_names=["column", "reference_column"])
        sql.query.side_effect = [[["b1"]]]  # binding exists only
        desired = [
            DesiredAppliedRule(rule_id="r1", column_mapping=[{"column": "a"}]),  # missing reference_column
        ]
        with pytest.raises(MappingIncompleteError):
            svc.save_applied_rules("b1", desired, "alice@x")
        sql.execute.assert_not_called()

    def test_last_writer_wins_on_duplicate_rule_id(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists (save_applied_rules)
            [],  # list_applied
            [["b1"]],  # binding exists (apply_rule)
            [],  # apply_rule natural-key lookup for the winning entry
        ]
        desired = [
            DesiredAppliedRule(rule_id="r1", column_mapping=[{"column": "a"}]),
            DesiredAppliedRule(rule_id="r1", column_mapping=[{"column": "b"}]),
        ]
        results = svc.save_applied_rules("b1", desired, "alice@x")
        assert len(results) == 1
        assert results[0].column_mapping == [{"column": "b"}]


# ---------------------------------------------------------------------------
# list_applied / get_applied
# ---------------------------------------------------------------------------


class TestListAndGet:
    def test_list_applied(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1"), _applied_row(id_="ar2")]
        applied = svc.list_applied("b1")
        assert [a.id for a in applied] == ["ar1", "ar2"]

    def test_get_applied_returns_none_when_missing(self, svc, sql):
        sql.query.return_value = []
        assert svc.get_applied("missing") is None


# ---------------------------------------------------------------------------
# list_bindings_for_rule
# ---------------------------------------------------------------------------


class TestListBindingsForRule:
    def test_returns_empty_when_unapplied(self, svc, sql):
        sql.query.return_value = []
        assert svc.list_bindings_for_rule("rule-not-applied-anywhere") == []

    def test_returns_applications_across_bindings(self, svc, sql):
        sql.query.return_value = [
            _applied_row(id_="ar1", binding_id="b1", rule_id="r1"),
            _applied_row(id_="ar2", binding_id="b2", rule_id="r1"),
        ]
        result = svc.list_bindings_for_rule("r1")
        assert [a.id for a in result] == ["ar1", "ar2"]
        assert [a.binding_id for a in result] == ["b1", "b2"]
        query_sql = sql.query.call_args[0][0]
        assert "rule_id = 'r1'" in query_sql
        assert "binding_id = " not in query_sql

    def test_escapes_rule_id(self, svc, sql):
        sql.query.return_value = []
        svc.list_bindings_for_rule("r'; DROP TABLE x --")
        query_sql = sql.query.call_args[0][0]
        assert "r''; DROP TABLE x --" in query_sql


# ---------------------------------------------------------------------------
# count_applications_for_rule
# ---------------------------------------------------------------------------


class TestCountApplicationsForRule:
    def test_counts_applications(self, svc, sql):
        sql.query.return_value = [["3"]]
        assert svc.count_applications_for_rule("r1") == 3

    def test_zero_when_no_rows(self, svc, sql):
        sql.query.return_value = []
        assert svc.count_applications_for_rule("r1") == 0

    def test_zero_when_null_count(self, svc, sql):
        sql.query.return_value = [[None]]
        assert svc.count_applications_for_rule("r1") == 0


# ---------------------------------------------------------------------------
# remove_applied
# ---------------------------------------------------------------------------


class TestRemoveApplied:
    def test_removes_link_and_materialized_rows(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1")]
        svc.remove_applied("ar1")
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_quality_rules" in c and "applied_rule_id" in c for c in calls)
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_applied_rules" in c for c in calls)

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.remove_applied("missing")
        sql.execute.assert_not_called()

    def test_auto_origin_records_suppression(self, svc, sql):
        # Removing a tag-auto row writes a suppression tombstone keyed by its
        # natural key, attributed to the acting remover.
        sql.query.return_value = [
            _applied_row(
                id_="ar1",
                binding_id="b1",
                rule_id="r1",
                mapping_hash="h1",
                user_metadata={"origin": "tag_auto"},
            )
        ]
        svc.remove_applied("ar1", "bob@x")
        sql.upsert.assert_called_once()
        call = sql.upsert.call_args
        assert call.args[0] == "dqx_test.dqx_app_test.dq_tag_auto_suppressions"
        assert call.kwargs["key_cols"] == {"binding_id": "b1", "rule_id": "r1", "mapping_hash": "h1"}
        assert call.kwargs["value_cols"]["suppressed_by"] == "bob@x"

    def test_hand_applied_no_suppression(self, svc, sql):
        # Removing a row with NO origin marker must NOT write a tombstone.
        sql.query.return_value = [_applied_row(id_="ar1", user_metadata={})]
        svc.remove_applied("ar1", "bob@x")
        sql.upsert.assert_not_called()


# ---------------------------------------------------------------------------
# set_pin / set_severity_override
# ---------------------------------------------------------------------------


class TestSetPin:
    def test_sets_pinned_version(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1")]
        applied = svc.set_pin("ar1", 3)
        assert applied.pinned_version == 3
        update_sql = sql.execute.call_args[0][0]
        assert "pinned_version = 3" in update_sql

    def test_clears_pinned_version(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1", pinned_version="2")]
        applied = svc.set_pin("ar1", None)
        assert applied.pinned_version is None
        update_sql = sql.execute.call_args[0][0]
        assert "pinned_version = NULL" in update_sql

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.set_pin("missing", 1)


class TestSetSeverityOverride:
    def test_sets_severity_override(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1")]
        applied = svc.set_severity_override("ar1", "Critical")
        assert applied.severity_override == "Critical"
        update_sql = sql.execute.call_args[0][0]
        assert "severity_override = 'Critical'" in update_sql

    def test_clears_severity_override(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1", severity_override="High")]
        applied = svc.set_severity_override("ar1", None)
        assert applied.severity_override is None
        update_sql = sql.execute.call_args[0][0]
        assert "severity_override = NULL" in update_sql

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.set_severity_override("missing", "Low")


# ---------------------------------------------------------------------------
# rule_display_tags (B2-26 — enriched saveAppliedRules response)
# ---------------------------------------------------------------------------


class TestRuleDisplayTags:
    def test_derives_tags_from_live_registry_metadata(self, svc, registry):
        rule = _published_rule()
        rule.user_metadata = {"name": "Not Null Check", "dimension": "Completeness", "severity": "High"}
        rule.source = "import"
        registry.get_rule.return_value = rule
        assert svc.rule_display_tags("r1") == ("Not Null Check", "Completeness", "High", "import")

    def test_missing_rule_degrades_to_blanks(self, svc, registry):
        registry.get_rule.return_value = None
        assert svc.rule_display_tags("gone") == (None, None, None, None)


# ---------------------------------------------------------------------------
# column_pass_thresholds — persisted into and read back from user_metadata
# ---------------------------------------------------------------------------


class TestColumnPassThresholds:
    """Verify that column_pass_thresholds round-trips through user_metadata via the tags path."""

    def test_apply_rule_persists_column_pass_thresholds_in_user_metadata(self, svc, sql, registry):
        """Saving tags that contain RESERVED_COLUMN_PASS_THRESHOLDS_KEY persists the map."""
        from databricks_labs_dqx_app.backend.registry_models import (
            RESERVED_COLUMN_PASS_THRESHOLDS_KEY,
            get_applied_column_pass_thresholds,
        )

        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        tags_with_thresholds = {RESERVED_COLUMN_PASS_THRESHOLDS_KEY: {"email": 90, "name": 75}}
        applied = svc.apply_rule(
            "b1",
            "r1",
            [{"column": "customer_id"}],
            "alice@x",
            tags=tags_with_thresholds,
        )
        # The persisted user_metadata carries the raw map
        assert applied.user_metadata[RESERVED_COLUMN_PASS_THRESHOLDS_KEY] == {"email": 90, "name": 75}
        # The accessor reads it back correctly
        assert get_applied_column_pass_thresholds(applied.user_metadata) == {"email": 90, "name": 75}

    def test_apply_rule_no_column_pass_thresholds_leaves_metadata_clean(self, svc, sql, registry):
        """When no column_pass_thresholds are provided the key is absent from user_metadata."""
        from databricks_labs_dqx_app.backend.registry_models import (
            RESERVED_COLUMN_PASS_THRESHOLDS_KEY,
            get_applied_column_pass_thresholds,
        )

        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],
            [],
        ]
        applied = svc.apply_rule(
            "b1", "r1", [{"column": "customer_id"}], "alice@x", tags={}
        )
        assert RESERVED_COLUMN_PASS_THRESHOLDS_KEY not in applied.user_metadata
        assert get_applied_column_pass_thresholds(applied.user_metadata) == {}

    def test_applied_rule_out_from_domain_exposes_column_pass_thresholds(self):
        """AppliedRuleOut.from_domain reads column_pass_thresholds from user_metadata."""
        from databricks_labs_dqx_app.backend.registry_models import (
            RESERVED_COLUMN_PASS_THRESHOLDS_KEY,
            AppliedRule,
        )
        from databricks_labs_dqx_app.backend.models import AppliedRuleOut

        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            column_mapping=[{"column": "customer_id"}],
            user_metadata={RESERVED_COLUMN_PASS_THRESHOLDS_KEY: {"email": 90, "name": 75}},
        )
        out = AppliedRuleOut.from_domain(applied)
        assert out.column_pass_thresholds == {"email": 90, "name": 75}

    def test_applied_rule_out_from_domain_drops_invalid_values(self):
        """Invalid threshold values are clamped or dropped via the accessor."""
        from databricks_labs_dqx_app.backend.registry_models import (
            RESERVED_COLUMN_PASS_THRESHOLDS_KEY,
            AppliedRule,
        )
        from databricks_labs_dqx_app.backend.models import AppliedRuleOut

        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            column_mapping=[{"column": "customer_id"}],
            user_metadata={RESERVED_COLUMN_PASS_THRESHOLDS_KEY: {"email": 999, "name": "bad", "score": 50}},
        )
        out = AppliedRuleOut.from_domain(applied)
        # 999 clamps to 100, "bad" is dropped, 50 is valid
        assert out.column_pass_thresholds == {"email": 100, "score": 50}

    def test_applied_rule_out_from_domain_empty_when_absent(self):
        """When user_metadata has no column_pass_thresholds key the field defaults to {}."""
        from databricks_labs_dqx_app.backend.registry_models import AppliedRule
        from databricks_labs_dqx_app.backend.models import AppliedRuleOut

        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[], user_metadata={}
        )
        out = AppliedRuleOut.from_domain(applied)
        assert out.column_pass_thresholds == {}
