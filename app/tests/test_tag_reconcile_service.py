"""Unit tests for :class:`TagReconcileService` (apply-on-tag orchestrator).

Uses the REAL pure resolver (``tag_mapping_service.resolve``) so the wiring is
exercised end-to-end; ``create_autospec`` fakes stand in for every I/O
collaborator and a plain callable fakes the SP-authed column-tag reader.
"""

from __future__ import annotations

from typing import cast
from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.registry_models import (
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
    RuleSlot,
    compute_mapping_hash,
    set_slot_tags,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.tag_mapping_service import ColumnInfo
from databricks_labs_dqx_app.backend.services.tag_reconcile_service import TagReconcileService


def _rule(
    rule_id: str = "r1", *, status: str = "approved", slot_tags: dict[str, list[str]] | None = None
) -> RegistryRule:
    slots = [RuleSlot(name="c1", family="any", position=0, cardinality="one")]
    user_metadata: dict[str, object] = {}
    if slot_tags is not None:
        user_metadata = set_slot_tags(user_metadata, slot_tags)
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status=status,
        version=1,
        definition=RuleDefinition(body={"function": "is_not_null", "arguments": {}}, slots=slots),
        user_metadata=user_metadata,
    )


def _summary(binding_id: str, table_fqn: str) -> MonitoredTableSummary:
    return MonitoredTableSummary(table=MonitoredTable(binding_id=binding_id, table_fqn=table_fqn))


def _make_service(
    *,
    tag_auto_apply: bool,
    columns_by_fqn: dict[str, list[ColumnInfo]],
) -> tuple[TagReconcileService, dict[str, object]]:
    registry = create_autospec(RegistryService, instance=True)
    monitored_tables = create_autospec(MonitoredTableService, instance=True)
    apply_rules = create_autospec(ApplyRulesService, instance=True)
    app_settings = create_autospec(AppSettingsService, instance=True)
    app_settings.get_tag_auto_apply.return_value = tag_auto_apply

    def read_columns(table_fqn: str) -> list[ColumnInfo]:
        return columns_by_fqn.get(table_fqn, [])

    svc = TagReconcileService(
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=apply_rules,
        app_settings=app_settings,
        read_columns=read_columns,
    )
    return svc, {
        "registry": registry,
        "monitored_tables": monitored_tables,
        "apply_rules": apply_rules,
        "app_settings": app_settings,
    }


def test_setting_off_is_a_no_op() -> None:
    svc, deps = _make_service(tag_auto_apply=False, columns_by_fqn={})
    deps["registry"].get_rule.return_value = _rule(slot_tags={"c1": ["class.pii"]})
    deps["registry"].list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]
    deps["monitored_tables"].list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    assert svc.reconcile_rule("r1", "u@x.com") == 0
    assert svc.reconcile_table("b1", "cat.sch.t1", "u@x.com") == 0
    assert svc.sweep("u@x.com") == 0
    deps["apply_rules"].attach_auto_mapping.assert_not_called()


def test_reconcile_rule_attaches_matching_column() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn=columns)
    deps["registry"].get_rule.return_value = _rule(slot_tags={"c1": ["class.pii"]})
    deps["monitored_tables"].list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    count = svc.reconcile_rule("r1", "u@x.com")

    assert count == 1
    deps["apply_rules"].attach_auto_mapping.assert_called_once_with("b1", "r1", [{"c1": "email"}], "u@x.com")


def test_reconcile_rule_no_matching_column_skips() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("age", "INT", ["class.other"])]}
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn=columns)
    deps["registry"].get_rule.return_value = _rule(slot_tags={"c1": ["class.pii"]})
    deps["monitored_tables"].list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    assert svc.reconcile_rule("r1", "u@x.com") == 0
    deps["apply_rules"].attach_auto_mapping.assert_not_called()


def test_reconcile_rule_not_approved_skips() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn=columns)
    deps["registry"].get_rule.return_value = _rule(status="draft", slot_tags={"c1": ["class.pii"]})
    deps["monitored_tables"].list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    assert svc.reconcile_rule("r1", "u@x.com") == 0
    deps["apply_rules"].attach_auto_mapping.assert_not_called()


def test_reconcile_rule_empty_slot_tags_skips() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn=columns)
    deps["registry"].get_rule.return_value = _rule(slot_tags=None)
    deps["monitored_tables"].list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    assert svc.reconcile_rule("r1", "u@x.com") == 0
    deps["apply_rules"].attach_auto_mapping.assert_not_called()


def test_reconcile_rule_missing_rule_skips() -> None:
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn={})
    deps["registry"].get_rule.return_value = None

    assert svc.reconcile_rule("does-not-exist", "u@x.com") == 0
    deps["apply_rules"].attach_auto_mapping.assert_not_called()


def test_reconcile_table_attaches_all_matching_rules() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [
        _rule("r1", slot_tags={"c1": ["class.pii"]}),
        _rule("r2", slot_tags=None),  # no slot tags -> filtered out
    ]
    deps["monitored_tables"].list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    count = svc.reconcile_table("b1", "cat.sch.t1", "u@x.com")

    assert count == 1
    deps["registry"].list_rules.assert_called_once_with(status="approved")
    deps["apply_rules"].attach_auto_mapping.assert_called_once_with("b1", "r1", [{"c1": "email"}], "u@x.com")


def test_apply_rule_failure_on_one_table_does_not_abort_others() -> None:
    columns = {
        "cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])],
        "cat.sch.t2": [ColumnInfo("email", "STRING", ["class.pii"])],
    }
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn=columns)
    deps["registry"].get_rule.return_value = _rule(slot_tags={"c1": ["class.pii"]})
    deps["monitored_tables"].list_monitored_tables.return_value = [
        _summary("b1", "cat.sch.t1"),
        _summary("b2", "cat.sch.t2"),
    ]

    def apply_side_effect(binding_id: str, *args: object, **kwargs: object) -> object:
        if binding_id == "b1":
            raise RuntimeError("boom")
        return object()

    deps["apply_rules"].attach_auto_mapping.side_effect = apply_side_effect

    count = svc.reconcile_rule("r1", "u@x.com")

    # b1 raised (0 counted), b2 still attached (1 counted).
    assert count == 1
    assert deps["apply_rules"].attach_auto_mapping.call_count == 2


def test_sweep_reconciles_all_rules_across_all_tables() -> None:
    columns = {
        "cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])],
        "cat.sch.t2": [ColumnInfo("age", "INT", ["class.other"])],  # no match
    }
    svc, deps = _make_service(tag_auto_apply=True, columns_by_fqn=columns)
    deps["registry"].list_rules.return_value = [_rule("r1", slot_tags={"c1": ["class.pii"]})]
    deps["monitored_tables"].list_monitored_tables.return_value = [
        _summary("b1", "cat.sch.t1"),
        _summary("b2", "cat.sch.t2"),
    ]

    count = svc.sweep("u@x.com")

    assert count == 1
    deps["apply_rules"].attach_auto_mapping.assert_called_once_with("b1", "r1", [{"c1": "email"}], "u@x.com")


class _FakeApplyRules:
    """Records attach_auto_mapping calls and models its add-only natural-key semantics.

    A second attach for an already-seen ``(binding_id, rule_id, mapping_hash)``
    is a no-op (no new insert), mirroring the real service — so re-running a
    reconcile method never mutates or duplicates an existing row.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, list[dict[str, str]], str]] = []
        self.inserts = 0
        self._seen: set[tuple[str, str, str]] = set()

    def attach_auto_mapping(
        self, binding_id: str, rule_id: str, column_mapping: list[dict[str, str]], user_email: str
    ) -> object:
        self.calls.append((binding_id, rule_id, column_mapping, user_email))
        key = (binding_id, rule_id, compute_mapping_hash(column_mapping))
        if key not in self._seen:
            self._seen.add(key)
            self.inserts += 1
        return object()


def test_reconcile_rule_is_idempotent_across_two_runs() -> None:
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    fake_apply = _FakeApplyRules()
    registry = create_autospec(RegistryService, instance=True)
    monitored_tables = create_autospec(MonitoredTableService, instance=True)
    app_settings = create_autospec(AppSettingsService, instance=True)
    app_settings.get_tag_auto_apply.return_value = True
    registry.get_rule.return_value = _rule(slot_tags={"c1": ["class.pii"]})
    monitored_tables.list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    svc = TagReconcileService(
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=cast(ApplyRulesService, fake_apply),
        app_settings=app_settings,
        read_columns=lambda fqn: columns.get(fqn, []),
    )

    assert svc.reconcile_rule("r1", "u@x.com") == 1
    assert svc.reconcile_rule("r1", "u@x.com") == 1
    # Both runs attach the same natural key; the second is a no-op insert.
    assert len(fake_apply.calls) == 2
    assert fake_apply.inserts == 1


class _SuppressingApplyRules:
    """Models attach_auto_mapping's suppression skip: a suppressed natural key returns None."""

    def __init__(self) -> None:
        self.inserts = 0
        self._suppressed: set[tuple[str, str, str]] = set()

    def suppress(self, binding_id: str, rule_id: str, column_mapping: list[dict[str, str]]) -> None:
        self._suppressed.add((binding_id, rule_id, compute_mapping_hash(column_mapping)))

    def attach_auto_mapping(
        self, binding_id: str, rule_id: str, column_mapping: list[dict[str, str]], user_email: str
    ) -> object | None:
        if (binding_id, rule_id, compute_mapping_hash(column_mapping)) in self._suppressed:
            return None
        self.inserts += 1
        return object()


def test_sweep_does_not_reattach_suppressed_mapping() -> None:
    # After a steward removes an auto-applied row (suppression recorded), a
    # later sweep must NOT re-attach that mapping.
    columns = {"cat.sch.t1": [ColumnInfo("email", "STRING", ["class.pii"])]}
    fake_apply = _SuppressingApplyRules()
    fake_apply.suppress("b1", "r1", [{"c1": "email"}])
    registry = create_autospec(RegistryService, instance=True)
    monitored_tables = create_autospec(MonitoredTableService, instance=True)
    app_settings = create_autospec(AppSettingsService, instance=True)
    app_settings.get_tag_auto_apply.return_value = True
    registry.list_rules.return_value = [_rule(slot_tags={"c1": ["class.pii"]})]
    monitored_tables.list_monitored_tables.return_value = [_summary("b1", "cat.sch.t1")]

    svc = TagReconcileService(
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=cast(ApplyRulesService, fake_apply),
        app_settings=app_settings,
        read_columns=lambda fqn: columns.get(fqn, []),
    )

    # attach_auto_mapping is still called, but returns None (skipped) — nothing inserted.
    assert svc.sweep("u@x.com") == 1
    assert fake_apply.inserts == 0
