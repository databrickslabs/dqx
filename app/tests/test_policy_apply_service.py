"""Tests for ``PolicyApplyService`` — rule policy apply / register / preview orchestration.

Every collaborator is an autospec fake; the real pure ``policy_engine`` decides
which mappings a policy produces.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.policy_models import (
    ColumnCondition,
    PolicyScope,
    RulePolicy,
    RulePolicyIn,
)
from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
    RuleStatus,
    compute_mapping_hash,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.policy_apply_service import (
    ActingUser,
    ApplyResult,
    PolicyApplyService,
)
from databricks_labs_dqx_app.backend.services.policy_engine import ColumnSnapshot, TableSnapshot
from databricks_labs_dqx_app.backend.services.policy_snapshot import PolicySnapshotReader, SnapshotResult
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_policy_service import RulePolicyNotFoundError, RulePolicyService

AUTHOR = ActingUser(email="alice@x", role=UserRole.RULE_AUTHOR, principal_ids=frozenset({"u-alice"}))
VIEWER = ActingUser(email="vic@x", role=UserRole.VIEWER, principal_ids=frozenset({"u-vic"}))


def _rule(rule_id: str = "r1", status: RuleStatus = "approved", slots: list[str] | None = None) -> RegistryRule:
    slot_names = ["column"] if slots is None else slots
    definition = RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {n: f"{{{{{n}}}}}" for n in slot_names}},
            "slots": [
                {"name": n, "family": "any", "position": i, "cardinality": "one"} for i, n in enumerate(slot_names)
            ],
            "parameters": [],
        }
    )
    return RegistryRule(rule_id=rule_id, mode="dqx_native", status=status, version=1, definition=definition)


def _policy_in(*, register_tables: bool = False, conditions: dict[str, list[str]] | None = None) -> RulePolicyIn:
    conds = {"column": ["pii"]} if conditions is None else conditions
    return RulePolicyIn(
        name="PII columns",
        scope=PolicyScope(),
        column_conditions={slot: ColumnCondition(kind="tags", tags=tags) for slot, tags in conds.items()},
        register_tables=register_tables,
    )


def _policy(
    policy_id: str = "p1",
    rule_id: str = "r1",
    *,
    register_tables: bool = False,
    conditions: dict[str, list[str]] | None = None,
) -> RulePolicy:
    body = _policy_in(register_tables=register_tables, conditions=conditions)
    return RulePolicy(**body.model_dump(), policy_id=policy_id, rule_id=rule_id)


def _col(name: str, *tags: str) -> ColumnSnapshot:
    return ColumnSnapshot(name=name, type_name="STRING", tags=tags)


def _snap(fqn: str, *columns: ColumnSnapshot) -> TableSnapshot:
    return TableSnapshot(
        table_fqn=fqn,
        owner=None,
        catalog_owner=None,
        schema_owner=None,
        catalog_tags=(),
        schema_tags=(),
        table_tags=(),
        columns=columns,
    )


@dataclass
class World:
    """Fakes plus the state they model (bindings, grants, applied rows, tombstones)."""

    policies: RulePolicyService
    registry: RegistryService
    monitored: MonitoredTableService
    apply_rules: ApplyRulesService
    permissions: PermissionsService
    snapshots: PolicySnapshotReader
    bindings: dict[str, str] = field(default_factory=dict)  # table_fqn -> binding_id
    grants: set[tuple[str, Privilege]] = field(default_factory=set)
    applied: dict[str, list[AppliedRule]] = field(default_factory=dict)
    suppressed: set[tuple[str, str, str]] = field(default_factory=set)
    registered: list[str] = field(default_factory=list)
    register_fails: set[str] = field(default_factory=set)
    attach_fails: set[str] = field(default_factory=set)
    attach_calls: list[tuple[str, str, list[dict[str, str]], str, str]] = field(default_factory=list)

    def register_table(self, table_fqn: str, user_email: str) -> MonitoredTable:
        if table_fqn in self.register_fails:
            raise RuntimeError("register failed")
        binding_id = f"new-{len(self.registered) + 1}"
        self.registered.append(table_fqn)
        self.bindings[table_fqn] = binding_id
        return MonitoredTable(binding_id=binding_id, table_fqn=table_fqn, created_by=user_email)

    def service(self) -> PolicyApplyService:
        register: Callable[[str, str], MonitoredTable] = self.register_table
        return PolicyApplyService(
            policies=self.policies,
            registry=self.registry,
            monitored_tables=self.monitored,
            apply_rules=self.apply_rules,
            permissions=self.permissions,
            snapshots=self.snapshots,
            register_table=register,
        )

    def set_tables(self, *tables: TableSnapshot) -> None:
        result = SnapshotResult(tables=list(tables), truncated=False, unreadable_catalogs=[])
        self.snapshots.for_scope.return_value = result
        self.snapshots.for_tables.side_effect = lambda fqns: SnapshotResult(
            tables=[t for t in tables if t.table_fqn in fqns], truncated=False, unreadable_catalogs=[]
        )

    def mark_applied(self, binding_id: str, rule_id: str, mapping: dict[str, str]) -> None:
        self.applied.setdefault(binding_id, []).append(
            AppliedRule(
                id=f"a{len(self.applied)}",
                binding_id=binding_id,
                rule_id=rule_id,
                column_mapping=[mapping],
                mapping_hash=compute_mapping_hash([mapping]),
            )
        )


@pytest.fixture
def world() -> World:
    w = World(
        policies=create_autospec(RulePolicyService, instance=True),
        registry=create_autospec(RegistryService, instance=True),
        monitored=create_autospec(MonitoredTableService, instance=True),
        apply_rules=create_autospec(ApplyRulesService, instance=True),
        permissions=create_autospec(PermissionsService, instance=True),
        snapshots=create_autospec(PolicySnapshotReader, instance=True),
    )
    w.registry.get_rule.side_effect = lambda rule_id: _rule(rule_id)
    w.registry.get_rules_many.side_effect = lambda ids: {i: _rule(i) for i in ids}
    w.monitored.get_binding_ids_by_table_fqn.side_effect = lambda fqns: {
        f: w.bindings[f] for f in fqns if f in w.bindings
    }
    w.monitored.get_table_fqns.side_effect = lambda ids: {b: f for f, b in w.bindings.items() if b in ids}
    w.permissions.get_object_owner.return_value = "owner@x"
    w.permissions.has_privilege.side_effect = lambda object_type, object_id, privilege, **_: (
        object_type == ObjectType.MONITORED_TABLE.value and (object_id, privilege) in w.grants
    )
    w.apply_rules.list_applied.side_effect = lambda binding_id: list(w.applied.get(binding_id, []))
    w.apply_rules.is_suppressed.side_effect = lambda b, r, h: (b, r, h) in w.suppressed

    def attach(
        binding_id: str, rule_id: str, mapping: list[dict[str, str]], user_email: str, policy_id: str
    ) -> AppliedRule:
        w.attach_calls.append((binding_id, rule_id, mapping, user_email, policy_id))
        if binding_id in w.attach_fails:
            raise RuntimeError("attach failed")
        return AppliedRule(id="x", binding_id=binding_id, rule_id=rule_id, column_mapping=mapping)

    w.apply_rules.attach_policy_mapping.side_effect = attach
    return w


def _attached(world: World) -> list[tuple[str, dict[str, str]]]:
    return [(b, m[0]) for b, _, m, _, _ in world.attach_calls]


# ---------------------------------------------------------------------------
# preview
# ---------------------------------------------------------------------------


def test_preview_counts_only_auto_and_permitted_tables(world: World) -> None:
    world.bindings = {"c.s.ok": "b-ok", "c.s.denied": "b-denied", "c.s.amb": "b-amb"}
    world.grants = {("b-ok", Privilege.APPLY), ("b-amb", Privilege.APPLY)}
    world.registry.get_rule.side_effect = lambda rule_id: _rule(rule_id, slots=["a", "b"])
    # Two-slot rule: "ok" has one candidate per slot -> auto; "denied" is not
    # applicable for the user; "amb" has two candidates for slot b -> ambiguous only.
    world.set_tables(
        _snap("c.s.ok", _col("home_email", "pii"), _col("home_phone", "tel")),
        _snap("c.s.denied", _col("home_email", "pii"), _col("home_phone", "tel")),
        _snap("c.s.amb", _col("p", "pii"), _col("q", "tel"), _col("r", "tel")),
    )
    body = _policy_in(conditions={"a": ["pii"], "b": ["tel"]})

    preview = world.service().preview("r1", body, AUTHOR)

    assert [t.table_fqn for t in preview.tables] == ["c.s.ok"]
    assert preview.tables_matched == 1
    assert preview.columns_matched == 2
    assert preview.new_tables == 0
    assert all(m.status == "auto" for t in preview.tables for m in t.matches)
    assert preview.tables[0].monitored is True
    world.apply_rules.attach_policy_mapping.assert_not_called()


def test_preview_evaluates_draft_rule(world: World) -> None:
    world.registry.get_rule.side_effect = lambda rule_id: _rule(rule_id, status="draft")
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    preview = world.service().preview("r1", _policy_in(), AUTHOR)
    assert preview.tables_matched == 1


def test_preview_new_tables_only_when_register_tables(world: World) -> None:
    world.bindings = {"c.s.monitored": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.monitored", _col("email", "pii")), _snap("c.s.new", _col("email", "pii")))
    svc = world.service()

    without = svc.preview("r1", _policy_in(register_tables=False), AUTHOR)
    assert [t.table_fqn for t in without.tables] == ["c.s.monitored"]
    assert without.new_tables == 0

    with_register = svc.preview("r1", _policy_in(register_tables=True), AUTHOR)
    assert {t.table_fqn: t.monitored for t in with_register.tables} == {"c.s.monitored": True, "c.s.new": False}
    assert with_register.new_tables == 1
    assert with_register.tables_matched == 2

    # A role that cannot register tables never sees new tables.
    viewer = svc.preview("r1", _policy_in(register_tables=True), VIEWER)
    assert viewer.new_tables == 0
    assert world.registered == []


def test_preview_reports_truncation_and_unreadable(world: World) -> None:
    world.snapshots.for_scope.return_value = SnapshotResult(tables=[], truncated=True, unreadable_catalogs=["x"])
    preview = world.service().preview("r1", _policy_in(), AUTHOR)
    assert preview.truncated is True
    assert preview.unreadable_catalogs == ["x"]


# ---------------------------------------------------------------------------
# permission gate (Review Focus #1)
# ---------------------------------------------------------------------------


def test_apply_skips_tables_without_apply_or_manage(world: World) -> None:
    world.bindings = {"c.s.ok": "b-ok", "c.s.denied": "b-denied"}
    world.grants = {("b-ok", Privilege.APPLY), ("b-denied", Privilege.SELECT)}
    world.set_tables(_snap("c.s.ok", _col("email", "pii")), _snap("c.s.denied", _col("email", "pii")))
    world.policies.get.return_value = _policy()

    result = world.service().run_policy("p1", AUTHOR)

    assert result.attached == 1
    assert _attached(world) == [("b-ok", {"column": "email"})]
    # The gate is evaluated for the acting user, with the binding owner resolved.
    kwargs = world.permissions.has_privilege.call_args.kwargs
    assert kwargs["role"] == UserRole.RULE_AUTHOR
    assert kwargs["principal_ids"] == {"u-alice"}
    assert kwargs["principal_email"] == "alice@x"
    assert kwargs["owner_email"] == "owner@x"


def test_apply_accepts_manage_without_apply(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.MANAGE)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    world.policies.get.return_value = _policy()

    result = world.service().run_policy("p1", AUTHOR)

    assert result.attached == 1
    assert world.attach_calls == [("b1", "r1", [{"column": "email"}], "alice@x", "p1")]


# ---------------------------------------------------------------------------
# suppression / already applied (Review Focus #2)
# ---------------------------------------------------------------------------


def test_apply_respects_suppression(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii"), _col("phone", "pii")))
    world.suppressed = {("b1", "r1", compute_mapping_hash([{"column": "email"}]))}
    world.policies.get.return_value = _policy()
    svc = world.service()

    result = svc.run_policy("p1", AUTHOR)
    assert _attached(world) == [("b1", {"column": "phone"})]
    assert result.attached == 1

    # No later trigger re-attaches the suppressed mapping either.
    world.attach_calls.clear()
    world.policies.list_all.return_value = [_policy()]
    world.policies.list_for_rule.return_value = [_policy()]
    svc.evaluate_binding("b1", AUTHOR)
    svc.run_rule("r1", AUTHOR)
    assert ("b1", {"column": "email"}) not in _attached(world)
    preview = svc.preview("r1", _policy_in(), AUTHOR)
    assert [m.column_mapping for t in preview.tables for m in t.matches] == [{"column": "phone"}]


def test_apply_skips_already_applied(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii"), _col("phone", "pii")))
    world.mark_applied("b1", "r1", {"column": "email"})
    world.policies.get.return_value = _policy()

    result = world.service().run_policy("p1", AUTHOR)

    assert _attached(world) == [("b1", {"column": "phone"})]
    assert result.attached == 1


# ---------------------------------------------------------------------------
# rule status (Review Focus #4)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", ["draft", "pending_approval", "deprecated", "rejected", None])
def test_unapproved_rule_attaches_nothing(world: World, status: RuleStatus | None) -> None:
    world.registry.get_rule.side_effect = lambda rule_id: None if status is None else _rule(rule_id, status=status)
    world.registry.get_rules_many.side_effect = lambda ids: (
        {} if status is None else {i: _rule(i, status=status) for i in ids}
    )
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    world.policies.get.return_value = _policy(register_tables=True)
    world.policies.list_for_rule.return_value = [_policy()]
    world.policies.list_all.return_value = [_policy()]
    svc = world.service()

    empty = ApplyResult(attached=0, registered=0, ambiguous=[])
    assert svc.run_policy("p1", AUTHOR) == empty
    assert svc.run_rule("r1", AUTHOR) == empty
    assert svc.evaluate_binding("b1", AUTHOR) == empty
    assert svc.ambiguous_for_binding("b1", AUTHOR) == []
    world.apply_rules.attach_policy_mapping.assert_not_called()
    assert world.registered == []


def test_run_policy_unknown_policy_raises(world: World) -> None:
    world.policies.get.return_value = None
    with pytest.raises(RulePolicyNotFoundError):
        world.service().run_policy("missing", AUTHOR)


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------


def test_run_policy_registers_unmonitored_then_attaches(world: World) -> None:
    world.set_tables(_snap("c.s.new", _col("email", "pii")), _snap("c.s.nomatch", _col("id")))
    world.policies.get.return_value = _policy(register_tables=True)

    result = world.service().run_policy("p1", AUTHOR)

    assert world.registered == ["c.s.new"]
    assert result.registered == 1
    assert result.attached == 1
    assert world.attach_calls == [("new-1", "r1", [{"column": "email"}], "alice@x", "p1")]


def test_run_policy_does_not_register_without_flag_or_role(world: World) -> None:
    world.set_tables(_snap("c.s.new", _col("email", "pii")))
    world.policies.get.return_value = _policy(register_tables=False)
    svc = world.service()
    assert svc.run_policy("p1", AUTHOR).registered == 0

    world.policies.get.return_value = _policy(register_tables=True)
    assert svc.run_policy("p1", VIEWER).registered == 0
    assert world.registered == []
    world.apply_rules.attach_policy_mapping.assert_not_called()


def test_register_failure_is_isolated(world: World) -> None:
    world.set_tables(_snap("c.s.bad", _col("email", "pii")), _snap("c.s.good", _col("email", "pii")))
    world.register_fails = {"c.s.bad"}
    world.policies.get.return_value = _policy(register_tables=True)

    result = world.service().run_policy("p1", AUTHOR)

    assert world.registered == ["c.s.good"]
    assert result.registered == 1
    assert result.attached == 1


# ---------------------------------------------------------------------------
# run_rule / evaluate_binding / ambiguous
# ---------------------------------------------------------------------------


def test_run_rule_runs_every_policy_of_the_rule(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii"), _col("ssn", "gov")))
    world.policies.list_for_rule.return_value = [
        _policy("p1", conditions={"column": ["pii"]}),
        _policy("p2", conditions={"column": ["gov"]}),
    ]

    result = world.service().run_rule("r1", AUTHOR)

    assert result.attached == 2
    assert {(m[0]["column"], pid) for _, _, m, _, pid in world.attach_calls} == {("email", "p1"), ("ssn", "p2")}


def test_evaluate_binding_runs_all_policies_for_table(world: World) -> None:
    world.bindings = {"c.s.t": "b1", "c.s.other": "b2"}
    world.grants = {("b1", Privilege.APPLY), ("b2", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii"), _col("ssn", "gov")), _snap("c.s.other", _col("x", "pii")))
    out_of_scope = _policy("p3", rule_id="r3").model_copy(update={"scope": PolicyScope(catalogs=["other"])})
    world.policies.list_all.return_value = [
        _policy("p1", rule_id="r1", conditions={"column": ["pii"]}),
        _policy("p2", rule_id="r2", conditions={"column": ["gov"]}),
        out_of_scope,
    ]

    result = world.service().evaluate_binding("b1", AUTHOR)

    assert result.attached == 2
    assert {(b, r, m[0]["column"], pid) for b, r, m, _, pid in world.attach_calls} == {
        ("b1", "r1", "email", "p1"),
        ("b1", "r2", "ssn", "p2"),
    }
    world.snapshots.for_tables.assert_called_once_with(["c.s.t"])
    assert set(world.registry.get_rules_many.call_args.args[0]) == {"r1", "r2"}


def test_evaluate_binding_without_permission_does_nothing(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    world.policies.list_all.return_value = [_policy()]
    assert world.service().evaluate_binding("b1", AUTHOR) == ApplyResult(attached=0, registered=0, ambiguous=[])
    world.apply_rules.attach_policy_mapping.assert_not_called()


def test_ambiguous_returned_not_attached(world: World) -> None:
    world.registry.get_rule.side_effect = lambda rule_id: _rule(rule_id, slots=["a", "b"])
    world.registry.get_rules_many.side_effect = lambda ids: {i: _rule(i, slots=["a", "b"]) for i in ids}
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    # p, q, r share no name stems -> every pairing ties -> ambiguous.
    world.set_tables(_snap("c.s.t", _col("p", "pii"), _col("q", "tel"), _col("r", "tel")))
    policy = _policy(conditions={"a": ["pii"], "b": ["tel"]})
    world.policies.get.return_value = policy
    world.policies.list_all.return_value = [policy]
    svc = world.service()

    result = svc.run_policy("p1", AUTHOR)

    world.apply_rules.attach_policy_mapping.assert_not_called()
    assert result.attached == 0
    assert {m.column_mapping["b"] for m in result.ambiguous} == {"q", "r"}
    assert all(m.status == "ambiguous" for m in result.ambiguous)
    assert {m.column_mapping["b"] for m in svc.ambiguous_for_binding("b1", AUTHOR)} == {"q", "r"}


def test_ambiguous_capped_per_policy_and_table(world: World) -> None:
    world.registry.get_rule.side_effect = lambda rule_id: _rule(rule_id, slots=["a", "b"])
    world.registry.get_rules_many.side_effect = lambda ids: {i: _rule(i, slots=["a", "b"]) for i in ids}
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    columns = [_col(f"a{i}", "pii") for i in range(6)] + [_col(f"z{i}", "tel") for i in range(6)]
    world.set_tables(_snap("c.s.t", *columns))
    policy = _policy(conditions={"a": ["pii"], "b": ["tel"]})
    world.policies.list_all.return_value = [policy]

    assert len(world.service().ambiguous_for_binding("b1", AUTHOR)) == 20


def test_attach_failure_is_isolated(world: World) -> None:
    world.bindings = {"c.s.bad": "b-bad", "c.s.good": "b-good"}
    world.grants = {("b-bad", Privilege.APPLY), ("b-good", Privilege.APPLY)}
    world.attach_fails = {"b-bad"}
    world.set_tables(_snap("c.s.bad", _col("email", "pii")), _snap("c.s.good", _col("email", "pii")))
    world.policies.get.return_value = _policy()

    result = world.service().run_policy("p1", AUTHOR)

    assert result.attached == 1
    assert {b for b, _ in _attached(world)} == {"b-bad", "b-good"}


def test_snapshot_failure_never_raises(world: World) -> None:
    world.snapshots.for_scope.side_effect = RuntimeError("warehouse down")
    world.policies.get.return_value = _policy()
    assert world.service().run_policy("p1", AUTHOR) == ApplyResult(attached=0, registered=0, ambiguous=[])
