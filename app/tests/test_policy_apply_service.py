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
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    DuplicateMonitoredTableError,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.policy_apply_service import (
    MAX_AMBIGUOUS_PER_RUN,
    MAX_REGISTRATIONS_PER_RUN,
    ActingUser,
    ApplyResult,
    PolicyApplyService,
    RuleNotFoundError,
)
from databricks_labs_dqx_app.backend.services.policy_evaluation_cache import (
    OpenEvaluationDebounce,
    PolicyCatalogCache,
    PolicyGeneration,
)
from databricks_labs_dqx_app.backend.services.policy_engine import ColumnSnapshot, SnapshotNeeds, TableSnapshot
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
        scope=PolicyScope(catalogs=["c"]) if register_tables else PolicyScope(),  # registering needs catalogs
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
    suppression_unreadable: set[str] = field(default_factory=set)
    registered: list[str] = field(default_factory=list)
    register_fails: set[str] = field(default_factory=set)
    attach_fails: set[str] = field(default_factory=set)
    attach_calls: list[tuple[str, str, list[dict[str, str]], str, str]] = field(default_factory=list)

    def register_table(self, table_fqn: str, user_email: str) -> MonitoredTable:
        if table_fqn in self.register_fails:
            raise RuntimeError("register failed")
        if table_fqn in self.bindings:
            raise DuplicateMonitoredTableError(f"already monitored: {table_fqn}")
        binding_id = f"new-{len(self.registered) + 1}"
        self.registered.append(table_fqn)
        self.bindings[table_fqn] = binding_id
        return MonitoredTable(binding_id=binding_id, table_fqn=table_fqn, created_by=user_email)

    def suppressed_keys(self, binding_id: str) -> set[tuple[str, str]]:
        if binding_id in self.suppression_unreadable:
            raise RuntimeError("suppression lookup failed")
        return {(r, h) for b, r, h in self.suppressed if b == binding_id}

    def service(
        self,
        *,
        catalog_cache: PolicyCatalogCache | None = None,
        open_debounce: OpenEvaluationDebounce | None = None,
        **caps: int,
    ) -> PolicyApplyService:
        register: Callable[[str, str], MonitoredTable] = self.register_table
        return PolicyApplyService(
            catalog_cache=catalog_cache,
            open_debounce=open_debounce,
            policies=self.policies,
            registry=self.registry,
            monitored_tables=self.monitored,
            apply_rules=self.apply_rules,
            permissions=self.permissions,
            snapshots=self.snapshots,
            register_table=register,
            **caps,
        )

    def set_tables(self, *tables: TableSnapshot) -> None:
        result = SnapshotResult(tables=list(tables), truncated=False, unreadable_catalogs=[])
        self.snapshots.for_scope.return_value = result
        self.snapshots.for_tables.side_effect = lambda fqns, **_: SnapshotResult(
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
    w.monitored.list_table_bindings.side_effect = lambda: dict(w.bindings)
    w.permissions.get_object_owner.return_value = "owner@x"
    w.permissions.has_privilege.side_effect = lambda object_type, object_id, privilege, **_: (
        object_type == ObjectType.MONITORED_TABLE.value and (object_id, privilege) in w.grants
    )
    w.apply_rules.list_applied.side_effect = lambda binding_id: list(w.applied.get(binding_id, []))
    w.apply_rules.is_suppressed.side_effect = lambda b, r, h: (b, r, h) in w.suppressed
    w.apply_rules.list_suppressed_keys.side_effect = w.suppressed_keys

    def attach(
        binding_id: str,
        rule_id: str,
        mapping: list[dict[str, str]],
        user_email: str,
        policy_id: str,
        *,
        suppression_checked: bool = False,
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
    preview = world.service().preview("r1", _policy_in(register_tables=True), AUTHOR)
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
    world.snapshots.for_tables.assert_called_once()
    assert world.snapshots.for_tables.call_args.args[0] == ["c.s.t"]
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


def test_suppression_lookup_failure_fails_closed(world: World) -> None:
    world.bindings = {"c.s.flaky": "b-flaky", "c.s.ok": "b-ok"}
    world.grants = {("b-flaky", Privilege.APPLY), ("b-ok", Privilege.APPLY)}
    world.suppression_unreadable = {"b-flaky"}
    world.set_tables(_snap("c.s.flaky", _col("email", "pii")), _snap("c.s.ok", _col("email", "pii")))
    world.policies.get.return_value = _policy()

    result = world.service().run_policy("p1", AUTHOR)

    assert _attached(world) == [("b-ok", {"column": "email"})]
    assert result.attached == 1


def test_suppressions_read_once_per_binding(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", *[_col(f"c{i}", "pii") for i in range(5)]))
    world.policies.get.return_value = _policy()

    assert world.service().run_policy("p1", AUTHOR).attached == 5
    world.apply_rules.list_suppressed_keys.assert_called_once_with("b1")
    world.apply_rules.is_suppressed.assert_not_called()


def test_registration_is_capped_per_run(world: World) -> None:
    world.set_tables(*[_snap(f"c.s.t{i}", _col("email", "pii")) for i in range(5)])
    world.policies.get.return_value = _policy(register_tables=True)

    result = world.service(max_registrations_per_run=2).run_policy("p1", AUTHOR)

    assert world.registered == ["c.s.t0", "c.s.t1"]
    assert result.registered == 2
    assert result.registration_capped is True
    assert result.registrations_skipped == 3
    assert result.attached == 2
    assert MAX_REGISTRATIONS_PER_RUN == 200


def test_registration_not_capped_under_limit(world: World) -> None:
    world.set_tables(_snap("c.s.t0", _col("email", "pii")))
    world.policies.get.return_value = _policy(register_tables=True)
    result = world.service().run_policy("p1", AUTHOR)
    assert result.registration_capped is False
    assert result.registrations_skipped == 0


def test_ambiguous_capped_per_run(world: World) -> None:
    world.registry.get_rule.side_effect = lambda rule_id: _rule(rule_id, slots=["a", "b"])
    world.bindings = {f"c.s.t{i}": f"b{i}" for i in range(3)}
    world.grants = {(f"b{i}", Privilege.APPLY) for i in range(3)}
    columns = [_col(f"a{i}", "pii") for i in range(6)] + [_col(f"z{i}", "tel") for i in range(6)]
    world.set_tables(*[_snap(f"c.s.t{i}", *columns) for i in range(3)])
    world.policies.get.return_value = _policy(conditions={"a": ["pii"], "b": ["tel"]})

    capped = world.service(max_ambiguous_per_run=30).run_policy("p1", AUTHOR)
    assert len(capped.ambiguous) == 30
    # Default run cap still keeps the per-(policy, table) cap of 20.
    assert len(world.service().run_policy("p1", AUTHOR).ambiguous) == 60
    assert MAX_AMBIGUOUS_PER_RUN == 500


def test_preview_missing_rule_raises_before_snapshot(world: World) -> None:
    world.registry.get_rule.side_effect = lambda rule_id: None
    with pytest.raises(RuleNotFoundError):
        world.service().preview("missing", _policy_in(), AUTHOR)
    world.snapshots.for_scope.assert_not_called()


# ---------------------------------------------------------------------------
# Shared snapshots (bulk register, multi-policy runs)
# ---------------------------------------------------------------------------


def test_evaluate_tables_reads_one_snapshot_for_all_tables(world: World) -> None:
    world.bindings = {"c.s.a": "b1", "c.s.b": "b2"}
    world.grants = {("b1", Privilege.APPLY), ("b2", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")), _snap("c.s.b", _col("phone", "pii")))
    world.policies.list_all.return_value = [_policy()]

    result = world.service().evaluate_tables(["c.s.a", "c.s.b"], AUTHOR)

    assert result.attached == 2
    assert {(b, m[0]["column"]) for b, _, m, _, _ in world.attach_calls} == {("b1", "email"), ("b2", "phone")}
    world.snapshots.for_tables.assert_called_once()
    assert set(world.snapshots.for_tables.call_args.args[0]) == {"c.s.a", "c.s.b"}
    world.policies.list_all.assert_called_once()


def test_evaluate_tables_skips_tables_without_permission_or_binding(world: World) -> None:
    world.bindings = {"c.s.a": "b1", "c.s.b": "b2"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")), _snap("c.s.b", _col("phone", "pii")))
    world.policies.list_all.return_value = [_policy()]

    result = world.service().evaluate_tables(["c.s.a", "c.s.b", "c.s.unmonitored"], AUTHOR)

    assert result.attached == 1
    assert world.snapshots.for_tables.call_args.args[0] == ["c.s.a"]


def test_evaluate_tables_never_raises(world: World) -> None:
    world.monitored.get_binding_ids_by_table_fqn.side_effect = RuntimeError("db down")
    assert world.service().evaluate_tables(["c.s.a"], AUTHOR) == ApplyResult(attached=0, registered=0, ambiguous=[])


def test_run_policies_shares_one_snapshot_per_scope(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    stored = {
        "p1": _policy("p1", rule_id="r1"),
        "p2": _policy("p2", rule_id="r2"),
        "p3": _policy("p3", rule_id="r3"),
    }
    world.policies.get.side_effect = stored.get

    result = world.service().run_policies(["p1", "p2", "p3"], AUTHOR)

    assert result.attached == 3
    world.snapshots.for_tables.assert_called_once()
    assert world.snapshots.for_tables.call_args.args[0] == ["c.s.t"]
    world.monitored.list_table_bindings.assert_called_once()


def test_run_policies_is_best_effort(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    stored = {"p2": _policy("p2", rule_id="r2")}
    world.policies.get.side_effect = stored.get

    result = world.service().run_policies(["missing", "p2"], AUTHOR)

    assert result.attached == 1


def test_run_policy_reads_its_scope_once(world: World) -> None:
    world.bindings = {"c.s.a": "b1", "c.s.b": "b2"}
    world.grants = {("b1", Privilege.APPLY), ("b2", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")), _snap("c.s.b", _col("phone", "pii")))
    world.policies.get.return_value = _policy()

    assert world.service().run_policy("p1", AUTHOR).attached == 2
    world.snapshots.for_tables.assert_called_once()


# ---------------------------------------------------------------------------
# Monitored-only reads when a policy does not register tables
# ---------------------------------------------------------------------------


def test_policy_without_register_reads_only_monitored_tables_in_scope(world: World) -> None:
    world.bindings = {"c.s.a": "b1", "other.s.b": "b2"}
    world.grants = {("b1", Privilege.APPLY), ("b2", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")), _snap("other.s.b", _col("email", "pii")))
    scoped = _policy().model_copy(update={"scope": PolicyScope(catalogs=["c"])})
    world.policies.get.return_value = scoped
    world.policies.list_for_rule.return_value = [scoped]
    svc = world.service()

    assert svc.run_policy("p1", AUTHOR).attached == 1
    assert svc.run_rule("r1", AUTHOR).attached == 1
    preview = svc.preview("r1", _policy_in().model_copy(update={"scope": PolicyScope(catalogs=["c"])}), AUTHOR)

    assert [t.table_fqn for t in preview.tables] == ["c.s.a"]
    world.snapshots.for_scope.assert_not_called()
    assert [c.args[0] for c in world.snapshots.for_tables.call_args_list] == [["c.s.a"]] * 3


def test_policy_with_register_scans_the_scope(world: World) -> None:
    world.bindings = {"c.s.a": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")), _snap("c.s.new", _col("email", "pii")))
    world.policies.get.return_value = _policy(register_tables=True)

    result = world.service().run_policy("p1", AUTHOR)

    assert (result.attached, result.registered) == (2, 1)
    world.snapshots.for_scope.assert_called_once()
    world.snapshots.for_tables.assert_called_once()
    assert world.snapshots.for_tables.call_args.args[0] == ["c.s.a"]
    assert world.registered == ["c.s.new"]


def test_truncated_scope_scan_never_drops_monitored_matches(world: World) -> None:
    world.bindings = {"c.s.a": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")))
    world.snapshots.for_scope.return_value = SnapshotResult(tables=[], truncated=True, unreadable_catalogs=[])
    body = _policy_in(register_tables=True).model_copy(update={"scope": PolicyScope(catalogs=["c"])})

    preview = world.service().preview("r1", body, AUTHOR)

    assert [t.table_fqn for t in preview.tables] == ["c.s.a"] and preview.truncated is True
    world.policies.get.return_value = _policy(register_tables=True)
    assert world.service().run_policy("p1", AUTHOR).attached == 1


def test_scope_scan_rows_for_monitored_tables_are_not_duplicated(world: World) -> None:
    world.bindings = {"c.s.a": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")), _snap("c.s.new", _col("email", "pii")))

    preview = world.service().preview("r1", _policy_in(register_tables=True), AUTHOR)

    assert sorted((t.table_fqn, t.monitored) for t in preview.tables) == [("c.s.a", True), ("c.s.new", False)]


def test_register_policy_for_a_user_who_cannot_register_reads_only_monitored(world: World) -> None:
    world.bindings = {"c.s.a": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.a", _col("email", "pii")))

    world.service().preview("r1", _policy_in(register_tables=True), VIEWER)

    world.snapshots.for_scope.assert_not_called()
    world.snapshots.for_tables.assert_called_once()
    assert world.snapshots.for_tables.call_args.args[0] == ["c.s.a"]


def test_no_monitored_table_in_scope_reads_nothing(world: World) -> None:
    world.bindings = {"other.s.b": "b2"}
    scoped = _policy().model_copy(update={"scope": PolicyScope(catalogs=["c"])})
    world.policies.get.return_value = scoped

    assert world.service().run_policy("p1", AUTHOR) == ApplyResult(attached=0, registered=0, ambiguous=[])
    preview = world.service().preview("r1", _policy_in().model_copy(update={"scope": scoped.scope}), AUTHOR)

    assert preview.tables_matched == 0 and not preview.truncated
    world.snapshots.for_scope.assert_not_called()
    world.snapshots.for_tables.assert_not_called()


# ---------------------------------------------------------------------------
# Preview shows already-attached matches (QA B1)
# ---------------------------------------------------------------------------


def test_preview_after_attach_still_lists_the_table_as_already_attached(world: World) -> None:
    world.bindings = {"c.s.t": "b1", "c.s.u": "b2"}
    world.grants = {("b1", Privilege.APPLY), ("b2", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")), _snap("c.s.u", _col("email", "pii")))
    world.mark_applied("b1", "r1", {"column": "email"})

    preview = world.service().preview("r1", _policy_in(), AUTHOR)

    by_table = {t.table_fqn: t for t in preview.tables}
    assert set(by_table) == {"c.s.t", "c.s.u"}
    assert by_table["c.s.t"].already_attached is True
    assert by_table["c.s.u"].already_attached is False
    assert preview.tables_matched == 2


def test_preview_still_hides_suppressed_mappings(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    world.suppressed = {("b1", "r1", compute_mapping_hash([{"column": "email"}]))}

    assert world.service().preview("r1", _policy_in(), AUTHOR).tables == []


def test_preview_with_some_mappings_attached_is_not_already_attached(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii"), _col("phone", "pii")))
    world.mark_applied("b1", "r1", {"column": "email"})

    [table] = world.service().preview("r1", _policy_in(), AUTHOR).tables

    assert len(table.matches) == 2 and table.already_attached is False


# ---------------------------------------------------------------------------
# Lakebase-only pre-check before any snapshot read (perf P2)
# ---------------------------------------------------------------------------


def _out_of_scope_world(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))


def test_no_policy_in_scope_reads_nothing(world: World) -> None:
    _out_of_scope_world(world)
    elsewhere = _policy().model_copy(update={"scope": PolicyScope(catalogs=["other"])})
    world.policies.list_all.return_value = [elsewhere]
    svc = world.service()

    assert svc.evaluate_binding("b1", AUTHOR).attached == 0
    assert svc.evaluate_tables(["c.s.t"], AUTHOR).attached == 0
    assert svc.ambiguous_for_binding("b1", AUTHOR) == []
    world.snapshots.for_tables.assert_not_called()
    world.snapshots.for_scope.assert_not_called()
    world.permissions.has_privilege.assert_not_called()


def test_in_scope_policy_of_unapproved_rule_reads_nothing(world: World) -> None:
    _out_of_scope_world(world)
    world.policies.list_all.return_value = [_policy()]
    world.registry.get_rules_many.side_effect = lambda ids: {i: _rule(i, status="draft") for i in ids}
    svc = world.service()

    assert svc.evaluate_binding("b1", AUTHOR).attached == 0
    assert svc.ambiguous_for_binding("b1", AUTHOR) == []
    world.snapshots.for_tables.assert_not_called()
    world.permissions.has_privilege.assert_not_called()


def test_reads_ask_only_for_the_facts_the_policies_use(world: World) -> None:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    world.policies.get.return_value = _policy()
    world.policies.list_all.return_value = [_policy()]
    svc = world.service()

    svc.run_policy("p1", AUTHOR)
    svc.evaluate_binding("b1", AUTHOR)
    svc.preview("r1", _policy_in(), AUTHOR)

    needs = [c.kwargs["needs"] for c in world.snapshots.for_tables.call_args_list]
    assert needs == [SnapshotNeeds(column_tags=True)] * 3


# ---------------------------------------------------------------------------
# One run: registrations visible to later policies, one gate
# ---------------------------------------------------------------------------


def _registering(policy_id: str, rule_id: str, scope: PolicyScope) -> RulePolicy:
    return _policy(policy_id, rule_id=rule_id, register_tables=True).model_copy(update={"scope": scope})


def test_table_registered_by_one_policy_is_attached_by_the_next(world: World) -> None:
    world.set_tables(_snap("c.s.new", _col("email", "pii")))
    stored = {
        "p1": _registering("p1", "r1", PolicyScope(catalogs=["c"])),
        "p2": _registering("p2", "r2", PolicyScope(catalogs=["c"], schemas=["c.s"])),  # overlapping scope
    }
    world.policies.get.side_effect = stored.get

    result = world.service().run_policies(["p1", "p2"], AUTHOR)

    assert world.registered == ["c.s.new"]
    assert result.registered == 1
    assert {(b, r) for b, r, _, _, _ in world.attach_calls} == {("new-1", "r1"), ("new-1", "r2")}


def test_table_registered_meanwhile_is_attached_to_the_existing_binding(world: World) -> None:
    world.set_tables(_snap("c.s.new", _col("email", "pii")))
    world.policies.get.return_value = _registering("p1", "r1", PolicyScope(catalogs=["c"]))
    world.grants = {("theirs", Privilege.APPLY)}
    svc = world.service()
    world.monitored.list_table_bindings.side_effect = dict  # not monitored when the run read the list
    world.bindings = {"c.s.new": "theirs"}  # ...but registered by someone else before this run registers it

    result = svc.run_policy("p1", AUTHOR)

    assert world.registered == []
    assert result.registered == 0
    assert world.attach_calls == [("theirs", "r1", [{"column": "email"}], "alice@x", "p1")]


def test_table_registered_meanwhile_without_permission_attaches_nothing(world: World) -> None:
    world.set_tables(_snap("c.s.new", _col("email", "pii")))
    world.policies.get.return_value = _registering("p1", "r1", PolicyScope(catalogs=["c"]))
    world.monitored.list_table_bindings.side_effect = dict
    world.bindings = {"c.s.new": "theirs"}

    assert world.service().run_policy("p1", AUTHOR).attached == 0
    world.apply_rules.attach_policy_mapping.assert_not_called()


def test_one_permission_and_tombstone_read_per_binding_per_run(world: World) -> None:
    n = 4
    world.bindings = {f"c.s.t{i}": f"b{i}" for i in range(n)}
    world.grants = {(f"b{i}", Privilege.APPLY) for i in range(n)}
    world.set_tables(*[_snap(f"c.s.t{i}", _col("email", "pii"), _col("ssn", "gov")) for i in range(n)])
    stored = {
        "p1": _policy("p1", rule_id="r1", conditions={"column": ["pii"]}),
        "p2": _policy("p2", rule_id="r2", conditions={"column": ["gov"]}),
    }
    world.policies.get.side_effect = stored.get
    world.policies.list_for_rule.return_value = [
        _policy("p1", conditions={"column": ["pii"]}),
        _policy("p2", conditions={"column": ["gov"]}),
    ]

    for run in (
        lambda svc: svc.run_policies(["p1", "p2"], AUTHOR),
        lambda svc: svc.run_rule("r1", AUTHOR),
    ):
        world.permissions.get_object_owner.reset_mock()
        world.apply_rules.list_suppressed_keys.reset_mock()
        world.apply_rules.list_applied.reset_mock()
        world.attach_calls.clear()

        assert run(world.service()).attached == 2 * n
        assert world.permissions.get_object_owner.call_count == n
        assert world.apply_rules.list_suppressed_keys.call_count == n
        assert world.apply_rules.list_applied.call_count == n
    world.apply_rules.is_suppressed.assert_not_called()
    assert all(
        c.kwargs.get("suppression_checked") is True for c in world.apply_rules.attach_policy_mapping.call_args_list
    )


# ---------------------------------------------------------------------------
# Opening a table: coverage check, debounce, cached policy list
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@dataclass
class _OpenWorld:
    world: World
    generation: PolicyGeneration
    clock: _Clock
    catalog: PolicyCatalogCache
    debounce: OpenEvaluationDebounce

    def service(self) -> PolicyApplyService:
        return self.world.service(catalog_cache=self.catalog, open_debounce=self.debounce)

    def open(self, user: ActingUser = AUTHOR) -> bool:
        svc = self.service()
        opened = svc.begin_open("b1", "c.s.t", user)
        if opened.run:
            svc.evaluate_on_open("b1", opened, user)
        return opened.pending


@pytest.fixture
def opening(world: World) -> _OpenWorld:
    world.bindings = {"c.s.t": "b1"}
    world.grants = {("b1", Privilege.APPLY), ("b1", Privilege.MANAGE)}
    world.set_tables(_snap("c.s.t", _col("email", "pii")))
    world.policies.list_all.return_value = [_policy()]
    generation = PolicyGeneration()
    clock = _Clock()
    return _OpenWorld(
        world=world,
        generation=generation,
        clock=clock,
        catalog=PolicyCatalogCache(generation, clock=clock),
        debounce=OpenEvaluationDebounce(generation, clock=clock),
    )


def test_second_open_within_the_window_does_not_evaluate(opening: _OpenWorld) -> None:
    assert opening.open() is True
    assert opening.world.snapshots.for_tables.call_count == 1

    opening.clock.now += 30
    assert opening.open() is False
    assert opening.world.snapshots.for_tables.call_count == 1
    opening.world.policies.list_all.assert_called_once()  # the policy list is cached too
    opening.world.registry.get_rules_many.assert_called_once()


def test_open_evaluates_again_after_a_policy_change(opening: _OpenWorld) -> None:
    opening.open()
    opening.generation.bump()  # e.g. a rule policy was saved

    assert opening.open() is True
    assert opening.world.snapshots.for_tables.call_count == 2
    assert opening.world.policies.list_all.call_count == 2


def test_open_by_a_different_user_evaluates(opening: _OpenWorld) -> None:
    opening.open()
    bob = ActingUser(email="bob@x", role=UserRole.RULE_AUTHOR, principal_ids=frozenset({"u-bob"}))

    assert opening.open(bob) is True
    assert opening.world.snapshots.for_tables.call_count == 2


def test_open_evaluates_again_after_the_window(opening: _OpenWorld) -> None:
    opening.open()
    opening.clock.now += 301

    assert opening.open() is True
    assert opening.world.snapshots.for_tables.call_count == 2


def test_open_while_an_evaluation_runs_is_pending_without_scheduling_another(opening: _OpenWorld) -> None:
    svc = opening.service()
    first = svc.begin_open("b1", "c.s.t", AUTHOR)
    second = svc.begin_open("b1", "c.s.t", AUTHOR)

    assert (first.pending, first.run) == (True, True)
    assert (second.pending, second.run) == (True, False)
    svc.evaluate_on_open("b1", first, AUTHOR)
    third = svc.begin_open("b1", "c.s.t", AUTHOR)
    assert (third.pending, third.run) == (False, False)


def test_open_of_a_table_no_policy_reaches_schedules_nothing(opening: _OpenWorld) -> None:
    elsewhere = _policy().model_copy(update={"scope": PolicyScope(catalogs=["other"])})
    opening.world.policies.list_all.return_value = [elsewhere]

    opened = opening.service().begin_open("b1", "c.s.t", AUTHOR)

    assert (opened.pending, opened.run) == (False, False)
    opening.world.snapshots.for_tables.assert_not_called()


def test_suggestions_reuse_the_cached_policy_list_but_read_tombstones_live(opening: _OpenWorld) -> None:
    world = opening.world
    world.registry.get_rules_many.side_effect = lambda ids: {i: _rule(i, slots=["a", "b"]) for i in ids}
    # p, q, r share no name stems -> every pairing ties -> ambiguous.
    world.set_tables(_snap("c.s.t", _col("p", "pii"), _col("q", "tel"), _col("r", "tel")))
    world.policies.list_all.return_value = [_policy(conditions={"a": ["pii"], "b": ["tel"]})]

    first = opening.service().ambiguous_for_binding("b1", AUTHOR)
    assert {m.column_mapping["b"] for m in first} == {"q", "r"}
    dismissed = next(m for m in first if m.column_mapping["b"] == "q")
    world.suppressed.add(("b1", "r1", compute_mapping_hash([dismissed.column_mapping])))  # the user dismissed it

    second = opening.service().ambiguous_for_binding("b1", AUTHOR)
    assert {m.column_mapping["b"] for m in second} == {"r"}
    world.policies.list_all.assert_called_once()
    world.registry.get_rules_many.assert_called_once()


def test_a_failed_open_evaluation_is_retried_on_the_next_open(opening: _OpenWorld) -> None:
    opening.world.snapshots.for_tables.side_effect = lambda fqns, **_: SnapshotResult(
        tables=[], truncated=False, unreadable_catalogs=["c"]
    )
    assert opening.open() is True

    opening.world.set_tables(_snap("c.s.t", _col("email", "pii")))
    assert opening.open() is True  # the incomplete read released the claim instead of debouncing
    assert opening.world.attach_calls == [("b1", "r1", [{"column": "email"}], "alice@x", "p1")]


def test_an_unreadable_tombstone_set_does_not_debounce_the_open(opening: _OpenWorld) -> None:
    opening.world.suppression_unreadable = {"b1"}
    assert opening.open() is True
    opening.world.suppression_unreadable = set()
    assert opening.open() is True
    assert len(opening.world.attach_calls) == 1


def test_open_by_the_same_user_with_other_groups_evaluates(opening: _OpenWorld) -> None:
    opening.open()
    regrouped = ActingUser(email=AUTHOR.email, role=AUTHOR.role, principal_ids=AUTHOR.principal_ids | {"g-stewards"})

    assert opening.open(regrouped) is True
    assert opening.world.snapshots.for_tables.call_count == 2


def test_a_failing_duplicate_lookup_skips_only_that_table(world: World) -> None:
    world.set_tables(_snap("c.s.dup", _col("email", "pii")), _snap("c.s.new", _col("email", "pii")))
    world.policies.get.return_value = _registering("p1", "r1", PolicyScope(catalogs=["c"]))
    world.monitored.list_table_bindings.side_effect = dict
    world.bindings = {"c.s.dup": "theirs"}
    world.monitored.get_binding_ids_by_table_fqn.side_effect = RuntimeError("lookup failed")

    result = world.service().run_policy("p1", AUTHOR)

    assert world.registered == ["c.s.new"]
    assert result.attached == 1


def test_preview_flags_the_catalogs_of_tables_that_could_not_be_read(world: World) -> None:
    world.bindings = {"c.s.a": "b1"}
    world.grants = {("b1", Privilege.APPLY)}
    world.snapshots.for_tables.side_effect = lambda fqns, **_: SnapshotResult(
        tables=[], truncated=False, unreadable_catalogs=["c"], unreadable_tables=["c.s.a"]
    )
    world.snapshots.for_scope.return_value = SnapshotResult(tables=[], truncated=False, unreadable_catalogs=["C"])

    preview = world.service().preview("r1", _policy_in(register_tables=True), AUTHOR)

    assert preview.unreadable_catalogs == ["c"]  # deduplicated case-insensitively
