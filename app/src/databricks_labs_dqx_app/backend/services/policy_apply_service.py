"""Rule policy apply, register and preview orchestration.

Ties the pure :mod:`policy_engine` to storage: reads a user-scoped snapshot of
the tables a policy can reach, evaluates the policy against each table, and
attaches the ``auto`` matches as policy-made applied rules. Every action runs
for the ACTING user (whoever saved, re-scanned, opened, registered or
approved), so a policy only ever reaches tables that user may apply rules to:

- monitored tables need APPLY or MANAGE on the binding (admins/approvers bypass);
- unmonitored tables are registered (as drafts) only when the policy opts in
  with ``register_tables`` and the user's role may register tables — the user
  then owns the new binding.

Mappings a user deliberately removed (suppression tombstones) and mappings
already applied are never attached again. ``ambiguous`` matches are returned as
suggestions; ``partial`` matches are dropped. Trigger paths are best-effort:
one failing table, registration or attach is logged (ids only) and skipped.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.policy_models import RulePolicy, RulePolicyIn
from databricks_labs_dqx_app.backend.registry_models import MonitoredTable, RegistryRule, compute_mapping_hash
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_registration import can_register_tables
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.policy_engine import PolicyMatch, TableSnapshot, evaluate, in_scope
from databricks_labs_dqx_app.backend.services.policy_snapshot import PolicySnapshotReader, SnapshotResult
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_policy_service import RulePolicyNotFoundError, RulePolicyService

logger = logging.getLogger(__name__)

MAX_AMBIGUOUS_PER_TABLE = 20
_BINDING_LOOKUP_CHUNK = 500
_PREVIEW_POLICY_ID = ""


@dataclass(frozen=True)
class ActingUser:
    """The user whose action triggers a policy evaluation."""

    email: str
    role: UserRole
    principal_ids: frozenset[str]


@dataclass(frozen=True)
class PreviewTable:
    table_fqn: str
    monitored: bool
    matches: list[PolicyMatch]  # only status == "auto"


@dataclass(frozen=True)
class PolicyPreview:
    tables: list[PreviewTable]  # only tables with at least one auto match the user can apply to
    tables_matched: int
    columns_matched: int
    new_tables: int  # unmonitored tables that would be registered
    truncated: bool
    unreadable_catalogs: list[str]


@dataclass(frozen=True)
class ApplyResult:
    attached: int
    registered: int
    ambiguous: list[PolicyMatch]  # surfaced as suggestions


_EMPTY_RESULT = ApplyResult(attached=0, registered=0, ambiguous=[])


@dataclass
class _Totals:
    attached: int = 0
    registered: int = 0
    ambiguous: list[PolicyMatch] = field(default_factory=list)

    def result(self) -> ApplyResult:
        return ApplyResult(attached=self.attached, registered=self.registered, ambiguous=list(self.ambiguous))


def _mapping_hash(match: PolicyMatch) -> str:
    return compute_mapping_hash([match.column_mapping])


def _matched_columns(matches: list[PolicyMatch]) -> set[str]:
    columns: set[str] = set()
    for match in matches:
        for bound in match.column_mapping.values():
            columns.update(name for name in bound.split(",") if name)
    return columns


class _UserGate:
    """Per-run cache of what the acting user may do on each binding, and what is already there."""

    def __init__(self, user: ActingUser, permissions: PermissionsService, apply_rules: ApplyRulesService) -> None:
        self._user = user
        self._permissions = permissions
        self._apply_rules = apply_rules
        self._can_apply: dict[str, bool] = {}
        self._applied: dict[str, set[tuple[str, str]]] = {}

    def can_apply(self, binding_id: str) -> bool:
        cached = self._can_apply.get(binding_id)
        if cached is None:
            cached = self._check(binding_id)
            self._can_apply[binding_id] = cached
        return cached

    def grant_owner(self, binding_id: str) -> None:
        """A binding the user just registered: they own it and nothing is applied yet."""
        self._can_apply[binding_id] = True
        self._applied[binding_id] = set()

    def _check(self, binding_id: str) -> bool:
        object_type = ObjectType.MONITORED_TABLE.value
        owner = self._permissions.get_object_owner(object_type, binding_id)
        return any(
            self._permissions.has_privilege(
                object_type,
                binding_id,
                privilege,
                role=self._user.role,
                principal_ids=set(self._user.principal_ids),
                owner_email=owner,
                principal_email=self._user.email,
            )
            for privilege in (Privilege.APPLY, Privilege.MANAGE)
        )

    def _applied_keys(self, binding_id: str) -> set[tuple[str, str]]:
        keys = self._applied.get(binding_id)
        if keys is None:
            keys = {
                (row.rule_id, row.mapping_hash)
                for row in self._apply_rules.list_applied(binding_id)
                if row.mapping_hash
            }
            self._applied[binding_id] = keys
        return keys

    def is_new(self, binding_id: str, match: PolicyMatch) -> bool:
        """Not already applied on the binding and not deliberately removed (suppressed)."""
        mapping_hash = _mapping_hash(match)
        if (match.rule_id, mapping_hash) in self._applied_keys(binding_id):
            return False
        return not self._apply_rules.is_suppressed(binding_id, match.rule_id, mapping_hash)

    def fresh(self, binding_id: str, matches: list[PolicyMatch], limit: int | None = None) -> list[PolicyMatch]:
        """New matches for the binding, deduplicated by mapping, at most *limit*."""
        out: list[PolicyMatch] = []
        seen: set[tuple[str, str]] = set()
        for match in matches:
            if limit is not None and len(out) >= limit:
                break
            key = (match.rule_id, _mapping_hash(match))
            if key in seen or not self.is_new(binding_id, match):
                continue
            seen.add(key)
            out.append(match)
        return out

    def mark_applied(self, binding_id: str, match: PolicyMatch) -> None:
        self._applied_keys(binding_id).add((match.rule_id, _mapping_hash(match)))


class PolicyApplyService:
    """Evaluates rule policies for an acting user and attaches what they produce."""

    def __init__(
        self,
        policies: RulePolicyService,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
        apply_rules: ApplyRulesService,
        permissions: PermissionsService,
        snapshots: PolicySnapshotReader,
        register_table: Callable[[str, str], MonitoredTable],
    ) -> None:
        self._policies = policies
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._apply_rules = apply_rules
        self._permissions = permissions
        self._snapshots = snapshots
        self._register_table = register_table

    # ------------------------------------------------------------------
    # Preview (dry run)
    # ------------------------------------------------------------------

    def preview(self, rule_id: str, body: RulePolicyIn, user: ActingUser) -> PolicyPreview:
        """Show what saving *body* as a policy of *rule_id* would attach for *user*.

        Evaluates regardless of the rule's status so authors can see the impact of a
        draft rule's policy. Only ``auto`` matches on tables the user can apply rules to
        (or would register) are included; already-applied and suppressed mappings are
        left out.
        """
        rule = self._registry.get_rule(rule_id)
        snapshot = self._snapshots.for_scope(body.scope)
        if rule is None:
            return PolicyPreview([], 0, 0, 0, snapshot.truncated, list(snapshot.unreadable_catalogs))
        policy = RulePolicy(**body.model_dump(), policy_id=_PREVIEW_POLICY_ID, rule_id=rule_id)
        gate = _UserGate(user, self._permissions, self._apply_rules)
        bindings = self._binding_ids([table.table_fqn for table in snapshot.tables])
        may_register = policy.register_tables and can_register_tables(user.role)
        tables: list[PreviewTable] = []
        for table in snapshot.tables:
            autos = [m for m in evaluate(policy, rule.definition.slots, table) if m.status == "auto"]
            if not autos:
                continue
            binding_id = bindings.get(table.table_fqn)
            if binding_id is None:
                if may_register:
                    tables.append(PreviewTable(table.table_fqn, monitored=False, matches=autos))
                continue
            if not gate.can_apply(binding_id):
                continue
            fresh = gate.fresh(binding_id, autos)
            if fresh:
                tables.append(PreviewTable(table.table_fqn, monitored=True, matches=fresh))
        return PolicyPreview(
            tables=tables,
            tables_matched=len(tables),
            columns_matched=sum(len(_matched_columns(t.matches)) for t in tables),
            new_tables=sum(1 for t in tables if not t.monitored),
            truncated=snapshot.truncated,
            unreadable_catalogs=list(snapshot.unreadable_catalogs),
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def run_policy(self, policy_id: str, user: ActingUser) -> ApplyResult:
        """Evaluate one policy across its scope (after save or on re-scan).

        Raises:
            RulePolicyNotFoundError: unknown *policy_id*.
        """
        try:
            policy = self._policies.get(policy_id)
        except Exception:  # best-effort trigger: a storage failure must not fail the parent action
            logger.warning(f"Could not load rule policy {policy_id}", exc_info=True)
            return _EMPTY_RESULT
        if policy is None:
            raise RulePolicyNotFoundError(f"Rule policy '{policy_id}' not found.")
        totals = _Totals()
        try:
            rule = self._approved_rule(policy.rule_id)
            if rule is not None:
                self._run_scope(policy, rule, user, totals)
        except Exception:  # best-effort trigger: report what was done so far
            logger.warning(f"Rule policy {policy_id} evaluation failed", exc_info=True)
        return totals.result()

    def run_rule(self, rule_id: str, user: ActingUser) -> ApplyResult:
        """Evaluate every policy of *rule_id* across its scope (after the rule is approved)."""
        totals = _Totals()
        try:
            rule = self._approved_rule(rule_id)
            if rule is None:
                return _EMPTY_RESULT
            policies = self._policies.list_for_rule(rule_id)
        except Exception:  # best-effort trigger: a storage failure must not fail the approval
            logger.warning(f"Could not load policies for rule {rule_id}", exc_info=True)
            return _EMPTY_RESULT
        for policy in policies:
            try:
                self._run_scope(policy, rule, user, totals)
            except Exception:  # best-effort trigger: one policy failing must not stop the others
                logger.warning(f"Rule policy {policy.policy_id} evaluation failed", exc_info=True)
        return totals.result()

    def evaluate_binding(self, binding_id: str, user: ActingUser) -> ApplyResult:
        """Evaluate every policy that reaches one monitored table (on open or register)."""
        totals = _Totals()
        try:
            self._run_binding(binding_id, user, totals, attach=True)
        except Exception:  # best-effort trigger: opening or registering a table must never fail on this
            logger.warning(f"Policy evaluation for binding {binding_id} failed", exc_info=True)
        return totals.result()

    def ambiguous_for_binding(self, binding_id: str, user: ActingUser) -> list[PolicyMatch]:
        """Ambiguous policy pairings on one monitored table, offered as suggestions (nothing attached)."""
        totals = _Totals()
        try:
            self._run_binding(binding_id, user, totals, attach=False)
        except Exception:  # best-effort: suggestions are optional
            logger.warning(f"Policy suggestions for binding {binding_id} failed", exc_info=True)
        return totals.ambiguous

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _approved_rule(self, rule_id: str) -> RegistryRule | None:
        rule = self._registry.get_rule(rule_id)
        return rule if rule is not None and rule.status == "approved" else None

    def _run_scope(self, policy: RulePolicy, rule: RegistryRule, user: ActingUser, totals: _Totals) -> None:
        snapshot = self._snapshots.for_scope(policy.scope)
        if snapshot.truncated:
            logger.info(f"Rule policy {policy.policy_id} evaluation was capped at the snapshot table limit")
        bindings = self._binding_ids([table.table_fqn for table in snapshot.tables])
        gate = _UserGate(user, self._permissions, self._apply_rules)
        for table in snapshot.tables:
            self._apply_table(policy, rule, table, bindings, gate, user, totals, attach=True)

    def _run_binding(self, binding_id: str, user: ActingUser, totals: _Totals, *, attach: bool) -> None:
        table_fqn = self._monitored_tables.get_table_fqns({binding_id}).get(binding_id)
        if table_fqn is None:
            return
        gate = _UserGate(user, self._permissions, self._apply_rules)
        if not gate.can_apply(binding_id):
            return
        policies = [p for p in self._policies.list_all() if in_scope(p.scope, table_fqn)]
        if not policies:
            return
        rules = self._registry.get_rules_many(sorted({p.rule_id for p in policies}))
        snapshot: SnapshotResult = self._snapshots.for_tables([table_fqn])
        bindings = {table.table_fqn: binding_id for table in snapshot.tables}
        for policy in policies:
            rule = rules.get(policy.rule_id)
            if rule is None or rule.status != "approved":
                continue
            for table in snapshot.tables:
                self._apply_table(policy, rule, table, bindings, gate, user, totals, attach=attach)

    def _apply_table(
        self,
        policy: RulePolicy,
        rule: RegistryRule,
        table: TableSnapshot,
        bindings: dict[str, str],
        gate: _UserGate,
        user: ActingUser,
        totals: _Totals,
        *,
        attach: bool,
    ) -> None:
        slots = rule.definition.slots
        matches = evaluate(policy, slots, table)
        autos = [m for m in matches if m.status == "auto"]
        # An ambiguous match is only a usable suggestion when it carries a mapping to attach.
        ambiguous = [m for m in matches if m.status == "ambiguous" and (m.column_mapping or not slots)]
        if not autos and not ambiguous:
            return
        binding_id = bindings.get(table.table_fqn)
        if binding_id is None:
            if not attach or not autos or not policy.register_tables or not can_register_tables(user.role):
                return
            binding_id = self._register(policy, table.table_fqn, user)
            if binding_id is None:
                return
            bindings[table.table_fqn] = binding_id
            gate.grant_owner(binding_id)
            totals.registered += 1
        elif not gate.can_apply(binding_id):
            return
        if attach:
            for match in gate.fresh(binding_id, autos):
                if self._attach(binding_id, match, user):
                    gate.mark_applied(binding_id, match)
                    totals.attached += 1
        totals.ambiguous.extend(gate.fresh(binding_id, ambiguous, MAX_AMBIGUOUS_PER_TABLE))

    def _register(self, policy: RulePolicy, table_fqn: str, user: ActingUser) -> str | None:
        try:
            return self._register_table(table_fqn, user.email).binding_id
        except Exception:  # best-effort: one table failing to register must not stop the others
            logger.warning(f"Rule policy {policy.policy_id} could not register a matching table", exc_info=True)
            return None

    def _attach(self, binding_id: str, match: PolicyMatch, user: ActingUser) -> bool:
        try:
            applied = self._apply_rules.attach_policy_mapping(
                binding_id, match.rule_id, [match.column_mapping], user.email, match.policy_id
            )
        except Exception:  # best-effort: one failing attach must not stop the others
            logger.warning(
                f"Rule policy {match.policy_id} could not attach rule {match.rule_id} to binding {binding_id}",
                exc_info=True,
            )
            return False
        return applied is not None

    def _binding_ids(self, table_fqns: list[str]) -> dict[str, str]:
        found: dict[str, str] = {}
        unique = list(dict.fromkeys(table_fqns))
        for start in range(0, len(unique), _BINDING_LOOKUP_CHUNK):
            found.update(
                self._monitored_tables.get_binding_ids_by_table_fqn(unique[start : start + _BINDING_LOOKUP_CHUNK])
            )
        return found
