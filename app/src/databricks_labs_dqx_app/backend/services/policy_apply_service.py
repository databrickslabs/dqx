"""Rule policy apply, register and preview orchestration.

Ties the pure :mod:`policy_engine` to storage: reads a user-scoped snapshot of
the tables a policy can reach, evaluates the policy against each table, and
attaches the ``auto`` matches as policy-made applied rules. The monitored
tables in a policy's scope are always read directly (nothing is read when there
are none); only a policy that may register tables (``register_tables`` and a
role that may register) also scans its scope in Unity Catalog, to find
unmonitored candidates. Every action runs
for the ACTING user (whoever saved, re-scanned, opened, registered or
approved), so a policy only ever reaches tables that user may apply rules to:

- monitored tables need APPLY or MANAGE on the binding (admins/approvers bypass);
- unmonitored tables are registered (as drafts) only when the policy opts in
  with ``register_tables`` and the user's role may register tables — the user
  then owns the new binding.

Mappings a user deliberately removed (suppression tombstones) and mappings
already applied are never attached again; when the tombstones of a binding
cannot be read, nothing is attached there (fail closed). ``ambiguous`` matches
are returned as suggestions; ``partial`` matches are dropped. Trigger paths are
best-effort: one failing table, registration or attach is logged (ids only) and
skipped. Each run registers at most ``MAX_REGISTRATIONS_PER_RUN`` tables and
returns at most ``MAX_AMBIGUOUS_PER_RUN`` ambiguous matches.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.policy_models import PolicyScope, RulePolicy, RulePolicyIn
from databricks_labs_dqx_app.backend.registry_models import MonitoredTable, RegistryRule, compute_mapping_hash
from databricks_labs_dqx_app.backend.sanitization import safe_log_value
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.monitored_table_registration import can_register_tables
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    DuplicateMonitoredTableError,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.policy_engine import (
    ALL_NEEDS,
    PolicyMatch,
    SnapshotNeeds,
    TableSnapshot,
    evaluate,
    in_scope,
    needs_of,
)
from databricks_labs_dqx_app.backend.services.policy_evaluation_cache import (
    OpenClaim,
    OpenEvaluationDebounce,
    PolicyCatalogCache,
)
from databricks_labs_dqx_app.backend.services.policy_snapshot import (
    PolicySnapshotReader,
    SnapshotResult,
    snapshot_cache_identity,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_policy_service import RulePolicyNotFoundError, RulePolicyService

logger = logging.getLogger(__name__)

MAX_AMBIGUOUS_PER_TABLE = 20
MAX_AMBIGUOUS_PER_RUN = 500
MAX_REGISTRATIONS_PER_RUN = 200
_BINDING_LOOKUP_CHUNK = 500
_PREVIEW_POLICY_ID = ""


class RuleNotFoundError(LookupError):
    """Raised by :meth:`PolicyApplyService.preview` when the rule does not exist."""


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
    already_attached: bool = False  # every match is already applied on the (monitored) table


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
    registration_capped: bool = False  # True when MAX_REGISTRATIONS_PER_RUN stopped further registrations
    registrations_skipped: int = 0  # not-yet-monitored matching tables left unregistered by the cap


_EMPTY_RESULT = ApplyResult(attached=0, registered=0, ambiguous=[])


@dataclass(frozen=True)
class OpenEvaluation:
    """What opening a monitored table does about rule policies (see :meth:`PolicyApplyService.begin_open`)."""

    pending: bool  # an evaluation was scheduled now or is still running: attached rules may appear shortly
    run: bool  # the caller must run :meth:`PolicyApplyService.evaluate_on_open` (in the background)
    key: tuple[str, str, int] | None = None  # debounce claim to release when the evaluation finishes


@dataclass
class _Totals:
    max_registrations: int
    max_ambiguous: int
    attached: int = 0
    registered: int = 0
    registrations_skipped: int = 0
    ambiguous: list[PolicyMatch] = field(default_factory=list)

    def ambiguous_room(self) -> int:
        return min(MAX_AMBIGUOUS_PER_TABLE, max(self.max_ambiguous - len(self.ambiguous), 0))

    def result(self) -> ApplyResult:
        return ApplyResult(
            attached=self.attached,
            registered=self.registered,
            ambiguous=list(self.ambiguous),
            registration_capped=self.registrations_skipped > 0,
            registrations_skipped=self.registrations_skipped,
        )


def _mapping_hash(match: PolicyMatch) -> str:
    return compute_mapping_hash([match.column_mapping])


def _matched_columns(matches: list[PolicyMatch]) -> set[str]:
    # A "many" slot is bound by the policy engine as one comma-joined column list; split it back here.
    columns: set[str] = set()
    for match in matches:
        for bound in match.column_mapping.values():
            columns.update(name for name in bound.split(",") if name)
    return columns


@dataclass
class _RunReads:
    """What one evaluation run has read so far, shared by every policy it evaluates."""

    needs: SnapshotNeeds = ALL_NEEDS  # facts the run's policies read (see policy_engine.needs_of)
    monitored: dict[str, str] | None = None  # table_fqn -> binding_id of every monitored table
    registered: dict[str, str] = field(default_factory=dict)  # lower-cased table_fqn -> binding_id found this run
    snapshots: dict[tuple[str, bool], tuple[SnapshotResult, dict[str, str]]] = field(default_factory=dict)


class _UserGate:
    """Per-run cache of what the acting user may do on each binding, and what is already there."""

    def __init__(self, user: ActingUser, permissions: PermissionsService, apply_rules: ApplyRulesService) -> None:
        self._user = user
        self._permissions = permissions
        self._apply_rules = apply_rules
        self._can_apply: dict[str, bool] = {}
        self._applied: dict[str, set[tuple[str, str]]] = {}
        self._suppressed: dict[str, set[tuple[str, str]] | None] = {}  # None = unreadable -> attach nothing

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
        self._suppressed[binding_id] = set()

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

    def _suppressed_keys(self, binding_id: str) -> set[tuple[str, str]] | None:
        if binding_id not in self._suppressed:
            try:
                self._suppressed[binding_id] = self._apply_rules.list_suppressed_keys(binding_id)
            except Exception:  # fail closed: unreadable tombstones block every attach on this binding
                logger.warning(
                    f"Suppression lookup failed for binding {safe_log_value(binding_id)}; attaching nothing",
                    exc_info=True,
                )
                self._suppressed[binding_id] = None
        return self._suppressed[binding_id]

    def is_new(self, binding_id: str, match: PolicyMatch) -> bool:
        """Not already applied on the binding and not deliberately removed (suppressed)."""
        key = (match.rule_id, _mapping_hash(match))
        if key in self._applied_keys(binding_id):
            return False
        suppressed = self._suppressed_keys(binding_id)
        return suppressed is not None and key not in suppressed

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

    def visible(self, binding_id: str, matches: list[PolicyMatch]) -> tuple[list[PolicyMatch], bool]:
        """Matches a preview shows (deduplicated; deliberately removed ones left out) and whether all are applied.

        Already-applied mappings are kept: a policy still "matches" a table it attached to.
        When the tombstones cannot be read, only already-applied mappings are shown.
        """
        applied = self._applied_keys(binding_id)
        suppressed = self._suppressed_keys(binding_id)
        out: list[PolicyMatch] = []
        seen: set[tuple[str, str]] = set()
        for match in matches:
            key = (match.rule_id, _mapping_hash(match))
            if key in seen:
                continue
            if key not in applied and (suppressed is None or key in suppressed):
                continue
            seen.add(key)
            out.append(match)
        return out, bool(out) and all((m.rule_id, _mapping_hash(m)) in applied for m in out)

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
        *,
        max_registrations_per_run: int = MAX_REGISTRATIONS_PER_RUN,
        max_ambiguous_per_run: int = MAX_AMBIGUOUS_PER_RUN,
        catalog_cache: PolicyCatalogCache | None = None,
        open_debounce: OpenEvaluationDebounce | None = None,
    ) -> None:
        """Create the service.

        Args:
            catalog_cache: Optional process-wide cache of the policy list and their rules
                (used by the per-table evaluations: open, register, suggestions).
            open_debounce: Optional process-wide debounce of on-open evaluations
                (see :meth:`begin_open`); without one every open evaluates.
        """
        self._policies = policies
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._apply_rules = apply_rules
        self._permissions = permissions
        self._snapshots = snapshots
        self._register_table = register_table
        self._max_registrations = max_registrations_per_run
        self._max_ambiguous = max_ambiguous_per_run
        self._catalog_cache = catalog_cache
        self._open_debounce = open_debounce

    # ------------------------------------------------------------------
    # Preview (dry run)
    # ------------------------------------------------------------------

    def preview(self, rule_id: str, body: RulePolicyIn, user: ActingUser) -> PolicyPreview:
        """Show what saving *body* as a policy of *rule_id* would attach for *user*.

        Evaluates regardless of the rule's status so authors can see the impact of a
        draft rule's policy. Only ``auto`` matches on tables the user can apply rules to
        (or would register) are included. Mappings already applied stay in (the table is
        flagged ``already_attached`` when all of them are); only deliberately removed
        (suppressed) mappings are left out.

        Unlike the trigger methods, preview may raise: it backs an interactive request.

        Raises:
            RuleNotFoundError: *rule_id* does not exist (checked before any snapshot read).
        """
        rule = self._registry.get_rule(rule_id)
        if rule is None:
            raise RuleNotFoundError(f"Registry rule '{rule_id}' not found.")
        policy = RulePolicy(**body.model_dump(), policy_id=_PREVIEW_POLICY_ID, rule_id=rule_id)
        may_register = policy.register_tables and can_register_tables(user.role)
        snapshot, bindings = self._read_scope(policy.scope, may_register, _RunReads(needs=needs_of([policy])))
        gate = _UserGate(user, self._permissions, self._apply_rules)
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
            shown, attached = gate.visible(binding_id, autos)
            if shown:
                tables.append(PreviewTable(table.table_fqn, monitored=True, matches=shown, already_attached=attached))
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
            logger.warning(f"Could not load rule policy {safe_log_value(policy_id)}", exc_info=True)
            return _EMPTY_RESULT
        if policy is None:
            raise RulePolicyNotFoundError(f"Rule policy '{policy_id}' not found.")
        totals = self._totals()
        try:
            rule = self._approved_rule(policy.rule_id)
            if rule is not None:
                gate = self._gate(user)
                self._run_scope(policy, rule, user, totals, _RunReads(needs=needs_of([policy])), gate)
        except Exception:  # best-effort trigger: report what was done so far
            logger.warning(f"Rule policy {safe_log_value(policy_id)} evaluation failed", exc_info=True)
        return totals.result()

    def run_rule(self, rule_id: str, user: ActingUser) -> ApplyResult:
        """Evaluate every policy of *rule_id* across its scope (after the rule is approved)."""
        totals = self._totals()
        try:
            rule = self._approved_rule(rule_id)
            if rule is None:
                return _EMPTY_RESULT
            policies = self._policies.list_for_rule(rule_id)
        except Exception:  # best-effort trigger: a storage failure must not fail the approval
            logger.warning(f"Could not load policies for rule {safe_log_value(rule_id)}", exc_info=True)
            return _EMPTY_RESULT
        reads = _RunReads(needs=needs_of(policies))
        gate = self._gate(user)  # one gate per run: permission / applied / tombstone reads happen once per binding
        for policy in policies:
            try:
                self._run_scope(policy, rule, user, totals, reads, gate)
            except Exception:  # best-effort trigger: one policy failing must not stop the others
                logger.warning(f"Rule policy {safe_log_value(policy.policy_id)} evaluation failed", exc_info=True)
        return totals.result()

    def run_policies(self, policy_ids: list[str], user: ActingUser) -> ApplyResult:
        """Evaluate several policies (after a multi-rule save); policies with the same scope share one snapshot.

        Best-effort and never raises: an unknown or failing policy is logged and skipped.
        """
        totals = self._totals()
        runnable: list[tuple[RulePolicy, RegistryRule]] = []
        for policy_id in policy_ids:
            try:
                policy = self._policies.get(policy_id)
                rule = self._approved_rule(policy.rule_id) if policy is not None else None
                if policy is not None and rule is not None:
                    runnable.append((policy, rule))
            except Exception:  # best-effort trigger: one policy failing to load must not stop the others
                logger.warning(f"Could not load rule policy {safe_log_value(policy_id)}", exc_info=True)
        reads = _RunReads(needs=needs_of(policy for policy, _ in runnable))
        gate = self._gate(user)  # one gate per run: permission / applied / tombstone reads happen once per binding
        for policy, rule in runnable:
            try:
                self._run_scope(policy, rule, user, totals, reads, gate)
            except Exception:  # best-effort trigger: one policy failing must not stop the others
                logger.warning(f"Rule policy {safe_log_value(policy.policy_id)} evaluation failed", exc_info=True)
        return totals.result()

    def evaluate_binding(self, binding_id: str, user: ActingUser) -> ApplyResult:
        """Evaluate every policy that reaches one monitored table (on open or register)."""
        totals = self._totals()
        try:
            table_fqn = self._monitored_tables.get_table_fqns({binding_id}).get(binding_id)
            if table_fqn is not None:
                self._run_bindings({table_fqn: binding_id}, user, totals, attach=True)
        except Exception:  # best-effort trigger: opening or registering a table must never fail on this
            logger.warning(f"Policy evaluation for binding {safe_log_value(binding_id)} failed", exc_info=True)
        return totals.result()

    def evaluate_tables(self, table_fqns: list[str], user: ActingUser) -> ApplyResult:
        """Evaluate every policy reaching each monitored table in *table_fqns* (after a bulk register).

        Reads one snapshot for all of the tables. Best-effort and never raises.
        """
        totals = self._totals()
        try:
            self._run_bindings(self._binding_ids(table_fqns), user, totals, attach=True)
        except Exception:  # best-effort trigger: registering tables must never fail on this
            logger.warning("Policy evaluation for registered tables failed", exc_info=True)
        return totals.result()

    def begin_open(self, binding_id: str, table_fqn: str, user: ActingUser) -> OpenEvaluation:
        """Decide what opening *binding_id* (monitored table *table_fqn*) does about rule policies.

        Nothing runs when no policy of an approved rule has the table in scope, or when the
        same user opened it in the current policy generation within the debounce window (its
        evaluation finished). When an earlier evaluation is still running the result is
        ``pending`` without scheduling another. Never raises: on a lookup failure the
        evaluation runs.
        """
        try:
            if not self._reaching(table_fqn):
                return OpenEvaluation(pending=False, run=False)
        except Exception:  # best-effort: let the background evaluation decide
            logger.warning(f"Policy coverage lookup for binding {safe_log_value(binding_id)} failed", exc_info=True)
        if self._open_debounce is None:
            return OpenEvaluation(pending=True, run=True)
        user_key = f"{snapshot_cache_identity(user.email)}:{user.role.value}"
        claim, key = self._open_debounce.claim(user_key, binding_id)
        if claim is OpenClaim.SKIP:
            return OpenEvaluation(pending=False, run=False)
        if claim is OpenClaim.PENDING:
            return OpenEvaluation(pending=True, run=False)
        return OpenEvaluation(pending=True, run=True, key=key)

    def evaluate_on_open(self, binding_id: str, opened: OpenEvaluation, user: ActingUser) -> ApplyResult:
        """Run the evaluation :meth:`begin_open` scheduled, then release its debounce claim. Never raises."""
        try:
            return self.evaluate_binding(binding_id, user)
        finally:
            if opened.key is not None and self._open_debounce is not None:
                self._open_debounce.finish(opened.key)

    def ambiguous_for_binding(self, binding_id: str, user: ActingUser) -> list[PolicyMatch]:
        """Ambiguous policy pairings on one monitored table, offered as suggestions (nothing attached)."""
        totals = self._totals()
        try:
            table_fqn = self._monitored_tables.get_table_fqns({binding_id}).get(binding_id)
            if table_fqn is not None:
                self._run_bindings({table_fqn: binding_id}, user, totals, attach=False)
        except Exception:  # best-effort: suggestions are optional
            logger.warning(f"Policy suggestions for binding {safe_log_value(binding_id)} failed", exc_info=True)
        return totals.ambiguous

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _totals(self) -> _Totals:
        return _Totals(max_registrations=self._max_registrations, max_ambiguous=self._max_ambiguous)

    def _gate(self, user: ActingUser) -> _UserGate:
        return _UserGate(user, self._permissions, self._apply_rules)

    def _all_policies(self) -> list[RulePolicy]:
        if self._catalog_cache is None:
            return self._policies.list_all()
        return self._catalog_cache.policies(self._policies.list_all)

    def _rules(self, rule_ids: list[str]) -> dict[str, RegistryRule]:
        if self._catalog_cache is None:
            return self._registry.get_rules_many(rule_ids)
        return self._catalog_cache.rules(rule_ids, self._registry.get_rules_many)

    def _reaching(self, table_fqn: str) -> list[RulePolicy]:
        """Policies of approved rules that have *table_fqn* in scope."""
        policies = [p for p in self._all_policies() if in_scope(p.scope, table_fqn)]
        if not policies:
            return []
        rules = self._rules(sorted({p.rule_id for p in policies}))
        return [p for p in policies if (rule := rules.get(p.rule_id)) is not None and rule.status == "approved"]

    def _approved_rule(self, rule_id: str) -> RegistryRule | None:
        rule = self._registry.get_rule(rule_id)
        return rule if rule is not None and rule.status == "approved" else None

    def _run_scope(
        self,
        policy: RulePolicy,
        rule: RegistryRule,
        user: ActingUser,
        totals: _Totals,
        reads: _RunReads,
        gate: _UserGate,
    ) -> None:
        """Evaluate *policy* across its scope, reusing what this run (*reads*, *gate*) already read."""
        full_scan = policy.register_tables and can_register_tables(user.role)
        snapshot, bindings = self._read_scope(policy.scope, full_scan, reads)
        if snapshot.truncated:
            logger.info(f"Rule policy {safe_log_value(policy.policy_id)} evaluation was capped at the snapshot limits")
        for table in snapshot.tables:
            self._apply_table(policy, rule, table, bindings, gate, user, totals, reads, attach=True)

    def _read_scope(
        self, scope: PolicyScope, full_scan: bool, reads: _RunReads
    ) -> tuple[SnapshotResult, dict[str, str]]:
        """Snapshot of the tables a policy over *scope* can reach, plus their ``table_fqn -> binding_id``.

        Monitored tables in scope are always read directly, in one bulk call (nothing is read
        when none are in scope). Only a policy that may register tables (*full_scan*) also
        scans the scope in Unity Catalog, and only to find unmonitored candidates: a
        truncated or failed scan sets ``truncated`` but never removes monitored tables.
        """
        key = (scope.model_dump_json(), full_scan)
        hit = reads.snapshots.get(key)
        if hit is not None:
            return hit
        snapshot, bindings = self._read_monitored(scope, reads)
        if full_scan:
            scan = self._snapshots.for_scope(scope, needs=reads.needs)
            known = {fqn.lower() for fqn in self._monitored(reads)}
            extra = [t for t in scan.tables if t.table_fqn.lower() not in known]
            snapshot = SnapshotResult(
                tables=snapshot.tables + extra,
                truncated=snapshot.truncated or scan.truncated,
                unreadable_catalogs=list(dict.fromkeys(snapshot.unreadable_catalogs + scan.unreadable_catalogs)),
            )
        reads.snapshots[key] = (snapshot, bindings)
        return snapshot, bindings

    def _monitored(self, reads: _RunReads) -> dict[str, str]:
        if reads.monitored is None:
            reads.monitored = self._monitored_tables.list_table_bindings()
        return reads.monitored

    def _read_monitored(self, scope: PolicyScope, reads: _RunReads) -> tuple[SnapshotResult, dict[str, str]]:
        reachable = {fqn: b for fqn, b in self._monitored(reads).items() if in_scope(scope, fqn)}
        if not reachable:
            return SnapshotResult(tables=[], truncated=False, unreadable_catalogs=[]), {}
        snapshot = self._snapshots.for_tables(list(reachable), needs=reads.needs)
        by_name = {fqn.lower(): b for fqn, b in reachable.items()}
        bindings = {
            t.table_fqn: by_name[t.table_fqn.lower()] for t in snapshot.tables if t.table_fqn.lower() in by_name
        }
        return snapshot, bindings

    def _run_bindings(self, table_bindings: dict[str, str], user: ActingUser, totals: _Totals, *, attach: bool) -> None:
        """Evaluate the policies reaching each ``table_fqn -> binding_id``, reading one snapshot for all.

        Cheap Lakebase-only checks come first: tables no approved rule's policy has in scope,
        and tables the user may not apply rules to, are dropped before any warehouse read.
        """
        if not table_bindings:
            return
        all_policies = self._all_policies()
        reaching = {fqn: [p for p in all_policies if in_scope(p.scope, fqn)] for fqn in table_bindings}
        rule_ids = sorted({p.rule_id for ps in reaching.values() for p in ps})
        if not rule_ids:
            return
        rules = {rule_id: rule for rule_id, rule in self._rules(rule_ids).items() if rule.status == "approved"}
        gate = self._gate(user)
        reads = _RunReads()
        reaching = {
            fqn: approved
            for fqn, policies in reaching.items()
            if (approved := [p for p in policies if p.rule_id in rules]) and gate.can_apply(table_bindings[fqn])
        }
        if not reaching:
            return
        allowed = {fqn: table_bindings[fqn] for fqn in reaching}
        needs = needs_of(policy for policies in reaching.values() for policy in policies)
        snapshot = self._snapshots.for_tables(list(reaching), needs=needs)
        tables = {table.table_fqn.lower(): table for table in snapshot.tables}
        for fqn, policies in reaching.items():
            table = tables.get(fqn.lower())
            if table is None:
                continue
            bindings = {table.table_fqn: allowed[fqn]}
            for policy in policies:
                try:
                    self._apply_table(
                        policy, rules[policy.rule_id], table, bindings, gate, user, totals, reads, attach=attach
                    )
                except Exception:  # best-effort: one table failing must not stop the others
                    logger.warning(
                        f"Rule policy {safe_log_value(policy.policy_id)} evaluation on binding "
                        f"{safe_log_value(allowed[fqn])} failed",
                        exc_info=True,
                    )

    def _apply_table(
        self,
        policy: RulePolicy,
        rule: RegistryRule,
        table: TableSnapshot,
        bindings: dict[str, str],
        gate: _UserGate,
        user: ActingUser,
        totals: _Totals,
        reads: _RunReads,
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
        # A table another policy of this run registered (or found registered) is monitored now.
        binding_id = bindings.get(table.table_fqn) or reads.registered.get(table.table_fqn.lower())
        if binding_id is None:
            if not attach or not autos or not policy.register_tables or not can_register_tables(user.role):
                return
            if totals.registered >= totals.max_registrations:
                totals.registrations_skipped += 1
                return
            registration = self._register(policy, table.table_fqn, user)
            if registration is None:
                return
            binding_id, owned = registration
            bindings[table.table_fqn] = binding_id
            reads.registered[table.table_fqn.lower()] = binding_id
            if reads.monitored is not None:
                reads.monitored[table.table_fqn] = binding_id
            if owned:
                gate.grant_owner(binding_id)
                totals.registered += 1
            elif not gate.can_apply(binding_id):
                return
        elif not gate.can_apply(binding_id):
            return
        if attach:
            for match in gate.fresh(binding_id, autos):
                if self._attach(binding_id, match, user):
                    gate.mark_applied(binding_id, match)
                    totals.attached += 1
        room = totals.ambiguous_room()
        if ambiguous and room:
            totals.ambiguous.extend(gate.fresh(binding_id, ambiguous, room))

    def _register(self, policy: RulePolicy, table_fqn: str, user: ActingUser) -> tuple[str, bool] | None:
        """Register *table_fqn* for *user*: ``(binding_id, True)``, or ``(binding_id, False)`` when it already was."""
        try:
            return self._register_table(table_fqn, user.email).binding_id, True
        except DuplicateMonitoredTableError:
            # Registered meanwhile (another user or request): attach to the existing binding if permitted.
            existing = self._monitored_tables.get_binding_ids_by_table_fqn([table_fqn])
            binding_id = existing.get(table_fqn) or next(
                (b for fqn, b in existing.items() if fqn.lower() == table_fqn.lower()), None
            )
            return (binding_id, False) if binding_id is not None else None
        except Exception:  # best-effort: one table failing to register must not stop the others
            logger.warning(
                f"Rule policy {safe_log_value(policy.policy_id)} could not register a matching table", exc_info=True
            )
            return None

    def _attach(self, binding_id: str, match: PolicyMatch, user: ActingUser) -> bool:
        try:
            applied = self._apply_rules.attach_policy_mapping(
                binding_id,
                match.rule_id,
                [match.column_mapping],
                user.email,
                match.policy_id,
                suppression_checked=True,  # only fresh matches get here: the gate read the binding's tombstones
            )
        except Exception:  # best-effort: one failing attach must not stop the others
            logger.warning(
                f"Rule policy {safe_log_value(match.policy_id)} could not attach rule {safe_log_value(match.rule_id)} "
                f"to binding {safe_log_value(binding_id)}",
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
