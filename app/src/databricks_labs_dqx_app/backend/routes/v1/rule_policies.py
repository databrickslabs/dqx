"""Rule policy routes.

A rule policy says which tables (and columns) a registry rule is auto-applied
to. Reading a policy needs SELECT on its rule; creating, editing, deleting and
re-scanning need MODIFY on it. Every evaluation (after save, on re-scan, in
preview) runs for the calling user, so it only reaches tables they may apply
rules to. Evaluation after a save runs as a background task once the response
is sent (best-effort; it never fails the save). Re-scan and preview stay
synchronous; every snapshot read is time-boxed by the snapshot reader.
"""

from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response, status

from databricks_labs_dqx_app.backend.common.authorization import CurrentUser, UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentActingUser,
    CurrentPrincipalIds,
    CurrentUserRole,
    get_apply_rules_service,
    get_permissions_service,
    get_policy_apply_service,
    get_registry_service,
    get_rule_policy_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    CreateRulePoliciesIn,
    PreviewRulePolicyIn,
    RulePolicyOut,
    RulePolicyPreviewMappingOut,
    RulePolicyPreviewOut,
    RulePolicyPreviewTableOut,
    RulePolicyRunOut,
)
from databricks_labs_dqx_app.backend.policy_models import ColumnCondition, RulePolicy, RulePolicyIn
from databricks_labs_dqx_app.backend.sanitization import safe_log_value
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.services.policy_apply_service import (
    ActingUser,
    PolicyApplyService,
    RuleNotFoundError,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_policy_service import (
    DuplicatePolicyNameError,
    RulePolicyNotFoundError,
    RulePolicyService,
)

_ALL_ROLES = (UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER)
_EDITORS = (UserRole.ADMIN, UserRole.RULE_AUTHOR, UserRole.RULE_APPROVER)

_DUPLICATE_NAME = "This rule already has a policy with that name."
_POLICY_NOT_FOUND = "Rule policy not found."
_RULE_NOT_FOUND = "Registry rule not found."

router = APIRouter(dependencies=[require_role(*_ALL_ROLES)])

Policies = Annotated[RulePolicyService, Depends(get_rule_policy_service)]
ApplyRules = Annotated[ApplyRulesService, Depends(get_apply_rules_service)]
Registry = Annotated[RegistryService, Depends(get_registry_service)]
Permissions = Annotated[PermissionsService, Depends(get_permissions_service)]
PolicyApply = Annotated[PolicyApplyService, Depends(get_policy_apply_service)]


class _Caller:
    """The caller's identity bundle used for object-permission checks."""

    def __init__(self, email: str, role: UserRole, principal_ids: frozenset[str], perms: PermissionsService) -> None:
        self._email = email
        self._role = role
        self._principal_ids = principal_ids
        self._perms = perms

    def require(self, rule_id: str, privilege: Privilege) -> None:
        """Raise 403 unless the caller holds *privilege* on registry rule *rule_id*."""
        self._perms.require_object(
            ObjectType.REGISTRY_RULE.value,
            rule_id,
            privilege,
            role=self._role,
            principal_ids=set(self._principal_ids),
            principal_email=self._email,
        )


def _caller(
    user_email: CurrentUser,
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Permissions,
) -> _Caller:
    return _Caller(user_email, role, principal_ids, perms)


Caller = Annotated[_Caller, Depends(_caller)]


def _server_error(action: str) -> HTTPException:
    return HTTPException(status_code=500, detail=f"Failed to {action}.")


def _load_policy(policies: RulePolicyService, policy_id: str) -> RulePolicy:
    try:
        policy = policies.get(policy_id)
    except Exception:
        logger.error(f"Failed to load rule policy {safe_log_value(policy_id)}", exc_info=True)
        raise _server_error("load the rule policy")
    if policy is None:
        raise HTTPException(status_code=404, detail=_POLICY_NOT_FOUND)
    return policy


def _to_out(policies: list[RulePolicy], apply_rules: ApplyRulesService) -> list[RulePolicyOut]:
    counts = apply_rules.count_bindings_by_policy([p.policy_id for p in policies]) if policies else {}
    return [RulePolicyOut.from_domain(p, counts.get(p.policy_id, 0)) for p in policies]


def _require_slot_coverage(rule_id: str, slot_names: set[str], conditions: dict[str, ColumnCondition]) -> None:
    """422 unless *conditions* has exactly one entry per slot of the rule."""
    if set(conditions) != slot_names:
        expected = ", ".join(sorted(slot_names)) or "none"
        raise HTTPException(
            status_code=422,
            detail=f"Column conditions for rule '{rule_id}' must cover exactly its slots ({expected}).",
        )


def _rule_slot_names(registry: RegistryService, rule_id: str) -> set[str]:
    try:
        rule = registry.get_rule(rule_id)
    except Exception:
        logger.error(f"Failed to load registry rule {safe_log_value(rule_id)}", exc_info=True)
        raise _server_error("load the registry rule")
    if rule is None:
        raise HTTPException(status_code=404, detail=_RULE_NOT_FOUND)
    return {slot.name for slot in rule.definition.slots}


def _run_best_effort(apply: PolicyApplyService, policy_ids: list[str], user: ActingUser) -> None:
    """Background task: evaluate saved policies for *user*; never raises.

    The request's OBO-backed services stay usable here: dependencies have no
    teardown and the OBO client is a token-bearing SDK client, valid for the
    token's lifetime.
    """
    try:
        apply.run_policies(policy_ids, user)
    except Exception:  # evaluating after a save must never fail the save
        ids = ", ".join(safe_log_value(policy_id) for policy_id in policy_ids)
        logger.warning(f"Rule policy evaluation after save failed ({ids})", exc_info=True)


def _rollback(policies: RulePolicyService, created: list[RulePolicy]) -> None:
    for policy in created:
        try:
            policies.delete(policy.policy_id)
        except Exception:
            logger.error(f"Could not roll back rule policy {safe_log_value(policy.policy_id)}", exc_info=True)


# ------------------------------------------------------------------
# Read
# ------------------------------------------------------------------


@router.get("", response_model=list[RulePolicyOut], operation_id="listRulePolicies")
def list_rule_policies(
    rule_id: Annotated[str, Query(min_length=1, description="Registry rule whose policies to list")],
    policies: Policies,
    apply_rules: ApplyRules,
    caller: Caller,
) -> list[RulePolicyOut]:
    """List the policies of one registry rule."""
    caller.require(rule_id, Privilege.SELECT)
    try:
        return _to_out(policies.list_for_rule(rule_id), apply_rules)
    except Exception:
        logger.error(f"Failed to list rule policies for rule {safe_log_value(rule_id)}", exc_info=True)
        raise _server_error("list rule policies")


@router.get("/{policy_id}", response_model=RulePolicyOut, operation_id="getRulePolicy")
def get_rule_policy(
    policy_id: str,
    policies: Policies,
    apply_rules: ApplyRules,
    caller: Caller,
) -> RulePolicyOut:
    """Get one rule policy."""
    policy = _load_policy(policies, policy_id)
    caller.require(policy.rule_id, Privilege.SELECT)
    try:
        return _to_out([policy], apply_rules)[0]
    except Exception:
        logger.error(f"Failed to read rule policy {safe_log_value(policy_id)}", exc_info=True)
        raise _server_error("load the rule policy")


# ------------------------------------------------------------------
# Write
# ------------------------------------------------------------------


@router.post(
    "",
    response_model=list[RulePolicyOut],
    operation_id="createRulePolicies",
    dependencies=[require_role(*_EDITORS)],
)
def create_rule_policies(
    body: CreateRulePoliciesIn,
    background_tasks: BackgroundTasks,
    policies: Policies,
    registry: Registry,
    apply_rules: ApplyRules,
    apply: PolicyApply,
    caller: Caller,
    user: CurrentActingUser,
) -> list[RulePolicyOut]:
    """Create one policy per rule in *rule_ids*, all or nothing; evaluate them after responding.

    MODIFY is checked on every rule and every rule's column conditions are
    validated before anything is written. If a create fails part-way, the
    policies already created by this request are deleted again.
    """
    for rule_id in body.rule_ids:
        caller.require(rule_id, Privilege.MODIFY)
    bodies: list[tuple[str, RulePolicyIn]] = []
    for rule_id in body.rule_ids:
        conditions = body.column_conditions_by_rule.get(rule_id, body.policy.column_conditions)
        _require_slot_coverage(rule_id, _rule_slot_names(registry, rule_id), conditions)
        bodies.append((rule_id, body.policy.model_copy(update={"column_conditions": conditions})))

    created: list[RulePolicy] = []
    try:
        for rule_id, policy_body in bodies:
            created.append(policies.create(rule_id, policy_body, user.email))
    except DuplicatePolicyNameError:
        _rollback(policies, created)
        raise HTTPException(status_code=409, detail=_DUPLICATE_NAME)
    except Exception:
        _rollback(policies, created)
        logger.error("Failed to create rule policies", exc_info=True)
        raise _server_error("create rule policies")

    background_tasks.add_task(_run_best_effort, apply, [policy.policy_id for policy in created], user)
    try:
        return _to_out(created, apply_rules)
    except Exception:
        logger.warning("Could not count tables for new rule policies", exc_info=True)
        return [RulePolicyOut.from_domain(p, 0) for p in created]


@router.put(
    "/{policy_id}",
    response_model=RulePolicyOut,
    operation_id="updateRulePolicy",
    dependencies=[require_role(*_EDITORS)],
)
def update_rule_policy(
    policy_id: str,
    body: RulePolicyIn,
    background_tasks: BackgroundTasks,
    policies: Policies,
    registry: Registry,
    apply_rules: ApplyRules,
    apply: PolicyApply,
    caller: Caller,
    user: CurrentActingUser,
) -> RulePolicyOut:
    """Replace a policy's definition; evaluate it after responding."""
    existing = _load_policy(policies, policy_id)
    caller.require(existing.rule_id, Privilege.MODIFY)
    _require_slot_coverage(existing.rule_id, _rule_slot_names(registry, existing.rule_id), body.column_conditions)
    try:
        updated = policies.update(policy_id, body, user.email)
    except DuplicatePolicyNameError:
        raise HTTPException(status_code=409, detail=_DUPLICATE_NAME)
    except RulePolicyNotFoundError:
        raise HTTPException(status_code=404, detail=_POLICY_NOT_FOUND)
    except Exception:
        logger.error(f"Failed to update rule policy {safe_log_value(policy_id)}", exc_info=True)
        raise _server_error("update the rule policy")

    background_tasks.add_task(_run_best_effort, apply, [policy_id], user)
    try:
        return _to_out([updated], apply_rules)[0]
    except Exception:
        logger.warning(f"Could not count tables for rule policy {safe_log_value(policy_id)}", exc_info=True)
        return RulePolicyOut.from_domain(updated, 0)


@router.delete(
    "/{policy_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    operation_id="deleteRulePolicy",
    dependencies=[require_role(*_EDITORS)],
)
def delete_rule_policy(policy_id: str, policies: Policies, caller: Caller) -> Response:
    """Delete a policy. Rules it already attached stay attached."""
    existing = _load_policy(policies, policy_id)
    caller.require(existing.rule_id, Privilege.MODIFY)
    try:
        policies.delete(policy_id)
    except RulePolicyNotFoundError:
        raise HTTPException(status_code=404, detail=_POLICY_NOT_FOUND)
    except Exception:
        logger.error(f"Failed to delete rule policy {safe_log_value(policy_id)}", exc_info=True)
        raise _server_error("delete the rule policy")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ------------------------------------------------------------------
# Preview / re-scan
# ------------------------------------------------------------------


@router.post("/preview", response_model=RulePolicyPreviewOut, operation_id="previewRulePolicy")
def preview_rule_policy(
    body: PreviewRulePolicyIn,
    apply: PolicyApply,
    caller: Caller,
    user: CurrentActingUser,
) -> RulePolicyPreviewOut:
    """Show what saving *policy* for *rule_id* would attach for the calling user (nothing is written)."""
    caller.require(body.rule_id, Privilege.SELECT)
    try:
        preview = apply.preview(body.rule_id, body.policy, user)
    except RuleNotFoundError:
        raise HTTPException(status_code=404, detail=_RULE_NOT_FOUND)
    except Exception:
        logger.error(f"Failed to preview a rule policy for rule {safe_log_value(body.rule_id)}", exc_info=True)
        raise _server_error("preview the rule policy")
    return RulePolicyPreviewOut(
        tables_matched=preview.tables_matched,
        columns_matched=preview.columns_matched,
        new_tables=preview.new_tables,
        truncated=preview.truncated,
        unreadable_catalogs=preview.unreadable_catalogs,
        tables=[
            RulePolicyPreviewTableOut(
                table_fqn=table.table_fqn,
                monitored=table.monitored,
                already_attached=table.already_attached,
                mappings=[
                    RulePolicyPreviewMappingOut(column_mapping=match.column_mapping, reason=match.reason)
                    for match in table.matches
                ],
            )
            for table in preview.tables
        ],
    )


@router.post(
    "/{policy_id}/rescan",
    response_model=RulePolicyRunOut,
    operation_id="rescanRulePolicy",
    dependencies=[require_role(*_EDITORS)],
)
def rescan_rule_policy(
    policy_id: str,
    policies: Policies,
    apply: PolicyApply,
    caller: Caller,
    user: CurrentActingUser,
) -> RulePolicyRunOut:
    """Re-evaluate a policy across its scope and attach anything new."""
    existing = _load_policy(policies, policy_id)
    caller.require(existing.rule_id, Privilege.MODIFY)
    try:
        result = apply.run_policy(policy_id, user)
    except RulePolicyNotFoundError:
        raise HTTPException(status_code=404, detail=_POLICY_NOT_FOUND)
    except Exception:
        logger.error(f"Failed to re-scan rule policy {safe_log_value(policy_id)}", exc_info=True)
        raise _server_error("re-scan the rule policy")
    return RulePolicyRunOut(
        attached=result.attached,
        registered=result.registered,
        registration_capped=result.registration_capped,
    )
