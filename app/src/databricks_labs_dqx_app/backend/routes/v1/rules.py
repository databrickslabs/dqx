from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.approvals import mark_auto_approver, should_auto_approve
from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_permissions_for_role
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentUserRole,
    get_app_settings_service,
    get_draft_run_gate_service,
    get_monitored_table_version_service,
    get_obo_ws,
    get_rules_catalog_service,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.draft_run_gate_service import (
    DraftRunGateService,
    DraftRunRequiredError,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    BatchSaveRulesIn,
    BatchSaveRulesOut,
    CheckDuplicatesIn,
    CheckDuplicatesOut,
    RuleCatalogEntryOut,
    RuleHistoryEntryOut,
    SaveRulesIn,
    SetStatusIn,
)
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]
_APPROVERS_ONLY = [UserRole.ADMIN, UserRole.RULE_APPROVER]

_SQL_CHECK_PREFIX = "__sql_check__/"


def _refreeze_binding_for_rule(version_svc: MonitoredTableVersionService, rule_id: str) -> None:
    """Best-effort re-freeze of the binding owning materialized check *rule_id*.

    Data Products Task 2 re-freeze hook (design spec §3.2): a per-rule
    approval or rejection in Drafts & Review changes a binding's approved
    rule set WITHOUT a table re-approval, so the binding's current version
    snapshot is rewritten in place (``refrozen_at`` stamped). No-op for a
    directly-authored (non-registry) rule with no ``applied_rule_id``, or a
    binding still at version 0. A hook failure must never turn a successful
    per-rule transition into a 5xx, so failures are logged and swallowed.
    """
    try:
        version_svc.refreeze_for_quality_rule(rule_id)
    except Exception:  # a bookkeeping refreeze must not fail the approve/reject
        logger.warning("Re-freeze after status change for rule %s failed", rule_id, exc_info=True)


def _catalog_of(fqn: str) -> str:
    """Extract the catalog part from a fully qualified table name."""
    if fqn.startswith(_SQL_CHECK_PREFIX):
        fqn = fqn[len(_SQL_CHECK_PREFIX) :]
    parts = fqn.split(".", 1)
    return parts[0] if parts else ""


def _display_name(fqn: str) -> str:
    return fqn[len(_SQL_CHECK_PREFIX) :] if fqn.startswith(_SQL_CHECK_PREFIX) else fqn


_PRIVILEGED_ROLES = frozenset({UserRole.ADMIN, UserRole.RULE_APPROVER})


def _ensure_owner_or_privileged(
    svc: RulesCatalogService,
    rule_id: str,
    user_email: str,
    user_role: UserRole,
    action: str,
) -> None:
    """Authors may only act on their own rules.

    Admins and approvers can act on any rule. For everyone else (i.e. plain
    ``RULE_AUTHOR``) we require the requesting user to be the rule's original
    creator. Raises 403 on mismatch and 404 on missing rule.

    ``action`` is interpolated into the error message ("delete", "submit", …).
    """
    if user_role in _PRIVILEGED_ROLES:
        return
    entry = svc.get_by_rule_id(rule_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Rule not found: {rule_id}")
    owner = (entry.created_by or "").strip().lower()
    me = (user_email or "").strip().lower()
    if not owner or owner != me:
        raise HTTPException(
            status_code=403,
            detail=(
                f"You can only {action} rules you authored. "
                f"This rule was created by {entry.created_by or 'someone else'}."
            ),
        )


def _entry_to_out(entry) -> RuleCatalogEntryOut:
    return RuleCatalogEntryOut(
        table_fqn=entry.table_fqn,
        display_name=_display_name(entry.table_fqn),
        checks=entry.checks,
        version=entry.version,
        status=entry.status,
        source=entry.source,
        rule_id=entry.rule_id,
        created_by=entry.created_by,
        created_at=entry.created_at,
        updated_by=entry.updated_by,
        updated_at=entry.updated_at,
    )


# ------------------------------------------------------------------
# List / Get
# ------------------------------------------------------------------


@router.get(
    "",
    response_model=list[RuleCatalogEntryOut],
    operation_id="listRules",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def list_rules(
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    status: Annotated[str | None, Query(description="Filter by status")] = None,
) -> list[RuleCatalogEntryOut]:
    """List rules filtered to catalogs the current user can access."""
    try:
        entries = svc.list_rules(status=status)
        return [
            _entry_to_out(e)
            for e in entries
            if e.table_fqn.startswith(_SQL_CHECK_PREFIX) or _catalog_of(e.table_fqn) in user_catalogs
        ]
    except Exception as e:
        logger.error(f"Failed to list rules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list rules: {e}")


@router.get(
    "/{rule_id}/history",
    response_model=list[RuleHistoryEntryOut],
    operation_id="getRuleHistory",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_rule_history(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> list[RuleHistoryEntryOut]:
    """Return a per-table rule's recorded change history (newest first).

    Backs the Drafts & Review change-diff popout: reads the
    ``dq_quality_rules_history`` audit trail so the UI can diff the two most
    recent recorded ``check`` payloads (previous vs proposed). Declared BEFORE
    the ``/{table_fqn:path}`` catch-all so the more-specific pattern wins.

    Scoped to the caller's Unity Catalog entitlements exactly as ``getRules``
    is: all history rows for one rule share the same ``table_fqn``, so if that
    catalog is not in the user's accessible set we raise 403 rather than leak
    the table name and ``check`` payloads. Cross-table SQL checks
    (``__sql_check__/``) carry no home catalog and are always allowed.
    """
    try:
        entries = svc.get_history(rule_id)
        if not entries:
            return []
        table_fqn = entries[0].get("table_fqn", "")
        if not table_fqn.startswith(_SQL_CHECK_PREFIX) and _catalog_of(table_fqn) not in user_catalogs:
            raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")
        return [RuleHistoryEntryOut(**entry) for entry in entries]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get history for rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get rule history: {e}")


@router.get(
    "/{table_fqn:path}",
    response_model=list[RuleCatalogEntryOut],
    operation_id="getRules",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_rules(
    table_fqn: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> list[RuleCatalogEntryOut]:
    """Get all individual rules for a specific table."""
    try:
        if not table_fqn.startswith(_SQL_CHECK_PREFIX) and _catalog_of(table_fqn) not in user_catalogs:
            raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")
        entries = svc.list_rules_for_table(table_fqn)
        if not entries:
            raise HTTPException(status_code=404, detail=f"No rules found for table: {table_fqn}")
        return [_entry_to_out(e) for e in entries]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get rules for {table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get rules: {e}")


# ------------------------------------------------------------------
# Save / Update
# ------------------------------------------------------------------


@router.post(
    "",
    response_model=list[RuleCatalogEntryOut],
    operation_id="saveRules",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def save_rules(
    body: SaveRulesIn,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    user_role: CurrentUserRole,
) -> list[RuleCatalogEntryOut]:
    """Save rules. Each check becomes an individual rule row.

    If ``rule_id`` is set, this updates an existing rule. Authors can only
    update rules they themselves authored — otherwise they could silently
    overwrite the contents of another user's rule (and then chain it with
    submit/delete, which our other gates would reject only by accident).
    Admins and approvers can update any rule.
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"

        if body.rule_id:
            _ensure_owner_or_privileged(svc, body.rule_id, user_email, user_role, "edit")
            entry = svc.update_rule(body.rule_id, body.checks, user_email)
            return [_entry_to_out(entry)]

        entries = svc.save(body.table_fqn, body.checks, user_email, source=body.source)
        return [_entry_to_out(e) for e in entries]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save rules for {body.table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save rules: {e}")


@router.post(
    "/batch",
    response_model=BatchSaveRulesOut,
    operation_id="batchSaveRules",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def batch_save_rules(
    body: BatchSaveRulesIn,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> BatchSaveRulesOut:
    """Save the same set of checks to multiple tables (reusable rules)."""
    if not body.table_fqns:
        raise HTTPException(status_code=400, detail="table_fqns must not be empty")
    user = obo_ws.current_user.me()
    user_email = user.user_name or "unknown"
    saved: list[RuleCatalogEntryOut] = []
    failed: list[dict[str, str]] = []
    for fqn in body.table_fqns:
        try:
            entries = svc.save(fqn, body.checks, user_email, source=body.source)
            saved.extend([_entry_to_out(e) for e in entries])
        except Exception as e:
            logger.error(f"Failed to save rules for {fqn}: {e}", exc_info=True)
            failed.append({"table_fqn": fqn, "error": str(e)})
    return BatchSaveRulesOut(saved=saved, failed=failed)


# ------------------------------------------------------------------
# Duplicate detection
# ------------------------------------------------------------------


@router.post(
    "/check-duplicates",
    response_model=CheckDuplicatesOut,
    operation_id="checkDuplicates",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def check_duplicates(
    body: CheckDuplicatesIn,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
) -> CheckDuplicatesOut:
    """Check if any of the provided checks already exist for the given table."""
    try:
        dupes = svc.find_duplicates(
            body.table_fqn,
            body.checks,
            exclude_rule_id=body.exclude_rule_id,
            exclude_rule_ids=body.exclude_rule_ids or None,
        )
        return CheckDuplicatesOut(duplicates=dupes)
    except Exception as e:
        logger.error(f"Failed to check duplicates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to check duplicates: {e}")


# ------------------------------------------------------------------
# Delete
# ------------------------------------------------------------------


@router.delete(
    "/{rule_id}",
    operation_id="deleteRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def delete_rule(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    user_role: CurrentUserRole,
) -> dict[str, str]:
    """Delete a single rule by rule_id.

    Authors can only delete rules they themselves created. Admins and
    approvers may delete any rule.
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        _ensure_owner_or_privileged(svc, rule_id, user_email, user_role, "delete")
        svc.delete(rule_id, user_email)
        return {"status": "deleted", "rule_id": rule_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete rule: {e}")


# ------------------------------------------------------------------
# Status transitions (per rule_id)
# ------------------------------------------------------------------


@router.post(
    "/{rule_id}/submit",
    response_model=RuleCatalogEntryOut,
    operation_id="submitRuleForApproval",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def submit_for_approval(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    draft_run_gate: Annotated[DraftRunGateService, Depends(get_draft_run_gate_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    user_role: CurrentUserRole,
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Submit an individual rule for approval.

    Authors can only submit rules they themselves drafted. Admins and
    approvers may submit any rule.

    Honours the app-wide approvals mode (issue #94): in ``disabled`` mode, or in
    ``auto_bypass`` mode when the caller could approve the rule themselves (any
    role holding ``approve_rules`` — i.e. admin/approver), the rule transitions
    straight through ``pending_approval`` to ``approved`` in the same call and
    the caller is recorded as the approver with an ``(auto)`` marker. A
    per-table rule has no object-grant surface of its own, so the auto-bypass
    predicate here is the role-level ``approve_rules`` permission.

    Honours the require-draft-run gate (issue B2-12): when the admin setting is
    on, a per-table rule cannot be submitted (nor auto-approved) until a draft
    run has been recorded for its target table. Cross-table SQL checks
    (``__sql_check__/`` FQNs) have no home table and are never gated. The gate
    is checked BEFORE any state transition, so it blocks both the plain submit
    and the auto-approve shortcut, returning 409 when unsatisfied.
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        _ensure_owner_or_privileged(svc, rule_id, user_email, user_role, "submit")
        gate_entry = svc.get_by_rule_id(rule_id)
        if gate_entry is None:
            raise HTTPException(status_code=404, detail=f"Rule not found: {rule_id}")
        draft_run_gate.enforce(
            enabled=app_settings.get_require_draft_run_before_submit(),
            table_fqns=[gate_entry.table_fqn],
        )
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "pending_approval", user_email, expected_version)
        can_edit_and_approve = "approve_rules" in get_permissions_for_role(user_role)
        if should_auto_approve(app_settings.get_approvals_mode(), can_edit_and_approve=can_edit_and_approve):
            entry = svc.set_status(rule_id, "approved", mark_auto_approver(user_email))
            _refreeze_binding_for_rule(version_svc, rule_id)
        return _entry_to_out(entry)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to submit rule for approval: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit for approval: {e}")


@router.post(
    "/{rule_id}/revoke",
    response_model=RuleCatalogEntryOut,
    operation_id="revokeSubmission",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def revoke_submission(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    user_role: CurrentUserRole,
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Revoke a pending submission back to draft.

    Authors can only revoke their own submissions. Admins and approvers may
    revoke any submission (paired with the existing ownership check on
    ``submit_for_approval`` so an author can't reach in and revoke someone
    else's submission they didn't make).
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        _ensure_owner_or_privileged(svc, rule_id, user_email, user_role, "revoke")
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "draft", user_email, expected_version)
        return _entry_to_out(entry)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to revoke submission: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to revoke submission: {e}")


@router.post(
    "/{rule_id}/approve",
    response_model=RuleCatalogEntryOut,
    operation_id="approveRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def approve_rules(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Approve an individual rule."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "approved", user_email, expected_version)
        _refreeze_binding_for_rule(version_svc, rule_id)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to approve rule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to approve rule: {e}")


@router.post(
    "/backfill-ids",
    operation_id="backfillRuleIds",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def backfill_rule_ids(
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
) -> dict[str, int]:
    """Assign UUIDs to rules that are missing a rule_id (legacy rows)."""
    try:
        count = svc.backfill_rule_ids()
        return {"repaired": count}
    except Exception as e:
        logger.error(f"Failed to backfill rule IDs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to backfill rule IDs: {e}")


@router.post(
    "/{rule_id}/reject",
    response_model=RuleCatalogEntryOut,
    operation_id="rejectRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def reject_rules(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Reject an individual rule."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "rejected", user_email, expected_version)
        _refreeze_binding_for_rule(version_svc, rule_id)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to reject rule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to reject rule: {e}")
