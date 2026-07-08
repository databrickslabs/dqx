"""Rules Registry routes — the REGISTRY (tier-1) approval gate.

Per ``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §5, this
is independent of the per-table application gate (tier 2, Phase 3). A
published (``approved``) registry rule can later be applied to a monitored
table via ``dq_applied_rules`` (see ``ApplyRulesService``) — the delete
route below blocks (409) deleting a rule that's still applied anywhere.
"""

from typing import Annotated

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import CurrentUser, UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentPrincipalIds,
    CurrentUserRole,
    get_app_settings_service,
    get_apply_rules_service,
    get_materializer,
    get_monitored_table_version_service,
    get_permissions_service,
    get_registry_service,
    get_rule_embeddings_service,
    require_role,
)
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    BackfillRuleEmbeddingsOut,
    CreateRegistryRuleIn,
    CreateRegistryRuleOut,
    RegistryRuleDetailOut,
    RegistryRuleOut,
    RegistryRuleVersionOut,
    UpdateRegistryRuleIn,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]
_APPROVERS_ONLY = [UserRole.ADMIN, UserRole.RULE_APPROVER]


# ------------------------------------------------------------------
# List / Get
# ------------------------------------------------------------------


@router.get(
    "",
    response_model=list[RegistryRuleOut],
    operation_id="listRegistryRules",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_registry_rules(
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    status: Annotated[str | None, Query(description="Filter by status")] = None,
    dimension: Annotated[str | None, Query(description="Filter by the 'dimension' tag")] = None,
    severity: Annotated[str | None, Query(description="Filter by the 'severity' tag")] = None,
    steward: Annotated[str | None, Query(description="Filter by steward")] = None,
    tag: Annotated[str | None, Query(description="Filter by presence of a free-text tag key")] = None,
) -> list[RegistryRuleOut]:
    """List Rules Registry entries, optionally filtered."""
    try:
        rules = svc.list_rules(status=status, dimension=dimension, severity=severity, steward=steward, tag=tag)
        return [RegistryRuleOut.from_domain(r) for r in rules]
    except Exception as e:
        logger.error(f"Failed to list registry rules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list registry rules: {e}")


@router.get(
    "/{rule_id}",
    response_model=RegistryRuleDetailOut,
    operation_id="getRegistryRule",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
) -> RegistryRuleDetailOut:
    """Get a single registry rule with its slots/params and current published snapshot."""
    try:
        result = svc.get_rule_with_version(rule_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Registry rule not found: {rule_id}")
        rule, version = result
        return RegistryRuleDetailOut(
            rule=RegistryRuleOut.from_domain(rule),
            current_version=RegistryRuleVersionOut.from_domain(version) if version else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get registry rule: {e}")


@router.get(
    "/{rule_id}/versions",
    response_model=list[RegistryRuleVersionOut],
    operation_id="listRegistryRuleVersions",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_registry_rule_versions(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
) -> list[RegistryRuleVersionOut]:
    """List a registry rule's published version snapshots (newest first)."""
    try:
        versions = svc.list_versions(rule_id)
        return [RegistryRuleVersionOut.from_domain(v) for v in versions]
    except Exception as e:
        logger.error(f"Failed to list versions for registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list registry rule versions: {e}")


# ------------------------------------------------------------------
# Create / update
# ------------------------------------------------------------------


@router.post(
    "",
    response_model=CreateRegistryRuleOut,
    operation_id="createRegistryRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def create_registry_rule(
    body: CreateRegistryRuleIn,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
) -> CreateRegistryRuleOut:
    """Create a new draft registry rule.

    A dedup warning (never a hard error) is returned when a published rule
    already shares this rule's structural fingerprint.
    """
    try:
        rule, warning = svc.create_rule(
            mode=body.mode,
            definition=body.definition,
            user_email=user_email,
            polarity=body.polarity,
            author_kind=body.author_kind,
            user_metadata=body.user_metadata,
            steward=body.steward,
        )
        return CreateRegistryRuleOut(rule=RegistryRuleOut.from_domain(rule), dedup_warning=warning)
    except UnsafeSqlQueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create registry rule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create registry rule: {e}")


@router.put(
    "/{rule_id}",
    response_model=RegistryRuleOut,
    operation_id="updateRegistryRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def update_registry_rule(
    rule_id: str,
    body: UpdateRegistryRuleIn,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> RegistryRuleOut:
    """Update a registry rule's live definition/tags in place.

    Editable for ``draft`` rules and for ``approved`` rules (the edit-in-place
    revision path — the edits stay inert behind the frozen vN snapshot until
    the rule is re-submitted and re-approved as vN+1). Rejected with 400 for
    any other status.

    Object-permission enforcement: requires ``MODIFY`` on the rule (direct,
    inherited, or via ownership) unless the caller is an admin/approver.
    """
    perms.require_object(
        ObjectType.REGISTRY_RULE.value,
        rule_id,
        Privilege.MODIFY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=user_email,
    )
    try:
        rule = svc.update_draft(
            rule_id,
            user_email=user_email,
            mode=body.mode,
            definition=body.definition,
            polarity=body.polarity,
            user_metadata=body.user_metadata,
            steward=body.steward,
            author_kind=body.author_kind,
        )
        return RegistryRuleOut.from_domain(rule)
    except UnsafeSqlQueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update registry rule: {e}")


# ------------------------------------------------------------------
# Delete
# ------------------------------------------------------------------


@router.delete(
    "/{rule_id}",
    operation_id="deleteRegistryRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def delete_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    user_email: CurrentUser,
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> dict[str, str]:
    """Delete a registry rule.

    Blocked (409) when the rule is currently applied to one or more
    monitored tables — remove every application first via the Apply Rules
    flow, then delete.

    Object-permission enforcement: requires ``MODIFY`` on the rule unless the
    caller is an admin/approver.
    """
    perms.require_object(
        ObjectType.REGISTRY_RULE.value,
        rule_id,
        Privilege.MODIFY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=user_email,
    )
    try:
        applied_count = apply_rules.count_applications_for_rule(rule_id)
        if applied_count > 0:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot delete: this rule is applied to {applied_count} monitored table(s). "
                    "Remove it from those tables before deleting."
                ),
            )
        svc.delete(rule_id, user_email)
        return {"status": "deleted", "rule_id": rule_id}
    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete registry rule: {e}")


# ------------------------------------------------------------------
# Lifecycle transitions
# ------------------------------------------------------------------


@router.post(
    "/{rule_id}/submit",
    response_model=RegistryRuleOut,
    operation_id="submitRegistryRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def submit_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
) -> RegistryRuleOut:
    """Submit a draft registry rule for approval."""
    try:
        rule = svc.submit(rule_id, user_email)
        return RegistryRuleOut.from_domain(rule)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to submit registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit registry rule: {e}")


@router.post(
    "/{rule_id}/approve",
    response_model=RegistryRuleOut,
    operation_id="approveRegistryRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def approve_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    embeddings: Annotated[RuleEmbeddingsService, Depends(get_rule_embeddings_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    user_email: CurrentUser,
) -> RegistryRuleOut:
    """Approve (publish) a pending registry rule — bumps version and freezes a snapshot.

    Re-embeds the rule into the ``dq_rule_embeddings`` corpus (Rules
    Registry Phase 4B) right after publish, so the mapping suggester picks
    up the latest text/version. ``RuleEmbeddingsService.embed_and_store``
    is itself a documented no-op when no embedding endpoint is configured
    and swallows call failures internally, so in practice this can never
    turn a successful publish into a 500.

    Also re-materializes every FOLLOWING (unpinned) application of this
    rule (design spec §5) so their ``dq_quality_rules`` copies pick up the
    new version — see ``Materializer.rematerialize_for_rule``. PINNED
    applications are untouched by a publish; they only change via a
    direct edit.

    Data Products Task 2 re-freeze hook (design spec §3.2 (a)): when
    ``auto_upgrade_without_approval`` is ON, a follower's approved
    ``dq_quality_rules`` row silently picks up the new content and STAYS
    approved, changing the binding's approved rule set without a table
    re-approval — so each re-materialized binding's current version snapshot
    is re-frozen in place. When auto-upgrade is OFF the changed rows drop to
    ``pending_approval`` (leaving the binding in a "Modified since vN" state,
    NOT a re-freeze), so the hook is skipped entirely. Best-effort: a
    re-freeze failure never turns a successful publish into a 5xx.
    """
    try:
        rule = svc.approve(rule_id, user_email)
        embeddings.embed_and_store(rule)
        rematerialized = materializer.rematerialize_for_rule(rule_id)
        if app_settings.get_auto_upgrade_without_approval():
            for binding_id in rematerialized:
                try:
                    version_svc.refreeze_current(binding_id)
                except Exception:  # a bookkeeping refreeze must not fail the publish
                    logger.warning(
                        "Auto-upgrade re-freeze for binding %s after publishing rule %s failed",
                        binding_id,
                        rule_id,
                        exc_info=True,
                    )
        return RegistryRuleOut.from_domain(rule)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to approve registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to approve registry rule: {e}")


@router.post(
    "/{rule_id}/reject",
    response_model=RegistryRuleOut,
    operation_id="rejectRegistryRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def reject_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
) -> RegistryRuleOut:
    """Reject a pending registry rule."""
    try:
        rule = svc.reject(rule_id, user_email)
        return RegistryRuleOut.from_domain(rule)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to reject registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to reject registry rule: {e}")


@router.post(
    "/{rule_id}/deprecate",
    response_model=RegistryRuleOut,
    operation_id="deprecateRegistryRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def deprecate_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
) -> RegistryRuleOut:
    """Deprecate a published registry rule."""
    try:
        rule = svc.deprecate(rule_id, user_email)
        return RegistryRuleOut.from_domain(rule)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to deprecate registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to deprecate registry rule: {e}")


@router.post(
    "/{rule_id}/undeprecate",
    response_model=RegistryRuleOut,
    operation_id="undeprecateRegistryRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def undeprecate_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
) -> RegistryRuleOut:
    """Reinstate a deprecated registry rule back to approved."""
    try:
        rule = svc.undeprecate(rule_id, user_email)
        return RegistryRuleOut.from_domain(rule)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to undeprecate registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to undeprecate registry rule: {e}")


# ------------------------------------------------------------------
# Embeddings backfill (Rules Registry Phase 4B) — manual re-embed pass over
# every currently-published rule. Approving a rule already re-embeds it
# (see approve_registry_rule); this exists for the one-time catch-up after
# an admin configures ``embedding_endpoint_name`` for the first time, or
# after rotating to a different embedding model.
# ------------------------------------------------------------------


@router.post(
    "/backfill-embeddings",
    response_model=BackfillRuleEmbeddingsOut,
    operation_id="backfillRuleEmbeddings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def backfill_rule_embeddings(
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    embeddings: Annotated[RuleEmbeddingsService, Depends(get_rule_embeddings_service)],
) -> BackfillRuleEmbeddingsOut:
    """Re-embed every currently-published registry rule (admin only).

    A no-op (``embedded=0``) when no embedding endpoint is configured — see
    ``RuleEmbeddingsService.is_configured``.
    """
    try:
        published = svc.list_rules(status="approved")
        embedded = embeddings.backfill(published)
        return BackfillRuleEmbeddingsOut(total_published=len(published), embedded=embedded)
    except Exception as e:
        logger.error(f"Failed to backfill rule embeddings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to backfill rule embeddings: {e}")
