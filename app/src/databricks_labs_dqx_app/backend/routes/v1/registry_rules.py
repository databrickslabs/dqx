"""Rules Registry routes — the REGISTRY (tier-1) approval gate.

Per ``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §5, this
is independent of the per-table application gate (tier 2, Phase 3). Only a
published (``approved``) registry rule can later be applied to a monitored
table — that plumbing (``dq_applied_rules``) doesn't exist yet.
"""

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_materializer,
    get_obo_ws,
    get_registry_service,
    get_rule_embeddings_service,
    require_role,
)
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
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]
_APPROVERS_ONLY = [UserRole.ADMIN, UserRole.RULE_APPROVER]


def _current_user_email(obo_ws: WorkspaceClient) -> str:
    user = obo_ws.current_user.me()
    return user.user_name or "unknown"


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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> CreateRegistryRuleOut:
    """Create a new draft registry rule.

    A dedup warning (never a hard error) is returned when a published rule
    already shares this rule's structural fingerprint.
    """
    try:
        user_email = _current_user_email(obo_ws)
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RegistryRuleOut:
    """Update a draft registry rule. Only draft rules can be edited."""
    try:
        user_email = _current_user_email(obo_ws)
        rule = svc.update_draft(
            rule_id,
            user_email=user_email,
            mode=body.mode,
            definition=body.definition,
            polarity=body.polarity,
            user_metadata=body.user_metadata,
            steward=body.steward,
        )
        return RegistryRuleOut.from_domain(rule)
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> dict[str, str]:
    """Delete a registry rule.

    TODO(Phase 3): block (409) deletion of a rule currently applied to any
    monitored table once ``dq_applied_rules`` exists.
    """
    try:
        user_email = _current_user_email(obo_ws)
        svc.delete(rule_id, user_email)
        return {"status": "deleted", "rule_id": rule_id}
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RegistryRuleOut:
    """Submit a draft registry rule for approval."""
    try:
        user_email = _current_user_email(obo_ws)
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
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
    """
    try:
        user_email = _current_user_email(obo_ws)
        rule = svc.approve(rule_id, user_email)
        embeddings.embed_and_store(rule)
        materializer.rematerialize_for_rule(rule_id)
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RegistryRuleOut:
    """Reject a pending registry rule."""
    try:
        user_email = _current_user_email(obo_ws)
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RegistryRuleOut:
    """Deprecate a published registry rule."""
    try:
        user_email = _current_user_email(obo_ws)
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RegistryRuleOut:
    """Reinstate a deprecated registry rule back to approved."""
    try:
        user_email = _current_user_email(obo_ws)
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
