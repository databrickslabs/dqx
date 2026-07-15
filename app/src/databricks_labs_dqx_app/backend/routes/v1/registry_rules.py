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

from databricks_labs_dqx_app.backend.common.approvals import ApprovalMode, mark_auto_approver, should_auto_approve
from databricks_labs_dqx_app.backend.common.authorization import CurrentUser, UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentPrincipalIds,
    CurrentUserRole,
    get_app_settings_service,
    get_apply_rules_service,
    get_materializer,
    get_monitored_table_service,
    get_monitored_table_version_service,
    get_pending_application_service,
    get_permissions_service,
    get_registry_service,
    get_rule_embeddings_service,
    get_tag_reconcile_service,
    require_role,
)
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    BackfillRuleEmbeddingsOut,
    BatchImportRegistryRulesFailure,
    BatchImportRegistryRulesIn,
    BatchImportRegistryRulesOut,
    CreateRegistryRuleIn,
    CreateRegistryRuleOut,
    RegistryRuleDetailOut,
    RegistryRuleOut,
    RegistryRuleVersionOut,
    UpdateRegistryRuleIn,
)
from databricks_labs_dqx_app.backend.registry_models import RegistryRule
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.pending_application_service import PendingApplicationService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService
from databricks_labs_dqx_app.backend.services.tag_reconcile_service import TagReconcileService

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


@router.post(
    "/batch-import",
    response_model=BatchImportRegistryRulesOut,
    operation_id="batchImportRegistryRules",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def batch_import_registry_rules(
    body: BatchImportRegistryRulesIn,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
) -> BatchImportRegistryRulesOut:
    """Bulk-create registry drafts from imported checks in a single request.

    Avoids N sequential round-trips (each of which re-resolves Databricks
    auth) when importing many rules from YAML or a data contract.

    The batch is a SYNCHRONOUS per-rule loop of DB writes running on one
    worker thread + connection, so the payload is bounded to
    ``BATCH_IMPORT_MAX_RULES`` (rejected at request validation before any DB
    work) to keep a single request from monopolising a worker / connection.
    Per-rule failures are collected (partial success) rather than aborting
    the batch.
    """
    created: list[CreateRegistryRuleOut] = []
    reused: list[CreateRegistryRuleOut] = []
    failed: list[BatchImportRegistryRulesFailure] = []
    submitted = 0
    submit_failed = 0
    # Fingerprints already materialized in THIS batch (created OR reused), so a
    # contract that lists the same rule twice collapses to one — matches the
    # cross-import dedup below (skip_duplicates only).
    seen_in_batch: dict[str, CreateRegistryRuleOut] = {}

    for index, rule_in in enumerate(body.rules):
        try:
            fingerprint: str | None = None
            if body.skip_duplicates:
                fingerprint = svc.compute_definition_fingerprint(
                    rule_in.mode, rule_in.definition, rule_in.polarity
                )
                # Intra-batch duplicate → reuse the earlier result, no DB work.
                in_batch = seen_in_batch.get(fingerprint)
                if in_batch is not None:
                    reused.append(in_batch)
                    continue
                # Cross-import duplicate → reuse the existing active rule rather
                # than minting another copy (keeps re-imports idempotent).
                existing = svc.get_active_rule_by_fingerprint(fingerprint)
                if existing is not None:
                    existing_out = CreateRegistryRuleOut(
                        rule=RegistryRuleOut.from_domain(existing), dedup_warning=None
                    )
                    reused.append(existing_out)
                    seen_in_batch[fingerprint] = existing_out
                    continue

            rule, warning = svc.create_rule(
                mode=rule_in.mode,
                definition=rule_in.definition,
                user_email=user_email,
                polarity=rule_in.polarity,
                author_kind=rule_in.author_kind,
                user_metadata=rule_in.user_metadata,
                steward=rule_in.steward,
                source="import",
            )
            out = CreateRegistryRuleOut(rule=RegistryRuleOut.from_domain(rule), dedup_warning=warning)
            created.append(out)
            if fingerprint is not None:
                seen_in_batch[fingerprint] = out

            if body.also_submit:
                try:
                    svc.submit(rule.rule_id, user_email)
                    submitted += 1
                except Exception as submit_err:
                    logger.warning(
                        "Batch import created rule %s but submit failed: %s",
                        rule.rule_id,
                        submit_err,
                    )
                    submit_failed += 1
        except (UnsafeSqlQueryError, ValueError) as e:
            # Intentionally user-facing: unsafe-SQL and validation errors carry
            # app-authored, safe messages the importer needs to fix the rule.
            failed.append(BatchImportRegistryRulesFailure(index=index, error=str(e)))
        except Exception as e:
            # CWE-209: never surface raw exception text for unexpected errors —
            # DB/driver exceptions (SQLAlchemy/psycopg) can leak table names,
            # SQL, or connection detail. Log the specifics server-side and
            # return a generic message to the caller.
            logger.error("Batch import failed for rule index %s: %s", index, e, exc_info=True)
            failed.append(
                BatchImportRegistryRulesFailure(
                    index=index, error="Failed to import this rule due to an internal error."
                )
            )

    return BatchImportRegistryRulesOut(
        created=created,
        reused=reused,
        saved=len(created),
        submitted=submitted,
        submit_failed=submit_failed,
        failed=failed,
    )


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


def _activate_pending_applications(
    rule_id: str,
    approver: str,
    *,
    apply_rules: ApplyRulesService,
    pending: PendingApplicationService,
) -> None:
    """Drain staged (Bulk Contract Import) applications for a just-approved rule.

    On publish, every ``dq_pending_applications`` row for ``rule_id`` becomes
    applicable (the rule is now ``approved``), so turn each into a real
    ``dq_applied_rules`` link via :meth:`ApplyRulesService.apply_rule` and
    delete the pending row. Runs BEFORE ``rematerialize_for_rule`` so the
    freshly-applied rows are materialized in the same publish. Best-effort per
    row: a single activation failure is logged and skipped (the pending row
    survives for a later retry) and never turns a successful publish into a
    5xx. ``apply_rule`` is idempotent for an identical mapping, so a
    delete-after-apply failure self-heals on the next approval.
    """
    try:
        staged = pending.list_for_rule(rule_id)
    except Exception:
        logger.warning("Failed to list pending applications for rule %s", rule_id, exc_info=True)
        return
    for p in staged:
        try:
            apply_rules.apply_rule(p.binding_id, rule_id, p.column_mapping, p.created_by or approver)
            if p.id:
                pending.delete(p.id)
        except Exception:
            logger.warning(
                "Failed to activate pending application %s (binding %s, rule %s)",
                p.id,
                p.binding_id,
                rule_id,
                exc_info=True,
            )


def _publish_registry_rule(
    rule_id: str,
    approver: str,
    *,
    svc: RegistryService,
    embeddings: RuleEmbeddingsService,
    materializer: Materializer,
    version_svc: MonitoredTableVersionService,
    monitored_tables: MonitoredTableService,
    app_settings: AppSettingsService,
    apply_rules: ApplyRulesService,
    pending: PendingApplicationService,
) -> RegistryRule:
    """Publish (approve) a pending registry rule and run its side effects.

    Shared by :func:`approve_registry_rule` (explicit approve) and
    :func:`submit_registry_rule` (auto-approve under the ``auto_bypass`` /
    ``disabled`` approvals modes) so both paths re-embed, re-materialize
    followers, and re-freeze/roll-up affected bindings identically. See
    :func:`approve_registry_rule` for the full behaviour contract. Returns the
    published rule.
    """
    rule = svc.approve(rule_id, approver)
    # Activate any Bulk Contract Import pre-staged applications BEFORE
    # rematerialize so their new dq_quality_rules copies are produced here.
    _activate_pending_applications(rule_id, approver, apply_rules=apply_rules, pending=pending)
    embeddings.embed_and_store(rule)
    rematerialized = materializer.rematerialize_for_rule(rule_id)
    auto_upgrade = app_settings.get_auto_upgrade_without_approval()
    for binding_id in rematerialized:
        try:
            if auto_upgrade:
                version_svc.refreeze_current(binding_id)
            else:
                monitored_tables.rollup_status(binding_id, approver)
        except Exception:  # bookkeeping must not fail the publish
            logger.warning(
                "Post-publish binding sync for %s after publishing rule %s failed (auto_upgrade=%s)",
                binding_id,
                rule_id,
                auto_upgrade,
                exc_info=True,
            )
    return rule


@router.post(
    "/{rule_id}/submit",
    response_model=RegistryRuleOut,
    operation_id="submitRegistryRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def submit_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    embeddings: Annotated[RuleEmbeddingsService, Depends(get_rule_embeddings_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    pending: Annotated[PendingApplicationService, Depends(get_pending_application_service)],
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    user_email: CurrentUser,
) -> RegistryRuleOut:
    """Submit a draft registry rule for approval.

    Honours the app-wide approvals mode (issue #94): in ``disabled`` mode, or in
    ``auto_bypass`` mode when the caller can edit AND approve the rule
    (:meth:`PermissionsService.can_edit_and_approve`), the rule is submitted and
    then published in the same call — running the identical publish side effects
    as the explicit approve route — with the caller recorded as the approver
    carrying an ``(auto)`` marker.

    Not gated by the require-draft-run setting (issue B2-12): a registry rule is
    a central, table-agnostic definition with no single table to dry-run against
    until it is APPLIED to a monitored table / table space. The draft-run
    requirement is therefore enforced where a concrete table exists — the MT/TS
    submit paths and the per-table applied-rule submit — not here.
    """
    try:
        rule = svc.submit(rule_id, user_email)
        # Only the auto-approving modes (``disabled`` / ``auto_bypass``) consult
        # the object-aware predicate; ``enabled`` never auto-approves, so skip
        # its permission + owner lookups entirely.
        mode = app_settings.get_approvals_mode()
        can_edit_and_approve = mode != ApprovalMode.ENABLED and perms.can_edit_and_approve(
            ObjectType.REGISTRY_RULE.value,
            rule_id,
            role=role,
            principal_ids=set(principal_ids),
            owner_email=perms.get_object_owner(ObjectType.REGISTRY_RULE.value, rule_id),
            principal_email=user_email,
        )
        if should_auto_approve(mode, can_edit_and_approve=can_edit_and_approve):
            rule = _publish_registry_rule(
                rule_id,
                mark_auto_approver(user_email),
                svc=svc,
                embeddings=embeddings,
                materializer=materializer,
                version_svc=version_svc,
                monitored_tables=monitored_tables,
                app_settings=app_settings,
                apply_rules=apply_rules,
                pending=pending,
            )
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
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    pending: Annotated[PendingApplicationService, Depends(get_pending_application_service)],
    tag_reconcile: Annotated[TagReconcileService, Depends(get_tag_reconcile_service)],
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
        rule = _publish_registry_rule(
            rule_id,
            user_email,
            svc=svc,
            embeddings=embeddings,
            materializer=materializer,
            version_svc=version_svc,
            monitored_tables=monitored_tables,
            app_settings=app_settings,
            apply_rules=apply_rules,
            pending=pending,
        )
        # Apply-on-tag (Task 7): after a successful publish, attach this rule
        # to every monitored table it now tag-matches. Best-effort and a no-op
        # when tag-auto-apply is off (the service gates internally); double-
        # guarded here so this post-success side effect can never turn a
        # successful approve into a 500.
        try:
            tag_reconcile.reconcile_rule(rule_id, user_email)
        except Exception:
            logger.warning("Tag auto-apply reconcile after approve failed (non-fatal)", exc_info=True)
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


def _can_revoke_registry_submission(rule: RegistryRule, user_email: str, role: UserRole) -> bool:
    """Authors may revoke their own pending submissions; approvers/admins any."""
    if role in (UserRole.ADMIN, UserRole.RULE_APPROVER):
        return True
    author = (rule.updated_by or rule.created_by or "").lower()
    return bool(author) and author == user_email.lower()


@router.post(
    "/{rule_id}/revoke",
    response_model=RegistryRuleOut,
    operation_id="revokeRegistryRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def revoke_registry_rule(
    rule_id: str,
    svc: Annotated[RegistryService, Depends(get_registry_service)],
    user_email: CurrentUser,
    role: CurrentUserRole,
) -> RegistryRuleOut:
    """Revoke a pending registry submission back to draft (or approved for revisions)."""
    try:
        existing = svc.get_rule(rule_id)
        if existing is None:
            raise RuntimeError(f"Registry rule not found: {rule_id}")
        if not _can_revoke_registry_submission(existing, user_email, role):
            raise HTTPException(status_code=403, detail="You can only revoke your own submissions.")
        rule = svc.revoke(rule_id, user_email)
        return RegistryRuleOut.from_domain(rule)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to revoke registry rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to revoke registry rule: {e}")


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
