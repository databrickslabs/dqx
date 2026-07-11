"""Ask-Genie chat proxy over the SP-owned DQ Genie space.

Six dqlake endpoints ported as five (start/poll/ask/space/feedback; the
dqlake ``embeds`` endpoint is intentionally omitted — this app already
serves its embedded dashboard through the config routes, and the Genie
space id/url is served by GET /space here).

Identity: everything runs as the app SERVICE PRINCIPAL. The space points
only at the SP-owned aggregate score objects (mv_dq_scores /
v_dq_check_results / v_dq_check_attribution) by design — see
``services/genie_space_service`` — so answering with SP credentials never
exposes row-level data the caller couldn't already see in aggregate.

Phase 4 adds POST /verify-entitlements: the UI fire-and-forgets it with the
tables on screen so the caller's row-level access (via the entitlement-gated
``v_dq_failing_rows`` dynamic view) is pre-verified before they ask Genie a
failing-rows question — see ``services/entitlement_service``.

Availability contract: when no space id is stored (provisioning skipped or
failed) every endpoint returns ``available=False`` with a 200 rather than
erroring, so the UI can hide/disable the chat cleanly.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_entitlement_service,
    get_preview_sql_executor,
    get_sp_ws,
    require_role,
)
from databricks_labs_dqx_app.backend.models import (
    GenieAnswerOut,
    GenieAskIn,
    GenieFeedbackIn,
    GenieFeedbackOut,
    GeniePollIn,
    GenieSpaceOut,
    GenieVerifyEntitlementsIn,
    GenieVerifyEntitlementsOut,
)
from databricks_labs_dqx_app.backend.services import genie_chat_service
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.entitlement_service import EntitlementService
from databricks_labs_dqx_app.backend.services.genie_chat_service import GenieChatState
from databricks_labs_dqx_app.backend.services.genie_space_service import (
    SAMPLE_QUESTIONS,
    SETTING_SPACE_ID,
    SETTING_STATUS,
    STATUS_READY,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)
router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]

SettingsDep = Annotated[AppSettingsService, Depends(get_app_settings_service)]
SpWsDep = Annotated[WorkspaceClient, Depends(get_sp_ws)]


def _to_answer(state: GenieChatState) -> GenieAnswerOut:
    return GenieAnswerOut(
        available=True,
        conversation_id=state.conversation_id,
        message_id=state.message_id,
        answer_text=state.answer_text,
        sql=state.sql,
        sql_description=state.sql_description,
        result_columns=state.result_columns,
        result_rows=state.result_rows,
        status=state.status,
        stage=state.stage,
        error=state.error,
    )


async def _space_id(settings: AppSettingsService) -> str | None:
    return await asyncio.to_thread(settings.get_setting, SETTING_SPACE_ID)


@router.post(
    "/ask",
    response_model=GenieAnswerOut,
    operation_id="askGenie",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def ask_genie(body: GenieAskIn, settings: SettingsDep, sp_ws: SpWsDep) -> GenieAnswerOut:
    """Blocking one-shot: start a message and poll it to a terminal state."""
    space_id = await _space_id(settings)
    if not space_id:
        return GenieAnswerOut(available=False)
    state = await asyncio.to_thread(genie_chat_service.ask, sp_ws, space_id, body.question, body.conversation_id)
    return _to_answer(state)


@router.post(
    "/start",
    response_model=GenieAnswerOut,
    operation_id="startGenieMessage",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def start_genie_message(body: GenieAskIn, settings: SettingsDep, sp_ws: SpWsDep) -> GenieAnswerOut:
    """Kick off a question and return ids immediately; the UI then polls
    /poll to show live progress."""
    space_id = await _space_id(settings)
    if not space_id:
        return GenieAnswerOut(available=False)
    state = await asyncio.to_thread(genie_chat_service.start, sp_ws, space_id, body.question, body.conversation_id)
    return _to_answer(state)


@router.post(
    "/poll",
    response_model=GenieAnswerOut,
    operation_id="pollGenieMessage",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def poll_genie_message(body: GeniePollIn, settings: SettingsDep, sp_ws: SpWsDep) -> GenieAnswerOut:
    """Fetch the current state of an in-flight message (partial or final)."""
    space_id = await _space_id(settings)
    if not space_id:
        return GenieAnswerOut(available=False)
    state = await asyncio.to_thread(
        genie_chat_service.poll, sp_ws, space_id, body.conversation_id, body.message_id
    )
    return _to_answer(state)


@router.get(
    "/space",
    response_model=GenieSpaceOut,
    operation_id="getGenieSpace",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def get_genie_space(settings: SettingsDep, sp_ws: SpWsDep) -> GenieSpaceOut:
    """Space availability, provisioning status, sample questions, deep link."""
    space_id = await _space_id(settings)
    status = await asyncio.to_thread(settings.get_setting, SETTING_STATUS)
    # A space present with no recorded status (provisioned before the status
    # setting existed) is usable — report ready so the UI doesn't stick on a
    # "getting ready…" state.
    if status is None and space_id:
        status = STATUS_READY
    host: str | None
    try:
        host = sp_ws.config.host
    except Exception:
        # Best-effort: the deep link is progressive enhancement — a config
        # without a resolvable host must not fail the availability probe.
        host = None
    space_url = f"{host.rstrip('/')}/genie/rooms/{space_id}" if (host and space_id) else None
    return GenieSpaceOut(
        available=bool(space_id),
        space_id=space_id,
        sample_questions=list(SAMPLE_QUESTIONS),
        status=status,
        space_url=space_url,
    )


@router.post(
    "/verify-entitlements",
    response_model=GenieVerifyEntitlementsOut,
    operation_id="verifyGenieEntitlements",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def verify_genie_entitlements(
    body: GenieVerifyEntitlementsIn,
    email: Annotated[str, Depends(get_user_email)],
    obo_sql: Annotated[SqlExecutor, Depends(get_preview_sql_executor)],
    entitlements: Annotated[EntitlementService, Depends(get_entitlement_service)],
) -> GenieVerifyEntitlementsOut:
    """Self-verify row-level (failing-rows) access for up to 50 tables (P4.1).

    Each FQN is validated before any probe; the live SELECT self-check runs
    through the CALLER's OBO executor (verifying your own access needs no
    elevated privilege) with bounded concurrency, and successes are cached
    SP-side so ``v_dq_failing_rows`` opens for this user for the TTL window.

    Fire-and-forget friendly: the UI ignores the response, and the service
    never raises — every failure mode degrades to a per-FQN outcome
    (``verified`` | ``denied`` | ``error``). Verification runs INLINE rather
    than as a background 202: the 50-FQN cap plus the probe semaphore keeps
    the worst case bounded, and inline execution keeps the per-FQN outcomes
    deterministic for callers (and tests) that do read them.
    """
    results = await entitlements.verify_and_record(obo_sql, email, body.table_fqns)
    return GenieVerifyEntitlementsOut(results=results)


@router.post(
    "/feedback",
    response_model=GenieFeedbackOut,
    operation_id="submitGenieFeedback",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def submit_genie_feedback(body: GenieFeedbackIn) -> GenieFeedbackOut:
    """Record a thumbs up/down on one answer (log-only, like dqlake).

    Both fields are pattern-validated by the model (no newlines or control
    characters), so they are safe to interpolate into the log line.
    """
    logger.info(f"genie feedback message_id={body.message_id} vote={body.vote}")
    return GenieFeedbackOut(ok=True)
