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

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
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
)
from databricks_labs_dqx_app.backend.services import genie_chat_service
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.genie_chat_service import GenieChatState
from databricks_labs_dqx_app.backend.services.genie_space_service import (
    SAMPLE_QUESTIONS,
    SETTING_SPACE_ID,
    SETTING_STATUS,
    STATUS_READY,
)

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
