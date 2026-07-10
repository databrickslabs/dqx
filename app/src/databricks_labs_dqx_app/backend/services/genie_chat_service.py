"""Genie Conversation-API orchestration for the Ask-Genie chat.

Faithful port of dqlake's ``genie_chat.py``. Best-effort: any failure
returns a state whose ``error`` field is set so the UI degrades cleanly.

Identity: every call runs with the app SERVICE PRINCIPAL's
WorkspaceClient — the Genie space is SP-owned and points only at the
SP-owned aggregate score objects (see ``genie_space_service``), so the
proxy deliberately does not use the caller's OBO token. Row-level data is
never reachable through this path by design.

Two flows share one parser:

- ``ask()``            — blocking one-shot (start, poll until terminal,
  return the final answer).
- ``start()`` + ``poll()`` — the chat UI's progressive flow: start returns
  the ids immediately, then the UI polls (~1s) so it can show live stages
  (writing SQL -> running query -> summarising) instead of one spinner.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_BASE = "/api/2.0/genie/spaces"

# Caps so a huge query result can't bloat the answer payload. The chat UI
# shows a preview of the result grid, not the full table.
_MAX_ROWS = 200
_MAX_COLS = 50
_MAX_CELL_CHARS = 500

_TERMINAL = ("COMPLETED", "FAILED", "CANCELLED")

# Map the Genie message status to a short human stage the chat UI can show
# while the answer is still being produced. Unknown statuses fall back to a
# generic "Thinking" label rather than leaking the raw enum.
_STAGE_BY_STATUS = {
    "SUBMITTED": "Understanding your question",
    "FETCHING_METADATA": "Understanding your question",
    "FILTERING_CONTEXT": "Understanding your question",
    "ASKING_AI": "Writing SQL",
    "PENDING_WAREHOUSE": "Preparing to run",
    "EXECUTING_QUERY": "Running query",
    "COMPLETED": "Done",
    "FAILED": "Failed",
    "CANCELLED": "Cancelled",
}


@dataclass(frozen=True)
class GenieChatState:
    """Partial-or-final state of one Genie message (the shared payload shape)."""

    conversation_id: str | None = None
    message_id: str | None = None
    status: str | None = None
    stage: str | None = None
    answer_text: str | None = None
    sql: str | None = None
    sql_description: str | None = None
    # Executed query result: column names + row cells, capped server-side.
    result_columns: list[str] | None = None
    result_rows: list[list[str | None]] | None = None
    error: str | None = None


def _stage(status: str | None, *, has_sql: bool, has_rows: bool) -> str:
    """Friendly stage label. Refine the generic statuses using what's already
    arrived: once SQL is present we're past the writing step, and once rows
    are present we're summarising."""
    if status == "COMPLETED":
        return "Done"
    if status in ("FAILED", "CANCELLED"):
        return _STAGE_BY_STATUS[status]
    if has_rows:
        return "Summarising results"
    base = _STAGE_BY_STATUS.get(status or "")
    if base:
        return base
    if has_sql:
        return "Running query"
    return "Thinking"


def _truncate_cell(value: object) -> str | None:
    """Stringify a result cell, capping very wide values. NULL stays None."""
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    if len(s) > _MAX_CELL_CHARS:
        return s[:_MAX_CELL_CHARS] + "…"
    return s


def _parse_query_result(resp: dict) -> tuple[list[str], list[list[str | None]]] | None:
    """Parse a Genie query-result payload (statement-execution shape) into
    (columns, rows). The payload nests a ``statement_response`` with
    ``manifest.schema.columns[].name`` and ``result.data_array[][]``. Some
    responses inline the manifest/result at the top level — handle both.
    Returns None when no schema/columns are present."""
    sr = resp.get("statement_response") or resp
    manifest = sr.get("manifest") or {}
    schema = manifest.get("schema") or {}
    cols_meta = schema.get("columns") or []
    if not cols_meta:
        return None
    columns = [str(c.get("name", "")) for c in cols_meta[:_MAX_COLS]]
    result = sr.get("result") or {}
    data = result.get("data_array") or []
    rows: list[list[str | None]] = []
    for raw_row in data[:_MAX_ROWS]:
        rows.append([_truncate_cell(v) for v in (raw_row or [])[:_MAX_COLS]])
    return columns, rows


def _fetch_attachment_result(
    ws: WorkspaceClient, space_id: str, cid: str, mid: str, attachment_id: str
) -> tuple[list[str], list[list[str | None]]] | None:
    """Best-effort fetch + parse of a message attachment's executed query
    result. Returns None on any failure or empty result (never raises)."""
    try:
        resp = ws.api_client.do(
            "GET",
            f"{_BASE}/{space_id}/conversations/{cid}/messages/{mid}/attachments/{attachment_id}/query-result",
        )
        return _parse_query_result(resp if isinstance(resp, dict) else {})
    except Exception as e:
        # Best-effort resilience contract: the query result is progressive
        # enrichment — a fetch failure must degrade to "no grid yet", never
        # fail the poll.
        logger.info(f"genie query-result fetch failed: {e}")
        return None


def _parse_message(ws: WorkspaceClient, space_id: str, cid: str, mid: str, msg: dict) -> GenieChatState:
    """Turn a polled message into the partial-or-final answer payload. Pulls
    the text, SQL + description out of the attachments, and (once executed)
    the query result. Safe to call on an in-flight message — fields are None
    until they arrive, and the stage reflects how far along we are."""
    answer_text: str | None = None
    sql: str | None = None
    sql_desc: str | None = None
    result_columns: list[str] | None = None
    result_rows: list[list[str | None]] | None = None
    for att in msg.get("attachments") or []:
        text = att.get("text") or {}
        query = att.get("query") or {}
        if text.get("content"):
            answer_text = text["content"]
        if query.get("query"):
            sql = query["query"]
            sql_desc = query.get("description")
            # The attachment carries the EXECUTED query result behind a
            # separate endpoint — fetch it so the UI can show the grid.
            # Returns None until the query has actually run.
            att_id = att.get("attachment_id") or att.get("id")
            if att_id:
                parsed = _fetch_attachment_result(ws, space_id, cid, mid, att_id)
                if parsed is not None:
                    result_columns, result_rows = parsed
    status = msg.get("status")
    return GenieChatState(
        conversation_id=cid,
        message_id=mid,
        status=status,
        stage=_stage(status, has_sql=bool(sql), has_rows=bool(result_rows)),
        answer_text=answer_text,
        sql=sql,
        sql_description=sql_desc,
        result_columns=result_columns,
        result_rows=result_rows,
    )


def start(ws: WorkspaceClient, space_id: str, question: str, conversation_id: str | None = None) -> GenieChatState:
    """Kick off a Genie message and return its ids immediately (no polling),
    so the UI can start showing progress. Returns an error state on failure."""
    try:
        if conversation_id:
            started = ws.api_client.do(
                "POST",
                f"{_BASE}/{space_id}/conversations/{conversation_id}/messages",
                body={"content": question},
            )
        else:
            started = ws.api_client.do(
                "POST",
                f"{_BASE}/{space_id}/start-conversation",
                body={"content": question},
            )
        started = started if isinstance(started, dict) else {}
        cid = started.get("conversation_id") or (started.get("conversation") or {}).get("id")
        mid = started.get("message_id") or (started.get("message") or {}).get("id")
        if not (cid and mid):
            return GenieChatState(error="genie did not return conversation/message id")
        status = (started.get("message") or {}).get("status") or started.get("status")
        return GenieChatState(
            conversation_id=cid,
            message_id=mid,
            status=status,
            stage=_stage(status, has_sql=False, has_rows=False),
        )
    except Exception as e:
        # Best-effort resilience contract: the chat endpoints return a clean
        # error payload the UI renders in-thread; a raised exception would
        # 500 the whole sidebar instead.
        logger.info(f"genie start failed: {e}")
        return GenieChatState(error=str(e))


def poll(ws: WorkspaceClient, space_id: str, conversation_id: str, message_id: str) -> GenieChatState:
    """Fetch the current state of an in-flight (or finished) message once and
    return the partial-or-final answer payload. The UI calls this on an
    interval until status is terminal."""
    try:
        msg = ws.api_client.do(
            "GET",
            f"{_BASE}/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        )
        return _parse_message(ws, space_id, conversation_id, message_id, msg if isinstance(msg, dict) else {})
    except Exception as e:
        # Best-effort resilience contract — see ``start``.
        logger.info(f"genie poll failed: {e}")
        return GenieChatState(error=str(e))


def ask(
    ws: WorkspaceClient,
    space_id: str,
    question: str,
    conversation_id: str | None = None,
    *,
    max_polls: int = 30,
    poll_s: float = 1.0,
) -> GenieChatState:
    """Blocking one-shot: start, poll until terminal, return the final answer."""
    started = start(ws, space_id, question, conversation_id)
    if started.error:
        return started
    cid, mid = started.conversation_id, started.message_id
    if not (cid and mid):  # defensive — start() guarantees ids when error is unset
        return GenieChatState(error="genie did not return conversation/message id")
    try:
        msg: dict = {}
        for _ in range(max_polls):
            resp = ws.api_client.do("GET", f"{_BASE}/{space_id}/conversations/{cid}/messages/{mid}")
            msg = resp if isinstance(resp, dict) else {}
            if msg.get("status") in _TERMINAL:
                break
            time.sleep(poll_s)
        return _parse_message(ws, space_id, cid, mid, msg)
    except Exception as e:
        # Best-effort resilience contract — see ``start``.
        logger.info(f"genie ask failed: {e}")
        return GenieChatState(error=str(e))
