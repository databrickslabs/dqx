"""Unit tests for ``backend.services.genie_chat_service``.

Pins the Genie Conversation-API request paths + payload shapes, the
message/attachment parsing (both start-response shapes, text + query
attachments, query-result fetching), the result caps (200 rows / 50 cols /
500 chars per cell), the stage labelling, and the never-raise error
contract — all against a mocked ``ws.api_client.do``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.services import genie_chat_service as gc

SPACE = "space-1"
BASE = f"/api/2.0/genie/spaces/{SPACE}"


@pytest.fixture
def ws() -> MagicMock:
    ws = MagicMock(name="WorkspaceClient")
    return ws


# ---------------------------------------------------------------------------
# _stage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "has_sql", "has_rows", "expected"),
    [
        ("SUBMITTED", False, False, "Understanding your question"),
        ("FETCHING_METADATA", False, False, "Understanding your question"),
        ("ASKING_AI", False, False, "Writing SQL"),
        ("PENDING_WAREHOUSE", False, False, "Preparing to run"),
        ("EXECUTING_QUERY", False, False, "Running query"),
        ("COMPLETED", False, False, "Done"),
        ("FAILED", False, False, "Failed"),
        ("CANCELLED", False, False, "Cancelled"),
        (None, False, False, "Thinking"),
        ("SOME_NEW_ENUM", False, False, "Thinking"),
        ("SOME_NEW_ENUM", True, False, "Running query"),
        ("EXECUTING_QUERY", True, True, "Summarising results"),
    ],
)
def test_stage_labels(status: str | None, has_sql: bool, has_rows: bool, expected: str) -> None:
    assert gc._stage(status, has_sql=has_sql, has_rows=has_rows) == expected


# ---------------------------------------------------------------------------
# _truncate_cell / _parse_query_result — caps
# ---------------------------------------------------------------------------


def test_truncate_cell_none_stays_none() -> None:
    assert gc._truncate_cell(None) is None


def test_truncate_cell_caps_wide_values() -> None:
    wide = "x" * 1000
    out = gc._truncate_cell(wide)
    assert out is not None
    assert len(out) == 501
    assert out.endswith("…")
    assert gc._truncate_cell("short") == "short"
    assert gc._truncate_cell(42) == "42"


def test_parse_query_result_nested_statement_response() -> None:
    resp = {
        "statement_response": {
            "manifest": {"schema": {"columns": [{"name": "a"}, {"name": "b"}]}},
            "result": {"data_array": [["1", None], ["2", "y"]]},
        }
    }
    parsed = gc._parse_query_result(resp)
    assert parsed == (["a", "b"], [["1", None], ["2", "y"]])


def test_parse_query_result_inline_top_level_shape() -> None:
    resp = {
        "manifest": {"schema": {"columns": [{"name": "a"}]}},
        "result": {"data_array": [["1"]]},
    }
    assert gc._parse_query_result(resp) == (["a"], [["1"]])


def test_parse_query_result_returns_none_without_columns() -> None:
    assert gc._parse_query_result({}) is None
    assert gc._parse_query_result({"manifest": {"schema": {"columns": []}}}) is None


def test_parse_query_result_caps_rows_and_cols() -> None:
    cols = [{"name": f"c{i}"} for i in range(80)]
    rows = [[str(i)] * 80 for i in range(300)]
    parsed = gc._parse_query_result({"manifest": {"schema": {"columns": cols}}, "result": {"data_array": rows}})
    assert parsed is not None
    columns, data = parsed
    assert len(columns) == 50  # _MAX_COLS
    assert len(data) == 200  # _MAX_ROWS
    assert all(len(r) == 50 for r in data)


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


def test_start_new_conversation_posts_start_conversation(ws: MagicMock) -> None:
    ws.api_client.do.return_value = {"conversation_id": "c1", "message_id": "m1", "status": "SUBMITTED"}
    state = gc.start(ws, SPACE, "What is my score?")
    ws.api_client.do.assert_called_once_with(
        "POST", f"{BASE}/start-conversation", body={"content": "What is my score?"}
    )
    assert state.conversation_id == "c1"
    assert state.message_id == "m1"
    assert state.status == "SUBMITTED"
    assert state.stage == "Understanding your question"
    assert state.error is None


def test_start_existing_conversation_posts_messages(ws: MagicMock) -> None:
    ws.api_client.do.return_value = {"conversation_id": "c1", "message_id": "m2"}
    state = gc.start(ws, SPACE, "And now?", conversation_id="c1")
    ws.api_client.do.assert_called_once_with("POST", f"{BASE}/conversations/c1/messages", body={"content": "And now?"})
    assert state.message_id == "m2"


def test_start_parses_nested_conversation_message_shape(ws: MagicMock) -> None:
    ws.api_client.do.return_value = {
        "conversation": {"id": "c9"},
        "message": {"id": "m9", "status": "ASKING_AI"},
    }
    state = gc.start(ws, SPACE, "q")
    assert state.conversation_id == "c9"
    assert state.message_id == "m9"
    assert state.status == "ASKING_AI"
    assert state.stage == "Writing SQL"


def test_start_missing_ids_is_an_error(ws: MagicMock) -> None:
    ws.api_client.do.return_value = {"unexpected": True}
    state = gc.start(ws, SPACE, "q")
    assert state.error == "genie did not return conversation/message id"


def test_start_never_raises(ws: MagicMock) -> None:
    ws.api_client.do.side_effect = RuntimeError("genie down")
    state = gc.start(ws, SPACE, "q")
    assert state.error == "genie down"


# ---------------------------------------------------------------------------
# poll — message + attachment parsing
# ---------------------------------------------------------------------------


def test_poll_parses_text_and_query_attachments_and_fetches_result(ws: MagicMock) -> None:
    message = {
        "status": "COMPLETED",
        "attachments": [
            {"text": {"content": "2 of 100 tests failed (2%)."}},
            {
                "attachment_id": "att-1",
                "query": {"query": "SELECT 1", "description": "counts failures"},
            },
        ],
    }
    query_result = {
        "statement_response": {
            "manifest": {"schema": {"columns": [{"name": "failed_tests"}]}},
            "result": {"data_array": [["2"]]},
        }
    }
    ws.api_client.do.side_effect = [message, query_result]

    state = gc.poll(ws, SPACE, "c1", "m1")

    get_msg, get_result = ws.api_client.do.call_args_list
    assert get_msg.args == ("GET", f"{BASE}/conversations/c1/messages/m1")
    assert get_result.args == ("GET", f"{BASE}/conversations/c1/messages/m1/attachments/att-1/query-result")
    assert state.answer_text == "2 of 100 tests failed (2%)."
    assert state.sql == "SELECT 1"
    assert state.sql_description == "counts failures"
    assert state.result_columns == ["failed_tests"]
    assert state.result_rows == [["2"]]
    assert state.status == "COMPLETED"
    assert state.stage == "Done"


def test_poll_in_flight_message_has_none_fields_and_progress_stage(ws: MagicMock) -> None:
    ws.api_client.do.return_value = {"status": "ASKING_AI", "attachments": []}
    state = gc.poll(ws, SPACE, "c1", "m1")
    assert state.answer_text is None
    assert state.sql is None
    assert state.result_rows is None
    assert state.stage == "Writing SQL"


def test_poll_attachment_id_fallback_to_id_key(ws: MagicMock) -> None:
    message = {
        "status": "EXECUTING_QUERY",
        "attachments": [{"id": "att-2", "query": {"query": "SELECT 2"}}],
    }
    ws.api_client.do.side_effect = [message, {}]
    state = gc.poll(ws, SPACE, "c1", "m1")
    assert ws.api_client.do.call_args_list[1].args[1].endswith("/attachments/att-2/query-result")
    # Empty result payload parses to None — no grid yet.
    assert state.result_columns is None


def test_poll_query_result_fetch_failure_degrades_to_no_grid(ws: MagicMock) -> None:
    message = {
        "status": "EXECUTING_QUERY",
        "attachments": [{"attachment_id": "att-1", "query": {"query": "SELECT 1"}}],
    }
    ws.api_client.do.side_effect = [message, RuntimeError("result not ready")]
    state = gc.poll(ws, SPACE, "c1", "m1")
    assert state.error is None
    assert state.sql == "SELECT 1"
    assert state.result_rows is None
    assert state.stage == "Running query"


def test_poll_never_raises(ws: MagicMock) -> None:
    ws.api_client.do.side_effect = RuntimeError("genie down")
    state = gc.poll(ws, SPACE, "c1", "m1")
    assert state.error == "genie down"


# ---------------------------------------------------------------------------
# ask — blocking loop
# ---------------------------------------------------------------------------


def test_ask_polls_until_terminal_and_parses_final_message(ws: MagicMock) -> None:
    ws.api_client.do.side_effect = [
        {"conversation_id": "c1", "message_id": "m1", "status": "SUBMITTED"},  # start
        {"status": "ASKING_AI"},  # poll 1 — not terminal
        {  # poll 2 — terminal
            "status": "COMPLETED",
            "attachments": [{"text": {"content": "All good."}}],
        },
    ]
    state = gc.ask(ws, SPACE, "q", max_polls=5, poll_s=0)
    assert state.status == "COMPLETED"
    assert state.answer_text == "All good."
    assert state.error is None


def test_ask_propagates_start_error_without_polling(ws: MagicMock) -> None:
    ws.api_client.do.side_effect = RuntimeError("no space")
    state = gc.ask(ws, SPACE, "q", poll_s=0)
    assert state.error == "no space"
    assert ws.api_client.do.call_count == 1


def test_ask_returns_last_state_when_polls_exhaust(ws: MagicMock) -> None:
    ws.api_client.do.side_effect = [
        {"conversation_id": "c1", "message_id": "m1"},
        {"status": "EXECUTING_QUERY"},
        {"status": "EXECUTING_QUERY"},
    ]
    state = gc.ask(ws, SPACE, "q", max_polls=2, poll_s=0)
    assert state.status == "EXECUTING_QUERY"
    assert state.error is None
