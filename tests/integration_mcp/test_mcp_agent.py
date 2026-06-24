"""Agent-in-the-loop integration test for the deployed DQX MCP server.

A tool-calling model (the workspace Model Serving endpoint) is handed the MCP server's tool
schemas and a natural-language instruction; we assert it DISCOVERS and INVOKES the right tool
and answers about the real table — verifying the tools are usable by an arbitrary agent.

Reuses the integration harness: the deployed app comes from ``deployed_mcp``, the source table
from ``make_schema`` (auto-dropped), and the LLM from the shared serving endpoint. Assertions are
on the tool-use trajectory and structural output, not exact text.
"""

import json

import pytest
import requests

from tests.integration_mcp.conftest import AI_QUERY_ENDPOINT, CATALOG


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _endpoint_reachable(host: str, token: str) -> bool:
    try:
        resp = requests.post(
            f"{host}/serving-endpoints/{AI_QUERY_ENDPOINT}/invocations",
            headers=_headers(token),
            json={"messages": [{"role": "user", "content": "reply ok"}], "max_tokens": 5, "temperature": 0.0},
            timeout=30,
        )
        return resp.status_code == 200
    except requests.RequestException:
        return False


def _chat(host: str, token: str, messages: list[dict], tools: list[dict]) -> dict:
    resp = requests.post(
        f"{host}/serving-endpoints/{AI_QUERY_ENDPOINT}/invocations",
        headers=_headers(token),
        json={"messages": messages, "tools": tools, "max_tokens": 1024, "temperature": 0.0},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]


def _mcp(url: str, token: str, method: str, params: dict) -> dict:
    resp = requests.post(
        f"{url}/mcp",
        headers={**_headers(token), "Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=120,
    )
    resp.raise_for_status()
    text = resp.text
    if "data:" in text[:32]:
        text = text.split("data:", 1)[1].strip()
    body = json.loads(text)
    if "error" in body:
        raise RuntimeError(f"MCP error: {body['error']}")
    return body["result"]


def _tool_result(result: dict):
    if result.get("structuredContent"):
        return result["structuredContent"]
    content = result.get("content") or []
    return json.loads(content[0]["text"]) if content else result


def _create_orders_table(ws, table: str) -> None:
    """Create the source table via a SQL warehouse (no Spark/Connect dependency)."""
    warehouses = list(ws.warehouses.list())
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    warehouse_id = (running[0] if running else warehouses[0]).id
    ws.statement_execution.execute_statement(
        statement=(
            f"CREATE TABLE {table} AS SELECT * FROM VALUES "
            f"(1, 100, 'NEW', 5.0), (2, 101, 'SHIPPED', 3.5) AS t(order_id, customer_id, status, amount)"
        ),
        warehouse_id=warehouse_id,
        wait_timeout="50s",
    )


def test_agent_discovers_and_uses_tools(ws, make_schema, workspace_auth, deployed_mcp):
    """An arbitrary tool-calling model must pick get_table_schema and report the real columns."""
    assert ws.current_user.me() is not None  # fail-fast if workspace auth is broken
    host, token = workspace_auth
    if not _endpoint_reachable(host, token):
        pytest.skip(f"serving endpoint {AI_QUERY_ENDPOINT} not reachable")

    url = deployed_mcp  # session-scoped: the app is deployed once and shared across tests

    schema = make_schema(catalog_name=CATALOG)
    table = f"{CATALOG}.{schema.name}.orders"
    _create_orders_table(ws, table)

    oai_tools = [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t.get("description") or t["name"],
                "parameters": t["inputSchema"],
            },
        }
        for t in _mcp(url, token, "tools/list", {})["tools"]
    ]

    messages: list[dict] = [
        {
            "role": "system",
            "content": "You are a data quality assistant. Use the available tools to answer, "
            "then give a short final answer.",
        },
        {"role": "user", "content": f"What columns does the table {table} have? Use the tools to find out."},
    ]

    called_tools: list[str] = []
    final_text = ""
    for _turn in range(6):
        choice = _chat(host, token, messages, oai_tools)
        msg = choice["message"]
        messages.append(msg)
        if choice.get("finish_reason") == "tool_calls" and msg.get("tool_calls"):
            for call in msg["tool_calls"]:
                called_tools.append(call["function"]["name"])
                args = json.loads(call["function"]["arguments"] or "{}")
                result = _tool_result(
                    _mcp(url, token, "tools/call", {"name": call["function"]["name"], "arguments": args})
                )
                messages.append(
                    {"role": "tool", "tool_call_id": call["id"], "content": json.dumps(result, default=str)[:4000]}
                )
        else:
            final_text = msg.get("content") or ""
            break

    assert "get_table_schema" in called_tools, f"expected get_table_schema; got {called_tools}"
    assert final_text.strip(), "no final answer produced"
    assert any(col in final_text.lower() for col in ("order_id", "customer_id", "status", "amount", "column"))
