"""Agent-in-the-loop integration test for the deployed DQX MCP server.

Mirrors the LLM-in-the-loop evaluation pattern (MCPEval-style): connect a real MCP client
to the deployed server, hand a tool-calling LLM (a Databricks model-serving endpoint) the
server's tool schemas plus a natural-language instruction, and assert the model DISCOVERS
and INVOKES the right tools — i.e. the tools are usable by an arbitrary agent, not just our
own code. Assertions are on the tool-use trajectory and structural output (not exact text),
following the DQX anomaly AI-explanation test style.

Scaffold & teardown: the `dq_test_table` fixture creates its OWN isolated table and drops it
afterwards (a yield-fixture, so teardown runs even if the test fails) — replicating the DQX
pytester `factory(create, delete)` guarantee. The mcp-server test env does not have the root
project's pytester fixtures, so we build the equivalent with the Databricks SDK.

Gated: skips unless a deployed server + workspace LLM endpoint + a writable test catalog are
configured. Probe-and-skip on the LLM endpoint mirrors the anomaly `ai_query_endpoint` fixture.

Env to run against a live deployment:
  DQX_MCP_SERVER_URL    - base URL of the deployed app (e.g. https://mcp-dqx-vb-....databricksapps.com)
  DATABRICKS_HOST       - workspace URL (serving endpoint + SDK)
  DATABRICKS_TOKEN      - user OAuth/PAT (bearer for the app's OBO proxy, the serving endpoint, the SDK)
  DQX_MCP_TEST_CATALOG  - catalog the test may create/drop a schema+table in
  DQX_MCP_LLM_ENDPOINT  - optional model-serving endpoint name (default databricks-claude-sonnet-4-5)
  DQX_MCP_TEST_WAREHOUSE- optional SQL warehouse id (else auto-discovered)
"""

import json
import os
import uuid

import pytest
import requests

SERVER_URL = os.environ.get("DQX_MCP_SERVER_URL", "").rstrip("/")
HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
TEST_CATALOG = os.environ.get("DQX_MCP_TEST_CATALOG", "")
LLM_ENDPOINT = os.environ.get("DQX_MCP_LLM_ENDPOINT", "databricks-claude-sonnet-4-5")

_MISSING = not (SERVER_URL and HOST and TOKEN and TEST_CATALOG)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        _MISSING, reason="set DQX_MCP_SERVER_URL/DATABRICKS_HOST/DATABRICKS_TOKEN/DQX_MCP_TEST_CATALOG to run"
    ),
]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _warehouse_id(ws) -> str:
    override = os.environ.get("DQX_MCP_TEST_WAREHOUSE")
    if override:
        return override
    warehouses = list(ws.warehouses.list())
    if not warehouses:
        pytest.skip("no SQL warehouse available for the test scaffold")
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    return (running[0] if running else warehouses[0]).id


@pytest.fixture
def dq_test_table():
    """Create an isolated test table, yield its FQN, and drop it on teardown.

    Replicates the DQX pytester `factory` create/delete guarantee: teardown runs even if
    the test body fails, so the scaffold never leaks.
    """
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient(host=HOST, token=TOKEN)
    warehouse_id = _warehouse_id(ws)
    schema = "dqx_mcp_it"
    table = f"agent_{uuid.uuid4().hex[:8]}"
    fqn = f"{TEST_CATALOG}.{schema}.{table}"

    def sql(query: str) -> None:
        result = ws.statement_execution.execute_statement(
            statement=query, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        state = result.status.state.value if result.status and result.status.state else "?"
        if state != "SUCCEEDED":
            raise RuntimeError(f"scaffold SQL failed ({state}): {query[:80]}")

    sql(f"CREATE SCHEMA IF NOT EXISTS `{TEST_CATALOG}`.`{schema}`")
    sql(
        f"CREATE OR REPLACE TABLE {fqn} AS SELECT * FROM VALUES "
        f"(1, 100, 'NEW', 5.0), (2, 101, 'SHIPPED', 3.5), (3, 102, 'CANCELLED', 9.0) "
        f"AS t(order_id, customer_id, status, amount)"
    )
    try:
        yield fqn
    finally:
        try:
            sql(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:  # teardown must not mask a test failure
            pass


def _llm_endpoint_reachable() -> bool:
    """Cheap probe; skip the test gracefully if the endpoint is down (anomaly-test style)."""
    try:
        r = requests.post(
            f"{HOST}/serving-endpoints/{LLM_ENDPOINT}/invocations",
            headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
            json={"messages": [{"role": "user", "content": "reply ok"}], "max_tokens": 5, "temperature": 0.0},
            timeout=30,
        )
        return r.status_code == 200
    except Exception:
        return False


def _chat(messages, tools):
    r = requests.post(
        f"{HOST}/serving-endpoints/{LLM_ENDPOINT}/invocations",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        json={"messages": messages, "tools": tools, "max_tokens": 1024, "temperature": 0.0},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()["choices"][0]


@pytest.mark.anyio
async def test_agent_discovers_and_uses_tools(dq_test_table):
    if not _llm_endpoint_reachable():
        pytest.skip(f"serving endpoint {LLM_ENDPOINT} not reachable")

    from fastmcp import Client
    from fastmcp.client.transports import StreamableHttpTransport

    transport = StreamableHttpTransport(f"{SERVER_URL}/mcp", headers={"Authorization": f"Bearer {TOKEN}"})

    async with Client(transport) as mcp:
        mcp_tools = await mcp.list_tools()
        # Expose the server's tools to the LLM in OpenAI function-calling format.
        oai_tools = [
            {
                "type": "function",
                "function": {"name": t.name, "description": t.description or t.name, "parameters": t.inputSchema},
            }
            for t in mcp_tools
        ]

        messages = [
            {
                "role": "system",
                "content": "You are a data quality assistant. Use the available tools to answer the "
                "user. When you have enough information, give a short final answer.",
            },
            {
                "role": "user",
                "content": f"What columns does the table {dq_test_table} have? Use the tools to find out.",
            },
        ]

        called_tools = []
        final_text = ""
        for _turn in range(6):
            choice = _chat(messages, oai_tools)
            msg = choice["message"]
            messages.append(msg)
            if choice.get("finish_reason") == "tool_calls" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    name = tc["function"]["name"]
                    args = json.loads(tc["function"]["arguments"] or "{}")
                    called_tools.append(name)
                    result = await mcp.call_tool(name, args)
                    payload = (
                        result.data if result.data is not None else (result.content[0].text if result.content else "")
                    )
                    messages.append(
                        {"role": "tool", "tool_call_id": tc["id"], "content": json.dumps(payload, default=str)[:4000]}
                    )
            else:
                final_text = msg.get("content") or ""
                break

        print(f"tool-use trajectory: {called_tools}")
        print(f"final answer: {final_text[:300]}")

        # The agent must have discovered and invoked the schema tool for this instruction...
        assert called_tools, "the model did not call any tool"
        assert "get_table_schema" in called_tools, f"expected get_table_schema; got {called_tools}"
        # ...and produced a sensible final answer referencing a column from OUR scaffolded table.
        assert final_text.strip(), "no final answer produced"
        assert any(
            col in final_text.lower() for col in ("order_id", "customer_id", "status", "amount", "column")
        ), f"final answer does not look schema-related: {final_text[:200]}"
