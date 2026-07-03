"""End-to-end integration test for the deployed DQX MCP server.

ONE self-contained test that deploys an isolated MCP app, exercises every tool against a seeded
dirty 'customers' table, and tears everything down. It is a single test (rather than many tests
sharing a fixture) on purpose: the acceptance harness runs each test in its own pytest session,
so a session-scoped "deploy once" fixture is re-run per test — see tests/integration_mcp/
conftest.py. Mirrors the repo's e2e bundle test (test_run_dqx_demo_asset_bundle), which likewise
owns its deploy + teardown.

Deterministic tool assertions use explicit error-level rules so failure counts are exact; the
agent-in-the-loop check runs last (skipped if the serving endpoint is unreachable).
"""

import json
from collections.abc import Callable

import requests

from tests.integration_mcp.conftest import (
    AI_QUERY_ENDPOINT,
    McpClient,
    _mcp_request,
    _tool_payload,
    deploy_mcp_app,
    seed_demo_data,
    wait_until_ready,
)

# The 10-row sample table (conftest._CUSTOMERS_ROWS) has exactly these error-level failures:
#   - customer_id NULL ....... 1 row  (is_not_null)
#   - age outside [0, 120] ... 2 rows (-3 and 210)
#   - name NULL .............. 1 row  (is_not_null_and_not_empty)
# Across 4 distinct rows, so 4 invalid / 6 valid out of 10.
EXPLICIT_CHECKS: list[dict] = [
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "customer_id"}}},
    {
        "criticality": "error",
        "check": {"function": "is_in_range", "arguments": {"column": "age", "min_limit": 0, "max_limit": 120}},
    },
    {"criticality": "error", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}}},
]
EXPECTED_TOTAL_ROWS = 10
EXPECTED_INVALID_ROWS = 4

ALL_TOOLS = {
    "get_workflow",
    "get_table_schema",
    "profile_table",
    "generate_rules",
    "generate_rules_from_contract",
    "list_available_checks",
    "validate_checks",
    "run_checks",
    "save_checks",
    "load_checks",
    "apply_checks_and_save_to_table",
    "get_run_result",
}


def _endpoint_reachable(host: str, get_token: Callable[[], str]) -> bool:
    try:
        resp = requests.post(
            f"{host}/serving-endpoints/{AI_QUERY_ENDPOINT}/invocations",
            headers={"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"},
            json={"messages": [{"role": "user", "content": "reply ok"}], "max_tokens": 5, "temperature": 0.0},
            timeout=30,
        )
        return resp.status_code == 200
    except requests.RequestException:
        return False


def _assert_agent_discovers_tools(
    host: str, get_token: Callable[[], str], get_app_token: Callable[[], str], url: str, table: str
) -> None:
    """Hand a tool-calling model the MCP tool schemas and assert it picks get_table_schema.

    *get_token* authenticates the Model Serving endpoint (workspace host); *get_app_token*
    authenticates the app's /mcp front-door (which only accepts OAuth tokens — see app_auth).
    """
    oai_tools = [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t.get("description") or t["name"],
                "parameters": t["inputSchema"],
            },
        }
        for t in _mcp_request(url, get_app_token(), "tools/list", {})["tools"]
    ]
    messages: list[dict] = [
        {"role": "system", "content": "You are a data quality assistant. Use the tools, then give a short answer."},
        {"role": "user", "content": f"What columns does the table {table} have? Use the tools to find out."},
    ]
    called_tools: list[str] = []
    final_text = ""
    for _turn in range(6):
        resp = requests.post(
            f"{host}/serving-endpoints/{AI_QUERY_ENDPOINT}/invocations",
            headers={"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"},
            json={"messages": messages, "tools": oai_tools, "max_tokens": 1024, "temperature": 0.0},
            timeout=120,
        )
        resp.raise_for_status()
        choice = resp.json()["choices"][0]
        msg = choice["message"]
        messages.append(msg)
        if choice.get("finish_reason") == "tool_calls" and msg.get("tool_calls"):
            for call in msg["tool_calls"]:
                called_tools.append(call["function"]["name"])
                args = json.loads(call["function"]["arguments"] or "{}")
                result = _tool_payload(
                    _mcp_request(
                        url, get_app_token(), "tools/call", {"name": call["function"]["name"], "arguments": args}
                    )
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


def _assert_persisting_tools(client: McpClient, table: str) -> None:
    """Exercise the tools that persist data — both write to the caller's private per-user schema.

    save_checks writes to ``<catalog>.dqx_mcp_<user>.customers_checks`` (loaded back by the FQN it
    reports); apply_checks_and_save_to_table writes the clean/quarantine split to the same schema.
    Kept as a helper so the end-to-end test stays within pylint's per-function statement budget.
    """
    saved = client.call(
        "save_checks", {"checks": EXPLICIT_CHECKS, "output_name": "customers_checks", "mode": "overwrite"}
    )
    assert saved["saved"] is True and saved["count"] == len(EXPLICIT_CHECKS)
    # grant-on-write: the calling user is granted access to the table the runner created
    assert saved.get("access_granted_to"), "save_checks should grant the caller access to the checks table"
    saved_location = saved["location"]
    assert ".dqx_mcp_" in saved_location and saved_location.endswith(".customers_checks"), saved_location
    loaded = client.call("load_checks", {"location": saved_location})
    assert loaded["count"] == len(EXPLICIT_CHECKS)
    assert {c["check"]["function"] for c in loaded["checks"]} == {c["check"]["function"] for c in EXPLICIT_CHECKS}

    applied = client.call(
        "apply_checks_and_save_to_table",
        {
            "table_name": table,
            "checks": EXPLICIT_CHECKS,
            "output_name": "customers_clean",
            "quarantine_name": "customers_quarantine",
            "mode": "overwrite",
        },
    )
    assert applied["quarantine_rows"] == EXPECTED_INVALID_ROWS
    assert applied["output_rows"] == EXPECTED_TOTAL_ROWS - EXPECTED_INVALID_ROWS
    # grant-on-write: the calling user is granted access to both output tables the runner created.
    # access_granted_to is set only because BOTH created tables were granted (partial grants report null).
    assert applied.get("access_granted_to"), "apply should grant the caller access to the outputs"
    assert applied.get("output_schema", "").rsplit(".", 1)[-1].startswith("dqx_mcp_"), applied.get("output_schema")
    assert applied["output_table"].endswith(".customers_clean"), applied["output_table"]
    assert len(applied.get("granted_tables") or []) == 2, applied.get("granted_tables")

    # A caller-supplied output name that is a dotted FQN or starts with a digit is rejected up front
    # (it would otherwise be interpolated unquoted into the FQN). Tolerate either surfacing: the MCP
    # error may raise in the client, or come back as a non-successful payload — but it must NOT save.
    for bad_name in ("catalog.schema.table", "2024_bad"):
        try:
            res = client.call("save_checks", {"checks": EXPLICIT_CHECKS, "output_name": bad_name})
        except Exception:
            continue  # rejected via raised MCP error — expected
        assert not res.get("saved") and res.get("status") != "completed", f"bad output_name accepted: {res}"


def test_mcp_server_end_to_end(workspace_auth, app_auth):
    """Deploy the MCP app once and exercise every tool end-to-end against the seeded table."""
    host, get_token = workspace_auth  # control-plane bearer: CLI deploy + Model Serving
    get_app_token = app_auth  # OAuth bearer the app's /mcp front-door accepts

    with deploy_mcp_app(host, get_token) as app, seed_demo_data(app["service_principal"]) as data:
        client = McpClient(app["url"], get_app_token)
        wait_until_ready(client)  # a freshly-deployed app needs a moment before /mcp serves
        table = data["table"]

        # 1. Discovery — every tool is exposed.
        names = {t["name"] for t in client.list_tools()}
        assert ALL_TOOLS <= names, f"missing tools: {ALL_TOOLS - names}"

        # 2. get_workflow (in-process, returns directly).
        workflow = client.call("get_workflow")
        assert workflow.get("steps"), "get_workflow should describe the recommended steps"

        # 3. list_available_checks — built-ins present.
        listed = client.call("list_available_checks")
        assert listed["count"] > 0
        assert {"is_not_null", "is_in_range"} <= {c["name"] for c in listed["checks"]}

        # 4. validate_checks — accepts valid, rejects invalid.
        assert client.call("validate_checks", {"checks": EXPLICIT_CHECKS})["valid"] is True
        bad = [{"criticality": "error", "check": {"function": "not_a_real_check", "arguments": {}}}]
        invalid = client.call("validate_checks", {"checks": bad})
        assert invalid["valid"] is False
        assert invalid["errors"], "an unknown check function should produce validation errors"

        # 5. get_table_schema (direct SQL via OBO).
        schema = client.call("get_table_schema", {"table_name": table})
        columns = {c["name"] for c in schema["columns"]}
        assert {"customer_id", "name", "email", "age", "country", "signup_date", "amount"} <= columns

        # 6. profile_table (full scan) -> generate_rules; generated rules must validate.
        profile = client.call("profile_table", {"table_name": table, "options": {"sample_fraction": 1.0}})
        assert profile["profiles"], "profiling should return per-column profiles"
        generated = client.call("generate_rules", {"profiles": profile["profiles"], "criticality": "error"})
        assert generated["count"] > 0
        assert client.call("validate_checks", {"checks": generated["rules"]})["valid"] is True

        # 7. generate_rules_from_contract (reads the ODCS contract from the UC volume).
        from_contract = client.call("generate_rules_from_contract", {"contract_file": data["contract"]})
        assert from_contract["count"] > 0
        assert client.call("validate_checks", {"checks": from_contract["rules"]})["valid"] is True

        # 8. run_checks — flags exactly the known-dirty rows.
        run = client.call("run_checks", {"table_name": table, "checks": EXPLICIT_CHECKS})
        assert run["total_rows"] == EXPECTED_TOTAL_ROWS
        assert run["invalid_rows"] == EXPECTED_INVALID_ROWS
        assert run["valid_rows"] == EXPECTED_TOTAL_ROWS - EXPECTED_INVALID_ROWS
        assert run["error_sample"] and run["rule_summary"]

        # 9-10. Persisting tools (save_checks -> load_checks round-trip, then
        #       apply_checks_and_save_to_table) — both write to the caller's private per-user schema.
        _assert_persisting_tools(client, table)

        # get_run_result for an unknown run_id returns a structured not_found (also confirms the
        # ownership/IDOR guard handles a missing run cleanly rather than erroring).
        unknown = client.call("get_run_result", {"run_id": 999999999999999})
        assert unknown["status"] == "not_found", unknown

        # 11. Agent-in-the-loop — a real model must discover + invoke a tool (skip if unreachable).
        if _endpoint_reachable(host, get_token):
            _assert_agent_discovers_tools(host, get_token, get_app_token, app["url"], table)
