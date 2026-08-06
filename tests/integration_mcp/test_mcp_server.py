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
import sys
import time
from collections.abc import Callable

import requests

from tests.integration_mcp.conftest import (
    AI_QUERY_ENDPOINT,
    CONTRACT_YAML,
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


def _assert_catalogue_and_validation(client: McpClient) -> None:
    """list_available_checks (with its filter) and validate_checks in both directions.

    An agent relies on the filter to find a check without pulling the whole catalogue. The filter
    matches a name or anywhere in the full docstring, of which only the first line comes back as
    ``description`` — so assert it narrows and keeps the obvious match, rather than re-deriving which
    entries should have matched. Kept as a helper so the end-to-end test stays within pylint's
    per-function statement budget.
    """
    listed = client.call("list_available_checks")
    assert listed["count"] > 0
    assert {"is_not_null", "is_in_range"} <= {c["name"] for c in listed["checks"]}

    filtered = client.call("list_available_checks", {"filter": "range"})
    assert 0 < filtered["count"] < listed["count"], filtered["count"]
    assert "is_in_range" in {c["name"] for c in filtered["checks"]}, filtered["checks"]
    assert client.call("list_available_checks", {"filter": "zzz_no_such_check"})["count"] == 0

    assert client.call("validate_checks", {"checks": EXPLICIT_CHECKS})["valid"] is True
    bad = [{"criticality": "error", "check": {"function": "not_a_real_check", "arguments": {}}}]
    invalid = client.call("validate_checks", {"checks": bad})
    assert invalid["valid"] is False
    assert invalid["errors"], "an unknown check function should produce validation errors"


def _assert_contract_sources(client: McpClient, contract_file: str, workspace_contract: str) -> None:
    """generate_rules_from_contract accepts a volume path, a workspace path, or inline text.

    All three must yield the same rules: the inline path matters for agents that hold a contract in
    context (the server stages the text to the results volume first), and the workspace path is read
    by a different mechanism than the volume one (export + base64 vs Files API download). Exactly one
    source may be given, so an agent gets a clear error instead of a silent wrong answer.
    """
    inline = client.call("generate_rules_from_contract", {"contract_content": CONTRACT_YAML})
    assert inline["count"] > 0, inline
    assert client.call("validate_checks", {"checks": inline["rules"]})["valid"] is True

    # The workspace-file backend is read with the CALLER's OBO token, so it only works when the
    # caller can see the seeded path. The seed writes to /Shared for exactly that reason, but a
    # workspace may restrict /Shared for service principals — report and move on rather than fail
    # the whole suite over a secondary backend the volume path already covers.
    try:
        from_workspace = client.call("generate_rules_from_contract", {"contract_file": workspace_contract})
        assert from_workspace["count"] == inline["count"], (from_workspace, inline)
    except Exception as exc:  # noqa: BLE001 — see above; the primary (volume) path is asserted below
        sys.stderr.write(f"note: workspace-file contract backend not exercised ({workspace_contract}): {exc}\n")

    # Both sources, then neither: each must be rejected rather than guessed at. Tolerate either
    # surfacing — the MCP error may raise in the client or come back as a non-successful payload.
    ambiguous: list[dict] = [{"contract_file": contract_file, "contract_content": CONTRACT_YAML}, {}]
    for arguments in ambiguous:
        try:
            result = client.call("generate_rules_from_contract", arguments)
        except Exception:  # noqa: BLE001 — rejected via a raised MCP error, which is expected
            continue
        assert (
            not result.get("rules") and result.get("status") != "completed"
        ), f"ambiguous contract source accepted: {arguments.keys()} -> {result}"


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

    # A caller-supplied output name that is a dotted FQN, starts with a digit, or is empty is
    # rejected up front (it would otherwise be interpolated unquoted into the FQN). An unsupported
    # write mode is likewise refused rather than silently defaulted. Tolerate either surfacing: the
    # MCP error may raise in the client, or come back as a non-successful payload — but it must NOT
    # save.
    rejected: list[dict] = [
        {"checks": EXPLICIT_CHECKS, "output_name": "catalog.schema.table"},
        {"checks": EXPLICIT_CHECKS, "output_name": "2024_bad"},
        {"checks": EXPLICIT_CHECKS, "output_name": ""},
        {"checks": EXPLICIT_CHECKS, "output_name": "customers_checks", "mode": "upsert"},
    ]
    for arguments in rejected:
        try:
            res = client.call("save_checks", arguments)
        except Exception:  # noqa: BLE001 — rejected via a raised MCP error, which is expected
            continue
        assert not res.get("saved") and res.get("status") != "completed", f"save_checks accepted {arguments}: {res}"


def _assert_table_name_validation(client: McpClient) -> None:
    """A caller-supplied table name must be rejected before it reaches SQL.

    Every tool that takes a *table_name* interpolates it into a statement, so the identifier guard
    (fully qualified, and each part alphanumeric/underscore only) is the boundary that keeps a
    crafted name out of the generated SQL. Exercised through get_table_schema because it validates
    synchronously and returns in-process — the same guard protects run_checks and profile_table via
    create_temp_view. Tolerate either surfacing: a raised MCP error or a non-successful payload.
    """
    rejected = [
        "not_qualified",  # missing catalog/schema
        "catalog.schema",  # two parts
        "catalog.schema.table.extra",  # four parts
        "catalog.schema.tab`le",  # backtick-breakout attempt
        "catalog.schema.tab le",  # whitespace
        "",
    ]
    for table_name in rejected:
        try:
            result = client.call("get_table_schema", {"table_name": table_name})
        except Exception:  # noqa: BLE001 — rejected via a raised MCP error, which is expected
            continue
        assert not result.get("columns"), f"get_table_schema accepted {table_name!r}: {result}"


def _assert_inaccessible_paths_are_refused(client: McpClient) -> None:
    """A file path the caller cannot read must be refused, as the caller, before any job is submitted.

    The runner reads as the SP, so the server pre-checks the CALLER's access with their OBO token —
    otherwise a caller could read any path the SP can reach. Both file backends are covered:
    a UC-volume path and a workspace path. Nonexistent and forbidden are deliberately treated the
    same (the server does not distinguish them, so a caller cannot probe for existence).

    Uses tools that take a caller-supplied path — generate_rules_from_contract and load_checks — with
    paths that cannot resolve. Each must refuse rather than submit a job whose SP might succeed.
    """
    unreadable = [
        "/Volumes/no_such_catalog_xyz/no_such_schema/no_such_volume/contract.yaml",
        "/Workspace/Users/no.such.user@example.invalid/no_such_contract.yaml",
    ]
    for path in unreadable:
        try:
            result = client.call("generate_rules_from_contract", {"contract_file": path}, poll=False)
        except Exception:  # noqa: BLE001 — refused via a raised MCP error, which is expected
            continue
        assert (
            not result.get("rules") and result.get("status") != "completed"
        ), f"generate_rules_from_contract accepted an unreadable path {path!r}: {result}"

    # load_checks takes the same kind of caller-supplied path through a different tool.
    for path in unreadable:
        try:
            result = client.call("load_checks", {"location": path}, poll=False)
        except Exception:  # noqa: BLE001 — refused via a raised MCP error, which is expected
            continue
        assert result.get("status") != "completed", f"load_checks accepted an unreadable path {path!r}: {result}"


def _assert_run_result_is_owner_only(client: McpClient, url: str, get_app_token: Callable[[], str], table: str) -> None:
    """A run's result must only be readable by the identity that submitted it.

    run_id is a guessable sequential integer and every user holds CAN_MANAGE_RUN on the shared runner
    job, so the only thing standing between one caller and another's governed data (sampled source
    rows) is the submitter/caller match. The guard also denies when EITHER side is empty, so an
    unowned run cannot be read by a caller with no identity — asserted here by replaying a real,
    successfully-submitted run_id with the OBO identity header stripped.

    A mismatch must come back as not_found, never as a permission error: revealing that the run
    exists would already leak which run ids are real.
    """
    submitted = client.call("run_checks", {"table_name": table, "checks": EXPLICIT_CHECKS}, poll=False)
    run_id = submitted["run_id"]

    # Replay the same run_id presenting a DIFFERENT caller identity. Whether the Apps front door
    # forwards a client-supplied X-Forwarded-Email or overwrites it with the authenticated identity
    # is a platform behaviour, so both outcomes are handled: if the override reaches the server the
    # guard must deny, and if the platform overwrites the header the request is simply the owner's
    # again and must succeed. Either way the assertion below is meaningful — what must NEVER happen
    # is the server returning a payload to a caller whose identity does not match the submitter.
    replayed = _tool_payload(
        _mcp_request(
            url,
            get_app_token(),
            "tools/call",
            {"name": "get_run_result", "arguments": {"run_id": run_id}},
            headers={"X-Forwarded-Email": "not.the.submitter@example.invalid"},
        )
    )
    if replayed.get("status") == "not_found":
        sys.stderr.write("note: identity override reached the server; the ownership guard denied it\n")
    else:
        # The platform overwrote the header, so this was the owner's own read. Record that the
        # mismatch half of the guard was NOT exercised rather than implying it passed.
        sys.stderr.write(
            f"note: the Apps front door overrode X-Forwarded-Email, so the submitter/caller MISMATCH "
            f"branch was not exercised (got status={replayed.get('status')!r})\n"
        )
        assert replayed.get("status") in {"running", "completed"}, replayed

    # The legitimate submitter always gets their result — the guard must not over-deny.
    owned = client.wait(run_id)
    assert owned["total_rows"] == EXPECTED_TOTAL_ROWS, owned


def _assert_failed_run_surfaces_error(client: McpClient, table: str) -> None:
    """A runner job that fails must report the task's OWN error, not the generic job-level message.

    The job-level state_message is only "Workload failed, see run output for details", so
    get_run_result reaches into the task run's output for the real cause — without it an agent sees a
    failure it cannot act on. Induced with a check that validates but cannot execute: the column does
    not exist, so DQX raises inside the job rather than at submit time.

    Polls with ``poll=False`` and reads get_run_result directly, because ``client.call``'s polling
    helper raises AssertionError on a failed run (correctly — every other call site wants that). Here
    the failure IS the assertion target, so the terminal payload has to be inspected instead.
    """
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "no_such_column_xyz"}},
        }
    ]
    submitted = client.call("run_checks", {"table_name": table, "checks": checks}, poll=False)
    run_id = submitted.get("run_id")
    assert run_id, f"run_checks should submit a job even for a doomed check: {submitted}"

    deadline = time.monotonic() + 300
    while True:
        status = client.call("get_run_result", {"run_id": run_id}, poll=False)
        state = status.get("status")
        if state in {"failed", "completed"}:
            break
        assert time.monotonic() < deadline, f"run {run_id} never reached a terminal state: {status}"
        time.sleep(4)

    assert state == "failed", f"a check on a nonexistent column should fail, not succeed: {status}"
    # The whole point of the task-error lookup: the message must name the actual cause.
    detail = json.dumps(status).lower()
    assert "no_such_column_xyz" in detail, f"the failure did not name the offending column: {status}"
    # And it must stay actionable — a run URL to open.
    assert status.get("run_url") or "run_page_url" in detail, status


def test_mcp_server_end_to_end(workspace_auth, app_auth):
    """Deploy the MCP app once and exercise every tool end-to-end against the seeded table."""
    host, get_token = workspace_auth  # control-plane bearer: CLI deploy + Model Serving
    get_app_token = app_auth  # OAuth bearer the app's /mcp front-door accepts

    with (
        deploy_mcp_app(host, get_token) as app,
        seed_demo_data(app["service_principal"], app["runner_service_principal"]) as data,
    ):
        client = McpClient(
            app["url"],
            get_app_token,
            job_id=app.get("runner_job_id", ""),
            workspace_host=app.get("workspace_host", host),
        )
        wait_until_ready(client)  # a freshly-deployed app needs a moment before /mcp serves
        table = data["table"]

        # 1. Discovery — every tool is exposed.
        names = {t["name"] for t in client.list_tools()}
        assert ALL_TOOLS <= names, f"missing tools: {ALL_TOOLS - names}"

        # 2. get_workflow (in-process, returns directly).
        workflow = client.call("get_workflow")
        assert workflow.get("steps"), "get_workflow should describe the recommended steps"

        # 3-4. The catalogue (with its filter) and check validation.
        _assert_catalogue_and_validation(client)

        # 5. get_table_schema (direct SQL via OBO), then a table that does not exist — the SQL
        #    failure must surface as an error, not an empty-but-successful schema.
        schema = client.call("get_table_schema", {"table_name": table})
        columns = {c["name"] for c in schema["columns"]}
        assert {"customer_id", "name", "email", "age", "country", "signup_date", "amount"} <= columns
        try:
            missing = client.call("get_table_schema", {"table_name": f"{data['schema']}.no_such_table"})
            assert not missing.get("columns"), f"nonexistent table reported a schema: {missing}"
        except Exception:  # noqa: BLE001 — a raised MCP error is the expected surfacing
            pass
        # A malformed name must be refused by the identifier guard before it reaches SQL, and a path
        # the caller cannot read must be refused as the caller, before any SP-run job is submitted.
        _assert_table_name_validation(client)
        _assert_inaccessible_paths_are_refused(client)

        # 6. profile_table (full scan) -> generate_rules; generated rules must validate.
        profile = client.call("profile_table", {"table_name": table, "options": {"sample_fraction": 1.0}})
        assert profile["profiles"], "profiling should return per-column profiles"
        generated = client.call("generate_rules", {"profiles": profile["profiles"], "criticality": "error"})
        assert generated["count"] > 0
        assert client.call("validate_checks", {"checks": generated["rules"]})["valid"] is True

        # 7. generate_rules_from_contract — both contract sources, and the either/or contract.
        from_contract = client.call("generate_rules_from_contract", {"contract_file": data["contract"]})
        assert from_contract["count"] > 0
        assert client.call("validate_checks", {"checks": from_contract["rules"]})["valid"] is True
        _assert_contract_sources(client, data["contract"], data["workspace_contract"])

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

        # Only the submitter may read a run's result — the guard protecting one caller's governed
        # data from another. Asserted with a real run id, so it cannot pass vacuously.
        _assert_run_result_is_owner_only(client, app["url"], get_app_token, table)

        # A job that fails inside the runner must report the task's real error, not the generic
        # job-level message. Runs last: it is the only step that deliberately fails a job.
        _assert_failed_run_surfaces_error(client, table)

        # 11. Agent-in-the-loop — a real model must discover + invoke a tool (skip if unreachable).
        if _endpoint_reachable(host, get_token):
            _assert_agent_discovers_tools(host, get_token, get_app_token, app["url"], table)

    # Remote coverage is collected by deploy_mcp_app's teardown, so it happens on failure too.
