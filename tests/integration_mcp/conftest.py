"""Fixtures for the DQX MCP server integration tests.

These run via the same acceptance harness as the other integration suites, so they reuse
its workspace, authentication, the shared ``TEST_CATALOG``, and ``make_schema`` (create +
automatic teardown). A ``deployed_mcp`` fixture stands up an isolated MCP app from the bundle
and tears it down. Auth is resolved by the SDK from the ambient credentials (see
``workspace_auth``) and handed to the deploy/teardown scripts and HTTP calls, so no
DATABRICKS_TOKEN needs to be set in the environment — it works under the acceptance action's
OIDC auth and under a local profile alike.
"""

import io
import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
import requests
from databricks.sdk import WorkspaceClient

from tests.constants import TEST_CATALOG

_MCP_SCRIPTS = Path(__file__).resolve().parents[2] / "mcp-server" / "scripts"

# Reuse the same Model Serving endpoint the anomaly AI-explanation tests use.
AI_QUERY_ENDPOINT = os.environ.get("DQX_AI_QUERY_TEST_ENDPOINT", "databricks-claude-sonnet-4-5")

# Catalog the test creates/drops schemas + temp views in. Defaults to the shared TEST_CATALOG
# (what CI uses); override with DQX_MCP_TEST_CATALOG to run locally against a workspace that
# uses a different catalog.
CATALOG = os.environ.get("DQX_MCP_TEST_CATALOG") or TEST_CATALOG


@pytest.fixture(scope="session")
def workspace_auth() -> tuple[str, str]:
    """(host, bearer token) resolved by the SDK from the ambient auth.

    Builds its own session-scoped client (pytester's ``ws`` is function-scoped, so a
    session fixture can't depend on it) using the SDK's default config resolution — the
    same auth the harness provides: acceptance-action OIDC env in CI, a profile or env
    vars locally. The token is minted via ``config.authenticate()``, so no DATABRICKS_TOKEN
    needs to be set anywhere. It's needed for the CLI deploy and for raw HTTP to the serving
    endpoint and the app's /mcp endpoint (a separate *.databricksapps.com host the SDK
    can't proxy).
    """
    config = WorkspaceClient().config
    token = (config.authenticate() or {}).get("Authorization", "").removeprefix("Bearer ").strip()
    assert token, "could not obtain a workspace bearer token from the SDK config"
    return config.host.rstrip("/"), token


def _script_env(name_prefix: str, secret_scope: str, host: str, token: str) -> dict[str, str]:
    return {
        **os.environ,
        "NAME_PREFIX": name_prefix,
        "CONFIG_SECRET_SCOPE": secret_scope,
        "DQX_MCP_TEST_CATALOG": CATALOG,
        "DATABRICKS_HOST": host,
        "DATABRICKS_TOKEN": token,
    }


def _emitted(stdout: str, key: str) -> str:
    """Return the value of the last ``KEY=VALUE`` line ci_deploy.sh printed for ``key``."""
    return next(
        (line.split("=", 1)[1].strip() for line in reversed(stdout.splitlines()) if line.startswith(f"{key}=")),
        "",
    )


@pytest.fixture(scope="session")
def deployed_mcp(workspace_auth) -> Iterator[dict[str, str]]:
    """Deploy ONE isolated MCP app for the whole test session and tear it down at the end.

    Session-scoped so the (slow) app deploy happens once and is shared across all tests, rather
    than redeploying per test. Teardown runs even if a test fails. If the workspace cannot host a
    Databricks App, the deploy fails and the suite is skipped with the underlying error rather
    than reporting a false failure.

    Yields a dict with ``url`` (the app's base URL) and ``service_principal`` (the app SP's
    application id — the identity the runner job runs as; tests grant it write access for the
    persisting tools).
    """
    host, token = workspace_auth
    name_prefix = f"mcp-dqx-it-{uuid4().hex[:6]}"
    secret_scope = f"dqx-config-{name_prefix}"
    env = _script_env(name_prefix, secret_scope, host, token)
    try:
        result = subprocess.run(
            ["bash", str(_MCP_SCRIPTS / "ci_deploy.sh")], env=env, capture_output=True, text=True, check=True
        )
    except subprocess.CalledProcessError as exc:
        # Surface the FULL deploy output (both streams) so the real failure is visible in the
        # CI log — the CLI's bundle errors land on stdout, while stderr often only carries a
        # benign trailing warning. Then clean up partial resources (the yield/finally below
        # never runs when the fixture fails here).
        sys.stderr.write(f"\n===== ci_deploy.sh STDOUT =====\n{exc.stdout}\n")
        sys.stderr.write(f"===== ci_deploy.sh STDERR =====\n{exc.stderr}\n")
        subprocess.run(["bash", str(_MCP_SCRIPTS / "ci_destroy.sh")], env=env, check=False)
        detail = f"STDOUT(tail):\n{(exc.stdout or '')[-2500:]}\n\nSTDERR(tail):\n{(exc.stderr or '')[-2500:]}"
        pytest.fail(f"MCP app deploy failed:\n{detail}")

    url = _emitted(result.stdout, "DQX_MCP_SERVER_URL")
    assert url, "ci_deploy.sh did not emit DQX_MCP_SERVER_URL"
    try:
        yield {"url": url, "service_principal": _emitted(result.stdout, "DQX_MCP_APP_SERVICE_PRINCIPAL")}
    finally:
        subprocess.run(["bash", str(_MCP_SCRIPTS / "ci_destroy.sh")], env=env, check=False)


# --- MCP-over-HTTP client used by the tool integration tests ---------------------------------


def _mcp_request(url: str, token: str, method: str, params: dict) -> dict:
    """Issue one JSON-RPC call to the app's /mcp endpoint and return the ``result`` payload."""
    resp = requests.post(
        f"{url}/mcp",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=120,
    )
    resp.raise_for_status()
    text = resp.text
    if "data:" in text[:32]:  # FastMCP can answer as an SSE event; unwrap the JSON line
        text = text.split("data:", 1)[1].strip()
    body = json.loads(text)
    if "error" in body:
        raise RuntimeError(f"MCP error: {body['error']}")
    return body["result"]


def _tool_payload(call_result: dict) -> dict:
    """Extract a tool's structured return value (always a dict for DQX tools)."""
    if call_result.get("structuredContent"):
        return call_result["structuredContent"]
    content = call_result.get("content") or []
    return json.loads(content[0]["text"]) if content else call_result


class McpClient:
    """Thin MCP client for tests: calls tools and resolves the async submit→poll pattern.

    Most DQX tools return ``{"status": "submitted", "run_id": ...}`` immediately; ``call`` polls
    ``get_run_result`` until the run is terminal and returns the inner ``result``, so a test just
    sees the final value. Synchronous tools (e.g. ``get_table_schema``, ``get_workflow``) return
    their payload directly.
    """

    def __init__(self, url: str, token: str):
        self._url = url
        self._token = token

    def list_tools(self) -> list[dict]:
        return _mcp_request(self._url, self._token, "tools/list", {})["tools"]

    def call(self, name: str, arguments: dict | None = None, *, poll: bool = True, timeout: float = 300.0) -> dict:
        payload = _tool_payload(
            _mcp_request(self._url, self._token, "tools/call", {"name": name, "arguments": arguments or {}})
        )
        if poll and payload.get("status") == "submitted" and payload.get("run_id"):
            return self.wait(payload["run_id"], timeout=timeout)
        return payload

    def wait(self, run_id: int, *, timeout: float = 300.0, interval: float = 4.0) -> dict:
        deadline = time.monotonic() + timeout
        last = "unknown"
        while True:
            status = _tool_payload(
                _mcp_request(
                    self._url, self._token, "tools/call", {"name": "get_run_result", "arguments": {"run_id": run_id}}
                )
            )
            last = status.get("status", "unknown")
            if last == "completed":
                return status.get("result", {})
            if last == "failed":
                raise AssertionError(f"run {run_id} failed: {status.get('error')}")
            if time.monotonic() >= deadline:
                raise AssertionError(f"run {run_id} not finished within {timeout:.0f}s (last status={last})")
            time.sleep(interval)


@pytest.fixture
def mcp(workspace_auth, deployed_mcp) -> McpClient:
    """An McpClient bound to the deployed app and the calling user's bearer token."""
    _host, token = workspace_auth
    return McpClient(deployed_mcp["url"], token)


# --- Sample "dirty customers" dataset + ODCS contract, shared across the tool tests ----------

# Mirrors the docs' "Try it with sample data": 10 rows with deliberate quality issues — a null
# id, a duplicate id (3), a null name, an invalid email, two out-of-range ages, a null country,
# and a negative amount.
_CUSTOMERS_ROWS = """
 (1,    'Alice',   'alice@example.com',   34,  'US', DATE'2023-01-15', 120.50),
 (2,    'Bob',     'bob@example.com',     41,  'UK', DATE'2023-02-20',  88.00),
 (3,    'Charlie', 'charlie@example.com', 29,  'DE', DATE'2023-03-10',  45.25),
 (4,    NULL,      'dora@example.com',    52,  'US', DATE'2023-04-01', 200.00),
 (5,    'Eve',     'not-an-email',        38,  'FR', DATE'2023-05-05',  60.00),
 (7,    'Grace',   'grace@example.com',   -3,  'IN', DATE'2023-07-08',  30.00),
 (8,    'Heidi',   'heidi@example.com',   210, 'US', DATE'2023-08-19',  95.00),
 (9,    'Ivan',    'ivan@example.com',    33,  NULL, DATE'2023-09-22', -15.00),
 (3,    'Charlie', 'charlie@example.com', 29,  'DE', DATE'2023-03-10',  45.25),
 (NULL, 'Peggy',   'peggy@example.com',   39,  'US', DATE'2024-01-05', 180.00)
"""

_CONTRACT_YAML = """\
kind: DataContract
apiVersion: v3.0.2
id: urn:datacontract:dqx_mcp_it:customers
name: Customers Data Quality Contract
version: 1.0.0
status: active
description:
  purpose: DQX MCP integration-test contract for the customers table.
schema:
  - name: customers
    physicalName: customers
    physicalType: table
    properties:
      - name: customer_id
        logicalType: integer
        physicalType: INT
        required: true
        unique: true
        primaryKey: true
      - name: email
        logicalType: string
        physicalType: STRING
        required: true
        logicalTypeOptions:
          pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
      - name: age
        logicalType: integer
        physicalType: INT
        required: true
        logicalTypeOptions:
          minimum: 0
          maximum: 120
      - name: amount
        logicalType: number
        physicalType: DOUBLE
        required: true
        logicalTypeOptions:
          minimum: 0.0
"""


def _resolve_warehouse_id(client: WorkspaceClient) -> str:
    env_wh = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if env_wh:
        return env_wh
    warehouses = list(client.warehouses.list())
    if not warehouses:
        pytest.skip("no SQL warehouse available to seed the demo dataset")
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    warehouse_id = (running[0] if running else warehouses[0]).id
    assert warehouse_id, "resolved warehouse has no id"
    return warehouse_id


@pytest.fixture(scope="session")
def demo_data(workspace_auth, deployed_mcp) -> Iterator[dict[str, str]]:
    """Seed the dirty 'customers' table + an ODCS contract once for the whole session.

    Creates a throwaway schema under ``CATALOG``, the sample table, and a workspace-file contract;
    grants the app service principal write access on the schema (so ``save_checks`` /
    ``apply_checks_and_save_to_table``, which run as the SP, can write); and drops everything on
    teardown. Yields the fully-qualified names the tool tests use.
    """
    host, token = workspace_auth
    client = WorkspaceClient(host=host, token=token)
    warehouse_id = _resolve_warehouse_id(client)
    schema = f"dqx_mcp_it_{uuid4().hex[:8]}"
    fq_schema = f"{CATALOG}.{schema}"
    table = f"{fq_schema}.customers"
    app_sp = deployed_mcp["service_principal"]

    def run_sql(statement: str) -> None:
        resp = client.statement_execution.execute_statement(
            statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
        if state != "SUCCEEDED":
            err = resp.status.error if resp.status else None
            raise RuntimeError(f"SQL {state}: {err} :: {statement[:120]}")

    run_sql(f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")
    run_sql(
        f"CREATE TABLE {table} (customer_id INT, name STRING, email STRING, age INT, "
        f"country STRING, signup_date DATE, amount DOUBLE)"
    )
    run_sql(f"INSERT INTO {table} VALUES {_CUSTOMERS_ROWS.strip()}")
    if app_sp:
        # The runner job writes as the app SP, so it needs write access on this schema for
        # save_checks / apply_checks_and_save_to_table (see the docs' "Write access" note).
        run_sql(f"GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {fq_schema} TO `{app_sp}`")

    # The contract is read by the runner job (as the app SP), so it must live somewhere the SP
    # can read — a UC volume in the same schema, with READ VOLUME granted to the SP. A workspace
    # file under the user's home would not be readable by the SP.
    run_sql(f"CREATE VOLUME IF NOT EXISTS {fq_schema}.contracts")
    if app_sp:
        run_sql(f"GRANT READ VOLUME ON VOLUME {fq_schema}.contracts TO `{app_sp}`")
    contract_path = f"/Volumes/{CATALOG}/{schema}/contracts/customers_contract.yaml"
    client.files.upload(contract_path, io.BytesIO(_CONTRACT_YAML.encode()), overwrite=True)

    try:
        yield {
            "table": table,
            "schema": fq_schema,
            "contract": contract_path,
            "checks_table": f"{fq_schema}.customers_checks",
            "clean_table": f"{fq_schema}.customers_clean",
            "quarantine_table": f"{fq_schema}.customers_quarantine",
            "service_principal": app_sp,
        }
    finally:
        try:
            run_sql(f"DROP SCHEMA IF EXISTS {fq_schema} CASCADE")
        except Exception:  # noqa: BLE001 — best-effort cleanup, never fail the session on teardown
            sys.stderr.write(f"warning: failed to drop schema {fq_schema}\n")
