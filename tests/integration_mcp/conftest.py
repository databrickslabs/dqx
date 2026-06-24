"""Fixtures for the DQX MCP server integration tests.

These run via the same acceptance harness as the other integration suites, so they reuse
its workspace, authentication, the shared ``TEST_CATALOG``, and ``make_schema`` (create +
automatic teardown). A ``deployed_mcp`` fixture stands up an isolated MCP app from the bundle
and tears it down. Auth is resolved by the SDK from the ambient credentials (see
``workspace_auth``) and handed to the deploy/teardown scripts and HTTP calls, so no
DATABRICKS_TOKEN needs to be set in the environment — it works under the acceptance action's
OIDC auth and under a local profile alike.
"""

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
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


@pytest.fixture(scope="session")
def deployed_mcp(workspace_auth) -> Iterator[str]:
    """Deploy ONE isolated MCP app for the whole test session and tear it down at the end.

    Session-scoped so the (slow) app deploy happens once and is shared across all tests, rather
    than redeploying per test. Teardown runs even if a test fails. If the workspace cannot host a
    Databricks App, the deploy fails and the suite is skipped with the underlying error rather
    than reporting a false failure.
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
        # Clean up any partially-created resources before skipping (the yield/finally below
        # never runs if we skip here).
        subprocess.run(["bash", str(_MCP_SCRIPTS / "ci_destroy.sh")], env=env, check=False)
        pytest.skip(f"MCP app deploy failed (workspace may not support Databricks Apps): {exc.stderr[-1000:]}")

    url = next(
        (
            line.split("=", 1)[1].strip()
            for line in result.stdout.splitlines()
            if line.startswith("DQX_MCP_SERVER_URL=")
        ),
        "",
    )
    assert url, "ci_deploy.sh did not emit DQX_MCP_SERVER_URL"
    try:
        yield url
    finally:
        subprocess.run(["bash", str(_MCP_SCRIPTS / "ci_destroy.sh")], env=env, check=False)
