"""Helpers for the DQX MCP server integration test.

The acceptance harness runs each test in its **own** pytest session (xdist workers on the first
pass, per-test re-runs on retry), so a ``scope="session"`` "deploy once" fixture is re-run per
test — deploying the app many times and colliding on the shared bundle state path. Following the
repo's e2e bundle test (``test_run_dqx_demo_asset_bundle``), the integration test instead OWNS its
deploy + teardown through the context managers below, so a single test == a single deploy.

Auth is resolved by the SDK from the ambient credentials (see ``workspace_auth``) and handed to
the deploy/teardown scripts and HTTP calls, so no DATABRICKS_TOKEN needs to be set in the env —
it works under the acceptance action's OIDC auth and under a local profile alike.
"""

import base64
import contextlib
import io
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import uuid4

import coverage
import pytest
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from tests.constants import TEST_CATALOG

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MCP_SCRIPTS = _REPO_ROOT / "mcp-server" / "scripts"

# Reuse the same Model Serving endpoint the anomaly AI-explanation tests use.
AI_QUERY_ENDPOINT = os.environ.get("DQX_AI_QUERY_TEST_ENDPOINT", "databricks-claude-sonnet-4-5")

# Catalog the test creates/drops schemas + temp views in. Defaults to the shared TEST_CATALOG
# (what CI uses); override with DQX_MCP_TEST_CATALOG to run locally against a workspace that
# uses a different catalog.
CATALOG = os.environ.get("DQX_MCP_TEST_CATALOG") or TEST_CATALOG

# The runner job's run_as SP — an *explicit override* only. For a real deploy, set this to a
# dedicated least-privilege SP. When empty (the CI default), ci_deploy.sh resolves run_as to the
# *deploying identity* via `databricks current-user me`, so run_as == the job's creator and no
# servicePrincipal.user grant is needed (the same pattern as the demo asset-bundle test and the
# SDK-created integration jobs). Databricks only lets you bind a *different* SP as run_as if the
# deployer holds servicePrincipal.user on it — which is why hard-coding TOOLS_CLIENT_ID 403'd.
RUNNER_SP = os.environ.get("DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID", "")


def _bearer_from(config) -> str:
    """Mint a fresh bearer from an SDK config (refreshes OAuth/metadata tokens as needed)."""
    token = (config.authenticate() or {}).get("Authorization", "").removeprefix("Bearer ").strip()
    assert token, "could not obtain a workspace bearer token from the SDK config"
    return token


def _databricks_oauth_m2m_token(host: str, client_id: str, client_secret: str) -> str:
    """Mint a **Databricks** OAuth (M2M) access token straight from the workspace OIDC token endpoint.

    Uses ``{host}/oidc/v1/token`` (client-credentials, scope ``all-apis``) directly rather than the
    SDK's ``auth_type`` routing: on Azure workspaces the SDK's oauth-m2m gets pulled toward Azure AD
    by the ambient ARM_* env and sends ``all-apis`` to Azure's endpoint (AADSTS1002012 — Azure wants
    ``<resource>/.default``), which would also yield an AAD token the Apps front-door rejects. Hitting
    the Databricks OIDC endpoint directly guarantees a Databricks-issued token, which the front-door
    accepts. Raises for a bad response so credential problems fail fast.
    """
    resp = requests.post(
        f"{host}/oidc/v1/token",
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(client_id, client_secret),
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token", "")
    assert token, f"no access_token in the OAuth response from {host}/oidc/v1/token"
    return token


@pytest.fixture(scope="session")
def workspace_auth() -> "tuple[str, Callable[[], str]]":
    """(host, get_token) resolved by the SDK from the ambient auth — the *control-plane* bearer.

    Uses the SDK's default config resolution — the same auth the harness provides:
    acceptance-action OIDC env in CI, a profile or env vars locally. ``get_token()`` mints a
    **fresh** bearer on each call via ``config.authenticate()`` (the SDK refreshes OAuth /
    metadata-service tokens), so the single long end-to-end test never reuses an expired token.
    No DATABRICKS_TOKEN needs to be set anywhere. This bearer is for the CLI deploy and for the
    Model Serving endpoint (both on the workspace host). The app's /mcp front-door needs a
    different bearer — see ``app_auth``.
    """
    config = WorkspaceClient().config
    _bearer_from(config)  # fail fast if auth is broken
    return config.host.rstrip("/"), lambda: _bearer_from(config)


@pytest.fixture(scope="session")
def app_auth(workspace_auth: "tuple[str, Callable[[], str]]") -> "Callable[[], str]":
    """``get_token`` for the app's /mcp front-door, which only accepts OAuth tokens.

    The Databricks Apps front-door honors **Databricks OAuth** tokens (user U2M, or service-principal
    M2M-with-secret minted at ``{host}/oidc/v1/token``) and rejects the acceptance harness's ambient
    token with 401 — that ambient token is an **Azure AD** token (``iss=sts.windows.net``,
    ``aud=2ff814a6-…`` = the AzureDatabricks API resource), which is valid for the control-plane REST
    APIs but NOT for the Apps front-door. This is a token-*type* limitation, not a permission one
    (confirmed by the 401 RCA in ``wait_until_ready``).

    So authenticate to the front-door via OAuth **M2M**: an SP client id + a **Databricks OAuth**
    secret (``auth_type="oauth-m2m"`` uses ``{host}/oidc/v1/token`` → a Databricks token the
    front-door accepts). Prefer the MCP-specific env (``DQX_MCP_APP_CLIENT_ID`` /
    ``DQX_MCP_APP_CLIENT_SECRET``); in CI fall back to the acceptance vault's ``TOOLS_CLIENT_ID`` +
    **``TOOLS_DATABRICKS_SECRET``** (the SP's *Databricks OAuth* secret — NOT ``TOOLS_CLIENT_SECRET``,
    which is the Azure AD secret that yields the rejected token type). When none are set, fall back to
    the ambient auth — a local ``databricks auth login`` OAuth profile is itself accepted.
    """
    host, ambient_get_token = workspace_auth
    client_id = os.environ.get("DQX_MCP_APP_CLIENT_ID") or os.environ.get("TOOLS_CLIENT_ID")
    client_secret = os.environ.get("DQX_MCP_APP_CLIENT_SECRET") or os.environ.get("TOOLS_DATABRICKS_SECRET")
    if not (client_id and client_secret):
        return ambient_get_token  # local OAuth profile (front-door accepts it)
    _databricks_oauth_m2m_token(host, client_id, client_secret)  # fail fast if the M2M credentials are bad
    return lambda: _databricks_oauth_m2m_token(host, client_id, client_secret)


def _script_env(name_prefix: str, host: str, token: str) -> dict[str, str]:
    return {
        **os.environ,
        "NAME_PREFIX": name_prefix,
        "DQX_MCP_TEST_CATALOG": CATALOG,
        # Pass through the explicit override (empty in CI, where ci_deploy.sh defaults run_as to the
        # deploying identity). Local runs set this to a real dedicated runner SP.
        "DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID": RUNNER_SP,
        "DATABRICKS_HOST": host,
        "DATABRICKS_TOKEN": token,
    }


def _emitted(stdout: str, key: str) -> str:
    """Return the value of the last ``KEY=VALUE`` line ci_deploy.sh printed for ``key``."""
    return next(
        (line.split("=", 1)[1].strip() for line in reversed(stdout.splitlines()) if line.startswith(f"{key}=")),
        "",
    )


# Mask anything credential-shaped before it can reach a log/skip message: a JWT (``eyJ…`.`.`…``),
# a ``Bearer <x>`` header, or any long opaque run (40+ chars) that could be a token/secret. UUID
# identifiers (36 chars) and OAuth scope words fall below the threshold, so RCA claims survive.
_TOKENISH = re.compile(r"eyJ[\w-]+\.[\w-]+\.[\w-]+|Bearer\s+\S+|[A-Za-z0-9_\-]{40,}", re.IGNORECASE)


def _redact(text: str) -> str:
    """Scrub token/secret-shaped substrings from server-controlled text before logging it."""
    return _TOKENISH.sub("[REDACTED]", text or "")


def _decode_jwt_claims(token: str) -> dict:
    """Best-effort decode of a bearer for 401 RCA — header alg + a few non-sensitive claims, with NO
    signature check and NEVER the raw token/signature. An opaque (non-JWT) token is itself the tell:
    the Databricks Apps front-door wants an OAuth JWT, not a metadata/PAT-style token.
    """
    parts = token.split(".")
    if len(parts) != 3:
        return {"format": "opaque (non-JWT) — a metadata/PAT-style token, not an OAuth JWT"}

    def _seg(seg: str) -> dict:
        try:
            return json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
        except ValueError:  # covers binascii.Error + json.JSONDecodeError + UnicodeDecodeError
            return {}

    header, payload = _seg(parts[0]), _seg(parts[1])
    claims = {k: payload[k] for k in ("iss", "aud", "sub", "azp", "scope", "scp", "tid", "client_id") if k in payload}
    return {"format": "jwt", "alg": header.get("alg"), "claims": claims}


@contextlib.contextmanager
def deploy_mcp_app(host: str, get_token: Callable[[], str]) -> Iterator[dict[str, str]]:
    """Deploy ONE isolated MCP app, yield {url, service_principal}, and tear it down.

    A context manager (not a fixture) so a single test owns exactly one deploy — the acceptance
    harness's per-test sessions defeat session-scoped fixtures (see the module docstring).
    Teardown always runs. ``service_principal`` is the app SP's application id (the identity the
    runner job runs as; the test grants it write access for the persisting tools).

    A fresh bearer is minted (``get_token()``) for both deploy and teardown — teardown runs after
    the long test, so a token minted at deploy time would be expired by then.
    """
    name_prefix = f"mcp-dqx-it-{uuid4().hex[:6]}"
    # Recorded BEFORE the deploy so every data file this run produces is at or after it. Coverage
    # collection uses it to tell this run's files from earlier runs' leftovers on the shared volume.
    started_at = time.time()

    def env() -> dict[str, str]:
        return _script_env(name_prefix, host, get_token())

    try:
        result = subprocess.run(
            ["bash", str(_MCP_SCRIPTS / "ci_deploy.sh")], env=env(), capture_output=True, text=True, check=True
        )
    except subprocess.CalledProcessError as exc:
        # Surface the FULL deploy output so the real failure is visible (the CLI's errors land on
        # stdout; stderr often only carries a benign warning), then clean up partial resources.
        sys.stderr.write(f"\n===== ci_deploy.sh STDOUT =====\n{exc.stdout}\n")
        sys.stderr.write(f"===== ci_deploy.sh STDERR =====\n{exc.stderr}\n")
        subprocess.run(["bash", str(_MCP_SCRIPTS / "ci_destroy.sh")], env=env(), check=False)
        detail = f"STDOUT(tail):\n{(exc.stdout or '')[-2500:]}\n\nSTDERR(tail):\n{(exc.stderr or '')[-2500:]}"
        raise AssertionError(f"MCP app deploy failed:\n{detail}") from exc

    url = _emitted(result.stdout, "DQX_MCP_SERVER_URL")
    assert url, "ci_deploy.sh did not emit DQX_MCP_SERVER_URL"
    deployment = {
        "url": url,
        "service_principal": _emitted(result.stdout, "DQX_MCP_APP_SERVICE_PRINCIPAL"),
        # The SP the runner job actually runs as (the deploying identity when no override is set).
        # The seed grants it READ on the contract volume so generate_rules_from_contract can read it.
        "runner_service_principal": _emitted(result.stdout, "DQX_MCP_RUNNER_SERVICE_PRINCIPAL"),
        # The deployed app's name — a coverage-enabled run stops it before teardown so its
        # graceful shutdown flushes the final coverage data (see collect_remote_coverage).
        "app_name": _emitted(result.stdout, "DQX_MCP_APP_NAME") or name_prefix,
        "runner_job_id": _emitted(result.stdout, "DQX_MCP_RUNNER_JOB_ID"),
        "workspace_host": _emitted(result.stdout, "DQX_MCP_WORKSPACE_HOST") or host,
        # The deployed temp schema, so a test can plant state the tools cannot reach (e.g. an
        # already-stale temp view for the sweeper).
        "tmp_schema": _emitted(result.stdout, "DQX_MCP_TMP_SCHEMA"),
    }
    _log_deployment(deployment)
    try:
        yield deployment
    finally:
        # Collect coverage BEFORE teardown destroys the app, and do it here rather than at the end of
        # the test body so a FAILING test still yields its data — that is when it is most useful.
        collect_remote_coverage(
            deployment["app_name"],
            started_at,
            deployment.get("tmp_schema") or "",
        )
        subprocess.run(["bash", str(_MCP_SCRIPTS / "ci_destroy.sh")], env=env(), check=False)


def _log_deployment(deployment: dict) -> None:
    """Print the deployed resources' identities and clickable URLs, once, up front.

    The acceptance harness truncates a failing test's traceback to a single line, so without this
    banner a CI failure gives you nothing to investigate with. Everything here is non-sensitive
    (names, ids, URLs — no tokens).
    """
    host = (deployment.get("workspace_host") or "").rstrip("/")
    job_id = deployment.get("runner_job_id") or ""
    lines = [
        "=" * 78,
        "MCP integration deployment",
        f"  app name        : {deployment.get('app_name')}",
        f"  app url         : {deployment.get('url')}",
        f"  app SP          : {deployment.get('service_principal')}",
        f"  runner run_as SP: {deployment.get('runner_service_principal')}",
        f"  runner job id   : {job_id or '(not resolved)'}",
        f"  catalog         : {CATALOG}",
    ]
    if host and job_id:
        lines.append(f"  runner job URL  : {host}/#job/{job_id}")
    if host and deployment.get("app_name"):
        lines.append(f"  app logs        : databricks apps logs {deployment['app_name']}")
    lines.append("=" * 78)
    sys.stderr.write("\n".join(lines) + "\n")
    sys.stderr.flush()


# --- MCP-over-HTTP client used by the integration test ---------------------------------------


def _mcp_request(url: str, token: str, method: str, params: dict, headers: dict | None = None) -> dict:
    """Issue one JSON-RPC call to the app's /mcp endpoint and return the ``result`` payload.

    *headers* overrides/adds request headers. Used to exercise the server's identity handling — the
    Apps front door normally injects the X-Forwarded-* OBO headers, so overriding them is the only
    way a black-box test can present a caller with no identity.
    """
    request_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    request_headers.update(headers or {})
    resp = requests.post(
        f"{url}/mcp",
        headers=request_headers,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=120,
    )
    resp.raise_for_status()
    text = resp.text
    if "data:" in text[:32]:  # FastMCP can answer as an SSE event; unwrap the JSON line
        text = text.split("data:", 1)[1].strip()
    try:
        body = json.loads(text)
    except json.JSONDecodeError as exc:
        # A 2xx with a body we can't parse (commonly an EMPTY body) says nothing on its own, and the
        # acceptance harness shows only the exception's first line — so name the request and quote
        # what actually came back. Redacted: the body is app-controlled, never a credential store.
        raise AssertionError(
            f"MCP response was not JSON for method={method!r} "
            f"params={_summarise_params(params)} status={resp.status_code} "
            f"content-type={resp.headers.get('Content-Type')!r} len={len(resp.text)} "
            f"body[:400]={_redact(resp.text[:400])!r}"
        ) from exc
    if "error" in body:
        raise RuntimeError(f"MCP error for method={method!r} params={_summarise_params(params)}: {body['error']}")
    if "result" not in body:
        raise AssertionError(f"MCP response has no 'result' for method={method!r}: {_redact(str(body)[:400])}")
    return body["result"]


def _summarise_params(params: dict) -> str:
    """Compact, non-sensitive description of a JSON-RPC params dict for error messages."""
    name = params.get("name")
    args = params.get("arguments")
    if name:
        keys = sorted(args) if isinstance(args, dict) else type(args).__name__
        return f"tool={name!r} arg_keys={keys}"
    return f"keys={sorted(params)}"


def _tool_payload(call_result: dict) -> dict:
    """Extract a tool's structured return value (always a dict for DQX tools)."""
    if call_result.get("structuredContent"):
        return call_result["structuredContent"]
    content = call_result.get("content") or []
    if not content:
        return call_result
    text = content[0].get("text") if isinstance(content[0], dict) else None
    try:
        return json.loads(text or "")
    except (json.JSONDecodeError, TypeError) as exc:
        # Same rationale as _mcp_request: quote what the tool actually returned. An empty `text`
        # here is the difference between "the tool failed" and "the transport hiccuped".
        raise AssertionError(
            f"tool content was not JSON: type={type(text).__name__} len={len(text or '')} "
            f"text[:400]={_redact(str(text)[:400])!r} keys={sorted(call_result)}"
        ) from exc


class McpClient:
    """Thin MCP client for tests: calls tools and resolves the async submit→poll pattern.

    Tools that submit a job return ``{"status": "submitted", "run_id": ...}``; ``call`` polls
    ``get_run_result`` until the run is terminal and returns the inner ``result``. Tools that
    answer in-process (e.g. ``get_table_schema``, ``get_workflow``) return their payload directly
    — ``call`` handles both.
    """

    def __init__(self, url: str, get_token: Callable[[], str], job_id: str = "", workspace_host: str = ""):
        self._url = url
        self._get_token = get_token  # mint a fresh bearer per request (no expiry over a long run)
        # Used only to build clickable job/run URLs when something fails — never for auth.
        self._job_id = job_id
        self._workspace_host = (workspace_host or os.environ.get("DATABRICKS_HOST", "")).rstrip("/")

    @property
    def url(self) -> str:
        return self._url

    def current_token(self) -> str:
        """Mint the bearer this client sends — for 401 diagnostics only."""
        return self._get_token()

    def list_tools(self) -> list[dict]:
        return _mcp_request(self._url, self._get_token(), "tools/list", {})["tools"]

    def call(self, name: str, arguments: dict | None = None, *, poll: bool = True, timeout: float = 300.0) -> dict:
        payload = _tool_payload(
            _mcp_request(self._url, self._get_token(), "tools/call", {"name": name, "arguments": arguments or {}})
        )
        if poll and payload.get("status") == "submitted" and payload.get("run_id"):
            return self.wait(payload["run_id"], timeout=timeout)
        return payload

    def wait(self, run_id: int, *, timeout: float = 300.0, interval: float = 0.0) -> dict:
        """Poll get_run_result until the run is terminal.

        *interval* defaults to 0 because the **server** now blocks for up to ~30s inside
        get_run_result (an agent has no sleep primitive, so the wait had to move server-side — see
        utils.get_run_status). Sleeping here as well would stack a client delay on top of a server
        delay that already returned as soon as the run finished, adding dead time to every one of the
        suite's ~33 polled calls. That is not hypothetical: it pushed a run that took 1773s of an
        1800s budget over the limit.
        """
        deadline = time.monotonic() + timeout
        last = "unknown"
        while True:
            status = _tool_payload(
                _mcp_request(
                    self._url,
                    self._get_token(),
                    "tools/call",
                    {"name": "get_run_result", "arguments": {"run_id": run_id}},
                )
            )
            last = status.get("status", "unknown")
            if last == "completed":
                return status.get("result", {})
            if last == "failed":
                # The acceptance harness truncates a failing test to ONE line, and the server's
                # error begins with a generic "Job failed: Task ... Workload failed, see run output
                # for details." prefix — so the runner's actual message, which is appended after it,
                # is exactly what gets cut off. Lead with the tail (where the real cause lives) and
                # keep the run URL, so a CI failure is diagnosable without workspace access.
                error = str(status.get("error") or "")
                cause = error.rsplit("details.", 1)[-1].strip() or error
                raise AssertionError(
                    f"run {run_id} failed: {cause[:400]}{self._run_hint(run_id)} | full: {error[:300]}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"run {run_id} not finished within {timeout:.0f}s (last status={last}){self._run_hint(run_id)}"
                )
            if interval:
                time.sleep(interval)

    def _run_hint(self, run_id: int) -> str:
        """A clickable run URL, so a failed run is one click from its driver logs."""
        if not (self._workspace_host and self._job_id):
            return ""
        return f"\n  run URL: {self._workspace_host}/#job/{self._job_id}/run/{run_id}"


def collect_remote_coverage(app_name: str, since: float, tmp_schema: str = "") -> list[str]:
    """CI/test-only: stop the app (forcing its final flush) and download this run's data files.

    No-op unless DQX_MCP_COVERAGE_DIR is set. On a coverage-enabled deploy the app and every runner
    job carry a test-only bootstrap wheel whose ``.pth`` traces from interpreter start and writes
    ``.coverage.*`` files into ``<results_volume>/coverage`` (see
    tests/integration_mcp/coverage_bootstrap). Stopping the app sends SIGTERM, so uvicorn drains and
    the interpreter exits normally, running the bootstrap's atexit hook that saves + uploads the
    app's complete data; a checkpoint thread bounds the loss if the platform's ~15s budget is
    exceeded. Files land in the REPO ROOT, which is where ``coverage combine`` must run (a [paths]
    rule whose rewritten target does not exist on disk is silently skipped). Best-effort
    throughout: coverage collection must never fail the test run.

    *since* is the epoch second this deployment began: only files written at or after it belong to
    this run, so the merged report describes THIS code and no other. The volume outlives any single
    deployment, so without that bound `coverage combine` silently merges every prior run's data —
    which is how a report ends up citing lines that the current source no longer has.

    """
    if not os.environ.get("DQX_MCP_COVERAGE_DIR"):
        return []
    ws = WorkspaceClient()
    try:
        ws.apps.stop_and_wait(app_name)
        sys.stderr.write(f"coverage: app {app_name} stopped (final flush)\n")
    except Exception as exc:  # noqa: BLE001 — best-effort: never fail the run over coverage
        sys.stderr.write(f"coverage: app stop failed (non-fatal): {exc}\n")
    downloaded: list[str] = []
    coverage_dir = _coverage_dir(tmp_schema)
    # This run owns the directory exclusively only when the path is the per-run derived one (a fresh
    # uuid schema, so no concurrent run shares it). An explicit DQX_MCP_COVERAGE_DIR can point several
    # runs at one directory, and then deleting is unsafe — see _download_coverage_files.
    explicit = os.environ.get("DQX_MCP_COVERAGE_DIR", "").rstrip("/")
    owns_dir = not explicit or explicit == "1"
    try:
        downloaded = _download_coverage_files(ws, coverage_dir, since, owns_dir=owns_dir)
    except Exception as exc:  # noqa: BLE001 — best-effort: never fail the run over coverage
        sys.stderr.write(f"coverage download failed (non-fatal): {exc}\n")
    # Reporting and the readability check live in one place, so "files arrived" and "usable coverage
    # arrived" cannot drift apart. Return only what survived: the discarded files no longer exist.
    return _log_collected_coverage(downloaded, coverage_dir)


def _log_collected_coverage(downloaded: list[str], coverage_dir: str) -> list[str]:
    """Report what arrived, drop any data file the merge step could not read, and return the rest.

    Coverage is keyed to the *interpreter*, not to a job run: one cumulative file per interpreter,
    rewritten as a superset on every flush. So there is no per-run file to account for, and counting
    files against submitted runs would always look short and mean nothing.

    The unreadable-file check matters more than it looks. ``coverage combine`` raises ``DataError``
    on a single corrupt member, and the CI merge step runs it under ``|| true`` — so one truncated
    file would silently zero the ENTIRE report while every job stayed green. A process SIGKILLed
    mid-upload can leave exactly that, and the Files API has no rename to publish atomically, so it
    cannot be prevented at the writing end. Dropping the bad file here costs one interpreter's data
    instead of all of it.
    """
    # No second filter: _download_coverage_files already admits only names starting with
    # '.coverage', so everything here is a data file.
    usable, unusable = _partition_readable(downloaded)
    for path in unusable:
        sys.stderr.write(f"coverage: WARNING discarding unreadable data file {Path(path).name}\n")
        with contextlib.suppress(OSError):
            Path(path).unlink()
    sys.stderr.write(f"coverage: {len(usable)} usable data file(s) from {coverage_dir}\n")
    if not usable:
        # Loud, and naming the directory. A silent empty download is how a wrong coverage path went
        # unnoticed once already: the workflow logged "no remote coverage data", skipped the upload,
        # and every job still went green.
        sys.stderr.write(
            f"coverage: WARNING 0 usable data files from {coverage_dir} — the mcp flag will not be "
            "uploaded. Check the app/runner installed the bootstrap wheel (dev-coverage target) and "
            "that this path matches the deployed tmp schema. The bootstrap logs its own progress to "
            "the app/job output under the 'dqx-coverage' logger.\n"
        )
    return usable


def _partition_readable(data_files: list[str]) -> tuple[list[str], list[str]]:
    """Split *data_files* into those ``coverage`` can read and those it cannot."""
    usable: list[str] = []
    unusable: list[str] = []
    for path in data_files:
        try:
            coverage.CoverageData(basename=path).read()
        except Exception:  # noqa: BLE001 — any unreadable file is one to drop, whatever the cause
            unusable.append(path)
        else:
            usable.append(path)
    return usable, unusable


def _coverage_dir(tmp_schema: str = "") -> str:
    """UC-volume directory the remote data files are written to.

    Mirrors the bootstrap's own derivation: ``<results_volume>/coverage``. DQX_MCP_COVERAGE_DIR may
    name that directory directly (it is forwarded to the app/runner as an explicit override).

    *tmp_schema* must be the schema the deploy actually used, which ci_deploy.sh derives per run from
    NAME_PREFIX and reports back. Defaulting to the bundle's ``dqx_mcp_tmp`` would look in a schema
    that does not exist for a CI deploy, and the only symptom is an empty download — "no remote
    coverage data" and a silently missing upload.
    """
    explicit = os.environ.get("DQX_MCP_COVERAGE_DIR", "").rstrip("/")
    if explicit and explicit != "1":
        return explicit
    schema = tmp_schema or "dqx_mcp_tmp"
    return f"/Volumes/{CATALOG}/{schema}/mcp_results/coverage"


# Cut-off for treating a data file as belonging to an earlier run. `since` is the deploy's start on
# the test runner's clock, while last_modified is stamped by the control plane, so a tolerance absorbs
# any NTP disagreement. It is generous on purpose: the errors are asymmetric — keeping a stale file
# only inflates the report slightly, whereas discarding a live one silently understates coverage and
# cannot be detected afterwards.
_COVERAGE_CLOCK_SKEW_TOLERANCE_SECONDS = 3600


def _download_coverage_files(ws: WorkspaceClient, coverage_dir: str, since: float, owns_dir: bool = True) -> list[str]:
    """Download this run's ``.coverage*`` data files from the UC volume dir into the repo root.

    Under the normal layout each run has this directory to itself: it lives under the per-run
    ``tmp_schema`` (``<name_prefix>_tmp``, and ``name_prefix`` carries a fresh uuid), so concurrent CI
    runs do not share it. Two cases still put a foreign file here — an explicit ``DQX_MCP_COVERAGE_DIR``
    pointing several runs at one directory, and a re-run against a surviving schema whose previous
    ``bundle destroy`` did not complete.

    Age decides what NOT to download (an earlier run's leftovers must stay out of this run's merged
    report). Deletion is the delicate part: a file newer than ``since`` is not necessarily ours — in
    the shared-directory case it can be a *concurrent* run's file that is still being checkpointed,
    and deleting it silently loses the coverage this mechanism exists to collect. So we delete only
    when *owns_dir* — this run has the directory to itself (the per-run derived path, not an explicit
    shared ``DQX_MCP_COVERAGE_DIR``). When the directory is shared, downloaded files are left in place
    for their owning run and for ``bundle destroy`` to remove with the volume.
    """
    downloaded: list[str] = []
    skipped = 0
    cutoff = since - _COVERAGE_CLOCK_SKEW_TOLERANCE_SECONDS
    for entry in ws.files.list_directory_contents(coverage_dir):
        path = entry.path or ""
        name = path.rsplit("/", 1)[-1]
        if not path or not name.startswith(".coverage"):
            continue
        # last_modified is epoch MILLIS on this API (files.get_metadata returns an HTTP date string
        # instead — do not mix them). An absent or non-numeric value is treated as current, so
        # missing metadata can never cost this run its data.
        last_modified = getattr(entry, "last_modified", None)
        if isinstance(last_modified, (int, float)) and last_modified / 1000 < cutoff:
            skipped += 1
            continue
        body = ws.files.download(path).contents
        payload = body.read() if body is not None else b""
        if not payload:
            # Never write a zero-byte .coverage.* file: it is not a valid coverage database, and the
            # combine step would have to cope with it. Leave the remote file alone so the run that owns
            # it can still flush; an empty body here means nothing has been written yet.
            sys.stderr.write(f"coverage: skipped empty data file {name}\n")
            continue
        dest = _REPO_ROOT / name
        dest.write_bytes(payload)
        downloaded.append(str(dest))
        if not owns_dir:
            # Shared directory: this file may belong to a concurrent run still checkpointing. We hold
            # a copy, so our report is complete; leave the remote file so its owner does not lose it.
            continue
        # Safe to delete now: this process holds the bytes, so nothing is lost, and the volume does
        # not accumulate. A failure here is ignored — the file is merely left for teardown.
        try:
            ws.files.delete(path)
        except Exception:  # noqa: BLE001 — housekeeping only, never a correctness dependency
            pass
    if skipped:
        sys.stderr.write(f"coverage: ignored {skipped} data file(s) from earlier runs (left in place)\n")
    return downloaded


def wait_until_ready(client: McpClient, *, timeout: float = 180.0, interval: float = 5.0) -> None:
    """Poll tools/list until the freshly-deployed app is serving.

    A just-deployed app needs a moment before its server answers; until then /mcp can return
    404/5xx — retry those until it lists tools (or time out with the last error).

    A **401/403**, however, is not a "warming up" state: the app *did* deploy and *did* respond
    (it wasn't refused) — the Databricks Apps front-door rejected the request. Retrying can't change
    that, so skip. The skip surfaces the *actual* response (status, ``WWW-Authenticate`` header,
    body) instead of a guess: a **401** means the token itself was not accepted (authentication) —
    e.g. the acceptance harness's metadata-service token, which the front-door does not honor; only
    OAuth tokens (user U2M, or SP M2M-with-secret) with the app's ``user_api_scopes`` are. A **403**
    would mean the identity authenticated but lacks CAN_USE (authorization). The suite runs fully
    under an OAuth profile locally, and in CI once an OAuth token is provided to the harness.
    """
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while True:
        try:
            client.list_tools()
            return
        except requests.HTTPError as exc:
            resp = exc.response
            status = resp.status_code if resp is not None else None
            if status in {401, 403}:
                www = _redact(resp.headers.get("WWW-Authenticate", "")) if resp is not None else ""
                loc = _redact(resp.headers.get("Location", "")) if resp is not None else ""
                body = _redact((resp.text or "")[:400]) if resp is not None else ""
                # Decodes claims only — never emits the raw token (see _decode_jwt_claims).
                token_info = _decode_jwt_claims(client.current_token())
                kind = (
                    "token not accepted (authentication)" if status == 401 else "identity lacks CAN_USE (authorization)"
                )
                # Single line on purpose: the acceptance harness only surfaces the first line of a
                # skip reason, so the RCA fields must not be newline-separated.
                pytest.skip(
                    f"MCP app front-door returned {status} at {client.url} — {kind}. The app DID "
                    f"deploy and respond (not refused). RCA token={token_info} || "
                    f"WWW-Authenticate={www!r} || Location={loc!r} || body[:400]={body!r} || "
                    "In CI auth comes from the metadata service; the Apps front-door needs an OAuth "
                    "token (U2M or SP M2M-with-secret) — set DQX_MCP_APP_CLIENT_ID / "
                    "DQX_MCP_APP_CLIENT_SECRET (an SP with CAN_USE) to run live; OAuth profile works locally."
                )
            last = exc
            if time.monotonic() >= deadline:
                raise AssertionError(f"MCP app not ready within {timeout:.0f}s: {last}") from last
            time.sleep(interval)
        except (requests.RequestException, RuntimeError) as exc:  # other HTTP errors + MCP error bodies
            last = exc
            if time.monotonic() >= deadline:
                raise AssertionError(f"MCP app not ready within {timeout:.0f}s: {last}") from last
            time.sleep(interval)


# --- Sample "dirty customers" dataset + ODCS contract ----------------------------------------

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

# Public so the test can also pass it as inline `contract_content` (the tool accepts either a
# volume path or the contract text) without reaching into a private name.
CONTRACT_YAML = """\
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
    try:
        warehouses = list(client.warehouses.list())
    except Exception as exc:
        # Self-diagnosing: surface WHICH identity/token shape the SDK actually used (claims only —
        # never the raw token) so an auth failure here is attributable, not a mystery.
        try:
            claims = _decode_jwt_claims(_bearer_from(client.config))
        except Exception as diag:  # noqa: BLE001 — diagnostics must not mask the original error
            claims = {"diag_error": str(diag)}
        sys.stderr.write(
            f"seed: warehouses.list failed: {exc}\n"
            f"seed: SDK auth_type={client.config.auth_type} host={client.config.host}\n"
            f"seed: token RCA={claims}\n"
        )
        raise
    if not warehouses:
        pytest.skip("no SQL warehouse available to seed the demo dataset")
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    warehouse_id = (running[0] if running else warehouses[0]).id
    assert warehouse_id, "resolved warehouse has no id"
    return warehouse_id


def execute_sql(statement: str, *, client: WorkspaceClient | None = None) -> list[list]:
    """Run one SQL statement against a resolved warehouse and return its rows. Raises on failure.

    Module-level so a test can set up or inspect state the tools cannot reach — planting a stale temp
    view for the sweeper, for instance. ``seed_demo_data`` keeps its own closure version because it
    already holds a client and a warehouse id.
    """
    client = client or WorkspaceClient()
    resp = client.statement_execution.execute_statement(
        statement=statement, warehouse_id=_resolve_warehouse_id(client), wait_timeout="50s"
    )
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        err = resp.status.error if resp.status else None
        raise RuntimeError(f"SQL {state}: {err} :: {statement[:160]}")
    return list((resp.result.data_array or []) if resp.result else [])


@contextlib.contextmanager
def seed_demo_data(app_sp: str, runner_sp: str = "") -> Iterator[dict[str, str]]:
    """Seed the dirty 'customers' table + an ODCS contract, then drop everything on exit.

    Creates a throwaway schema under ``CATALOG``, the sample table, and a UC-volume contract.
    The **source table** is read through the caller's OBO definer's-rights view (the caller owns
    this schema, so no SP grant is needed). The **contract** is read by the runner job *as the
    runner SP* (``generate_rules_from_contract`` opens it locally), so the runner SP is granted
    READ VOLUME on the contract volume. Persisting tools (``save_checks`` /
    ``apply_checks_and_save_to_table``) write to the *caller's own per-user schema*
    (``dqx_mcp_<user>``, created + owned by the runner SP) — no grants on this seed schema needed.

    Builds its own ambient ``WorkspaceClient`` (rather than a static-token one) so its SDK calls —
    including the teardown that runs after the long test — refresh the bearer and don't expire.
    """
    client = WorkspaceClient()
    warehouse_id = _resolve_warehouse_id(client)
    # Prefer the SP the deploy actually resolved run_as to; fall back to the explicit override.
    runner_sp = (runner_sp or RUNNER_SP).strip()
    schema = f"dqx_mcp_it_{uuid4().hex[:8]}"
    fq_schema = f"{CATALOG}.{schema}"
    table = f"{fq_schema}.customers"

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

    # The contract is read by the runner job as the runner SP, so it must live somewhere the runner
    # SP can read — a UC volume in the seed schema. Reading a volume file via FUSE open() needs
    # USE SCHEMA on the parent schema *and* READ VOLUME on the volume (USE CATALOG comes from setup).
    run_sql(f"CREATE VOLUME IF NOT EXISTS {fq_schema}.contracts")
    if runner_sp:
        run_sql(f"GRANT USE SCHEMA ON SCHEMA {fq_schema} TO `{runner_sp}`")
        run_sql(f"GRANT READ VOLUME ON VOLUME {fq_schema}.contracts TO `{runner_sp}`")
    contract_path = f"/Volumes/{CATALOG}/{schema}/contracts/customers_contract.yaml"
    client.files.upload(contract_path, io.BytesIO(CONTRACT_YAML.encode()), overwrite=True)

    # The same contract as a *workspace* file. The tools accept either backend and read them
    # differently (Files API download vs workspace export + base64), so the test covers both.
    #
    # It goes under /Shared, NOT the seeding identity's /Users home: the app reads the file with the
    # *caller's* OBO token, and in CI the caller (an OAuth SP) is a different principal from the one
    # that seeds, so it cannot read another identity's home folder ("Path doesn't exist"). Locally
    # both are the same user, which is why a /Users path passed on a dev workspace and failed in CI.
    #
    # ImportFormat.AUTO with a .yml path keeps it a plain file (mirrors tests/conftest.py's
    # make_check_file); without an explicit format the SDK tries to treat the bytes as an archive.
    ws_contract_dir = "/Shared/dqx_mcp_it"
    ws_contract_path = f"{ws_contract_dir}/{schema}_contract.yml"
    client.workspace.mkdirs(ws_contract_dir)
    client.workspace.upload(
        path=ws_contract_path, format=ImportFormat.AUTO, content=CONTRACT_YAML.encode(), overwrite=True
    )

    try:
        yield {
            "table": table,
            "schema": fq_schema,
            "contract": contract_path,
            "workspace_contract": ws_contract_path,
            "service_principal": app_sp,
        }
    finally:
        try:
            run_sql(f"DROP SCHEMA IF EXISTS {fq_schema} CASCADE")
        except Exception:  # best-effort cleanup, never fail the session on teardown
            sys.stderr.write(f"warning: failed to drop schema {fq_schema}\n")
        try:
            client.workspace.delete(ws_contract_path)
        except Exception:  # noqa: BLE001 — best-effort cleanup, never fail the session on teardown
            sys.stderr.write(f"warning: failed to delete {ws_contract_path}\n")
