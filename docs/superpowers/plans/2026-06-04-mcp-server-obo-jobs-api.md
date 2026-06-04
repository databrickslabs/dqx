# MCP Server OBO via Jobs API — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Databricks Connect with Jobs API notebook submissions so the MCP server works with OBO tokens in Databricks Apps.

**Architecture:** MCP tools that need Spark submit a serverless notebook job via the OBO `WorkspaceClient`. A dispatcher notebook receives the operation name + JSON params, imports DQX, runs the operation with native `spark`, and returns results via `dbutils.notebook.exit()`. Tools that don't need Spark (`list_available_checks`, `validate_checks`, `get_workflow`) stay in-process.

**Tech Stack:** databricks-sdk (Jobs API), FastMCP, Starlette, DQX

**Spec:** `docs/superpowers/specs/2026-06-04-mcp-server-obo-jobs-api-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `mcp-server/notebooks/runner.py` | Dispatcher notebook — receives operation + params, runs DQX with native spark, returns JSON |
| Modify | `mcp-server/server/utils.py` | Remove Connect code, add `submit_notebook_job()` |
| Modify | `mcp-server/server/tools.py` | Thin wrappers calling `submit_notebook_job()` for Spark tools |
| Modify | `mcp-server/databricks.yml` | Deploy notebook, add `clusters` scope |
| Modify | `mcp-server/requirements.txt` | Remove `databricks-connect` |
| Modify | `mcp-server/pyproject.toml` | Remove `databricks-connect` from dependencies |
| Create | `mcp-server/tests/test_utils.py` | Unit tests for `submit_notebook_job()` |
| Create | `mcp-server/tests/test_tools.py` | Unit tests for refactored tools |
| Create | `mcp-server/tests/test_runner.py` | Unit tests for runner notebook dispatch logic |

---

### Task 1: Create the Runner Notebook

**Files:**
- Create: `mcp-server/notebooks/runner.py`

The notebook is a dispatcher that reads `operation` and `params` widgets, calls the right DQX function, and returns JSON via `dbutils.notebook.exit()`.

- [ ] **Step 1: Create the runner notebook**

Create `mcp-server/notebooks/runner.py`:

```python
# Databricks notebook source

# COMMAND ----------
# %pip install databricks-labs-dqx

# COMMAND ----------

import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dqx-mcp-runner")

# COMMAND ----------

# Read parameters from notebook widgets
dbutils.widgets.text("operation", "")
dbutils.widgets.text("params", "{}")

operation = dbutils.widgets.get("operation")
params = json.loads(dbutils.widgets.get("params"))

logger.info(f"Running operation: {operation}, params keys: {list(params.keys())}")

# COMMAND ----------

def get_table_schema(params: dict) -> dict:
    """Retrieve schema and row count for a table."""
    table_name = params["table_name"]
    df = spark.table(table_name)
    columns = [
        {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
        for f in df.schema.fields
    ]
    row_count = df.count()
    return {
        "table_name": table_name,
        "columns": columns,
        "row_count": row_count,
    }

# COMMAND ----------

def profile_table(params: dict) -> dict:
    """Profile a table and return summary stats + profiles."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.profiler.profiler import DQProfiler
    from databricks.labs.dqx.config import InputConfig
    from dataclasses import asdict

    table_name = params["table_name"]
    columns = params.get("columns")
    options = params.get("options")

    ws = WorkspaceClient()
    profiler = DQProfiler(workspace_client=ws, spark=spark)
    input_config = InputConfig(location=table_name)
    summary_stats, profiles = profiler.profile_table(input_config, columns=columns, options=options)

    profiles_dicts = [asdict(p) for p in profiles]
    return {
        "table_name": table_name,
        "summary_stats": _make_json_safe(summary_stats),
        "profiles": _make_json_safe(profiles_dicts),
    }

# COMMAND ----------

def generate_rules(params: dict) -> dict:
    """Generate DQX rules from profiling output."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.profiler.profiler import DQProfile
    from databricks.labs.dqx.profiler.generator import DQGenerator

    profiles = params["profiles"]
    criticality = params.get("criticality", "error")

    dq_profiles = [
        DQProfile(
            name=p["name"],
            column=p["column"],
            description=p.get("description"),
            parameters=p.get("parameters"),
            filter=p.get("filter"),
        )
        for p in profiles
    ]

    ws = WorkspaceClient()
    generator = DQGenerator(workspace_client=ws, spark=spark)
    rules = generator.generate_dq_rules(dq_profiles, criticality=criticality)

    return {
        "rules": _make_json_safe(rules),
        "count": len(rules),
    }

# COMMAND ----------

def run_checks(params: dict) -> dict:
    """Run DQX checks against a table."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine
    import pyspark.sql.functions as F

    table_name = params["table_name"]
    checks = params["checks"]
    sample_size = params.get("sample_size", 50)

    ws = WorkspaceClient()
    engine = DQEngine(workspace_client=ws, spark=spark)

    df = spark.table(table_name)
    valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, checks)

    total_rows = df.count()
    valid_rows = valid_df.count()
    invalid_rows = invalid_df.count()

    error_sample_rows = invalid_df.limit(sample_size).collect()
    error_sample = [_make_json_safe(row.asDict(recursive=True)) for row in error_sample_rows]

    # Compute per-rule summary
    rule_summary = _compute_rule_summary(invalid_df)

    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "error_sample": error_sample,
        "rule_summary": rule_summary,
    }

# COMMAND ----------

def _make_json_safe(value):
    """Recursively convert non-JSON-serializable values."""
    import datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_make_json_safe(v) for v in value]
    return value

# COMMAND ----------

def _compute_rule_summary(invalid_df) -> list:
    """Aggregate per-rule error/warning counts."""
    import pyspark.sql.functions as F

    summary = {}
    for col_name in ("_errors", "_warnings"):
        if col_name not in invalid_df.columns:
            continue
        exploded = invalid_df.select(F.explode(F.col(col_name)).alias("item"))
        rows = exploded.groupBy("item.name").count().collect()
        for row in rows:
            rule_name = row["name"] or "unknown"
            if rule_name not in summary:
                summary[rule_name] = {"error_count": 0, "warning_count": 0}
            if col_name == "_errors":
                summary[rule_name]["error_count"] = row["count"]
            else:
                summary[rule_name]["warning_count"] = row["count"]

    return [{"rule_name": name, **counts} for name, counts in summary.items()]

# COMMAND ----------

# Operation dispatch
OPERATIONS = {
    "get_table_schema": get_table_schema,
    "profile_table": profile_table,
    "generate_rules": generate_rules,
    "run_checks": run_checks,
}

# COMMAND ----------

try:
    if operation not in OPERATIONS:
        result = {"error": f"Unknown operation: {operation}. Valid: {list(OPERATIONS.keys())}"}
    else:
        result = OPERATIONS[operation](params)
except Exception as e:
    logger.error(f"Operation '{operation}' failed: {e}", exc_info=True)
    result = {"error": f"{type(e).__name__}: {str(e)}"}

# Check output size before exit (5MB limit)
output_json = json.dumps(result)
if len(output_json) > 4_500_000:  # 4.5MB safety margin
    result = {
        "error": "Output too large for notebook.exit() (>4.5MB). Try reducing sample_size.",
        "truncated": True,
    }
    output_json = json.dumps(result)

logger.info(f"Operation '{operation}' complete. Output size: {len(output_json)} bytes")
dbutils.notebook.exit(output_json)
```

- [ ] **Step 2: Commit**

```bash
git add mcp-server/notebooks/runner.py
git commit -m "feat(mcp): add runner notebook for Jobs API dispatch"
```

---

### Task 2: Add `submit_notebook_job()` to `utils.py`

**Files:**
- Modify: `mcp-server/server/utils.py`
- Create: `mcp-server/tests/test_utils.py`

- [ ] **Step 1: Write failing tests for `submit_notebook_job()`**

Create `mcp-server/tests/test_utils.py`:

```python
import json
from datetime import timedelta
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, RunOutput, NotebookOutput, RunState, RunLifeCycleState, RunResultState


def _make_mock_ws(notebook_output_json: str, run_id: int = 123) -> MagicMock:
    """Create a mock WorkspaceClient that simulates a successful job run."""
    ws = create_autospec(WorkspaceClient)

    # Mock the run object returned by .result()
    run = MagicMock(spec=Run)
    run.run_id = run_id
    run.run_page_url = f"https://workspace.databricks.com/jobs/{run_id}"
    run.state = MagicMock(spec=RunState)
    run.state.life_cycle_state = RunLifeCycleState.TERMINATED
    run.state.result_state = RunResultState.SUCCESS

    # Mock submit() -> Wait -> .result() -> Run
    wait_obj = MagicMock()
    wait_obj.result.return_value = run
    ws.jobs.submit.return_value = wait_obj

    # Mock get_run_output()
    output = MagicMock(spec=RunOutput)
    output.notebook_output = MagicMock(spec=NotebookOutput)
    output.notebook_output.result = notebook_output_json
    output.notebook_output.truncated = False
    # Need to get the task run_id from run.tasks
    task_run = MagicMock()
    task_run.run_id = 456
    run.tasks = [task_run]
    ws.jobs.get_run_output.return_value = output

    return ws


class TestSubmitNotebookJob:
    def test_successful_submission(self):
        from server.utils import submit_notebook_job

        expected_result = {"table_name": "catalog.schema.table", "columns": [], "row_count": 0}
        ws = _make_mock_ws(json.dumps(expected_result))

        with patch("server.utils.get_workspace_client", return_value=ws):
            result = submit_notebook_job("get_table_schema", {"table_name": "catalog.schema.table"})

        assert result == expected_result
        ws.jobs.submit.assert_called_once()

    def test_passes_operation_and_params_to_notebook(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"result": "ok"}))

        with patch("server.utils.get_workspace_client", return_value=ws):
            submit_notebook_job("profile_table", {"table_name": "t", "columns": ["a"]})

        call_kwargs = ws.jobs.submit.call_args.kwargs
        tasks = call_kwargs["tasks"]
        assert len(tasks) == 1
        base_params = tasks[0].notebook_task.base_parameters
        assert base_params["operation"] == "profile_table"
        assert json.loads(base_params["params"]) == {"table_name": "t", "columns": ["a"]}

    def test_job_failure_raises_error(self):
        from server.utils import submit_notebook_job

        ws = create_autospec(WorkspaceClient)

        run = MagicMock(spec=Run)
        run.run_id = 999
        run.run_page_url = "https://workspace.databricks.com/jobs/999"
        run.state = MagicMock(spec=RunState)
        run.state.life_cycle_state = RunLifeCycleState.TERMINATED
        run.state.result_state = RunResultState.FAILED
        run.state.state_message = "Something went wrong"
        run.tasks = [MagicMock(run_id=1000)]

        wait_obj = MagicMock()
        wait_obj.result.return_value = run
        ws.jobs.submit.return_value = wait_obj

        output = MagicMock(spec=RunOutput)
        output.notebook_output = None
        output.error = "Something went wrong"
        ws.jobs.get_run_output.return_value = output

        with patch("server.utils.get_workspace_client", return_value=ws):
            with pytest.raises(RuntimeError, match="DQX job failed"):
                submit_notebook_job("run_checks", {"table_name": "t", "checks": []})

    def test_notebook_returns_error_dict(self):
        from server.utils import submit_notebook_job

        error_result = {"error": "AnalysisException: Table not found"}
        ws = _make_mock_ws(json.dumps(error_result))

        with patch("server.utils.get_workspace_client", return_value=ws):
            result = submit_notebook_job("get_table_schema", {"table_name": "bad.table"})

        assert result == error_result

    def test_uses_configured_notebook_path(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"ok": True}))

        with patch("server.utils.get_workspace_client", return_value=ws), \
             patch.dict("os.environ", {"DQX_RUNNER_NOTEBOOK_PATH": "/custom/path/runner"}):
            submit_notebook_job("get_table_schema", {"table_name": "t"})

        tasks = ws.jobs.submit.call_args.kwargs["tasks"]
        assert tasks[0].notebook_task.notebook_path == "/custom/path/runner"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
python -m pytest tests/test_utils.py -v
```

Expected: FAIL — `submit_notebook_job` does not exist yet.

- [ ] **Step 3: Implement `submit_notebook_job()` in `utils.py`**

Replace the Spark/Connect section of `mcp-server/server/utils.py`. The file should become:

```python
"""
Utility functions for the DQX MCP server.

Key patterns:
- Pure ASGI middleware for OBO (not BaseHTTPMiddleware — avoids streaming timeouts)
- Lazy WorkspaceClient creation per-request using contextvars
- auth_type="pat" to avoid conflict with auto-injected SP env vars
- Jobs API notebook submission for Spark operations (no Databricks Connect)
"""

import contextvars
import json
import logging
import os
from datetime import timedelta
from typing import Any

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

# Default notebook path — matches databricks bundle deploy location
DEFAULT_NOTEBOOK_PATH = "/Workspace/.bundle/mcp-dqx/notebooks/runner"

# ── OBO Auth via contextvars ──────────────────────────────────────────

_user_token_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "user_token", default=None
)

_sp_client = None


class OBOAuthMiddleware:
    """Pure ASGI middleware for on-behalf-of authentication.

    Extracts X-Forwarded-Access-Token from Databricks Apps proxy headers
    and stores it in a context var. WorkspaceClient is created lazily
    only when get_workspace_client() is called.

    Using pure ASGI (not BaseHTTPMiddleware) is critical — BaseHTTPMiddleware
    buffers response bodies which causes MCP streaming timeouts.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        user_token = headers.get(b"x-forwarded-access-token", b"").decode() or None

        if user_token:
            host = os.environ.get("DATABRICKS_HOST", "")
            _user_token_var.set((host, user_token))
        else:
            _user_token_var.set(None)

        await self.app(scope, receive, send)


def get_workspace_client():
    """Get a WorkspaceClient for the current request.

    Returns an OBO client (user's identity) if running in a Databricks App
    with a forwarded token, otherwise falls back to the service principal.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    token_info = _user_token_var.get(None)
    if token_info is not None:
        host, token = token_info
        cfg = Config(host=host, token=token, auth_type="pat")
        return WorkspaceClient(config=cfg)

    global _sp_client
    if _sp_client is None:
        _sp_client = WorkspaceClient()
    return _sp_client


# ── Jobs API notebook submission ─────────────────────────────────────


def submit_notebook_job(operation: str, params: dict[str, Any]) -> dict[str, Any]:
    """Submit a DQX operation as a serverless notebook job and wait for results.

    Uses the OBO WorkspaceClient so the job runs as the logged-in user.
    Falls back to service principal when no OBO token is present.

    Args:
        operation: The DQX operation name (e.g. 'get_table_schema', 'run_checks').
        params: Dict of parameters to pass to the notebook as JSON.

    Returns:
        Parsed JSON dict from dbutils.notebook.exit() output.

    Raises:
        RuntimeError: If the job fails or times out.
    """
    from databricks.sdk.service.jobs import NotebookTask, SubmitTask

    ws = get_workspace_client()
    notebook_path = os.environ.get("DQX_RUNNER_NOTEBOOK_PATH", DEFAULT_NOTEBOOK_PATH)

    logger.info(f"Submitting notebook job: operation={operation}, notebook={notebook_path}")

    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "operation": operation,
            "params": json.dumps(params),
        },
    )

    task = SubmitTask(
        task_key="dqx_run",
        notebook_task=notebook_task,
    )

    wait = ws.jobs.submit(
        run_name=f"mcp-dqx-{operation}",
        tasks=[task],
    )

    run = wait.result(timeout=timedelta(minutes=10))
    logger.info(f"Job completed: run_id={run.run_id}, url={run.run_page_url}")

    # Read notebook output from the task run (not the parent run)
    task_run_id = run.tasks[0].run_id if run.tasks else run.run_id
    output = ws.jobs.get_run_output(task_run_id)

    if output.notebook_output and output.notebook_output.result:
        return json.loads(output.notebook_output.result)

    # Job ran but no notebook output — likely a failure
    error_msg = output.error or "No output from notebook"
    run_url = run.run_page_url or ""
    raise RuntimeError(
        f"DQX job failed (operation={operation}): {error_msg}. Debug at: {run_url}"
    )


# ── JSON serialization helpers ────────────────────────────────────────


def make_json_safe(value: Any) -> Any:
    """Recursively convert values that are not JSON-serializable (e.g. Decimal, datetime)."""
    import datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [make_json_safe(v) for v in value]
    return value
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
python -m pytest tests/test_utils.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add mcp-server/server/utils.py mcp-server/tests/test_utils.py
git commit -m "feat(mcp): replace Databricks Connect with Jobs API submit_notebook_job()"
```

---

### Task 3: Refactor MCP Tools

**Files:**
- Modify: `mcp-server/server/tools.py`
- Create: `mcp-server/tests/test_tools.py`

- [ ] **Step 1: Write failing tests for refactored tools**

Create `mcp-server/tests/test_tools.py`:

```python
import json
from unittest.mock import patch, MagicMock

from fastmcp import FastMCP


def _load_tools():
    """Load tools onto a fresh MCP server and return it."""
    mcp = FastMCP(name="test-dqx")
    from server.tools import load_tools
    load_tools(mcp)
    return mcp


class TestJobBasedTools:
    """Tools that delegate to submit_notebook_job()."""

    @patch("server.utils.submit_notebook_job")
    def test_get_table_schema_delegates_to_job(self, mock_submit):
        mock_submit.return_value = {"table_name": "t", "columns": [], "row_count": 0}

        mcp = _load_tools()
        tool_fn = mcp._tool_manager._tools["get_table_schema"].fn
        result = tool_fn(table_name="catalog.schema.table")

        mock_submit.assert_called_once_with("get_table_schema", {"table_name": "catalog.schema.table"})
        assert result == {"table_name": "t", "columns": [], "row_count": 0}

    @patch("server.utils.submit_notebook_job")
    def test_profile_table_delegates_to_job(self, mock_submit):
        mock_submit.return_value = {"table_name": "t", "summary_stats": {}, "profiles": []}

        mcp = _load_tools()
        tool_fn = mcp._tool_manager._tools["profile_table"].fn
        result = tool_fn(table_name="t", columns=["a"], options={"limit": 100})

        mock_submit.assert_called_once_with("profile_table", {
            "table_name": "t",
            "columns": ["a"],
            "options": {"limit": 100},
        })

    @patch("server.utils.submit_notebook_job")
    def test_generate_rules_delegates_to_job(self, mock_submit):
        profiles = [{"name": "is_not_null", "column": "id"}]
        mock_submit.return_value = {"rules": [], "count": 0}

        mcp = _load_tools()
        tool_fn = mcp._tool_manager._tools["generate_rules"].fn
        result = tool_fn(profiles=profiles, criticality="warn")

        mock_submit.assert_called_once_with("generate_rules", {
            "profiles": profiles,
            "criticality": "warn",
        })

    @patch("server.utils.submit_notebook_job")
    def test_run_checks_delegates_to_job(self, mock_submit):
        checks = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
        mock_submit.return_value = {"total_rows": 100, "valid_rows": 95, "invalid_rows": 5}

        mcp = _load_tools()
        tool_fn = mcp._tool_manager._tools["run_checks"].fn
        result = tool_fn(table_name="t", checks=checks, sample_size=10)

        mock_submit.assert_called_once_with("run_checks", {
            "table_name": "t",
            "checks": checks,
            "sample_size": 10,
        })


class TestInProcessTools:
    """Tools that stay in-process (no job submission)."""

    def test_list_available_checks_returns_checks(self):
        mcp = _load_tools()
        tool_fn = mcp._tool_manager._tools["list_available_checks"].fn
        result = tool_fn()
        assert "checks" in result
        assert "count" in result
        assert result["count"] > 0

    def test_validate_checks_valid(self):
        mcp = _load_tools()
        tool_fn = mcp._tool_manager._tools["validate_checks"].fn
        result = tool_fn(checks=[{
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "criticality": "error",
        }])
        assert result["valid"] is True

    def test_get_workflow_returns_steps(self):
        mcp = _load_tools()
        tool_fn = mcp._tool_manager._tools["get_workflow"].fn
        result = tool_fn()
        assert "steps" in result
        assert len(result["steps"]) == 5
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
python -m pytest tests/test_tools.py -v
```

Expected: FAIL — tools still use old Spark-based approach.

- [ ] **Step 3: Refactor `tools.py`**

Replace `mcp-server/server/tools.py` with:

```python
import inspect
import logging

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
from databricks.labs.dqx.checks_resolver import resolve_check_function

from server import utils

logger = logging.getLogger(__name__)


def load_tools(mcp_server):
    """Register all DQX MCP tools with the server."""

    # ── Job-based tools (submit to notebook) ─────────────────────────

    @mcp_server.tool
    def get_table_schema(table_name: str):
        """Retrieve the schema and basic metadata for a Databricks table.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').

        Returns a dict with:
            - 'table_name': the input table name
            - 'columns': list of {name, type, nullable} for each column
            - 'row_count': approximate number of rows
        """
        logger.info(f"Getting schema for table: {table_name}")
        return utils.submit_notebook_job("get_table_schema", {"table_name": table_name})

    @mcp_server.tool
    def profile_table(
        table_name: str,
        columns: list[str] | None = None,
        options: dict | None = None,
    ):
        """Profile a Databricks table to get summary statistics and auto-generated data quality profiles.

        The profiles describe patterns found in the data (nullability, value ranges, allowed
        values, uniqueness, etc.) and can be fed directly into `generate_rules`.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').
            columns: Optional list of column names to profile. Profiles all columns if omitted.
            options: Optional profiling options. Supported keys include:
                - sample_fraction (float): Fraction of data to sample (default 0.3).
                - limit (int): Max rows to profile (default 1000).
                - max_in_count (int): Max distinct values for is_in rules (default 10).
                - distinct_ratio (float): Threshold for is_in rules (default 0.05).
                - max_null_ratio (float): Threshold for is_not_null rules (default 0.01).
                - remove_outliers (bool): Whether to remove outliers for range rules (default True).
                - trim_strings (bool): Whether to trim strings for empty checks (default True).
                - max_empty_ratio (float): Threshold for is_not_null_or_empty rules (default 0.01).

        Returns a dict with:
            - 'table_name': the input table name
            - 'summary_stats': per-column summary statistics
            - 'profiles': list of profile dicts, each with {name, column, description, parameters}
        """
        logger.info(f"Profiling table: {table_name}, columns={columns}, options={options}")
        return utils.submit_notebook_job("profile_table", {
            "table_name": table_name,
            "columns": columns,
            "options": options,
        })

    @mcp_server.tool
    def generate_rules(profiles: list[dict], criticality: str = "error"):
        """Generate DQX data quality check definitions from profiling output.

        Takes the 'profiles' list returned by `profile_table` and converts them into
        fully formed DQX check definitions that can be validated and executed.

        Args:
            profiles: List of profile dicts from `profile_table`, each with keys
                {name, column, description, parameters, filter}.
            criticality: Default criticality for generated rules: 'error' or 'warn' (default 'error').

        Returns a dict with:
            - 'rules': list of DQX check definitions (metadata format)
            - 'count': number of rules generated
        """
        logger.info(f"Generating rules from {len(profiles)} profiles, criticality={criticality}")
        return utils.submit_notebook_job("generate_rules", {
            "profiles": profiles,
            "criticality": criticality,
        })

    # ── In-process tools (no Spark needed) ───────────────────────────

    @mcp_server.tool
    def validate_checks(checks: list[dict]):
        """Validate a list of DQX data quality check definitions for correctness.

        Each check should be a dictionary with at minimum a 'check' key containing
        'function' (the check function name) and 'arguments' (dict of arguments).

        Returns a dict with 'valid' (bool) and 'errors' (list of error strings).
        """
        logger.info(f"Validating {len(checks)} check(s)")
        status = DQEngine.validate_checks(checks)
        result = {
            "valid": not status.has_errors,
            "errors": status.errors,
        }
        logger.info(f"Validation result: valid={result['valid']}, errors={len(result['errors'])}")
        return result

    @mcp_server.tool
    def run_checks(table_name: str, checks: list[dict], sample_size: int = 50):
        """Execute DQX data quality checks against a Databricks table and return results.

        Applies the given check definitions to the table and returns a summary of
        valid/invalid rows plus a sample of failing rows for inspection.

        Args:
            table_name: Fully qualified table name (e.g. 'catalog.schema.table').
            checks: List of DQX check definitions (metadata format), as returned by
                `generate_rules` or written manually.
            sample_size: Max number of invalid rows to include in the sample (default 50).

        Returns a dict with:
            - 'table_name': the input table name
            - 'total_rows': total number of rows in the table
            - 'valid_rows': number of rows passing all checks
            - 'invalid_rows': number of rows failing at least one check
            - 'error_sample': list of dicts, each representing an invalid row
            - 'rule_summary': per-rule counts of errors and warnings
        """
        logger.info(f"Running {len(checks)} checks on table: {table_name}")
        return utils.submit_notebook_job("run_checks", {
            "table_name": table_name,
            "checks": checks,
            "sample_size": sample_size,
        })

    @mcp_server.tool
    def list_available_checks():
        """List all built-in DQX check functions available for use in rules.

        Returns a dict with:
            - 'checks': list of {name, type, signature, description} for each registered function.
            - 'count': total number of available check functions.
        """
        logger.info("Listing available check functions")
        checks = []
        for name, func_type in sorted(CHECK_FUNC_REGISTRY.items()):
            func = resolve_check_function(name, fail_on_missing=False)
            if func is None:
                continue
            sig = inspect.signature(func)
            params = [
                {"name": p.name, "type": str(p.annotation) if p.annotation != inspect.Parameter.empty else "Any"}
                for p in sig.parameters.values()
            ]
            doc = inspect.getdoc(func) or ""
            first_line = doc.split("\n")[0] if doc else ""
            checks.append(
                {
                    "name": name,
                    "type": func_type,
                    "signature": f"{name}{sig}",
                    "description": first_line,
                    "parameters": params,
                }
            )

        result = {"checks": checks, "count": len(checks)}
        logger.info(f"Found {len(checks)} check functions")
        return result

    @mcp_server.tool
    def get_workflow():
        """Get the recommended workflow for running DQX data quality checks on a table.

        Call this tool FIRST to understand the correct sequence of tool calls.
        """
        return {
            "description": "DQX data quality workflow for profiling a table, generating rules, and running checks.",
            "steps": [
                {
                    "step": 1,
                    "tool": "get_table_schema",
                    "purpose": "Understand the table structure before profiling.",
                    "required_input": {"table_name": "Fully qualified table name (e.g. 'catalog.schema.table')"},
                    "output": "Column names, types, nullability, and row count.",
                    "optional": False,
                },
                {
                    "step": 2,
                    "tool": "profile_table",
                    "purpose": "Profile the table data to discover patterns.",
                    "required_input": {"table_name": "Same table name from step 1"},
                    "optional_input": {
                        "columns": "List of column names to profile (default: all columns)",
                        "options": "Profiling options like sample_fraction, limit, etc.",
                    },
                    "output": "Summary statistics and a list of 'profiles' to feed into step 3.",
                    "optional": False,
                },
                {
                    "step": 3,
                    "tool": "generate_rules",
                    "purpose": "Convert profiling output into DQX check rule definitions.",
                    "required_input": {"profiles": "The 'profiles' list from step 2 output"},
                    "optional_input": {"criticality": "'error' (default) or 'warn'"},
                    "output": "A list of 'rules' (check definitions) to feed into steps 4 and 5.",
                    "optional": False,
                },
                {
                    "step": 4,
                    "tool": "validate_checks",
                    "purpose": "Validate that the rule definitions are correct before running them.",
                    "required_input": {"checks": "The 'rules' list from step 3 output"},
                    "output": "Whether rules are valid and any error messages.",
                    "optional": True,
                    "note": "Recommended but optional. Catches errors before execution.",
                },
                {
                    "step": 5,
                    "tool": "run_checks",
                    "purpose": "Execute the rules against the table and get data quality results.",
                    "required_input": {
                        "table_name": "Same table name from step 1",
                        "checks": "The validated 'rules' from step 3",
                    },
                    "optional_input": {"sample_size": "Max invalid rows to return (default: 50)"},
                    "output": "Total/valid/invalid row counts, per-rule summary, and a sample of failing rows.",
                    "optional": False,
                },
            ],
            "helper_tools": [
                {
                    "tool": "list_available_checks",
                    "purpose": "Discover all 68+ built-in check functions.",
                },
            ],
            "notes": [
                "All tools require a real Unity Catalog table name.",
                "You can modify the generated rules between steps 3 and 5.",
                "Re-run validate_checks after any manual edits to rules.",
            ],
        }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
python -m pytest tests/test_tools.py -v
```

Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add mcp-server/server/tools.py mcp-server/tests/test_tools.py
git commit -m "feat(mcp): refactor tools to use Jobs API instead of Databricks Connect"
```

---

### Task 4: Update Deployment Config

**Files:**
- Modify: `mcp-server/databricks.yml`
- Modify: `mcp-server/requirements.txt`
- Modify: `mcp-server/pyproject.toml`

- [ ] **Step 1: Update `databricks.yml` to deploy notebook and add scopes**

Replace `mcp-server/databricks.yml` with:

```yaml
bundle:
  name: "mcp-dqx"

resources:
  apps:
    mcp-dqx:
      name: "mcp-dqx"
      description: "DQX MCP Server - AI agent tools for data quality checks"
      source_code_path: "."

      # OBO: request user token scopes so API calls run as the user
      user_api_scopes:
        - sql
        - files.files
        - clusters

      permissions:
        - level: CAN_USE
          group_name: users

sync:
  include:
    - "notebooks/**"

targets:
  dev:
    default: true
```

- [ ] **Step 2: Update `requirements.txt` — remove databricks-connect**

Replace `mcp-server/requirements.txt` with:

```
fastmcp>=2.12.5
uvicorn>=0.34.2
databricks-sdk>=0.60.0
databricks-labs-dqx
starlette
```

- [ ] **Step 3: Update `pyproject.toml` — remove databricks-connect**

In `mcp-server/pyproject.toml`, replace the `dependencies` list with:

```toml
dependencies = [
    "fastapi>=0.115.12",
    "mcp[cli]>=1.14.0",
    "uvicorn>=0.34.2",
    "databricks-sdk>=0.60.0",
    "pydantic>=2",
    "fastmcp>=2.12.5",
    "databricks-labs-dqx",
]
```

- [ ] **Step 4: Regenerate lock file**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
uv lock
```

- [ ] **Step 5: Commit**

```bash
git add mcp-server/databricks.yml mcp-server/requirements.txt mcp-server/pyproject.toml mcp-server/uv.lock
git commit -m "chore(mcp): remove databricks-connect, add clusters scope, deploy notebook"
```

---

### Task 5: Clean Up Old Files

**Files:**
- Delete: `mcp-server/server/CHANGES_NOTES.md` (obsolete — documented the Connect-based OBO approach)
- Delete: `mcp-server/server/test1.py` (appears to be a scratch file)

- [ ] **Step 1: Remove obsolete files**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx
rm mcp-server/server/CHANGES_NOTES.md mcp-server/server/test1.py
```

- [ ] **Step 2: Commit**

```bash
git add -u mcp-server/server/CHANGES_NOTES.md mcp-server/server/test1.py
git commit -m "chore(mcp): remove obsolete Connect-based OBO notes and scratch file"
```

---

### Task 6: Smoke Test Full Stack Locally

- [ ] **Step 1: Verify the MCP server starts without import errors**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
uv sync
python -c "from server.app import combined_app; print('App loaded OK')"
```

Expected: `App loaded OK` (no import errors from removed databricks-connect).

- [ ] **Step 2: Run all unit tests**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 3: Verify health check endpoint starts**

```bash
cd /Users/sourav.gulati/projects/dqx/dqx/mcp-server
timeout 5 python -m server.main || true
# Should see "Uvicorn running on http://0.0.0.0:8000" before timeout
```

- [ ] **Step 4: Commit if any fixes were needed**

Only commit if steps 1-3 revealed issues that required fixes.
