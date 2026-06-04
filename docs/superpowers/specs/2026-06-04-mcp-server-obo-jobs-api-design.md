# MCP Server OBO Support via Jobs API

**Date:** 2026-06-04
**Status:** Approved
**Author:** Sourav Gulati

## Problem

The DQX MCP server currently uses Databricks Connect to run Spark operations (table schema retrieval, profiling, running checks). Databricks Connect requires the `databricks-connect` OAuth scope, but this scope **cannot be granted** to a Databricks App via `user_api_scopes`. As a result, the OBO (On-Behalf-Of) token forwarded by the Databricks App proxy lacks the scope to create a `DatabricksSession`, making OBO impossible for Spark-dependent tools.

## Solution

Replace all Databricks Connect usage with **Jobs API notebook submissions**. Each Spark-dependent MCP tool submits a serverless notebook job via the user's OBO `WorkspaceClient`. The job runs as the user (preserving Unity Catalog governance), uses the native `spark` session available in the notebook, and returns results via `dbutils.notebook.exit()`.

## Architecture

```
User -> Databricks App (OBO proxy) -> MCP Server
  -> OBOAuthMiddleware extracts X-Forwarded-Access-Token
  -> get_workspace_client() returns OBO WorkspaceClient
  -> MCP tool builds params dict
  -> submit_notebook_job(operation, params)
     -> ws.jobs.submit(notebook_task) on serverless compute
     -> Notebook runs as the user with native spark
     -> dbutils.notebook.exit(json_results)
  -> SDK .result() waits for completion
  -> MCP tool parses output, returns to caller

Fallback: When no OBO token is present (e.g., direct API call),
falls back to the service principal WorkspaceClient for job submission.
```

## Components

### 1. Runner Notebook (`notebooks/runner.py`)

A single dispatcher notebook deployed to the workspace via `databricks bundle deploy`.

**Responsibilities:**
- Reads `operation` (string) and `params` (JSON string) from notebook widgets
- Dispatches to the appropriate DQX function based on `operation`
- Uses the native `spark` session (auto-available in serverless notebooks)
- Returns JSON results via `dbutils.notebook.exit()`
- Installs DQX via `%pip install databricks-labs-dqx` at the top

**Supported operations:**

| Operation | DQX Function | Needs Spark |
|---|---|---|
| `get_table_schema` | `spark.table()` + schema introspection | Yes |
| `profile_table` | `DQProfiler.profile_table()` | Yes |
| `generate_rules` | `DQGenerator.generate_dq_rules()` | Yes |
| `run_checks` | `DQEngine.apply_checks_by_metadata_and_split()` | Yes |

**Result size:** `dbutils.notebook.exit()` supports up to 5MB. The notebook checks serialized JSON size before calling `exit()` and truncates with a warning if needed. The `run_checks` operation's `sample_size` parameter (default 50) keeps results well within this limit.

### 2. Job Submission Helper (`utils.py`)

A new `submit_notebook_job(operation, params)` function replaces all Databricks Connect code.

**Behavior:**
1. Gets the OBO `WorkspaceClient` via existing `get_workspace_client()` (falls back to SP if no token)
2. Calls `ws.jobs.submit()` with:
   - `run_name`: `"mcp-dqx-{operation}"`
   - `tasks`: single `NotebookTask` pointing to the runner notebook path
   - `base_parameters`: `{"operation": operation, "params": json.dumps(params)}`
   - Serverless compute (no cluster spec)
3. Calls `.result()` on the returned `Wait` object to block until completion (10-minute timeout)
4. Reads the notebook output from the run result
5. Parses JSON output and returns as dict
6. Raises a descriptive error on job failure (with run URL for debugging)

**Notebook path:** Read from `DQX_RUNNER_NOTEBOOK_PATH` env var with a default matching the bundle deployment path.

**Code removed from `utils.py`:**
- `_get_spark()` — no more Databricks Connect
- `_get_profiler()`, `_get_generator()`, `_get_engine()` — DQX instantiation moves to the notebook
- `_without_oauth_env_vars()` — only needed for Connect
- `compute_rule_summary()` — moves to the notebook (needs Spark)

**Code retained in `utils.py`:**
- `OBOAuthMiddleware` — still extracts the OBO token into contextvars
- `get_workspace_client()` — used to submit jobs
- `make_json_safe()` — used in the notebook (can be duplicated or extracted to a shared module)

### 3. Refactored MCP Tools (`tools.py`)

Tools split into two categories:

**Job-based (submit to notebook):**
- `get_table_schema(table_name)` — submits `get_table_schema` operation
- `profile_table(table_name, columns, options)` — submits `profile_table` operation
- `generate_rules(profiles, criticality)` — submits `generate_rules` operation
- `run_checks(table_name, checks, sample_size)` — submits `run_checks` operation

Each becomes a thin wrapper:
```python
def run_checks(table_name: str, checks: list[dict], sample_size: int = 50):
    return utils.submit_notebook_job("run_checks", {
        "table_name": table_name,
        "checks": checks,
        "sample_size": sample_size,
    })
```

**In-process (no Spark needed):**
- `list_available_checks()` — reads `CHECK_FUNC_REGISTRY` directly
- `validate_checks(checks)` — calls `DQEngine.validate_checks()` (static validation)
- `get_workflow()` — returns static workflow description dict

### 4. Deployment & Configuration

**`databricks.yml` changes:**
- Add the runner notebook as a bundled resource deployed to a known workspace path
- Add `clusters` scope to `user_api_scopes` (needed for `jobs.submit()`)

```yaml
resources:
  apps:
    mcp-dqx:
      user_api_scopes:
        - sql
        - files.files
        - clusters
```

**File structure:**
```
mcp-server/
  server/
    app.py          # unchanged
    main.py         # unchanged
    tools.py        # simplified — thin wrappers calling submit_notebook_job()
    utils.py        # OBO middleware + submit_notebook_job() (Connect code removed)
    __init__.py
  notebooks/
    runner.py       # new — dispatcher notebook
  app.yaml          # unchanged
  databricks.yml    # updated — deploy notebook + add clusters scope
  requirements.txt  # remove databricks-connect, keep databricks-sdk
```

**`requirements.txt` changes:**
- Remove `databricks-connect`
- Keep `databricks-sdk`, `fastmcp`, `starlette`, `uvicorn`
- DQX installed inside the notebook via `%pip install databricks-labs-dqx`

### 5. Error Handling

| Scenario | Behavior |
|---|---|
| Job failure | Extract `run_page_url` + `state_message`, raise descriptive error |
| Timeout (>10 min) | Raise timeout error with run URL for debugging |
| Output too large (>5MB) | Notebook truncates sample, adds warning before `notebook.exit()` |
| Invalid operation | Notebook returns `{"error": "Unknown operation: xyz"}` |
| Auth failure | SDK raises `PermissionDenied` naturally |
| Table not found | Spark raises `AnalysisException`, caught and returned as error JSON |

## Latency Expectations

Serverless notebook jobs have a cold start of ~15-30 seconds. This means every MCP tool call that goes through the job path will take at minimum 15-30 seconds, plus actual execution time. This is a tradeoff for correct OBO support.

The three in-process tools (`list_available_checks`, `validate_checks`, `get_workflow`) remain instant.

## What Stays Unchanged

- `app.py` — ASGI app configuration, CORS, health check
- `main.py` — uvicorn entry point
- `OBOAuthMiddleware` — token extraction into contextvars
- `get_workspace_client()` — OBO/SP client creation
- MCP tool signatures and return types — callers see no API change
