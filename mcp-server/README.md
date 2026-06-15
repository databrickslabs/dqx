# DQX MCP Server

An MCP (Model Context Protocol) server that exposes DQX data quality tools for AI agents. Runs as a Databricks App with on-behalf-of (OBO) authentication — every operation respects the calling user's Unity Catalog permissions.

## Architecture

```
User (via MCP client)
  │
  ├─ get_table_schema ──→ Direct SQL (DESCRIBE TABLE) via user's OBO token
  │
  ├─ profile_table ─────→ OBO creates temp view → SP triggers runner job → returns run_id
  ├─ run_checks ────────→ OBO creates temp view → SP triggers runner job → returns run_id
  │
  ├─ generate_rules ────→ SP triggers runner job → returns run_id
  ├─ validate_checks ───→ SP triggers runner job → returns run_id
  ├─ list_available_checks → SP triggers runner job → returns run_id
  │
  └─ get_run_result ────→ Polls job status, returns result when done, cleans up temp views
```

**Async pattern:** Long-running tools (profiling, checks, rule generation) submit a Databricks job and return a `run_id` immediately. The client then calls `get_run_result(run_id)` to poll for results. This avoids HTTP timeouts in clients like Genie Code. The `get_run_result` tool polls internally for up to 90 seconds before returning, so most jobs complete in a single call.

**UC governance:** Tools that read data create a temporary view using the user's OBO token. If the user can't read the source table, view creation fails (UC enforced). The SP job reads through the view using definer's rights. Views are cleaned up automatically when results are retrieved.

## Available Tools

| Tool | Description | Execution | Returns |
|------|-------------|-----------|---------|
| `get_table_schema` | Get column names, types, and comments | Direct SQL via OBO | Result directly |
| `profile_table` | Profile data patterns (nulls, ranges, distributions) | View + runner job | `run_id` |
| `generate_rules` | Generate DQX check rules from profiles | Runner job | `run_id` |
| `validate_checks` | Validate check definitions for correctness | Runner job | `run_id` |
| `run_checks` | Execute checks and get quality results | View + runner job | `run_id` |
| `list_available_checks` | List all 68+ built-in check functions | Runner job | `run_id` |
| `get_run_result` | Poll for job results (blocks up to 90s internally) | Job status check | Result or status |
| `get_workflow` | Get the recommended tool call sequence | In-process | Workflow JSON |

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (v0.200+)
- A Databricks workspace with Apps enabled
- An existing Unity Catalog catalog (the setup job creates the schema and grants)

## Deploy

### 1. Authenticate

```bash
databricks auth login --host https://<your-workspace-url> --profile <profile>
```

### 2. Configure the catalog name (one-time)

The catalog name is stored as a Databricks secret. Both the app and the setup job read from it.

```bash
# Create the secret scope (one-time)
databricks secrets create-scope dqx-config --profile <profile>

# Set the catalog name
databricks secrets put-secret dqx-config catalog_name --string-value "<your_catalog>" --profile <profile>
```

To change the catalog later, update the secret and restart the app.

### 3. Deploy the bundle

```bash
cd mcp-server
databricks bundle deploy --profile <profile>
```

This deploys:
- The MCP server app (`mcp-dqx`)
- A runner notebook job (`mcp-dqx-runner`) with `databricks-labs-dqx` pre-installed
- A one-time setup job (`mcp-dqx-setup`)

### 4. Run the setup job (one-time)

```bash
databricks bundle run dqx_setup --profile <profile>
```

This reads the catalog name from the secret and automatically:
- Creates the `<catalog>.tmp` schema for temporary views
- Grants `USE CATALOG` on the catalog to all users and the app SP
- Grants `USE SCHEMA` + `CREATE TABLE` on the tmp schema to all users
- Grants `USE SCHEMA` + `SELECT` on the tmp schema to the app SP

Safe to re-run (all statements are idempotent).

### 5. Restart the app

```bash
databricks apps deploy mcp-dqx --profile <profile>
```

### 6. Find your MCP endpoint

The MCP endpoint is at:

```
https://<app-url>/mcp
```

Find the app URL in the Databricks UI under **Compute > Apps > mcp-dqx**.

## Configuration

### Secrets

| Secret Scope | Key | Description |
|-------------|-----|-------------|
| `dqx-config` | `catalog_name` | UC catalog for temp views |

### Bundle variables

| Variable | Description | Default |
|----------|-------------|---------|
| `users_group` | Group name for all workspace users | `account users` |

Override at deploy time:

```bash
databricks bundle deploy \
  --var users_group="account users" \
  --profile <profile>
```

### How it works

1. **Catalog name:** Read from Databricks secret (`dqx-config/catalog_name`) by both the app and the setup job. Update the secret and restart the app to change it.
2. **Async job execution:** Tools trigger the pre-deployed `mcp-dqx-runner` job via `run_now()`, which uses a serverless environment with `databricks-labs-dqx` pre-installed. Jobs support up to 10 concurrent runs.
3. **SQL warehouse:** Auto-discovered at runtime from the user's available warehouses. No configuration needed.
4. **App SP permissions:** Granted automatically by the setup job and the bundle's resource bindings.

## Usage

### With Genie Code / Mosaic AI Agent

The app exposes a standard MCP HTTP endpoint. Connect any MCP-compatible client to `https://<app-url>/mcp`.

### Recommended workflow

Call `get_workflow` first to get the recommended sequence:

1. `get_table_schema` — Understand the table structure (returns result directly)
2. `profile_table` — Profile data to discover patterns (returns `run_id`)
3. `get_run_result(run_id)` — Retrieve profiling results
4. `generate_rules` — Convert profiles into check rules (returns `run_id`)
5. `get_run_result(run_id)` — Retrieve generated rules
6. `validate_checks` — Validate rules before execution (optional, returns `run_id`)
7. `get_run_result(run_id)` — Retrieve validation status
8. `run_checks` — Execute rules and get quality results (returns `run_id`)
9. `get_run_result(run_id)` — Retrieve check results

`get_run_result` polls internally for up to 90 seconds, so most jobs complete in a single call without the client needing to retry.

### With Cursor / Claude Desktop (local development)

Add to your MCP config (`.cursor/mcp.json` or Claude Desktop config):

```json
{
  "mcpServers": {
    "dqx": {
      "command": "uv",
      "args": ["--directory", "/path/to/dqx/mcp-server", "run", "python", "server/main.py"],
      "env": {
        "MCP_TRANSPORT": "stdio"
      }
    }
  }
}
```

## Development

### Run tests

```bash
cd mcp-server
pip install -e ".[dev]"
python -m pytest tests/ -v
```

### Project structure

```
mcp-server/
  ├── server/
  │   ├── main.py          # Entry point (uvicorn)
  │   ├── app.py           # FastMCP app + middleware setup
  │   ├── tools.py         # MCP tool definitions (async submit + poll pattern)
  │   └── utils.py         # OBO auth, SQL helpers, temp views, async job submission
  ├── notebooks/
  │   ├── runner.py        # Notebook job: profile, check, validate, generate
  │   └── setup.py         # One-time setup: schema + grants
  ├── tests/
  │   ├── test_utils.py    # Unit tests for utils
  │   └── test_tools.py    # Unit tests for tools
  ├── databricks.yml       # Bundle config (jobs, app, variables)
  ├── app.yaml             # App command + env vars
  └── requirements.txt     # App dependencies (no pyspark/dqx — runs in notebook)
```
