# DQX MCP Server

An MCP (Model Context Protocol) server that exposes DQX data quality tools for AI agents. Runs as a Databricks App with on-behalf-of (OBO) authentication — every operation respects the calling user's Unity Catalog permissions.

## Architecture

```
User (via MCP client)
  │
  ├─ get_table_schema ──→ Direct SQL (DESCRIBE TABLE) via user's OBO token
  │
  ├─ profile_table ─────→ OBO creates temp view → SP submits notebook job → cleanup
  ├─ run_checks ────────→ OBO creates temp view → SP submits notebook job → cleanup
  │
  ├─ generate_rules ────→ SP submits notebook job (no table access)
  ├─ validate_checks ───→ SP submits notebook job (no table access)
  └─ list_available_checks → SP submits notebook job (no table access)
```

**UC governance:** Tools that read data create a temporary view using the user's OBO token. If the user can't read the source table, view creation fails (UC enforced). The SP job reads through the view using definer's rights. Views are cleaned up after each operation.

## Available Tools

| Tool | Description | Execution |
|------|-------------|-----------|
| `get_table_schema` | Get column names, types, and comments | Direct SQL via OBO |
| `profile_table` | Profile data patterns (nulls, ranges, distributions) | View + notebook job |
| `generate_rules` | Generate DQX check rules from profiles | Notebook job |
| `validate_checks` | Validate check definitions for correctness | Notebook job |
| `run_checks` | Execute checks and get quality results | View + notebook job |
| `list_available_checks` | List all 68+ built-in check functions | Notebook job |
| `get_workflow` | Get the recommended tool call sequence | In-process |

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (v0.200+)
- A Databricks workspace with Apps enabled
- An existing Unity Catalog catalog (the setup job creates the schema and grants)

## Deploy

### 1. Authenticate

```bash
databricks auth login --host https://<your-workspace-url> --profile <profile>
```

### 2. Deploy the bundle

```bash
cd mcp-server
databricks bundle deploy --var catalog_name=<your_catalog> --profile <profile>
```

This deploys:
- The MCP server app (`mcp-dqx`)
- A runner notebook job (`mcp-dqx-runner`)
- A one-time setup job (`mcp-dqx-setup`)

### 3. Run the setup job (one-time)

```bash
databricks bundle run dqx_setup --profile <profile>
```

This automatically:
- Creates the `<catalog>.tmp` schema for temporary views
- Grants `USE CATALOG` on the catalog to all users and the app SP
- Grants `USE SCHEMA` + `CREATE TABLE` on the tmp schema to all users
- Grants `USE SCHEMA` + `SELECT` on the tmp schema to the app SP

Safe to re-run (all statements are idempotent).

### 4. Restart the app

```bash
databricks apps restart mcp-dqx --profile <profile>
```

### 5. Find your MCP endpoint

The MCP endpoint is at:

```
https://<app-url>/mcp
```

Find the app URL in the Databricks UI under **Compute > Apps > mcp-dqx**.

## Configuration

### Bundle variables

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog_name` | UC catalog for temp views | `dqx_mcp` |
| `users_group` | Group name for all workspace users | `account users` |

Override at deploy time:

```bash
databricks bundle deploy \
  --var catalog_name=my_catalog \
  --var users_group="account users" \
  --profile <profile>
```

### How it works

1. **SQL warehouse:** Auto-discovered at runtime from the user's available warehouses. No configuration needed.
2. **Notebook path:** Resolved at runtime from the deployed job definition. No hardcoded paths.
3. **App SP permissions:** Granted automatically by the setup job and the bundle's resource bindings.

## Usage

### With Mosaic AI Agent / Genie

The app exposes a standard MCP HTTP endpoint. Connect any MCP-compatible client to `https://<app-url>/mcp`.

### Recommended workflow

Call `get_workflow` first to get the recommended sequence:

1. `get_table_schema` — Understand the table structure
2. `profile_table` — Profile data to discover patterns
3. `generate_rules` — Convert profiles into check rules
4. `validate_checks` — Validate rules before execution (optional)
5. `run_checks` — Execute rules and get quality results

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
  │   ├── tools.py         # MCP tool definitions (view lifecycle, SQL, job dispatch)
  │   └── utils.py         # OBO auth, SQL helpers, temp views, job submission
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
