# DQX MCP Server

An MCP (Model Context Protocol) server that exposes DQX data quality tools for AI agents. Deploy it as a Databricks App and connect it to Mosaic AI Agents, Cursor, Claude Desktop, or any MCP-compatible client.

## Available Tools

| Tool | Description |
|------|-------------|
| `validate_checks` | Validate a list of DQX check definitions for correctness |

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (v0.200+)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- A Databricks workspace with Apps enabled

## Deploy to Databricks

### 1. Authenticate

```bash
databricks auth login --host https://<your-workspace-url>
```

### 2. Deploy

```bash
cd mcp-server
./deploy.sh
```

Or manually with DABs:

```bash
cd mcp-server
databricks bundle deploy
```

### 3. Find Your MCP Endpoint

After deployment, the script prints the app URL. Your MCP endpoint is:

```
https://<app-url>/mcp
```

You can also find the URL in the Databricks UI under **Compute > Apps**.

## Usage

### With Mosaic AI Agent

```python
from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
mcp_client = DatabricksMCPClient(
    server_url="https://<app-url>/mcp",
    workspace_client=ws,
)

# List available tools
tools = mcp_client.list_tools()
print([t.name for t in tools])

# Validate checks
result = mcp_client.call_tool("validate_checks", {
    "checks": [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "criticality": "error",
            "name": "id_not_null"
        }
    ]
})
print(result.content)
```

### With Cursor / Claude Desktop (local development)

Add to your MCP config (`.cursor/mcp.json` or Claude Desktop config):

```json
{
  "mcpServers": {
    "dqx": {
      "command": "uv",
      "args": ["--directory", "/path/to/dqx/mcp-server", "run", "dqx-mcp-server"],
      "env": {
        "MCP_TRANSPORT": "stdio"
      }
    }
  }
}
```

### Test locally with MCP Inspector

```bash
cd mcp-server

# Start the server
MCP_TRANSPORT=streamable-http uv run dqx-mcp-server

# In another terminal, open the inspector
npx -y @modelcontextprotocol/inspector
# Connect to http://localhost:8000/mcp
```

## Adding More Tools

Edit `src/dqx_mcp_server/server.py` and add new tools with the `@mcp.tool()` decorator:

```python
@mcp.tool()
def generate_dq_rules_from_table(
    table_name: str,
    criticality: str = "error",
) -> list[dict]:
    """Profile a Unity Catalog table and generate data quality rules."""
    # Implementation here
    ...
```

Redeploy with `./deploy.sh` after changes.
