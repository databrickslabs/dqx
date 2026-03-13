#!/usr/bin/env bash
set -euo pipefail

PROFILE="${DATABRICKS_CLI_PROFILE:-DEFAULT}"
APP_NAME="dqx-mcp-server"

echo "=== DQX MCP Server Deployment ==="
echo ""

# Check prerequisites
if ! command -v databricks &> /dev/null; then
    echo "ERROR: Databricks CLI is not installed."
    echo "Install it: https://docs.databricks.com/dev-tools/cli/install.html"
    exit 1
fi

echo "Using Databricks CLI profile: $PROFILE"
echo ""

# Validate authentication
if ! databricks auth env --profile "$PROFILE" &> /dev/null; then
    echo "ERROR: Not authenticated. Run:"
    echo "  databricks auth login --host https://<your-workspace-url> --profile $PROFILE"
    exit 1
fi

# Get current user for workspace path
USERNAME=$(databricks current-user me --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))")
if [ -z "$USERNAME" ]; then
    echo "ERROR: Could not determine current user."
    exit 1
fi

echo "Authenticated as: $USERNAME"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_PATH="/Workspace/Users/$USERNAME/$APP_NAME"

# Step 1: Create app if it doesn't exist (waits until ready)
if databricks apps get "$APP_NAME" --profile "$PROFILE" &> /dev/null; then
    echo "App '$APP_NAME' already exists."
else
    echo "Creating app '$APP_NAME' (this may take 1-2 minutes)..."
    databricks apps create "$APP_NAME" \
        --description "DQX MCP Server - AI agent tools for data quality checks" \
        --profile "$PROFILE"
    echo "App created."
fi
echo ""

# Step 2: Start app if it is stopped
APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','UNKNOWN'))")

echo "App compute state: $APP_STATE"

if [ "$APP_STATE" = "STOPPED" ] || [ "$APP_STATE" = "STOPPING" ]; then
    echo "Starting app..."
    databricks apps start "$APP_NAME" --profile "$PROFILE" --no-wait 2>&1 || true
    echo "Waiting for app to start..."
    for i in $(seq 1 30); do
        sleep 5
        APP_STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" --output json 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','UNKNOWN'))")
        echo "  [$i] State: $APP_STATE"
        if [ "$APP_STATE" = "ACTIVE" ] || [ "$APP_STATE" = "RUNNING" ]; then
            break
        fi
    done
fi
echo ""

# Step 3: Upload source code
echo "Uploading source code to $SOURCE_PATH ..."
databricks sync "$SCRIPT_DIR" "$SOURCE_PATH" --profile "$PROFILE" --full
echo ""

# Step 4: Deploy the app with source code
echo "Deploying app with source code..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "$SOURCE_PATH" \
    --profile "$PROFILE"

echo ""
echo "=== Deployment Complete ==="
echo ""

# Display the app URL
APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || echo "")

if [ -n "$APP_URL" ] && [ "$APP_URL" != "Unavailable" ]; then
    echo "MCP Server URL: ${APP_URL}/mcp"
    echo ""
    echo "To use with Mosaic AI Agent:"
    echo ""
    echo "  from databricks_mcp import DatabricksMCPClient"
    echo "  from databricks.sdk import WorkspaceClient"
    echo ""
    echo "  ws = WorkspaceClient()"
    echo "  client = DatabricksMCPClient("
    echo "      server_url='${APP_URL}/mcp',"
    echo "      workspace_client=ws,"
    echo "  )"
    echo "  tools = client.list_tools()"
else
    echo "App deployed. Check the Databricks UI (Compute > Apps) for the app URL."
    echo "The MCP endpoint will be at: https://<app-url>/mcp"
fi
