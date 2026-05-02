#!/usr/bin/env bash
#
# Grant Unity Catalog permissions required after `databricks bundle deploy`.
#
# DABs creates schemas and volumes but cannot grant catalog-level access.
# This script discovers the app SP and job SP, then executes the necessary
# GRANT statements via the Statement Execution API.
#
# Usage:
#   ./scripts/post_deploy_grants.sh -p <profile>
#
# Requirements:
#   - databricks CLI authenticated
#   - jq installed
#   - The bundle must already be deployed (app and warehouse must exist)

set -euo pipefail

PROFILE=""
APP_NAME="databricks-labs-dqx-app"

usage() {
  echo "Usage: $0 -p <databricks-profile>"
  exit 1
}

while getopts "p:" opt; do
  case $opt in
    p) PROFILE="$OPTARG" ;;
    *) usage ;;
  esac
done

[[ -z "$PROFILE" ]] && usage

CLI="databricks -p $PROFILE"

echo "==> Discovering app configuration..."

APP_JSON=$($CLI apps get "$APP_NAME" -o json)

# The app's auto-created SP has a client_id (UUID) suitable for GRANT statements.
# The numeric service_principal_id is a workspace-internal ID and cannot be used in SQL.
APP_SP_ID=$(echo "$APP_JSON" | jq -r '.service_principal_client_id // empty')
if [[ -z "$APP_SP_ID" ]]; then
  echo "ERROR: Could not determine app service principal client ID from 'databricks apps get'."
  echo "       Make sure the app has been deployed."
  exit 1
fi

echo "   App SP: $APP_SP_ID"

# Warehouse ID comes from the deployed app's resource list, not bundle validate
# (bundle validate doesn't have the runtime-assigned ID).
WH_ID=$(echo "$APP_JSON" | jq -r '.resources[] | select(.sql_warehouse) | .sql_warehouse.id // empty')
if [[ -z "$WH_ID" ]]; then
  echo "ERROR: Could not determine SQL warehouse ID from app resources. Has the bundle been deployed?"
  exit 1
fi
echo "   Warehouse: $WH_ID"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUNDLE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BUNDLE_DIR"

BUNDLE_JSON=$($CLI bundle validate -o json 2>/dev/null)
CATALOG=$(echo "$BUNDLE_JSON" | jq -r '.variables.catalog_name.value // .variables.catalog_name.default // empty')
SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.schema_name.value // .variables.schema_name.default // "dqx_app"')
TMP_SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.tmp_schema_name.value // .variables.tmp_schema_name.default // "dqx_app_tmp"')
VOLUME=$(echo "$BUNDLE_JSON" | jq -r '.variables.wheels_volume_name.value // .variables.wheels_volume_name.default // "wheels"')
JOB_SP=$(echo "$BUNDLE_JSON" | jq -r '.variables.dqx_service_principal_application_id.value // .variables.dqx_service_principal_application_id.default // empty')

if [[ -z "$CATALOG" ]]; then
  echo "ERROR: Could not determine catalog_name from bundle variables."
  exit 1
fi

if [[ -z "$JOB_SP" || "$JOB_SP" == "00000000-0000-0000-0000-000000000000" ]]; then
  echo "ERROR: dqx_service_principal_application_id is not configured in the bundle target."
  exit 1
fi

echo "   Job SP: $JOB_SP"
echo "   Catalog: $CATALOG"
echo "   Schema: $SCHEMA"
echo "   Tmp Schema: $TMP_SCHEMA"
echo "   Volume: $VOLUME"

run_sql() {
  local stmt="$1"
  echo "   SQL: $stmt"
  RESULT=$($CLI api post /api/2.0/sql/statements \
    --json "{\"warehouse_id\": \"$WH_ID\", \"statement\": \"$stmt\", \"wait_timeout\": \"30s\"}" \
    -o json 2>&1)
  STATUS=$(echo "$RESULT" | jq -r '.status.state // "UNKNOWN"')
  if [[ "$STATUS" != "SUCCEEDED" ]]; then
    ERROR_MSG=$(echo "$RESULT" | jq -r '.status.error.message // .message // "unknown error"')
    echo "   WARNING: $STATUS — $ERROR_MSG"
  fi
}

echo ""
echo "==> Granting permissions to App SP ($APP_SP_ID)..."
run_sql "GRANT USE CATALOG ON CATALOG \`$CATALOG\` TO \`$APP_SP_ID\`"
run_sql "GRANT ALL PRIVILEGES ON SCHEMA \`$CATALOG\`.\`$SCHEMA\` TO \`$APP_SP_ID\`"
run_sql "GRANT ALL PRIVILEGES ON SCHEMA \`$CATALOG\`.\`$TMP_SCHEMA\` TO \`$APP_SP_ID\`"
run_sql "GRANT ALL PRIVILEGES ON VOLUME \`$CATALOG\`.\`$SCHEMA\`.\`$VOLUME\` TO \`$APP_SP_ID\`"

echo ""
echo "==> Granting permissions to Job SP ($JOB_SP)..."
run_sql "GRANT USE CATALOG ON CATALOG \`$CATALOG\` TO \`$JOB_SP\`"
run_sql "GRANT ALL PRIVILEGES ON SCHEMA \`$CATALOG\`.\`$SCHEMA\` TO \`$JOB_SP\`"
run_sql "GRANT ALL PRIVILEGES ON SCHEMA \`$CATALOG\`.\`$TMP_SCHEMA\` TO \`$JOB_SP\`"
run_sql "GRANT ALL PRIVILEGES ON VOLUME \`$CATALOG\`.\`$SCHEMA\`.\`$VOLUME\` TO \`$JOB_SP\`"

echo ""
echo "==> Granting USE CATALOG to account users (for end-user tmp view creation)..."
run_sql "GRANT USE CATALOG ON CATALOG \`$CATALOG\` TO \`account users\`"

echo ""
echo "==> Done. All grants applied."
