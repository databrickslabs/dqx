#!/usr/bin/env bash
#
# Grant Unity Catalog permissions required after `databricks bundle deploy`.
#
# DABs creates schemas and volumes but cannot grant catalog-level access.
# This script discovers the app SP and job SP, then executes the necessary
# GRANT statements via the Statement Execution API.
#
# Usage:
#   ./scripts/post_deploy_grants.sh -p <profile> [-t <bundle-target>] [-- <bundle-var-overrides...>]
#
# The bundle target is required when the bundle defines more than one
# target and none is marked as default. Without it, ``databricks bundle
# validate`` errors out and we have no way to discover the catalog name
# or job SP from variables.
#
# Everything after a ``--`` separator is forwarded to ``bundle validate``
# as extra ``--var key=value`` overrides. Pass the SAME overrides used
# at ``bundle deploy`` time — otherwise the script reads the default
# catalog/schema/volume names from the bundle and issues GRANTs on the
# wrong objects (the overridden resources receive no permissions and
# the app SP cannot read them).
#
# Requirements:
#   - databricks CLI authenticated
#   - jq installed
#   - The bundle must already be deployed (app and warehouse must exist)

set -euo pipefail

PROFILE=""
TARGET=""

usage() {
  echo "Usage: $0 -p <databricks-profile> [-t <bundle-target>] [-- <bundle-var-overrides...>]"
  exit 1
}

while getopts "p:t:" opt; do
  case $opt in
    p) PROFILE="$OPTARG" ;;
    t) TARGET="$OPTARG" ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))

[[ -z "$PROFILE" ]] && usage

# Forwarded ``--var key=value`` overrides. Threading them into
# ``bundle validate`` matters because that call produces the JSON we
# parse below for catalog / schema / volume / job-SP names. Without
# forwarding, a deploy-time override (e.g. ``--var=catalog_name=foo``)
# would issue GRANTs on the bundle's default catalog instead of the
# one actually deployed.
EXTRA_VARS=("$@")

CLI="databricks -p $PROFILE"
# Bundle commands need the target unless one is marked default.
BUNDLE_FLAGS=()
if [[ -n "$TARGET" ]]; then
  BUNDLE_FLAGS+=(-t "$TARGET")
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUNDLE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BUNDLE_DIR"

# Capture stderr so a validate failure produces a useful diagnostic
# instead of an empty $BUNDLE_JSON and a confusing downstream error.
BUNDLE_VALIDATE_STDERR=$(mktemp)
trap 'rm -f "$BUNDLE_VALIDATE_STDERR"' EXIT
# macOS bash 3.2 + ``set -u`` treats expansion of an empty array as
# unbound. Guard with the ``${arr[@]+...}`` idiom so a run with no
# extra ``--var`` overrides (or no ``-t``) does not abort here.
if ! BUNDLE_JSON=$($CLI bundle validate ${BUNDLE_FLAGS[@]+"${BUNDLE_FLAGS[@]}"} ${EXTRA_VARS[@]+"${EXTRA_VARS[@]}"} -o json 2>"$BUNDLE_VALIDATE_STDERR"); then
  echo "ERROR: 'databricks bundle validate' failed:" >&2
  cat "$BUNDLE_VALIDATE_STDERR" >&2
  if [[ -z "$TARGET" ]]; then
    echo "       Hint: re-run with -t <bundle-target> (the bundle defines multiple targets)." >&2
  fi
  exit 1
fi

APP_NAME=$(echo "$BUNDLE_JSON" | jq -r '.variables.app_name.value // .variables.app_name.default // "dqx-studio"')

echo "==> Discovering app configuration..."
echo "   App: $APP_NAME"

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

CATALOG=$(echo "$BUNDLE_JSON" | jq -r '.variables.catalog_name.value // .variables.catalog_name.default // empty')
SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.schema_name.value // .variables.schema_name.default // "dqx_studio"')
TMP_SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.tmp_schema_name.value // .variables.tmp_schema_name.default // "dqx_studio_tmp"')
VOLUME=$(echo "$BUNDLE_JSON" | jq -r '.variables.wheels_volume_name.value // .variables.wheels_volume_name.default // "wheels"')
JOB_SP=$(echo "$BUNDLE_JSON" | jq -r '.variables.dqx_service_principal_application_id.value // .variables.dqx_service_principal_application_id.default // empty')
# External warehouse ID — set on targets that point the app at an existing
# warehouse instead of the bundle-managed one. When non-empty, the bundle
# does NOT own this warehouse, so we can't grant CAN_USE via the
# ``databricks_permissions.sql_endpoint_*`` resource — we have to call
# the warehouses permissions API directly post-deploy.
EXTERNAL_WH_ID=$(echo "$BUNDLE_JSON" | jq -r '.variables.sql_warehouse_id.value // .variables.sql_warehouse_id.default // empty')

if [[ -z "$CATALOG" ]]; then
  echo "ERROR: Could not determine catalog_name from bundle variables." >&2
  exit 1
fi

if [[ -z "$JOB_SP" || "$JOB_SP" == "00000000-0000-0000-0000-000000000000" ]]; then
  echo "ERROR: dqx_service_principal_application_id is not configured in the bundle target." >&2
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

# Only run the warehouse-permissions PATCH when ``sql_warehouse_id``
# resolved to a *literal* warehouse ID (Mode B). In Mode A the variable
# is set to ``${resources.sql_warehouses.dqx_sql_warehouse.id}`` and
# ``bundle validate`` leaves that as a deferred Terraform reference
# string (the real ID isn't known until ``bundle deploy`` runs apply),
# so the value starts with ``$``. Sending that to the warehouses
# permissions API would produce a bogus URL like
# ``/api/2.0/permissions/warehouses/${resources...}``. The Mode-A
# permissions block under ``sql_warehouses.dqx_sql_warehouse`` handles
# those grants via Terraform instead, so skipping here is correct.
if [[ -n "$EXTERNAL_WH_ID" && "$EXTERNAL_WH_ID" != \$* ]]; then
  echo ""
  echo "==> Granting CAN_USE on external warehouse $EXTERNAL_WH_ID..."
  # The warehouses permissions API takes a PATCH with the principals we
  # want to ADD; existing grants are preserved. We grant CAN_USE to the
  # app SP, the job SP, and the workspace ``users`` group so the OBO-
  # token dry-run path also works.
  # Note: the API expects the warehouse ID as a path segment and the
  # principal as either ``user_name`` (for users), ``group_name`` (for
  # workspace groups), or ``service_principal_name`` (for SPs, identified
  # by their application ID UUID, NOT the numeric workspace ID). The UC
  # ``account users`` group is account-scoped and rejected here — we use
  # workspace ``users`` (which every workspace member is in by default)
  # to match the original bundle-managed warehouse's permissions block.
  $CLI api patch "/api/2.0/permissions/warehouses/$EXTERNAL_WH_ID" --json "$(cat <<EOF
{
  "access_control_list": [
    {"service_principal_name": "$APP_SP_ID", "permission_level": "CAN_USE"},
    {"service_principal_name": "$JOB_SP",    "permission_level": "CAN_USE"},
    {"group_name": "users",                  "permission_level": "CAN_USE"}
  ]
}
EOF
)" -o json | jq -r '.access_control_list[]? | "   granted \(.permission_level) to \(.user_name // .group_name // .service_principal_name)"' || {
    echo "   WARNING: failed to patch warehouse permissions — grant CAN_USE manually via Databricks UI"
  }
fi

echo ""
echo "==> Granting end-user permissions for tmp view creation..."
# The app's dry-run / preview feature creates temp views via the user's
# OBO token (see backend/services/view_service.py + dependencies.get_view_service)
# so the user's source-table read permissions are enforced. The view
# itself lives in $CATALOG.$TMP_SCHEMA, so end users need CREATE TABLE
# and USE SCHEMA there in addition to USE CATALOG on the parent catalog.
# Granting only USE CATALOG (the historical behavior) made every dry-run
# fail with PERMISSION_DENIED on the CREATE OR REPLACE VIEW.
run_sql "GRANT USE CATALOG ON CATALOG \`$CATALOG\` TO \`account users\`"
run_sql "GRANT USE SCHEMA, CREATE TABLE ON SCHEMA \`$CATALOG\`.\`$TMP_SCHEMA\` TO \`account users\`"

echo ""
echo "==> Done. All grants applied."
