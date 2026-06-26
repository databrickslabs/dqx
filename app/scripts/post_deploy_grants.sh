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

# Validate that a value is safe to interpolate into a backticked Unity
# Catalog identifier in SQL. These are operator-controlled deploy-time
# values, but the script runs with deploy privileges and a value
# containing a backtick, quote, or backslash would either corrupt the
# JSON envelope or escape the backticked identifier and execute
# arbitrary SQL. Allow alphanumerics, underscores, and hyphens only.
validate_uc_identifier() {
  local name="$1" value="$2"
  if [[ -z "$value" ]]; then
    echo "ERROR: $name is empty." >&2
    exit 1
  fi
  if [[ ! "$value" =~ ^[A-Za-z0-9_][A-Za-z0-9_-]*$ ]]; then
    echo "ERROR: $name '$value' contains characters that are unsafe to interpolate into SQL." >&2
    echo "       Allowed: [A-Za-z0-9_-], must start with a letter, digit, or underscore." >&2
    exit 1
  fi
}

# Service principal application/client IDs are UUIDs. The all-zero
# UUID is a deployment misconfiguration and is rejected separately
# upstream with a more specific error.
validate_uuid() {
  local name="$1" value="$2"
  if [[ ! "$value" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; then
    echo "ERROR: $name '$value' is not a valid UUID." >&2
    exit 1
  fi
}

# Warehouse IDs are short alphanumeric tokens; reject anything that
# could be smuggled into a URL path segment.
validate_warehouse_id() {
  local name="$1" value="$2"
  if [[ ! "$value" =~ ^[A-Za-z0-9]+$ ]]; then
    echo "ERROR: $name '$value' is not a valid warehouse ID (expected alphanumeric)." >&2
    exit 1
  fi
}

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

# Reject anything that would either escape a backticked SQL identifier
# or corrupt a JSON payload before we hand the values to the SQL or
# permissions APIs. Failing fast here keeps the SQL/JSON construction
# below safe even though we still interpolate via shell.
validate_uc_identifier "catalog_name"      "$CATALOG"
validate_uc_identifier "schema_name"       "$SCHEMA"
validate_uc_identifier "tmp_schema_name"   "$TMP_SCHEMA"
validate_uc_identifier "wheels_volume_name" "$VOLUME"
validate_uuid          "app SP client_id"  "$APP_SP_ID"
validate_uuid          "dqx_service_principal_application_id" "$JOB_SP"
validate_warehouse_id  "warehouse_id"      "$WH_ID"

echo "   Job SP: $JOB_SP"
echo "   Catalog: $CATALOG"
echo "   Schema: $SCHEMA"
echo "   Tmp Schema: $TMP_SCHEMA"
echo "   Volume: $VOLUME"

# Counter for grants that did not succeed. Every call site that swallows
# a non-zero result must bump this so the final summary can surface the
# failure to CI/operators instead of exiting 0 with the app
# half-provisioned. We deliberately do NOT abort on the first failure —
# the remaining GRANTs are independent and we want to apply as many as
# we can in a single run, then fail loudly at the end.
FAILURES=0

run_sql() {
  local stmt="$1"
  echo "   SQL: $stmt"
  # Build the request body with jq -n --arg so any quote, backslash, or
  # newline in $stmt is escaped per JSON rules. The SQL itself is safe
  # because the identifier variables interpolated above were validated
  # by validate_uc_identifier / validate_uuid.
  local payload
  payload=$(jq -n \
    --arg warehouse_id "$WH_ID" \
    --arg statement    "$stmt" \
    --arg wait_timeout "30s" \
    '{warehouse_id: $warehouse_id, statement: $statement, wait_timeout: $wait_timeout}')
  RESULT=$($CLI api post /api/2.0/sql/statements --json "$payload" -o json 2>&1)
  STATUS=$(echo "$RESULT" | jq -r '.status.state // "UNKNOWN"')
  if [[ "$STATUS" != "SUCCEEDED" ]]; then
    ERROR_MSG=$(echo "$RESULT" | jq -r '.status.error.message // .message // "unknown error"')
    echo "   FAILED: $STATUS — $ERROR_MSG" >&2
    FAILURES=$((FAILURES + 1))
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
  # Validate before composing a URL path segment so a pathological value
  # cannot redirect the PATCH at a different resource.
  validate_warehouse_id "sql_warehouse_id" "$EXTERNAL_WH_ID"
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
  PATCH_PAYLOAD=$(jq -n \
    --arg app_sp "$APP_SP_ID" \
    --arg job_sp "$JOB_SP" \
    '{
      access_control_list: [
        {service_principal_name: $app_sp, permission_level: "CAN_USE"},
        {service_principal_name: $job_sp, permission_level: "CAN_USE"},
        {group_name: "users",             permission_level: "CAN_USE"}
      ]
    }')
  # Track the patch+jq pipeline's outcome explicitly so a failure in
  # either stage bumps FAILURES. PIPESTATUS gets reset by the next
  # command, so snapshot it into a regular array on the very next line.
  set +e
  $CLI api patch "/api/2.0/permissions/warehouses/$EXTERNAL_WH_ID" --json "$PATCH_PAYLOAD" -o json \
    | jq -r '.access_control_list[]? | "   granted \(.permission_level) to \(.user_name // .group_name // .service_principal_name)"'
  PIPELINE_RCS=("${PIPESTATUS[@]}")
  set -e
  if (( PIPELINE_RCS[0] != 0 || PIPELINE_RCS[1] != 0 )); then
    echo "   FAILED: warehouse permissions PATCH (api rc=${PIPELINE_RCS[0]}, jq rc=${PIPELINE_RCS[1]}) — grant CAN_USE manually via Databricks UI" >&2
    FAILURES=$((FAILURES + 1))
  fi
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
if (( FAILURES > 0 )); then
  # Exiting non-zero is what makes ``make app-deploy`` (and any CI
  # wrapping it) actually fail. Without this the bundle deploys, the
  # GRANTs silently fail, and the app starts up half-provisioned with
  # PERMISSION_DENIED errors at first SQL request — which is much
  # harder to diagnose than a loud failure here.
  echo "==> FAILED: $FAILURES grant(s) did not succeed — see warnings above." >&2
  exit 1
fi
echo "==> Done. All grants applied."
