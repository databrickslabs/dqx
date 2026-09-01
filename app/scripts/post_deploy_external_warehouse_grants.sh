#!/usr/bin/env bash
#
# Grant CAN_USE on an external (Mode B) SQL warehouse after ``bundle deploy``.
#
# Mode A targets manage warehouse permissions via the bundle's
# ``sql_warehouses.dqx_sql_warehouse.permissions`` block. Mode B targets point
# at an existing warehouse — the app binding grants the app SP CAN_USE, but the
# job SP and workspace ``users`` group still need CAN_USE for task runs and OBO
# dry-run queries. This script PATCHes those grants additively.
#
# Usage:
#   ./scripts/post_deploy_external_warehouse_grants.sh -p <profile> -t <target> [-- <bundle-var-overrides...>]
#
# No-op when ``sql_warehouse_id`` is unset or resolves to a Terraform reference
# (Mode A: ``${resources.sql_warehouses...}``).

set -euo pipefail

validate_uuid() {
  local name="$1" value="$2"
  if [[ ! "$value" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; then
    echo "ERROR: $name '$value' is not a valid UUID." >&2
    exit 1
  fi
}

validate_warehouse_id() {
  local name="$1" value="$2"
  if [[ ! "$value" =~ ^[A-Za-z0-9]+$ ]]; then
    echo "ERROR: $name '$value' is not a valid warehouse ID." >&2
    exit 1
  fi
}

PROFILE=""
TARGET=""

usage() {
  echo "Usage: $0 -p <databricks-profile> -t <bundle-target> [-- <bundle-var-overrides...>]"
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

[[ -z "$PROFILE" || -z "$TARGET" ]] && usage

EXTRA_VARS=("$@")
CLI="databricks -p $PROFILE"
BUNDLE_FLAGS=(-t "$TARGET")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUNDLE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BUNDLE_DIR"

BUNDLE_VALIDATE_STDERR=$(mktemp)
trap 'rm -f "$BUNDLE_VALIDATE_STDERR"' EXIT

if ! BUNDLE_JSON=$($CLI bundle validate ${BUNDLE_FLAGS[@]+"${BUNDLE_FLAGS[@]}"} ${EXTRA_VARS[@]+"${EXTRA_VARS[@]}"} -o json 2>"$BUNDLE_VALIDATE_STDERR"); then
  echo "ERROR: 'databricks bundle validate' failed:" >&2
  cat "$BUNDLE_VALIDATE_STDERR" >&2
  exit 1
fi

EXTERNAL_WH_ID=$(echo "$BUNDLE_JSON" | jq -r '.variables.sql_warehouse_id.value // .variables.sql_warehouse_id.default // empty')
JOB_SP=$(echo "$BUNDLE_JSON" | jq -r '.variables.dqx_service_principal_application_id.value // .variables.dqx_service_principal_application_id.default // empty')
APP_NAME=$(echo "$BUNDLE_JSON" | jq -r '.variables.app_name.value // .variables.app_name.default // "dqx-studio"')

# Mode A — bundle-managed warehouse; permissions are handled by Terraform.
if [[ -z "$EXTERNAL_WH_ID" || "$EXTERNAL_WH_ID" == \$* ]]; then
  echo "==> Skipping external warehouse grants (bundle-managed warehouse / Mode A)."
  exit 0
fi

validate_warehouse_id "sql_warehouse_id" "$EXTERNAL_WH_ID"
validate_uuid "dqx_service_principal_application_id" "$JOB_SP"

APP_JSON=$($CLI apps get "$APP_NAME" -o json)
APP_SP_ID=$(echo "$APP_JSON" | jq -r '.service_principal_client_id // empty')
if [[ -z "$APP_SP_ID" ]]; then
  echo "ERROR: Could not determine app SP client id from 'databricks apps get $APP_NAME'." >&2
  exit 1
fi
validate_uuid "app SP client_id" "$APP_SP_ID"

echo "==> Granting CAN_USE on external warehouse $EXTERNAL_WH_ID..."
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

set +e
$CLI api patch "/api/2.0/permissions/warehouses/$EXTERNAL_WH_ID" --json "$PATCH_PAYLOAD" -o json \
  | jq -r '.access_control_list[]? | "   granted \(.permission_level) to \(.user_name // .group_name // .service_principal_name)"'
PIPELINE_RCS=("${PIPESTATUS[@]}")
set -e

if (( PIPELINE_RCS[0] != 0 || PIPELINE_RCS[1] != 0 )); then
  echo "ERROR: warehouse permissions PATCH failed — grant CAN_USE manually in the Databricks UI." >&2
  exit 1
fi

echo "==> Done."
