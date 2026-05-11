#!/usr/bin/env bash
#
# One-time binding of pre-existing storage resources to the bundle.
#
# Use this when the schemas / volume / Lakebase instance / Lakebase
# logical database already exist in the target workspace (e.g. from
# the previous bootstrap-script flow, or from manual creation) and
# you're adopting them into the bundle for the first time. Without
# binding, ``databricks bundle deploy`` tries to CREATE the resources
# and fails with "already exists".
#
# Bind is idempotent at the CLI level; re-running this script on a
# fully-bound workspace is a no-op (the CLI replies "already bound").
#
# Skip this script for fresh workspaces — ``databricks bundle deploy``
# creates the resources directly.
#
# Usage:
#   ./scripts/bind_resources.sh -p <profile> -t <bundle-target>
#
# Requirements:
#   - databricks CLI v0.268+ (lifecycle.prevent_destroy support)
#   - jq installed

set -euo pipefail

PROFILE=""
TARGET=""

usage() {
  echo "Usage: $0 -p <databricks-profile> -t <bundle-target>"
  exit 1
}

while getopts "p:t:" opt; do
  case $opt in
    p) PROFILE="$OPTARG" ;;
    t) TARGET="$OPTARG" ;;
    *) usage ;;
  esac
done

[[ -z "$PROFILE" || -z "$TARGET" ]] && usage

CLI="databricks -p $PROFILE"
BUNDLE_FLAGS=(-t "$TARGET")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUNDLE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BUNDLE_DIR"

# ---------------------------------------------------------------------------
# Read resource identifiers from the bundle config. Identifiers must
# match the ``name`` / ``catalog_name`` / etc. fields rendered for the
# selected target, so we ask the CLI for the resolved bundle.
# ---------------------------------------------------------------------------
BUNDLE_VALIDATE_STDERR=$(mktemp)
trap 'rm -f "$BUNDLE_VALIDATE_STDERR"' EXIT
if ! BUNDLE_JSON=$($CLI bundle validate "${BUNDLE_FLAGS[@]}" -o json 2>"$BUNDLE_VALIDATE_STDERR"); then
  echo "ERROR: 'databricks bundle validate' failed:" >&2
  cat "$BUNDLE_VALIDATE_STDERR" >&2
  exit 1
fi

CATALOG=$(echo "$BUNDLE_JSON" | jq -r '.variables.catalog_name.value // .variables.catalog_name.default // empty')
SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.schema_name.value // .variables.schema_name.default // "dqx_studio"')
TMP_SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.tmp_schema_name.value // .variables.tmp_schema_name.default // "dqx_studio_tmp"')
VOLUME=$(echo "$BUNDLE_JSON" | jq -r '.variables.wheels_volume_name.value // .variables.wheels_volume_name.default // "wheels"')
LB_INSTANCE=$(echo "$BUNDLE_JSON" | jq -r '.variables.lakebase_instance_name.value // .variables.lakebase_instance_name.default // empty')
LB_UC_CATALOG=$(echo "$BUNDLE_JSON" | jq -r '.variables.lakebase_uc_catalog_name.value // .variables.lakebase_uc_catalog_name.default // empty')

if [[ -z "$CATALOG" ]]; then
  echo "ERROR: catalog_name is not configured in the bundle target." >&2
  exit 1
fi

echo "==> Binding pre-existing resources (target=$TARGET, profile=$PROFILE)"
echo "   Catalog:         $CATALOG"
echo "   Main schema:     $SCHEMA"
echo "   Tmp schema:      $TMP_SCHEMA"
echo "   Volume:          $VOLUME"
echo "   Lakebase:        $LB_INSTANCE"
echo "   Lakebase UC cat: $LB_UC_CATALOG"
echo ""

# ``databricks bundle deployment bind`` is interactive by default. We
# pipe ``yes`` so this runs unattended; the prompt only confirms that
# updates to the resource in the bundle will be applied to the
# existing remote resource on the next deploy — which is exactly what
# we want, so auto-confirming is safe.
bind() {
  local key="$1"
  local id="$2"
  echo "   binding ${key} -> ${id}"
  if ! yes | $CLI bundle deployment bind "$key" "$id" "${BUNDLE_FLAGS[@]}"; then
    echo "   WARNING: bind for ${key} failed. It may already be bound, or the remote resource may not exist yet." >&2
  fi
}

bind main_schema "${CATALOG}.${SCHEMA}"
bind tmp_schema  "${CATALOG}.${TMP_SCHEMA}"
bind wheels      "${CATALOG}.${SCHEMA}.${VOLUME}"

if [[ -n "$LB_INSTANCE" ]]; then
  bind lakebase    "$LB_INSTANCE"
fi
if [[ -n "$LB_UC_CATALOG" ]]; then
  bind lakebase_db "$LB_UC_CATALOG"
fi

echo ""
echo "==> Bind complete. Run 'make app-deploy PROFILE=$PROFILE TARGET=$TARGET' next."
