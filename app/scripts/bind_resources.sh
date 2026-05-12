#!/usr/bin/env bash
#
# One-time binding of pre-existing storage resources to the bundle.
#
# Use this when the schemas / volume / Lakebase instance already
# exist in the target workspace (e.g. from the previous bootstrap-
# script flow, or from manual creation) and you're adopting them into
# the bundle for the first time. Without binding, ``databricks bundle
# deploy`` tries to CREATE the resources and fails with "already
# exists".
#
# The app connects to the always-present ``databricks_postgres`` admin
# database on the Lakebase instance (no separate logical database to
# provision or bind), and creates its ``dqx_studio`` Postgres schema
# there on first start.
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
shift $((OPTIND - 1))

[[ -z "$PROFILE" || -z "$TARGET" ]] && usage

# Everything after a ``--`` separator is forwarded to every bundle
# subcommand as extra ``--var key=value`` overrides. Threading them
# into ``bundle validate`` matters: that call is what produces the
# JSON we parse below to learn which instance/schema name to bind to.
# Without forwarding, a deploy-time override would bind the wrong
# resource and the next deploy would still see a state-vs-config drift.
EXTRA_VARS=("$@")

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
if ! BUNDLE_JSON=$($CLI bundle validate "${BUNDLE_FLAGS[@]}" "${EXTRA_VARS[@]}" -o json 2>"$BUNDLE_VALIDATE_STDERR"); then
  echo "ERROR: 'databricks bundle validate' failed:" >&2
  cat "$BUNDLE_VALIDATE_STDERR" >&2
  exit 1
fi

CATALOG=$(echo "$BUNDLE_JSON" | jq -r '.variables.catalog_name.value // .variables.catalog_name.default // empty')
SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.schema_name.value // .variables.schema_name.default // "dqx_studio"')
TMP_SCHEMA=$(echo "$BUNDLE_JSON" | jq -r '.variables.tmp_schema_name.value // .variables.tmp_schema_name.default // "dqx_studio_tmp"')
VOLUME=$(echo "$BUNDLE_JSON" | jq -r '.variables.wheels_volume_name.value // .variables.wheels_volume_name.default // "wheels"')
LB_INSTANCE=$(echo "$BUNDLE_JSON" | jq -r '.variables.lakebase_instance_name.value // .variables.lakebase_instance_name.default // empty')

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
echo ""

# ``databricks bundle deployment bind`` requires explicit ``--auto-approve``
# when stdin is not a TTY (newer CLI versions stopped accepting piped
# ``yes`` and now error out with "current console does not support
# prompting"). The confirmation it would otherwise prompt for is just
# "apply bundle resource updates to the existing remote resource on the
# next deploy" — which is exactly what we want, so auto-approving is
# safe.
#
# Bind is idempotent on the CLI side: re-binding an already-bound
# resource exits 0. Any non-zero exit from ``bundle deployment bind``
# therefore signals a genuine problem (the resource doesn't exist, the
# principal lacks permission, the target points at the wrong workspace,
# …) rather than a benign "already bound" state. We surface it loudly
# instead of warning-and-continuing so the user sees the real cause
# now, not a confusing "resource already exists" failure inside the
# next ``bundle deploy``.
bind() {
  local key="$1"
  local id="$2"
  echo "   binding ${key} -> ${id}"
  if ! $CLI bundle deployment bind "$key" "$id" --auto-approve "${BUNDLE_FLAGS[@]}" "${EXTRA_VARS[@]}"; then
    echo "" >&2
    echo "ERROR: failed to bind ${key} -> ${id}" >&2
    echo "" >&2
    echo "Common causes:" >&2
    echo "  - The remote resource does not exist yet. This script adopts" >&2
    echo "    pre-existing resources; on a fresh workspace, skip it and" >&2
    echo "    run 'make app-deploy' directly — the bundle creates the" >&2
    echo "    resources for you." >&2
    echo "  - The current principal (profile=${PROFILE}) lacks permission" >&2
    echo "    to read or bind the resource." >&2
    echo "  - The bundle target (${TARGET}) points at a different" >&2
    echo "    workspace than the one where the resource was created." >&2
    echo "" >&2
    echo "Re-run after fixing the underlying cause, or run this single" >&2
    echo "bind manually if you're certain the resource exists and is" >&2
    echo "accessible:" >&2
    echo "  databricks -p ${PROFILE} bundle deployment bind \\" >&2
    echo "      ${key} ${id} --auto-approve -t ${TARGET}" >&2
    exit 1
  fi
}

bind main_schema "${CATALOG}.${SCHEMA}"
bind tmp_schema  "${CATALOG}.${TMP_SCHEMA}"
bind wheels      "${CATALOG}.${SCHEMA}.${VOLUME}"

if [[ -n "$LB_INSTANCE" ]]; then
  bind lakebase    "$LB_INSTANCE"
fi

echo ""
echo "==> Bind complete. Run 'make app-deploy PROFILE=$PROFILE TARGET=$TARGET' next."
