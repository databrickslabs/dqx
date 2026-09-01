#!/usr/bin/env bash
#
# Apply pending Lakebase Postgres migrations as the bundle deployer.
#
# The Databricks App runs migrations on startup as its service principal, but
# OLTP tables are often owned by the human who first created them (local dev,
# seed_demo, etc.). Postgres requires ownership for ALTER TABLE — the app SP's
# DATABRICKS_SUPERUSER membership does not substitute. This script runs the
# same PgMigrationRunner catalogue once per deploy, before ``bundle run`` starts
# the app, using the deployer's Lakebase OAuth credential.
#
# Usage:
#   ./scripts/post_deploy_lakebase_migrations.sh -p <profile> -t <target> [-- <bundle-var-overrides...>]
#
# No-op when ``lakebase_endpoint`` is unset or ``-`` (Delta-only mode).

set -euo pipefail

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
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUNDLE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BUNDLE_DIR"

uv run python scripts/post_deploy_lakebase_migrations.py \
  -p "$PROFILE" \
  -t "$TARGET" \
  -- "${EXTRA_VARS[@]+"${EXTRA_VARS[@]}"}"
