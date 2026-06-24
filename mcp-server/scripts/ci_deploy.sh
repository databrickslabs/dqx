#!/usr/bin/env bash
# Deploy an isolated copy of the DQX MCP server (bundle + runner job + setup + app) for
# integration testing, and print the deployed app URL. Idempotent / safe to re-run.
#
# Auth comes from the standard Databricks CLI env (DATABRICKS_HOST + DATABRICKS_TOKEN, or a
# configured profile). Override the bundle's name/catalog/secret-scope via the env vars below
# so this never collides with a real deployment.
#
# Env:
#   NAME_PREFIX           resource name prefix (default: mcp-dqx-ci)
#   DQX_MCP_TEST_CATALOG  catalog the setup job may create/drop a temp schema in (required)
#   CONFIG_SECRET_SCOPE   secret scope holding catalog_name (default: dqx-config-ci)
#   BUNDLE_TARGET         bundle target (default: dev)
#   DATABRICKS_PROFILE    optional CLI profile (else relies on DATABRICKS_HOST/TOKEN)
#
# Emits: DQX_MCP_SERVER_URL=<url> to stdout and, if set, to $GITHUB_OUTPUT.
set -euo pipefail

NAME_PREFIX="${NAME_PREFIX:-mcp-dqx-ci}"
CONFIG_SECRET_SCOPE="${CONFIG_SECRET_SCOPE:-dqx-config-ci}"
BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"
# Required CI config — names the GitHub secret/var to set when missing.
: "${DATABRICKS_HOST:?set the DQX_MCP_CI_HOST secret (CI workspace URL)}"
: "${DATABRICKS_TOKEN:?set the DQX_MCP_CI_TOKEN secret (token able to deploy apps/jobs + create schemas)}"
: "${DQX_MCP_TEST_CATALOG:?set the DQX_MCP_CI_CATALOG var (a catalog the deployer can create schemas in)}"
PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG}"
      --var "config_secret_scope=${CONFIG_SECRET_SCOPE}")

cd "$(dirname "$0")/.."  # mcp-server/

echo "::group::Configure catalog secret (${CONFIG_SECRET_SCOPE})"
databricks secrets create-scope "${CONFIG_SECRET_SCOPE}" "${PROFILE_ARG[@]}" 2>/dev/null || true
databricks secrets put-secret "${CONFIG_SECRET_SCOPE}" catalog_name \
  --string-value "${DQX_MCP_TEST_CATALOG}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

echo "::group::bundle deploy (${NAME_PREFIX}, target ${BUNDLE_TARGET})"
databricks bundle deploy -t "${BUNDLE_TARGET}" "${VARS[@]}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

echo "::group::run setup job (UC grants + temp-schema ownership)"
databricks bundle run dqx_setup -t "${BUNDLE_TARGET}" "${VARS[@]}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

echo "::group::start + deploy app code"
databricks apps start "${NAME_PREFIX}" "${PROFILE_ARG[@]}" || true
# The app deploys from the bundle's synced files dir (workspace.file_path).
FILE_PATH="$(databricks bundle summary -t "${BUNDLE_TARGET}" "${VARS[@]}" "${PROFILE_ARG[@]}" -o json \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["workspace"]["file_path"])')"
databricks apps deploy "${NAME_PREFIX}" --source-code-path "${FILE_PATH}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

APP_URL="$(databricks apps get "${NAME_PREFIX}" "${PROFILE_ARG[@]}" -o json \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["url"])')"
echo "DQX_MCP_SERVER_URL=${APP_URL}"
[ -n "${GITHUB_OUTPUT:-}" ] && echo "server_url=${APP_URL}" >> "$GITHUB_OUTPUT"
[ -n "${GITHUB_ENV:-}" ] && echo "DQX_MCP_SERVER_URL=${APP_URL}" >> "$GITHUB_ENV"
