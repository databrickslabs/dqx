#!/usr/bin/env bash
# Deploy an isolated copy of the DQX MCP server (bundle + runner job + setup + app) for
# integration testing, and print the deployed app URL. Idempotent / safe to re-run.
#
# Auth comes from the standard Databricks CLI env (DATABRICKS_HOST + DATABRICKS_TOKEN, or a
# configured profile). Override the bundle's name/catalog via the env vars below so this never
# collides with a real deployment. The catalog is passed as a bundle var (no secret scope — the
# catalog name is not sensitive, and the app reads it from a plain config env value).
#
# Env:
#   NAME_PREFIX                          resource name prefix (default: mcp-dqx-ci)
#   DQX_MCP_TEST_CATALOG                 catalog the setup job may create/drop a temp schema in (required)
#   DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID  application id of the workspace SP the runner job runs as
#                                        (required — the runner job's run_as; the deploying identity
#                                        needs the "User" role on it)
#   BUNDLE_TARGET                        bundle target (default: dev)
#   DATABRICKS_PROFILE                   optional CLI profile (else relies on DATABRICKS_HOST/TOKEN)
#
# Emits: DQX_MCP_SERVER_URL=<url> to stdout and, if set, to $GITHUB_OUTPUT.
set -euo pipefail

NAME_PREFIX="${NAME_PREFIX:-mcp-dqx-ci}"
BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"
# Required config (CI: provided by the acceptance harness env + the deploy fixture).
: "${DATABRICKS_HOST:?DATABRICKS_HOST is not set (workspace URL)}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is not set (workspace auth)}"
: "${DQX_MCP_TEST_CATALOG:?DQX_MCP_TEST_CATALOG is not set (a catalog the deployer can create schemas in)}"
: "${DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID:?DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID is not set (application id of the runner-job run_as SP)}"
PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG}"
      --var "runner_service_principal_id=${DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID}")

cd "$(dirname "$0")/.."  # mcp-server/

echo "::group::ensure artifacts volume (${DQX_MCP_TEST_CATALOG}.tmp.dqx_artifacts)"
# workspace.artifact_path is a UC volume; it must exist before `bundle deploy` uploads the wheels.
DQX_MCP_CATALOG="${DQX_MCP_TEST_CATALOG}" DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-}" \
  ./scripts/ensure_artifacts_volume.sh
echo "::endgroup::"

echo "::group::bundle deploy (${NAME_PREFIX}, target ${BUNDLE_TARGET})"
# The bundle uses `engine: direct` (no Terraform), so this does not download a provider.
databricks bundle deploy -t "${BUNDLE_TARGET}" "${VARS[@]}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

echo "::group::run setup job (UC grants + temp-schema ownership)"
databricks bundle run dqx_setup -t "${BUNDLE_TARGET}" "${VARS[@]}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

echo "::group::start + deploy app code"
databricks apps start "${NAME_PREFIX}" "${PROFILE_ARG[@]}" || true
# `apps deploy` requires the app COMPUTE to be ACTIVE. A brand-new app's app_status stays
# "UNAVAILABLE" until code is deployed, so we wait on compute_status (which `start` brings up),
# not app_status.
for _ in $(seq 1 60); do
  state="$(databricks apps get "${NAME_PREFIX}" "${PROFILE_ARG[@]}" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("compute_status",{}).get("state",""))')"
  [ "${state}" = "ACTIVE" ] && break
  echo "waiting for app compute to become ACTIVE (state=${state:-unknown})..."
  sleep 10
done
# The app deploys from the bundle's synced files dir (workspace.file_path).
FILE_PATH="$(databricks bundle summary -t "${BUNDLE_TARGET}" "${VARS[@]}" "${PROFILE_ARG[@]}" -o json \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["workspace"]["file_path"])')"
databricks apps deploy "${NAME_PREFIX}" --source-code-path "${FILE_PATH}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

# Emit the app URL and the app's service principal (application id). The SP is the identity
# the runner job runs as, so tests that exercise the writing tools (save_checks /
# apply_checks_and_save_to_table) grant it write access on their throwaway schema.
read -r APP_URL APP_SP < <(databricks apps get "${NAME_PREFIX}" "${PROFILE_ARG[@]}" -o json \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["url"], d.get("service_principal_client_id",""))')
echo "DQX_MCP_SERVER_URL=${APP_URL}"
echo "DQX_MCP_APP_SERVICE_PRINCIPAL=${APP_SP}"
# Use if-blocks (not `[ ] && echo`): the latter returns non-zero when the var is unset (local
# runs), which would make this script exit 1 on an otherwise successful deploy.
if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "server_url=${APP_URL}" >> "$GITHUB_OUTPUT"; fi
if [ -n "${GITHUB_ENV:-}" ]; then echo "DQX_MCP_SERVER_URL=${APP_URL}" >> "$GITHUB_ENV"; fi
