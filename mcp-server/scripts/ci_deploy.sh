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
# Required config (CI: provided by the acceptance harness env + the deploy fixture).
: "${DATABRICKS_HOST:?DATABRICKS_HOST is not set (workspace URL)}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is not set (workspace auth)}"
: "${DQX_MCP_TEST_CATALOG:?DQX_MCP_TEST_CATALOG is not set (a catalog the deployer can create schemas in)}"
PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG}"
      --var "config_secret_scope=${CONFIG_SECRET_SCOPE}")

cd "$(dirname "$0")/.."  # mcp-server/

# `bundle deploy` runs `terraform init`, which downloads the databricks/databricks provider
# from registry.terraform.io. On CI runners that fetch is intermittently reset ("EOF") — the
# repo's own e2e demo-bundle test hits the same flakiness and relies on the harness retrying
# it. Our deploy runs in a session-scoped fixture that is NOT retried, so retry here instead.
retry() {
  local attempts="$1" delay="$2"
  shift 2
  local n=1
  until "$@"; do
    if [ "${n}" -ge "${attempts}" ]; then
      echo "command failed after ${attempts} attempts: $*" >&2
      return 1
    fi
    echo "attempt ${n}/${attempts} failed, retrying in ${delay}s: $*" >&2
    sleep "${delay}"
    n=$((n + 1))
  done
}

echo "::group::Configure catalog secret (${CONFIG_SECRET_SCOPE})"
databricks secrets create-scope "${CONFIG_SECRET_SCOPE}" "${PROFILE_ARG[@]}" 2>/dev/null || true
databricks secrets put-secret "${CONFIG_SECRET_SCOPE}" catalog_name \
  --string-value "${DQX_MCP_TEST_CATALOG}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

echo "::group::bundle deploy (${NAME_PREFIX}, target ${BUNDLE_TARGET})"
# --force-lock: a previous, retried attempt may hold the deployment lock (matches the e2e demo).
retry 3 15 databricks bundle deploy -t "${BUNDLE_TARGET}" --force-lock "${VARS[@]}" "${PROFILE_ARG[@]}"
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
