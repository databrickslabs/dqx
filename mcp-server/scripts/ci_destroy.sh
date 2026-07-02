#!/usr/bin/env bash
# Tear down the isolated CI deployment created by ci_deploy.sh. Best-effort; never fails the job.
set -uo pipefail

NAME_PREFIX="${NAME_PREFIX:-mcp-dqx-ci}"
CONFIG_SECRET_SCOPE="${CONFIG_SECRET_SCOPE:-dqx-config-ci}"
BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"
PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG:-}"
      --var "config_secret_scope=${CONFIG_SECRET_SCOPE}")

cd "$(dirname "$0")/.."  # mcp-server/

echo "::group::bundle destroy (${NAME_PREFIX})"
databricks bundle destroy -t "${BUNDLE_TARGET}" --auto-approve "${VARS[@]}" "${PROFILE_ARG[@]}" || true
echo "::endgroup::"

# bundle destroy removes the app; drop the CI secret scope too (ignore if absent).
databricks secrets delete-scope "${CONFIG_SECRET_SCOPE}" "${PROFILE_ARG[@]}" 2>/dev/null || true

# The artifacts volume is created out-of-band (ensure_artifacts_volume.sh), so bundle destroy does
# not remove it. Drop it best-effort (ignore if absent / catalog unset).
if [ -n "${DQX_MCP_TEST_CATALOG:-}" ]; then
  databricks volumes delete "${DQX_MCP_TEST_CATALOG}.tmp.dqx_artifacts" "${PROFILE_ARG[@]}" 2>/dev/null || true
fi
