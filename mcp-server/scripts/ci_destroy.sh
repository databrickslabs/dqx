#!/usr/bin/env bash
# Tear down the isolated CI deployment created by ci_deploy.sh. Best-effort; never fails the job.
set -uo pipefail

NAME_PREFIX="${NAME_PREFIX:-mcp-dqx-ci}"
BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"
PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG:-}")

cd "$(dirname "$0")/.."  # mcp-server/

echo "::group::bundle destroy (${NAME_PREFIX})"
databricks bundle destroy -t "${BUNDLE_TARGET}" --auto-approve "${VARS[@]}" "${PROFILE_ARG[@]}" || true
echo "::endgroup::"

# The artifacts volume is created out-of-band (ensure_artifacts_volume.sh) and shared across
# concurrent runs, so bundle destroy does not touch it. Remove only THIS run's per-prefix wheel
# subfolder (workspace.artifact_path = .../dqx_artifacts/${name_prefix}); deleting the whole volume
# would wipe a concurrently-running deploy's wheels. Best-effort (ignore if absent / catalog unset).
if [ -n "${DQX_MCP_TEST_CATALOG:-}" ]; then
  databricks fs rm -r "dbfs:/Volumes/${DQX_MCP_TEST_CATALOG}/dqx_mcp_tmp/dqx_artifacts/${NAME_PREFIX}" \
    "${PROFILE_ARG[@]}" 2>/dev/null || true
fi
