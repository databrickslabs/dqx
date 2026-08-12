#!/usr/bin/env bash
# Tear down the isolated CI deployment created by ci_deploy.sh. Best-effort; never fails the job.
set -uo pipefail

NAME_PREFIX="${NAME_PREFIX:-mcp-dqx-ci}"
# Must mirror ci_deploy.sh's target selection: each bundle target has its own state root, so a
# deploy made under dev-coverage (DQX_MCP_COVERAGE_DIR set) must be destroyed under it too.
if [ -n "${DQX_MCP_COVERAGE_DIR:-}" ]; then
  BUNDLE_TARGET="${BUNDLE_TARGET:-dev-coverage}"
else
  BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"
fi
PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

# Must match ci_deploy.sh: the schema name is part of the resource identities, so destroy has to
# resolve the same variables the deploy did.
TMP_SCHEMA="$(printf '%s' "${NAME_PREFIX}_tmp" | tr -c 'A-Za-z0-9_' '_')"

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG:-}"
      --var "tmp_schema_name=${TMP_SCHEMA}")

cd "$(dirname "$0")/.."  # mcp-server/

echo "::group::bundle destroy (${NAME_PREFIX})"
# One command: the schema and both volumes are bundle resources now, so this removes them along with
# the app and the runner job — no follow-up `fs rm`/`volumes delete` needed, and no shared artifacts
# volume that concurrent runs could clobber (each run has its own schema).
databricks bundle destroy -t "${BUNDLE_TARGET}" --auto-approve "${VARS[@]}" "${PROFILE_ARG[@]}" || true
echo "::endgroup::"
