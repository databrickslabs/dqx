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
#   DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID  application id of the workspace SP the runner job runs as.
#                                        Optional: a real deploy sets this to a dedicated
#                                        least-privilege SP (the deploying identity then needs the
#                                        "User"/servicePrincipal.user role on it). When unset (CI),
#                                        run_as defaults to the *deploying identity* itself — resolved
#                                        via `databricks current-user me` — so run_as == creator and
#                                        no cross-SP binding grant is needed.
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
PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

# The runner job's run_as SP. Use the explicit override if given (a real deploy's dedicated SP);
# otherwise default to the *deploying identity* — whoever the CLI authenticates as — so run_as ==
# the job's creator and Databricks needs no servicePrincipal.user grant to bind it (pinning a
# *different* SP requires that grant, which is what made a hard-coded runner SP 403 in CI).
RUNNER_SP="${DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID:-}"
if [ -z "$RUNNER_SP" ]; then
  RUNNER_SP="$(databricks current-user me "${PROFILE_ARG[@]}" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("userName",""))')"
  echo "runner_service_principal_id unset; defaulting run_as to the deploying identity: ${RUNNER_SP}"
fi
: "${RUNNER_SP:?could not resolve the runner run_as SP (set DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID, or ensure the deploy identity is resolvable via 'databricks current-user me')}"

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG}"
      --var "runner_service_principal_id=${RUNNER_SP}")

cd "$(dirname "$0")/.."  # mcp-server/

echo "::group::ensure artifacts volume (${DQX_MCP_TEST_CATALOG}.dqx_mcp_tmp.dqx_artifacts)"
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

echo "::group::deploy + start app"
# Use `bundle run` (not raw `apps deploy --source-code-path`) so the app's runtime config —
# command + env, incl. DQX_CATALOG — comes from the bundle's inline `config:` block (there is no
# app.yaml). This matches `make mcp-deploy`; raw `apps deploy` reads app.yaml from the source and
# would fail with "No command to run" now that the config lives in databricks.yml.
databricks bundle run mcp-dqx -t "${BUNDLE_TARGET}" "${VARS[@]}" "${PROFILE_ARG[@]}"
echo "::endgroup::"

# Emit the app URL and the app's service principal (application id). The SP is the identity
# the runner job runs as, so tests that exercise the writing tools (save_checks /
# apply_checks_and_save_to_table) grant it write access on their throwaway schema.
read -r APP_URL APP_SP < <(databricks apps get "${NAME_PREFIX}" "${PROFILE_ARG[@]}" -o json \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["url"], d.get("service_principal_client_id",""))')
echo "DQX_MCP_SERVER_URL=${APP_URL}"
echo "DQX_MCP_APP_SERVICE_PRINCIPAL=${APP_SP}"
# The runner job's resolved run_as SP — tests grant it READ on the seed contract volume.
echo "DQX_MCP_RUNNER_SERVICE_PRINCIPAL=${RUNNER_SP}"
# Use if-blocks (not `[ ] && echo`): the latter returns non-zero when the var is unset (local
# runs), which would make this script exit 1 on an otherwise successful deploy.
if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "server_url=${APP_URL}" >> "$GITHUB_OUTPUT"; fi
if [ -n "${GITHUB_ENV:-}" ]; then echo "DQX_MCP_SERVER_URL=${APP_URL}" >> "$GITHUB_ENV"; fi
