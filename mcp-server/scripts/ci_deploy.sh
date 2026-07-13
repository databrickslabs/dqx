#!/usr/bin/env bash
# Deploy an isolated copy of the DQX MCP server (schema, volumes, runner job, app) for integration
# testing, and print the deployed app URL. Idempotent / safe to re-run.
#
# Auth comes from the standard Databricks CLI env (DATABRICKS_HOST + DATABRICKS_TOKEN, or a
# configured profile). Override the bundle's name/catalog via the env vars below so this never
# collides with a real deployment. The catalog is passed as a bundle var (no secret scope — the
# catalog name is not sensitive, and the app reads it from a plain config env value).
#
# Env:
#   NAME_PREFIX                          resource name prefix (default: mcp-dqx-ci)
#   DQX_MCP_TEST_CATALOG                 catalog the bundle creates its schema + volumes in (required)
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
# Coverage-enabled test deploys use the dev-coverage bundle target (adds `coverage` to the runner
# job env — production/user deploys never ship it). An explicit BUNDLE_TARGET always wins.
if [ -n "${DQX_MCP_COVERAGE_DIR:-}" ]; then
  BUNDLE_TARGET="${BUNDLE_TARGET:-dev-coverage}"
else
  BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"
fi
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
#
# The fallback only works when the deploying identity is a SERVICE PRINCIPAL, which is the case in
# CI. The bundle declares run_as.service_principal_name, and DAB has no way to switch that key to
# user_name from a variable — so handing it a human's email produces
#   "'<email>' cannot be set as run_as service principal, because it doesn't exist"
# which reads like a missing SP rather than the wrong field. `current-user me` reports an SP's
# application id in userName, so detect the human case by shape (an email has an '@') and fail with
# a message that names the fix.
RUNNER_SP="${DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID:-}"
if [ -z "$RUNNER_SP" ]; then
  RUNNER_SP="$(databricks current-user me ${PROFILE_ARG[@]+"${PROFILE_ARG[@]}"} -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("userName",""))')"
  echo "runner_service_principal_id unset; defaulting run_as to the deploying identity: ${RUNNER_SP}"
  case "$RUNNER_SP" in
    *@*)
      echo "ERROR: the deploying identity (${RUNNER_SP}) is a USER, not a service principal." >&2
      echo "The runner job's run_as must be a service principal, so there is nothing to fall back" >&2
      echo "to here. Create a workspace service principal, grant yourself the 'User' role on it," >&2
      echo "and set DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID to its application id (see the 'Create the" >&2
      echo "runner service principal' section of the DQX MCP Server installation docs)." >&2
      exit 1
      ;;
  esac
fi
: "${RUNNER_SP:?could not resolve the runner run_as SP (set DQX_MCP_RUNNER_SERVICE_PRINCIPAL_ID, or ensure the deploy identity is resolvable via 'databricks current-user me')}"

# Schema identifiers cannot contain '-' but NAME_PREFIX can (mcp-dqx-ci), so sanitise it. A
# per-run schema keeps concurrent CI deploys from sharing the schema and its volumes.
TMP_SCHEMA="$(printf '%s' "${NAME_PREFIX}_tmp" | tr -c 'A-Za-z0-9_' '_')"

VARS=(--var "name_prefix=${NAME_PREFIX}"
      --var "catalog_name=${DQX_MCP_TEST_CATALOG}"
      --var "tmp_schema_name=${TMP_SCHEMA}"
      --var "runner_service_principal_id=${RUNNER_SP}")

cd "$(dirname "$0")/.."  # mcp-server/

# Coverage-enabled deploys only (DQX_MCP_COVERAGE_DIR set): install the TEST-ONLY coverage
# bootstrap wheel into the app. Its .pth starts a coverage tracer at interpreter start, so no
# production module is coverage-aware — see tests/integration_mcp/coverage_bootstrap. The wheel is
# deliberately NOT in the committed requirements.txt (users must not ship test instrumentation):
# append it for the duration of the deploy — the bundle snapshots source files at sync time — and
# always restore the original afterwards so local working trees stay clean (CI checkouts are
# throwaway either way). The runner job gets the same wheel via the dev-coverage bundle target.
if [ -n "${DQX_MCP_COVERAGE_DIR:-}" ]; then
  cp requirements.txt requirements.txt.orig
  trap 'mv requirements.txt.orig requirements.txt' EXIT
  # Built into ./.build, which sync.include ships with the app source, so the relative path
  # resolves in the app container. Resolve the glob here: requirements.txt has no glob support.
  uv build ../tests/integration_mcp/coverage_bootstrap --wheel --out-dir ./.build
  BOOTSTRAP_WHEEL="$(ls -t ./.build/dqx_mcp_coverage_bootstrap-*.whl | head -1)"
  printf './%s\n' "${BOOTSTRAP_WHEEL#./}" >> requirements.txt
fi

echo "::group::one-time catalog grants (users + runner SP)"
# The bundle applies every grant on the objects it owns natively. Catalog-level grants are the
# exception — the catalog pre-exists and is not bundle-managed — so apply them here. The app SP's
# grant comes after the deploy, once its service principal exists.
DQX_MCP_CATALOG="${DQX_MCP_TEST_CATALOG}" DQX_MCP_RUNNER_SP="${RUNNER_SP}" \
  DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-}" ./scripts/grant_catalog_prereqs.sh
echo "::endgroup::"

echo "::group::bundle deploy (${NAME_PREFIX}, target ${BUNDLE_TARGET})"
# Creates the schema, both volumes, the runner job and the app, and applies their native grants.
# The bundle uses `engine: direct` (no Terraform), so this does not download a provider.
databricks bundle deploy -t "${BUNDLE_TARGET}" "${VARS[@]}" ${PROFILE_ARG[@]+"${PROFILE_ARG[@]}"}
echo "::endgroup::"

echo "::group::grant the app SP USE CATALOG (only resolvable after the app exists)"
# The app's SP is created with the app, so this grant cannot be native and cannot precede the
# deploy. Without it the app cannot publish the runner wheel and every data tool fails to install
# it — so this must happen BEFORE the app is started below.
APP_SP_ID="$(databricks apps get "${NAME_PREFIX}" "${PROFILE_ARG[@]}" -o json \
  | python3 -c 'import sys,json; print(json.load(sys.stdin).get("service_principal_client_id",""))')"
if [ -n "${APP_SP_ID}" ]; then
  DQX_MCP_CATALOG="${DQX_MCP_TEST_CATALOG}" DQX_MCP_RUNNER_SP="${RUNNER_SP}" DQX_MCP_APP_SP="${APP_SP_ID}" \
    DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-}" ./scripts/grant_catalog_prereqs.sh
else
  echo "WARNING: could not resolve the app's service principal; the wheel publish may fail"
fi
echo "::endgroup::"

echo "::group::deploy + start app"
# Use `bundle run` (not raw `apps deploy --source-code-path`) so the app's runtime config —
# command + env, incl. DQX_CATALOG — comes from the bundle's inline `config:` block (there is no
# app.yaml). This matches `make mcp-deploy`; raw `apps deploy` reads app.yaml from the source and
# would fail with "No command to run" now that the config lives in databricks.yml.
databricks bundle run mcp-dqx -t "${BUNDLE_TARGET}" "${VARS[@]}" ${PROFILE_ARG[@]+"${PROFILE_ARG[@]}"}
echo "::endgroup::"

echo "::group::wait for the app to publish the runner wheel"
# The app publishes the wheel at startup (server/bootstrap.py) and the runner job installs it from
# that volume path, so a tool call before the publish completes would fail on library install.
# Fail loudly here rather than let the test fail later with a confusing error.
WHEEL_DIR="dbfs:/Volumes/${DQX_MCP_TEST_CATALOG}/${TMP_SCHEMA}/dqx_artifacts"
# Record the outcome in a flag rather than re-running `fs ls` after the loop: a fresh call could hit
# a transient API error moments after the wheel was seen, and the re-check would then abort an
# otherwise healthy deploy.
WHEEL_PUBLISHED=0
for _ in $(seq 1 36); do
  if databricks fs ls "${WHEEL_DIR}" "${PROFILE_ARG[@]}" 2>/dev/null | grep -q 'dqx_mcp_runner-.*\.whl'; then
    echo "runner wheel published under ${WHEEL_DIR}"
    WHEEL_PUBLISHED=1
    break
  fi
  sleep 5
done
if [ "${WHEEL_PUBLISHED}" -ne 1 ]; then
  echo "ERROR: the app did not publish the runner wheel to ${WHEEL_DIR} within 3 minutes." >&2
  echo "Check 'databricks apps logs ${NAME_PREFIX}' for the reason (usually a missing USE CATALOG" >&2
  echo "grant for the app's service principal ${APP_SP_ID:-<unresolved>})." >&2
  exit 1
fi
echo "::endgroup::"

# Emit the app URL and the app's service principal (application id). The SP is the identity
# the runner job runs as, so tests that exercise the writing tools (save_checks /
# apply_checks_and_save_to_table) grant it write access on their throwaway schema.
read -r APP_URL APP_SP < <(databricks apps get "${NAME_PREFIX}" ${PROFILE_ARG[@]+"${PROFILE_ARG[@]}"} -o json \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["url"], d.get("service_principal_client_id",""))')
echo "DQX_MCP_SERVER_URL=${APP_URL}"
echo "DQX_MCP_APP_SERVICE_PRINCIPAL=${APP_SP}"
# The runner job's resolved run_as SP — tests grant it READ on the seed contract volume.
echo "DQX_MCP_RUNNER_SERVICE_PRINCIPAL=${RUNNER_SP}"
# The deployed app's name — a coverage-enabled run stops the app via the Apps API before teardown
# so its graceful shutdown flushes the final coverage data (see conftest.collect_remote_coverage).
echo "DQX_MCP_APP_NAME=${NAME_PREFIX}"
# Use if-blocks (not `[ ] && echo`): the latter returns non-zero when the var is unset (local
# runs), which would make this script exit 1 on an otherwise successful deploy.
if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "server_url=${APP_URL}" >> "$GITHUB_OUTPUT"; fi
if [ -n "${GITHUB_ENV:-}" ]; then echo "DQX_MCP_SERVER_URL=${APP_URL}" >> "$GITHUB_ENV"; fi
