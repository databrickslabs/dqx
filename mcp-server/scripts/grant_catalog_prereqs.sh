#!/usr/bin/env bash
# Apply the ONE-TIME, catalog-level Unity Catalog grants that `bundle deploy` cannot.
#
# Everything the MCP bundle OWNS (the tmp schema, the mcp_results and dqx_artifacts volumes, the
# runner job ACL) carries native `grants:`/`permissions:` in databricks.yml and is applied by
# `bundle deploy`. But the *catalog* pre-exists and is not bundle-managed, so grants on it can only
# be applied out of band — which is what this script does. Idempotent: re-running is a no-op.
#
# Run as the catalog owner (or a principal holding MANAGE on it). Uses the Unity Catalog permissions
# API rather than SQL, so no SQL warehouse is required.
#
# Env:
#   DQX_MCP_CATALOG      catalog to grant on (required)
#   DQX_MCP_USERS_GROUP  group that may use the server (default: 'account users')
#   DQX_MCP_RUNNER_SP    application id of the runner job's run_as SP (optional but recommended)
#   DQX_MCP_APP_SP       application id of the app's SP. Usually omitted: when DQX_MCP_APP_NAME is
#                        set (or defaulted) this script resolves it from the deployed app.
#   DQX_MCP_APP_NAME     deployed app name to resolve the app SP from (default: mcp-dqx). Set empty
#                        to skip resolution, e.g. when running this BEFORE the first deploy.
#   DATABRICKS_PROFILE   optional CLI profile
set -euo pipefail

: "${DQX_MCP_CATALOG:?DQX_MCP_CATALOG is not set (the catalog to grant on)}"
USERS_GROUP="${DQX_MCP_USERS_GROUP:-account users}"
RUNNER_SP="${DQX_MCP_RUNNER_SP:-}"
APP_SP="${DQX_MCP_APP_SP:-}"
APP_NAME="${DQX_MCP_APP_NAME-mcp-dqx}"

PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

# Resolve the app's service principal when it wasn't supplied. The app SP is created with the app,
# so this only succeeds after `bundle deploy` — which is exactly when the deploy flow calls this,
# and why the install needs no second "grant, then restart" pass. A missing app (first run before
# any deploy) is fine: the grants for users and the runner SP still apply.
if [ -z "${APP_SP}" ] && [ -n "${APP_NAME}" ]; then
  APP_SP="$(databricks apps get "${APP_NAME}" ${PROFILE_ARG[@]+"${PROFILE_ARG[@]}"} -o json 2>/dev/null \
    | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("service_principal_client_id",""))
except Exception: print("")' 2>/dev/null || true)"
  [ -n "${APP_SP}" ] && echo "Resolved the app service principal from app '${APP_NAME}': ${APP_SP}"
fi

# USE_CATALOG for everyone who touches the catalog, plus CREATE_SCHEMA for the runner SP: it creates
# a per-user `dqx_mcp_<user>` output schema on demand for save_checks / apply_checks_and_save_to_table.
CHANGES="$(
  DQX_USERS_GROUP="$USERS_GROUP" DQX_RUNNER_SP="$RUNNER_SP" DQX_APP_SP="$APP_SP" python3 - <<'PY'
import json, os

changes = [{"principal": os.environ["DQX_USERS_GROUP"], "add": ["USE_CATALOG"]}]
runner = os.environ.get("DQX_RUNNER_SP", "").strip()
if runner:
    changes.append({"principal": runner, "add": ["USE_CATALOG", "CREATE_SCHEMA"]})
app = os.environ.get("DQX_APP_SP", "").strip()
if app:
    changes.append({"principal": app, "add": ["USE_CATALOG"]})
print(json.dumps({"changes": changes}))
PY
)"

echo "Applying catalog grants on ${DQX_MCP_CATALOG}:"
echo "  ${CHANGES}"
databricks grants update catalog "${DQX_MCP_CATALOG}" ${PROFILE_ARG[@]+"${PROFILE_ARG[@]}"} --json "${CHANGES}" >/dev/null
echo "Done. Verify with: databricks grants get catalog ${DQX_MCP_CATALOG}"

if [ -z "$APP_SP" ]; then
  echo ""
  echo "NOTE: the app's service principal could not be resolved (app '${APP_NAME:-<unset>}' not found"
  echo "yet?), so it has NOT been granted USE CATALOG. That is expected when running this before the"
  echo "first deploy — 'make mcp-deploy' runs it again after the app exists. If you are deploying by"
  echo "hand, re-run this after 'bundle deploy' and before starting the app, otherwise the app cannot"
  echo "publish the runner wheel and every data tool fails to install it."
fi
