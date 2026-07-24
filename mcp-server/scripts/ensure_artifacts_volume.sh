#!/usr/bin/env bash
# Create the UC volume that hosts the runner wheels, BEFORE `bundle deploy` uploads artifacts
# into it. The bundle's workspace.artifact_path is /Volumes/<catalog>/dqx_mcp_tmp/dqx_artifacts, and DAB
# uploads artifacts during deploy (before it would create any bundle-managed resources), so the
# volume must already exist. Idempotent / safe to re-run — creating an existing schema or volume
# is ignored. The runner job's run_as SP is later granted READ VOLUME here by the setup job.
#
# Env:
#   DQX_MCP_CATALOG     catalog holding the dqx_mcp_tmp schema + artifacts volume (required)
#   DATABRICKS_PROFILE  optional CLI profile (else relies on DATABRICKS_HOST/TOKEN)
set -euo pipefail

: "${DQX_MCP_CATALOG:?DQX_MCP_CATALOG is not set (catalog that will host the dqx_artifacts volume)}"
SCHEMA="dqx_mcp_tmp"
VOLUME="dqx_artifacts"

PROFILE_ARG=()
[ -n "${DATABRICKS_PROFILE:-}" ] && PROFILE_ARG=(--profile "$DATABRICKS_PROFILE")

echo "Ensuring ${DQX_MCP_CATALOG}.${SCHEMA}.${VOLUME} exists (for runner wheels)..."
# Creating an existing schema/volume returns a non-zero "already exists" — tolerate it. Both
# commands create MANAGED objects owned by the calling (deploy) principal, so it can upload wheels.
databricks schemas create "${SCHEMA}" "${DQX_MCP_CATALOG}" "${PROFILE_ARG[@]}" 2>/dev/null || true
databricks volumes create "${DQX_MCP_CATALOG}" "${SCHEMA}" "${VOLUME}" MANAGED "${PROFILE_ARG[@]}" 2>/dev/null || true
echo "Artifacts volume /Volumes/${DQX_MCP_CATALOG}/${SCHEMA}/${VOLUME} ready."
