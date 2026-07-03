# Databricks notebook source

# COMMAND ----------

"""
One-time setup notebook for the DQX MCP server.

Run after `databricks bundle deploy` to create the temp view schema and
grant all required UC permissions. Safe to re-run (all statements are idempotent).

Grants:
  Catalog level:
    - users          → USE CATALOG
    - app SP         → USE CATALOG
    - runner SP      → USE CATALOG, CREATE SCHEMA (creates each caller's private per-user output
                       schema dqx_mcp_<user> on demand for save_checks / apply_checks_and_save_to_table).
                       Only granted when provided.
  Schema level (catalog.tmp) — the deploy principal OWNS the schema (so setup can create the
  results volume + manage grants on every run); the SPs get MANAGE, which is enough to DROP
  temp views in UC (owner / parent-schema owner / MANAGE can DROP):
    - users          → USE SCHEMA, CREATE TABLE (so OBO token can create views)
    - app SP         → USE SCHEMA, SELECT, MANAGE (in-app stale-view sweeper)
    - runner SP      → USE SCHEMA, SELECT, MANAGE (the runner job runs as this dedicated
                       workspace SP: it reads through definer's-rights views and drops its own
                       temp view at the end of each run). Only granted when provided.
  Volume level (catalog.tmp.mcp_results):
    - app SP, runner SP → READ VOLUME, WRITE VOLUME (result-file exchange)
  Volume level (catalog.tmp.dqx_artifacts) — hosts the runner wheels (workspace.artifact_path):
    - runner SP → READ VOLUME (so the serverless env can install the runner wheel). The volume is
      created out-of-band before `bundle deploy` (scripts/ensure_artifacts_volume.sh); setup only
      ensures it exists and applies the grant.

Note: outputs (apply_checks_and_save_to_table / save_checks) go to the caller's own SP-owned
per-user schema (dqx_mcp_<user>), created on demand via the runner SP's CREATE SCHEMA grant — no
per-deployment write grants are needed. The runner SP still needs read access to any contract/checks
files callers load; keep those in locations the runner SP can reach.

Parameters:
  - catalog_name: UC catalog for temp views + per-user output schemas (required; passed as a
                  bundle var at deploy time — the catalog name is not sensitive, so no secret scope)
  - app_name: Databricks App name (e.g. 'mcp-dqx') — used to look up the app SP
  - users_group: Group name for all users (default: 'account users')
  - runner_service_principal_id: application id of the runner job's run_as SP (optional)
"""

# COMMAND ----------

import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dqx-mcp-setup")

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_ ]+$")
# Service principal application ids are UUIDs (hex + hyphens). Validated separately because the
# identifier charset above disallows hyphens.
_SP_ID_RE = re.compile(r"^[0-9a-fA-F-]+$")
_PLACEHOLDER_SP_ID = "00000000-0000-0000-0000-000000000000"


def _validate_identifier(value, label):
    """Validate a SQL identifier to prevent injection via backtick breakout."""
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid {label}: '{value}'. Only alphanumeric characters, underscores, and spaces are allowed."
        )
    return value


def _validate_sp_id(value, label):
    """Validate a service-principal application id (UUID) before SQL interpolation."""
    if not _SP_ID_RE.match(value):
        raise ValueError(f"Invalid {label}: '{value}'. Expected a service principal application id (UUID).")
    return value


# A user/SP/group principal (email addresses, application ids, group names) for GRANT/ALTER OWNER.
_SAFE_PRINCIPAL_RE = re.compile(r"^[A-Za-z0-9._%+\-@ ]+$")


def _validate_principal(value, label):
    """Validate a UC principal name before SQL interpolation (backtick-breakout guard)."""
    if not _SAFE_PRINCIPAL_RE.match(value):
        raise ValueError(f"Invalid {label}: '{value}'.")
    return value


# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("app_name", "mcp-dqx")
dbutils.widgets.text("users_group", "account users")
dbutils.widgets.text("runner_service_principal_id", "")
dbutils.widgets.text("runner_job_id", "")

catalog_name = dbutils.widgets.get("catalog_name")
app_name = dbutils.widgets.get("app_name")
users_group = dbutils.widgets.get("users_group")
runner_sp = dbutils.widgets.get("runner_service_principal_id").strip()
runner_job_id = dbutils.widgets.get("runner_job_id").strip()

# catalog_name is passed as a bundle var at deploy time (required — it also builds the artifacts
# volume path). No secret scope is involved: the catalog name is not sensitive.
if not catalog_name:
    raise ValueError("catalog_name must be provided (pass --var catalog_name=<catalog> at deploy time)")

_validate_identifier(catalog_name, "catalog_name")
# users_group is interpolated into the GRANT statements below, so it must be validated too
# (a backtick would otherwise break out of the quoted identifier). app_name is NOT validated
# here: it is only passed to ws.apps.get() (never interpolated into SQL) and legitimately
# contains hyphens (e.g. 'mcp-dqx'), which this identifier charset disallows.
_validate_identifier(users_group, "users_group")

# The runner job's run_as service principal (a dedicated workspace SP, distinct from the app SP).
# Empty or the all-zeros placeholder means it was not configured for this deploy — skip its grants
# (the job's run_as would then be misconfigured, which the bundle deploy surfaces separately).
runner_sp_configured = bool(runner_sp) and runner_sp != _PLACEHOLDER_SP_ID
if runner_sp_configured:
    _validate_sp_id(runner_sp, "runner_service_principal_id")

logger.info(
    f"Setting up DQX MCP: catalog={catalog_name}, app={app_name}, users_group={users_group}, "
    f"runner_sp={'<set>' if runner_sp_configured else '<unset>'}"
)

# COMMAND ----------

# Look up the app's service principal
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
app = ws.apps.get(app_name)
sp_id = app.service_principal_id

# Resolve SP application_id — UC GRANTs use application_id, not display_name
sp = ws.service_principals.get(sp_id)
sp_principal = sp.application_id
logger.info(f"App SP: display_name={sp.display_name}, application_id={sp_principal} (id={sp_id})")

# COMMAND ----------

schema_name = "tmp"
# Constant today, but validated as defense-in-depth since it is interpolated into the DDL below.
_validate_identifier(schema_name, "schema_name")

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
logger.info(f"Schema `{catalog_name}`.`{schema_name}` ready")

# Keep the temp schema owned by the setup (deploy) principal so this job can create the results
# volume and manage grants on every run. Earlier versions transferred ownership to the app SP; we
# now keep it with the deployer and grant the SPs MANAGE instead (enough to drop temp views), which
# also lets the deployer CREATE VOLUME below. Reclaiming here is idempotent and fixes workspaces
# where a prior run already handed ownership to the app SP.
deploy_principal = _validate_principal(ws.current_user.me().user_name, "deploy principal")
spark.sql(f"ALTER SCHEMA `{catalog_name}`.`{schema_name}` OWNER TO `{deploy_principal}`")
logger.info(f"Schema owner set to deploy principal {deploy_principal}")

# COMMAND ----------

# All grants — single source of truth
grants = [
    # Catalog-level
    f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{users_group}`",
    f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{sp_principal}`",
    # Schema-level: users can create views via OBO token
    f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{users_group}`",
    f"GRANT CREATE TABLE ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{users_group}`",
    # Schema-level: app SP reads through views and drops stale views (the in-app sweeper). MANAGE
    # (not ownership) is enough to DROP views created by any user, so the deployer keeps ownership.
    f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{sp_principal}`",
    f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{sp_principal}`",
    f"GRANT MANAGE ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{sp_principal}`",
]

if runner_sp_configured:
    # The runner job runs as a dedicated workspace SP. It must: use the catalog; read through the
    # definer's-rights views (USE SCHEMA + SELECT); and DROP its own temp view at the end of each
    # run (MANAGE — a non-owner with MANAGE can drop, same as the app SP / schema owner).
    grants += [
        f"GRANT USE CATALOG ON CATALOG `{catalog_name}` TO `{runner_sp}`",
        # save_checks / apply_checks_and_save_to_table write each caller's outputs to their own
        # private per-user schema (dqx_mcp_<user>), which the runner creates on demand and owns —
        # so the runner SP needs CREATE SCHEMA on the catalog.
        f"GRANT CREATE SCHEMA ON CATALOG `{catalog_name}` TO `{runner_sp}`",
        f"GRANT USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{runner_sp}`",
        f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{runner_sp}`",
        f"GRANT MANAGE ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{runner_sp}`",
    ]

for sql in grants:
    logger.info(f"Executing: {sql}")
    spark.sql(sql)

# COMMAND ----------

# Results volume: the runner job (python_wheel_task) writes each operation's JSON result here,
# keyed by run id, and the app reads it back via the Files API (no SQL warehouse needed — the app
# has no warehouse of its own). Both SPs get READ+WRITE: the runner writes results, the app reads
# them and sweeps stale files. The setup principal owns the schema (above), so it can create this.
results_volume = "mcp_results"
_validate_identifier(results_volume, "results_volume")
volume_fqn = f"`{catalog_name}`.`{schema_name}`.`{results_volume}`"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_fqn}")
logger.info(f"Results volume {volume_fqn} ready")

volume_grants = [f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {volume_fqn} TO `{sp_principal}`"]
if runner_sp_configured:
    volume_grants.append(f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {volume_fqn} TO `{runner_sp}`")
for sql in volume_grants:
    logger.info(f"Executing: {sql}")
    spark.sql(sql)

# COMMAND ----------

# Grant the app SP CAN_MANAGE_RUN on the runner job so the app can submit runs and poll their
# results. The app→job resource binding (databricks.yml apps.resources) did not reliably land this
# grant in the job ACL, so apply it explicitly here. Idempotent (update_permissions is additive).
if runner_job_id:
    from databricks.sdk.service.jobs import JobAccessControlRequest, JobPermissionLevel

    ws.jobs.update_permissions(
        job_id=runner_job_id,
        access_control_list=[
            JobAccessControlRequest(
                service_principal_name=sp_principal, permission_level=JobPermissionLevel.CAN_MANAGE_RUN
            )
        ],
    )
    logger.info(f"Granted app SP {sp_principal} CAN_MANAGE_RUN on runner job {runner_job_id}")
else:
    logger.warning("runner_job_id not provided — skipping app-SP CAN_MANAGE_RUN grant on the runner job")

# COMMAND ----------

# Artifacts volume: the bundle's workspace.artifact_path points here, so `bundle deploy` uploads
# the runner wheels into this volume. The serverless environment then installs them as the runner
# SP, which needs READ VOLUME. A UC volume grant is explicit and volume-wide, so — unlike the old
# workspace-folder approach — it doesn't depend on ACL inheritance from the deployer's home folder.
# The volume is created before deploy (scripts/ensure_artifacts_volume.sh); CREATE ... IF NOT EXISTS
# here is idempotent defense-in-depth (the deploy principal owns the schema, so it can create it).
artifacts_volume = "dqx_artifacts"
_validate_identifier(artifacts_volume, "artifacts_volume")
artifacts_volume_fqn = f"`{catalog_name}`.`{schema_name}`.`{artifacts_volume}`"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {artifacts_volume_fqn}")
logger.info(f"Artifacts volume {artifacts_volume_fqn} ready")

if runner_sp_configured:
    sql = f"GRANT READ VOLUME ON VOLUME {artifacts_volume_fqn} TO `{runner_sp}`"
    logger.info(f"Executing: {sql}")
    spark.sql(sql)
else:
    logger.warning("runner SP not provided — skipping runner-SP READ VOLUME grant on the artifacts volume")

# COMMAND ----------

logger.info(
    "Setup complete — schema owned by the deploy principal; app SP and runner SP granted MANAGE "
    "(temp-view cleanup) and results-volume READ/WRITE; runner SP granted CREATE SCHEMA on the "
    "catalog (per-user output schemas) and READ VOLUME on the artifacts volume; app SP granted "
    "CAN_MANAGE_RUN on the runner job."
)
