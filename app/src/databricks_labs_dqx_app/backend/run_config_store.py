"""Stage oversized task-runner configs in the Lakebase ``dq_run_configs`` table.

Databricks Jobs reject ``run_now`` payloads whose job parameters exceed
10,000 characters (JSON representation). Monitored tables with many applied
rules can exceed that limit when the full ``checks`` list is inlined in
``config_json``. When that happens the app upserts the fully-resolved config
into the ``dq_run_configs`` Lakebase table keyed by ``run_id`` and passes a
tiny stub ``{"__manifest__": true}`` instead. The task runner reads the config
from Lakebase using the ``run_id``.

The stub also carries the few small config fields the app reads back from
the Jobs API (see *MANIFEST_SUMMARY_KEYS*), so a staged run can still be
attributed to its table and recognised as a preview without a SQL lookup.
"""

import json
import logging
from typing import Any

from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql
from databricks_labs_dqx_app.backend.sql_utils import validate_object_id

logger = logging.getLogger(__name__)

# Databricks Jobs hard limit on job_parameters JSON size.
JOB_PARAMETERS_CHAR_LIMIT = 10_000

# Marker key in the inline stub passed to the task runner.
MANIFEST_CONFIG_KEY = "__manifest__"

# Small config fields copied into the stub so Jobs API readers
# (services.task_runner_runs) can name a staged run's table and tell previews
# apart without reading the staged config. The runner ignores them: it loads
# the full config from the table whenever the manifest key is set.
MANIFEST_SUMMARY_KEYS = ("source_table_fqn", "skip_history", "run_type")

# Lakebase OLTP table holding run configs that are too large to inline.
RUN_CONFIGS_TABLE = "dq_run_configs"


class RunConfigError(RuntimeError):
    """Base class for failures preparing a run config for submission."""


class RunConfigTooLargeError(RunConfigError):
    """Raised when a run config cannot be submitted even as a manifest stub."""

    def __init__(self, size: int, *, limit: int = JOB_PARAMETERS_CHAR_LIMIT) -> None:
        self.size = size
        self.limit = limit
        super().__init__(
            f"The run configuration is too large to submit ({size} characters in job parameters; limit is {limit})."
        )


class RunConfigStagingError(RunConfigError):
    """Raised when an oversized run config can't be staged to the manifest table."""

    def __init__(self, run_id: str, table: str, cause: Exception) -> None:
        self.run_id = run_id
        self.table = table
        super().__init__(
            f"Could not stage the run configuration for run {run_id} to {table}: {cause}. "
            f"Check that Lakebase is reachable and that the table exists (run the app's migrations)."
        )


class RunConfigStagingUnavailableError(RunConfigError):
    """Raised when an oversized run config needs Lakebase staging but Lakebase is disabled.

    Oversized configs are staged to the *dq_run_configs* Lakebase table and read back
    by the task runner over Postgres. With the Delta OLTP fallback (Lakebase disabled)
    the runner has no Postgres connection to read from, so submission fails fast with
    actionable guidance rather than staging to a table the runner can never reach.
    """

    def __init__(self, size: int, dialect: str, *, limit: int = JOB_PARAMETERS_CHAR_LIMIT) -> None:
        self.size = size
        self.dialect = dialect
        self.limit = limit
        super().__init__(
            f"The run configuration is too large to inline in job parameters "
            f"({size} characters; limit is {limit}) and must be staged to the Lakebase "
            f"'{RUN_CONFIGS_TABLE}' table, but Lakebase is not enabled (OLTP backend is "
            f"'{dialect}', not 'postgres'). Enable Lakebase to submit large rule sets, or "
            f"reduce the number of checks applied to this table."
        )


def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


def job_parameters_size(job_parameters: dict[str, str]) -> int:
    """Return the JSON character count Databricks enforces on job parameters."""
    return len(_compact_json(job_parameters))


def build_inline_config_payload(config: dict[str, Any]) -> str:
    """Serialize *config* for inline job submission."""
    return _compact_json(config)


def build_manifest_config_payload(config: dict[str, Any]) -> str:
    """Serialize the stub the task runner uses to load *config* back from the table."""
    summary = {key: config[key] for key in MANIFEST_SUMMARY_KEYS if key in config}
    return _compact_json({MANIFEST_CONFIG_KEY: True, **summary})


def stage_config_to_table(sql: OltpExecutorProtocol, run_id: str, config: dict[str, Any]) -> None:
    """Upsert *config* as a row in the ``dq_run_configs`` table.

    The runner reads the row back by *run_id* (already a job parameter), so an
    oversized rule set never travels through job parameters. Upserting the row
    using the ``run_id`` means a resubmit replaces the row rather than accumulating
    duplicate rows. The portable ``upsert`` helper renders the JSON
    payload as a bound literal, so regex/multiline check bodies round-trip
    intact without hand-escaping.
    """
    validate_object_id(run_id)
    table = sql.fqn(RUN_CONFIGS_TABLE)
    compacted_run_config = _compact_json(config)
    sql.upsert(
        table,
        key_cols={"run_id": run_id},
        value_cols={"config": compacted_run_config, "created_at": RawSql("current_timestamp()")},
    )
    logger.info("Staged run config for %s in %s (%d chars)", run_id, table, len(compacted_run_config))


def delete_staged_config(sql: OltpExecutorProtocol, run_id: str) -> None:
    """Best-effort delete of a staged ``dq_run_configs`` row.

    Called when job submission fails after the config was staged, so an
    oversized payload is not orphaned in the table until the retention sweep.
    Failures are intentionally logged, not raised because he caller is already
    handling a submit error.
    """
    try:
        validate_object_id(run_id)
        table = sql.fqn(RUN_CONFIGS_TABLE)
        sql.delete(table, where={"run_id": run_id})
        logger.info("Deleted orphaned staged run config for %s from %s", run_id, table)
    except Exception as exc:
        logger.warning("Could not delete orphaned staged run config for %s: %s", run_id, exc)


def prepare_config_json(
    sql: OltpExecutorProtocol,
    *,
    run_id: str,
    config: dict[str, Any],
    job_parameters_without_config: dict[str, str],
) -> str:
    """Return ``config_json`` for job submission, staging to the manifest table when needed."""
    inline = build_inline_config_payload(config)
    params = {**job_parameters_without_config, "config_json": inline}
    if job_parameters_size(params) <= JOB_PARAMETERS_CHAR_LIMIT:
        return inline

    # Validate the run_id to ensure a malformed id surfaces as ValueError
    # instead of being caught below and mislabeled as a Lakebase staging failure.
    validate_object_id(run_id)
    # Staging targets the Lakebase table and is read back by the runner over
    # Postgres. With the Delta OLTP fallback (Lakebase disabled) there is no
    # Postgres connection for the runner, so fail fast with actionable guidance
    # rather than staging to a table the runner can never read.
    dialect = getattr(sql, "dialect", "")
    if dialect != "postgres":
        raise RunConfigStagingUnavailableError(job_parameters_size(params), dialect)
    try:
        stage_config_to_table(sql, run_id, config)
    except RunConfigError:
        raise
    except Exception as exc:
        raise RunConfigStagingError(run_id, sql.fqn(RUN_CONFIGS_TABLE), exc) from exc
    manifest = build_manifest_config_payload(config)
    manifest_params = {**job_parameters_without_config, "config_json": manifest}
    manifest_size = job_parameters_size(manifest_params)
    if manifest_size > JOB_PARAMETERS_CHAR_LIMIT:
        raise RunConfigTooLargeError(manifest_size)
    return manifest
