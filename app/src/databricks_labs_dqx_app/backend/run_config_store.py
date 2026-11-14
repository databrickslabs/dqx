"""Stage oversized task-runner configs in the Delta ``dq_run_configs`` table.

Databricks Jobs reject ``run_now`` payloads whose job parameters exceed
10,000 characters (JSON representation). Monitored tables with many applied
rules can exceed that limit when the full ``checks`` list is inlined in
``config_json``. When that happens the app upserts the fully-resolved config
into the ``dq_run_configs`` Delta table keyed by ``run_id`` and passes a
tiny stub ``{"__manifest__": true}`` instead. The task runner reads the config
from the table using the ``run_id`` and deletes the row once the run finishes.

The stub also carries the few small config fields the app reads back from
the Jobs API (see *MANIFEST_SUMMARY_KEYS*), so a staged run can still be
attributed to its table and recognised as a preview without a SQL lookup.
"""

import base64
import json
import logging
from typing import Any

from databricks_labs_dqx_app.backend.sql_executor import RawSql, SqlExecutor
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

# Delta table holding run configs that are too large to inline.
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
            f"Could not stage the run configuration for run {run_id} to {table}: {_first_line(cause)}. "
            f"Check that the SQL warehouse is running and that the table exists (run the app's migrations)."
        )


def _base64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _first_line(exc: Exception) -> str:
    return next(iter(str(exc).splitlines()), "")


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


def stage_config_to_table(sql: SqlExecutor, run_id: str, config: dict[str, Any]) -> None:
    """Upsert *config* as a row in the ``dq_run_configs`` table.

    The runner reads the row back by *run_id* (already a job parameter), so an
    oversized rule set never travels through job parameters. Upserting the row
    using the ``run_id`` means a resubmit replaces the row rather than accumulating
    duplicate rows. The JSON travels base64-encoded and is decoded in SQL, so
    quotes, backslashes, and newlines in check bodies never need escaping.
    """
    validate_object_id(run_id)
    table = sql.fqn(RUN_CONFIGS_TABLE)
    compacted_run_config = _compact_json(config)
    sql.upsert(
        table,
        key_cols={"run_id": run_id},
        value_cols={
            "config": RawSql(f"decode(unbase64('{_base64(compacted_run_config)}'), 'UTF-8')"),
            "created_at": RawSql("current_timestamp()"),
        },
    )
    logger.info("Staged run config for %s in %s (%d chars)", run_id, table, len(compacted_run_config))


def delete_staged_config(sql: SqlExecutor, run_id: str) -> None:
    """Best-effort delete of a staged ``dq_run_configs`` row.

    Called when job submission fails after the config was staged, so an
    oversized payload is not orphaned in the table until the retention sweep.
    Failures are intentionally logged, not raised, because the caller is already
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
    sql: SqlExecutor,
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
    # instead of being caught below and mislabeled as a staging failure.
    validate_object_id(run_id)
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
