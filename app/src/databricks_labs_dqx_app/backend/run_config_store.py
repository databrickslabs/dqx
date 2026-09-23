"""Stage oversized task-runner configs in a Delta manifest table.

Databricks Jobs reject ``run_now`` payloads whose job parameters exceed
10,000 characters (JSON representation). Monitored tables with many applied
rules can exceed that limit when the full ``checks`` list is inlined in
``config_json``. When that happens the app writes the config to the
``dq_run_configs`` Delta table keyed by ``run_id`` and passes a tiny stub
``{"__manifest__": true}`` instead. The task runner reads the row back via
Spark using the ``run_id``.
"""

import json
import logging
from typing import Any

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_json_for_sql_string_literal, escape_sql_string

logger = logging.getLogger(__name__)

# Databricks Jobs hard limit on job_parameters JSON size.
JOB_PARAMETERS_CHAR_LIMIT = 10_000

# Marker key in the inline stub passed to the task runner.
MANIFEST_CONFIG_KEY = "__manifest__"

# Delta table holding run configs that are too large to inline.
RUN_CONFIGS_TABLE = "dq_run_configs"


class RunConfigTooLargeError(RuntimeError):
    """Raised when a run config cannot be submitted even as a manifest stub."""

    def __init__(self, size: int, *, limit: int = JOB_PARAMETERS_CHAR_LIMIT) -> None:
        self.size = size
        self.limit = limit
        super().__init__(
            f"The run configuration is too large to submit ({size} characters in job "
            f"parameters; limit is {limit})."
        )


def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


def job_parameters_size(job_parameters: dict[str, str]) -> int:
    """Return the JSON character count Databricks enforces on job parameters."""
    return len(_compact_json(job_parameters))


def build_inline_config_payload(config: dict[str, Any]) -> str:
    """Serialize *config* for inline job submission."""
    return _compact_json(config)


def build_manifest_config_payload() -> str:
    """Serialize the stub the task runner uses to load a staged config from the table."""
    return _compact_json({MANIFEST_CONFIG_KEY: True})


def stage_config_to_table(sql: SqlExecutor, run_id: str, config: dict[str, Any]) -> None:
    """Persist *config* as one row in the ``dq_run_configs`` manifest table.

    The runner reads the row back by *run_id* (already a job parameter), so an oversized
    rule set never travels through job parameters. The JSON is stored as text and escaped
    for the Delta string-literal path (backslash then quote doubling) so regex/multiline
    check bodies round-trip intact.
    """
    table = sql.fqn(RUN_CONFIGS_TABLE)
    compacted_run_config = _compact_json(config)
    escaped_run_id = escape_sql_string(run_id)
    escaped_run_config = escape_json_for_sql_string_literal(compacted_run_config)
    stmt = f"INSERT INTO {table} (run_id, config, created_at) VALUES ('{escaped_run_id}', '{escaped_run_config}', current_timestamp())"  # noqa: S608
    sql.execute(stmt)
    logger.info("Staged run config for %s in %s (%d chars)", run_id, table, len(compacted_run_config))


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

    stage_config_to_table(sql, run_id, config)
    manifest = build_manifest_config_payload()
    manifest_params = {**job_parameters_without_config, "config_json": manifest}
    manifest_size = job_parameters_size(manifest_params)
    if manifest_size > JOB_PARAMETERS_CHAR_LIMIT:
        raise RunConfigTooLargeError(manifest_size)
    return manifest
