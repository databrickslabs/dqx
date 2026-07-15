"""Stage oversized task-runner configs on a UC volume.

Databricks Jobs reject ``run_now`` payloads whose job parameters exceed
10,000 characters (JSON representation). Monitored tables with many applied
rules can exceed that limit when the full ``checks`` list is inlined in
``config_json``. When ``DQX_WHEELS_VOLUME`` is configured we write the
config to ``{volume}/run-configs/{run_id}.json`` and pass a tiny stub
``{"__staged__": "<path>"}`` instead.
"""

from __future__ import annotations

import io
import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Databricks Jobs hard limit on job_parameters JSON size.
JOB_PARAMETERS_CHAR_LIMIT = 10_000

# Marker key in the inline stub passed to the task runner.
STAGED_CONFIG_KEY = "__staged__"

_RUN_CONFIGS_DIR = "run-configs"


class RunConfigTooLargeError(RuntimeError):
    """Raised when a run config cannot be submitted inline or staged."""

    def __init__(self, size: int, *, limit: int = JOB_PARAMETERS_CHAR_LIMIT, staged: bool = False) -> None:
        self.size = size
        self.limit = limit
        self.staged = staged
        if staged:
            msg = (
                f"The run configuration is too large to submit even after staging "
                f"({size} characters in job parameters; limit is {limit})."
            )
        else:
            msg = (
                f"The run configuration is too large to submit ({size} characters in job "
                f"parameters; limit is {limit}). Configure DQX_WHEELS_VOLUME so configs can "
                f"be staged, or reduce the number of applied rules."
            )
        super().__init__(msg)


def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"))


def staged_config_path(wheels_volume: str, run_id: str) -> str:
    base = (wheels_volume or "").rstrip("/")
    if not base:
        raise ValueError("wheels_volume is required to stage a run config")
    return f"{base}/{_RUN_CONFIGS_DIR}/{run_id}.json"


def job_parameters_size(job_parameters: dict[str, str]) -> int:
    """Return the JSON character count Databricks enforces on job parameters."""
    return len(_compact_json(job_parameters))


def stage_config_to_volume(ws: WorkspaceClient, wheels_volume: str, run_id: str, config: dict[str, Any]) -> str:
    """Persist *config* on the wheels volume and return its UC path."""
    path = staged_config_path(wheels_volume, run_id)
    payload = _compact_json(config).encode("utf-8")
    ws.files.upload(path, io.BytesIO(payload), overwrite=True)
    logger.info("Staged run config for %s at %s (%d bytes)", run_id, path, len(payload))
    return path


def build_inline_config_payload(config: dict[str, Any]) -> str:
    """Serialize *config* for inline job submission."""
    return _compact_json(config)


def build_staged_config_payload(staged_path: str) -> str:
    """Serialize the stub the task runner uses to load a staged config."""
    return _compact_json({STAGED_CONFIG_KEY: staged_path})


def prepare_config_json(
    ws: WorkspaceClient,
    *,
    wheels_volume: str,
    run_id: str,
    config: dict[str, Any],
    job_parameters_without_config: dict[str, str],
) -> str:
    """Return ``config_json`` for job submission, staging to volume when needed."""
    inline = build_inline_config_payload(config)
    params = {**job_parameters_without_config, "config_json": inline}
    if job_parameters_size(params) <= JOB_PARAMETERS_CHAR_LIMIT:
        return inline

    volume = (wheels_volume or "").strip()
    if not volume:
        raise RunConfigTooLargeError(job_parameters_size(params))

    staged_path = stage_config_to_volume(ws, volume, run_id, config)
    staged = build_staged_config_payload(staged_path)
    staged_params = {**job_parameters_without_config, "config_json": staged}
    staged_size = job_parameters_size(staged_params)
    if staged_size > JOB_PARAMETERS_CHAR_LIMIT:
        raise RunConfigTooLargeError(staged_size, staged=True)
    return staged


def read_volume_json(ws: WorkspaceClient, path: str) -> dict[str, Any]:
    """Load a JSON object previously written by :func:`stage_config_to_volume`."""
    resp = ws.files.download(path)
    if not resp.contents:
        raise RuntimeError(f"Staged run config is empty at {path}")
    raw = resp.contents.read().decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Staged run config at {path} is not a JSON object")
    return parsed


def resolve_config(ws: WorkspaceClient, config: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    """Expand a staged stub into the full config dict.

    Returns ``(config, staged_path)`` where *staged_path* is set when the
  config was loaded from a volume (for post-run cleanup).
    """
    staged = config.get(STAGED_CONFIG_KEY)
    if isinstance(staged, str) and staged.strip():
        path = staged.strip()
        return read_volume_json(ws, path), path
    return config, None


def delete_staged_config(ws: WorkspaceClient, path: str) -> None:
    """Best-effort removal of a staged config file."""
    try:
        ws.files.delete(path)
        logger.info("Deleted staged run config at %s", path)
    except Exception as exc:
        logger.debug("Could not delete staged run config at %s: %s", path, exc)
