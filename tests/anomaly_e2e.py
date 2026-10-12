"""Artifact and task contracts for packaged anomaly end-to-end tests."""

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

from databricks.sdk.service.jobs import NotebookTask, RunOutput, RunTask, Task, TaskDependency

from tests.anomaly_diagnostics import dependency_provenance, worker_traceback


@dataclass(frozen=True, repr=False)
class PackagedAnomalyCase:
    """Expected immutable package and the disposable job that exercises it."""

    job_id: int
    wheel_sha256: str
    revision: str
    serverless: bool
    cluster_id: str | None = None


@dataclass(frozen=True, repr=False)
class AnomalyModelCleanup:
    """Register a future model's cleanup before a remote training task starts."""

    full_name: str


def wheel_manifest(wheel: Path, repository: Path, revision: str) -> dict[str, object]:
    """Verify the wheel contains the current source and return public-safe hashes.

    CI intentionally replaces the package version while building its unique wheel,
    so that one file is included in the artifact manifest but excluded from the
    comparison with the restored checkout.
    """
    if not re.fullmatch(r"[a-f0-9]{40,64}", revision):
        raise ValueError("A full source revision is required for packaged anomaly tests")
    files = {}
    with ZipFile(wheel) as archive:
        for name in archive.namelist():
            if not name.startswith("databricks/labs/dqx/") or not name.endswith(".py"):
                continue
            if ".." in Path(name).parts:
                raise ValueError("Unsafe package member in anomaly wheel")
            files[name] = hashlib.sha256(archive.read(name)).hexdigest()
    sources = repository / "src" / "databricks" / "labs" / "dqx"
    expected = {str(path.relative_to(repository / "src")) for path in sources.rglob("*.py")}
    if not expected or set(files) != expected:
        raise ValueError("The anomaly wheel does not contain the complete current package")
    for name, digest in files.items():
        if name.endswith("/__version__.py"):
            continue
        if hashlib.sha256((repository / "src" / name).read_bytes()).hexdigest() != digest:
            raise ValueError("The anomaly wheel is stale; rebuild it from the current checkout")
    return {
        "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "revision": revision,
        "files": files,
    }


def packaged_anomaly_tasks(
    notebook_path: str, parameters: dict[str, str], *, serverless: bool, cluster_id: str | None
) -> list[Task]:
    """Train and reload in separate tasks on the suite's selected compute mode."""
    if not serverless and not cluster_id:
        raise ValueError("Classic packaged anomaly tests require the configured cluster")
    return [
        Task(
            task_key=phase,
            notebook_task=NotebookTask(notebook_path=notebook_path, base_parameters={**parameters, "phase": phase}),
            existing_cluster_id=None if serverless else cluster_id,
            depends_on=[TaskDependency(task_key="train")] if phase == "score" else [],
            max_retries=0,
            timeout_seconds=1800,
        )
        for phase in ("train", "score")
    ]


def safe_packaged_result(result: str, case: PackagedAnomalyCase, phase: str) -> dict[str, object]:
    """Validate remote evidence and return only allowlisted package provenance."""
    try:
        parsed: object = json.loads(result)
    except (ValueError, TypeError):
        raise ValueError("Packaged anomaly task did not return a valid manifest") from None
    if not isinstance(parsed, dict):
        raise ValueError("Packaged anomaly task did not return a manifest object")
    if (
        parsed.get("wheel_sha256") != case.wheel_sha256
        or parsed.get("revision") != case.revision
        or parsed.get("phase") != phase
        or parsed.get("status") != "passed"
    ):
        raise ValueError("Packaged anomaly task provenance does not match the submitted artifact")
    clients = parsed.get("environments")
    if not isinstance(clients, list) or len(clients) < 2:
        raise ValueError("Packaged anomaly task did not verify both driver and worker environments")
    environments = [_safe_environment(client, case) for client in clients]
    if {environment["source"] for environment in environments} != {"installed", "worker"}:
        raise ValueError("Packaged anomaly task did not verify both driver and worker environments")
    spark = parsed.get("spark")
    return {
        "phase": phase,
        "wheel_sha256": case.wheel_sha256,
        "revision": case.revision,
        "requested_compute": "serverless" if case.serverless else "classic",
        "spark": (
            spark if isinstance(spark, str) and re.fullmatch(r"[0-9][A-Za-z0-9.+_-]{0,60}", spark) else "unavailable"
        ),
        "environments": environments,
    }


def _safe_environment(client: object, case: PackagedAnomalyCase) -> dict[str, object]:
    if not isinstance(client, dict):
        raise ValueError("Invalid packaged anomaly environment manifest")
    versions = client.get("packages")
    if not isinstance(versions, dict):
        raise ValueError("Missing packaged anomaly dependency versions")
    safe_versions = {
        name: value for name, value in versions.items() if isinstance(name, str) and isinstance(value, str)
    }
    python = client.get("python")
    source = client.get("source")
    if source not in {"installed", "worker"} or client.get("sources_verified") is not True:
        raise ValueError("Packaged anomaly task did not verify its installed source bytes")
    return dependency_provenance(
        safe_versions,
        python if isinstance(python, str) else "unavailable",
        wheel_sha256=case.wheel_sha256,
        revision=case.revision,
        source=str(source),
    )


def safe_packaged_task_failure(task: RunTask, output: RunOutput | None) -> dict[str, object]:
    """Retain task states and all available error channels without copying their payloads."""
    state = task.state
    details = task.status.termination_details if task.status else None
    messages = {
        "state_message": state.state_message if state else None,
        "termination_message": details.message if details else None,
        "error": output.error if output else None,
        "error_trace": output.error_trace if output else None,
    }
    return {
        "phase": task.task_key if task.task_key in {"train", "score"} else "unknown",
        "life_cycle_state": state.life_cycle_state.value if state and state.life_cycle_state else None,
        "result_state": state.result_state.value if state and state.result_state else None,
        "termination_code": details.code.value if details and details.code else None,
        "output_available": output is not None,
        "message_channels": [name for name, message in messages.items() if message],
        "message_sha256": {
            name: hashlib.sha256(message.encode()).hexdigest() for name, message in messages.items() if message
        },
        "diagnostics": worker_traceback("\n".join(message for message in messages.values() if message)),
    }
