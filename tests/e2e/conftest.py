import json
import logging
from collections.abc import Callable, Generator, MutableMapping
from io import BytesIO
import os
import subprocess
import time
from datetime import timedelta
from pathlib import Path
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import (
    Job,
    JobSettings,
    NotebookTask,
    PerformanceTarget,
    Task,
    TerminationTypeType,
    Run,
)
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Kind

from databricks.sdk.service.catalog import SchemaInfo, VolumeInfo
from databricks.sdk.service.ml import CreateExperimentResponse
from databricks.labs.blueprint.paths import WorkspacePath
from databricks.labs.pytester.fixtures.baseline import factory

from tests.anomaly_resources import drop_registered_model
from tests.anomaly_e2e import (
    PACKAGED_MODEL_CONFIGS,
    packaged_model_suffixes,
    AnomalyModelCleanup,
    PackagedAnomalyCase,
    packaged_anomaly_tasks,
    wheel_manifest,
)
from tests.constants import TEST_CATALOG

logger = logging.getLogger(__name__)
logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")


def _get_local_git_branch() -> str | None:
    """Get the current git branch name, or None if not available."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None


def _get_remote_url(branch: str) -> str | None:
    """Get the git remote URL for the current branch, or None if not available."""
    try:
        remote = subprocess.check_output(
            ["git", "config", f"branch.{branch}.remote"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        url = subprocess.check_output(
            ["git", "config", f"remote.{remote}.url"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if url.endswith(".git"):
            url = url[:-4]
        # Convert SSH URLs to HTTPS so pip can use them (git+git@... is invalid)
        if url.startswith("git@github.com:"):
            url = url.replace("git@github.com:", "https://github.com/", 1)
        return url
    except Exception:
        return None


@pytest.fixture
def library_ref() -> str:
    """
    Returns a git reference to DQX library for testing.

    In GitHub Actions: uses REF_NAME env var set from github.ref_name.
        - For pull_request events this is a PR merge ref like "123/merge",
          which becomes "refs/pull/123/merge".
        - For push events this is a branch name like "main",
          which is used directly as the git ref.
    Locally: automatically detects and uses current git branch.
    Default: falls back to the main branch (no ref suffix).

    Note: For local testing, the code must be pushed to GitHub (branch or PR).
    """
    base_ref = "git+https://github.com/databrickslabs/dqx"
    is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"

    if is_github_actions:
        ref_name = os.getenv("REF_NAME")
        if ref_name:
            # PR merge refs look like "123/merge"; branch names don't contain "/"
            if "/" in ref_name:
                return f"{base_ref}.git@refs/pull/{ref_name}"
            return f"{base_ref}.git@{ref_name}"
    else:
        # Local behavior: use current git branch
        ref_name = _get_local_git_branch()
        if ref_name and ref_name != "HEAD":
            remote_url = _get_remote_url(ref_name)
            if remote_url:
                return f"git+{remote_url}.git@{ref_name}"
            return f"{base_ref}.git@{ref_name}"

    # Default to main branch
    return base_ref


def new_classic_job_cluster(ws=None):
    if ws is None:
        ws = WorkspaceClient()
    spark_version = ws.clusters.select_spark_version(latest=True)
    node_type = ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)
    return ClusterSpec(
        is_single_node=True,
        node_type_id=node_type,
        spark_version=spark_version,
        kind=Kind.CLASSIC_PREVIEW,
        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
        single_user_name=ws.current_user.me().user_name,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
    )


def run_notebook_job(
    notebook_path: Path,
    make_notebook,
    make_job,
    library_reference=None,
    base_parameters=None,
    timeout_minutes=30,
    task_key="notebook_task",
    ws=None,
    new_cluster=None,
):
    if ws is None:
        ws = WorkspaceClient()
    with open(notebook_path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    notebook_fs_path = notebook.as_fuse().as_posix()
    params = base_parameters or {}
    if library_reference:
        params.setdefault("test_library_ref", library_reference)
    notebook_task = NotebookTask(notebook_path=notebook_fs_path, base_parameters=params)
    job = make_job(tasks=[Task(task_key=task_key, notebook_task=notebook_task, new_cluster=new_cluster)])
    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=timeout_minutes),
        callback=lambda r: validate_run_status(r, ws),
    )
    logger.info(f"Job run {run.run_id} completed successfully for {task_key}")


def validate_run_status(run: Run, client: WorkspaceClient) -> None:
    """
    Validates that a job task run completed successfully.

    Args:
        run: `Run` object returned from a `WorkspaceClient.jobs.submit(...)` command
        client: `WorkspaceClient` object for getting task output
    """
    task = run.tasks[0]
    termination_details = run.status.termination_details

    run_output = client.jobs.get_run_output(task.run_id)
    logger.info("Run output:")
    logger.info(run_output.as_dict())
    assert termination_details.type == TerminationTypeType.SUCCESS, (
        f"Run of '{task.task_key}' "
        f"failed with message: {run_output.error}, "
        f"error trace: {run_output.error_trace}"
    )


def run_job_and_validate(
    ws: WorkspaceClient,
    job_id: int,
    task_key: str,
    *,
    timeout: timedelta = timedelta(minutes=30),
    max_attempts: int = 3,
) -> Run:
    """Trigger a demo job, wait for it to succeed, and retry serverless ``INTERNAL_ERROR``.

    Serverless workloads occasionally terminate in ``RunLifeCycleState.INTERNAL_ERROR`` — a
    platform/environment transient (e.g. a cold-start library install or a model-serving blip on
    demos that pip-install heavy extras or call an LLM) rather than a data-quality failure. The SDK
    waiter surfaces that state as *OperationFailed*, so the run is re-submitted a few times with
    backoff. A genuine task failure terminates normally and is caught by *validate_run_status*
    (*AssertionError*), which is not a transient and is not retried.

    Args:
        ws: Workspace client used to trigger and poll the run.
        job_id: The job to run.
        task_key: Task key, used only for logging.
        timeout: Per-attempt wait for the run to terminate.
        max_attempts: Total attempts before giving up (>= 1).

    Returns:
        The successful terminated *Run*.
    """
    last_error: OperationFailed | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            waiter = ws.jobs.run_now_and_wait(job_id, timeout=timeout)
            run = ws.jobs.wait_get_run_job_terminated_or_skipped(
                run_id=waiter.run_id,
                timeout=timeout,
                callback=lambda r: validate_run_status(r, ws),
            )
            logger.info(f"Job run {run.run_id} completed successfully for {task_key}")
            return run
        except OperationFailed as exc:
            # Only serverless platform transients (INTERNAL_ERROR) are retried; every other
            # OperationFailed (and any AssertionError from validate_run_status) propagates.
            if "INTERNAL_ERROR" not in str(exc):
                raise
            last_error = exc
            logger.warning(
                f"'{task_key}' hit serverless INTERNAL_ERROR on attempt {attempt}/{max_attempts}; " f"retrying: {exc}"
            )
            time.sleep(min(2**attempt, 30))
    assert last_error is not None  # loop ran at least once
    raise last_error


@pytest.fixture(scope="session")
def anomaly_wheel() -> tuple[Path, dict[str, object]]:
    """Build once, or verify the exact wheel supplied by the calling CI job."""
    repository = Path(__file__).resolve().parents[2]
    supplied = os.environ.get("DQX_PREBUILT_WHEEL")
    if supplied:
        wheel = Path(supplied).resolve(strict=True)
    else:
        completed = subprocess.run(["make", "build"], cwd=repository, capture_output=True, check=False)
        if completed.returncode:
            raise AssertionError("The packaged anomaly wheel build failed")
        wheels = list((repository / "dist").glob("databricks_labs_dqx-*.whl"))
        if len(wheels) != 1:
            raise AssertionError("Expected exactly one freshly built DQX wheel")
        wheel = wheels[0]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    return wheel, wheel_manifest(wheel, repository, revision)


@pytest.fixture
def packaged_anomaly_schema(make_schema: Callable[..., SchemaInfo]) -> SchemaInfo:
    """Own the registry schema until model cleanup has completed."""
    return make_schema(catalog_name=TEST_CATALOG)


@pytest.fixture
def make_anomaly_model_intent(
    ws: WorkspaceClient, packaged_anomaly_schema: SchemaInfo
) -> Generator[Callable[..., AnomalyModelCleanup], None, None]:
    """Register model cleanup before training can partially create a model."""

    def create(suffix: str = "distributed_model") -> AnomalyModelCleanup:
        assert packaged_anomaly_schema.full_name
        return AnomalyModelCleanup(f"{packaged_anomaly_schema.full_name}.{suffix}")

    failures: list[str] = []

    def remove(model: AnomalyModelCleanup) -> None:
        failures.extend(drop_registered_model(ws, model.full_name))

    yield from factory("anomaly model cleanup intent", create, remove)
    if failures:
        raise AssertionError(
            f"Packaged anomaly model cleanup failed in {len(failures)} operations "
            f"({', '.join(sorted(set(failures)))}); owned schema requires inspection"
        )


@pytest.fixture
def packaged_anomaly_case(
    ws: WorkspaceClient,
    debug_env: MutableMapping[str, str],
    anomaly_wheel: tuple[Path, dict[str, object]],
    packaged_anomaly_schema: SchemaInfo,
    make_volume: Callable[..., VolumeInfo],
    make_experiment: Callable[..., CreateExperimentResponse],
    make_anomaly_model_intent: Callable[..., AnomalyModelCleanup],
    make_notebook: Callable[..., WorkspacePath],
    make_job: Callable[..., Job],
) -> PackagedAnomalyCase:
    """Upload one immutable wheel and register every resource before the run."""
    wheel, manifest = anomaly_wheel
    volume = make_volume(catalog_name=packaged_anomaly_schema.catalog_name, schema_name=packaged_anomaly_schema.name)
    assert volume.catalog_name and volume.schema_name and volume.name
    directory = f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}"
    remote_wheel = f"{directory}/{wheel.name}"
    with wheel.open("rb") as content:
        ws.files.upload(remote_wheel, content, overwrite=False)
    manifest_path = f"{directory}/manifest.json"
    ws.files.upload(manifest_path, BytesIO(json.dumps(manifest).encode()), overwrite=False)
    experiment = make_experiment()
    assert experiment.experiment_id
    # Each ensemble member is a separate registered model. Own all names before
    # submitting training, including models a partially failed task may create.
    for suffix in packaged_model_suffixes():
        make_anomaly_model_intent(suffix=suffix)
    parameters = {
        "wheel_path": remote_wheel,
        "manifest_path": manifest_path,
        "wheel_sha256": str(manifest["wheel_sha256"]),
        "source_revision": str(manifest["revision"]),
        "model_name": f"{packaged_anomaly_schema.full_name}.distributed_model",
        "model_configs": json.dumps(PACKAGED_MODEL_CONFIGS),
        "registry_table": f"{packaged_anomaly_schema.full_name}.registry",
        "experiment_id": experiment.experiment_id,
    }
    notebook_path = Path(__file__).parent / "notebooks" / "anomaly_distributed_scoring_notebook.py"
    with notebook_path.open("rb") as content:
        notebook = make_notebook(content=content, format=ImportFormat.SOURCE)
    serverless = bool(debug_env.get("DATABRICKS_SERVERLESS_COMPUTE_ID"))
    tasks = packaged_anomaly_tasks(
        notebook.as_fuse().as_posix(), parameters, serverless=serverless, cluster_id=ws.config.cluster_id
    )
    job = make_job(tasks=tasks)
    assert job.job_id is not None
    ws.jobs.update(
        job_id=job.job_id,
        new_settings=JobSettings(
            timeout_seconds=3600,
            performance_target=PerformanceTarget.STANDARD if serverless else None,
        ),
    )
    return PackagedAnomalyCase(
        job.job_id,
        str(manifest["wheel_sha256"]),
        str(manifest["revision"]),
        serverless,
        None if serverless else ws.config.cluster_id,
    )
