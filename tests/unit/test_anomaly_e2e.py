"""Local contracts for the packaged anomaly test's artifacts and remote evidence."""

import hashlib
import json
from pathlib import Path
from datetime import timedelta
from zipfile import ZipFile
from unittest.mock import create_autospec

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, OperationFailed, PermissionDenied
from databricks.sdk.service.jobs import (
    Run,
    RunOutput,
    RunTask,
    RunState,
    RunLifeCycleState,
    RunResultState,
    RunStatus,
    TerminationDetails,
    TerminationCodeCode,
)

from tests.anomaly_e2e import (
    PACKAGED_MODEL_CONFIGS,
    PackagedAnomalyCase,
    packaged_anomaly_tasks,
    safe_packaged_result,
    safe_packaged_task_failure,
    wheel_manifest,
)

from tests.e2e.test_anomaly_distributed_scoring import (
    test_run_anomaly_distributed_scoring_notebook as run_packaged_anomaly,
)

pytest_plugins = ("pytester",)


def test_wheel_manifest_rejects_stale_or_incomplete_source(tmp_path: Path) -> None:
    package = tmp_path / "src" / "databricks" / "labs" / "dqx"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("value = 1\n")
    wheel = tmp_path / "test.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("databricks/labs/dqx/__init__.py", "value = 1\n")
    manifest = wheel_manifest(wheel, tmp_path, "a" * 40)
    assert manifest["wheel_sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()
    (package / "__init__.py").write_text("value = 2\n")
    with pytest.raises(ValueError, match="stale"):
        wheel_manifest(wheel, tmp_path, "a" * 40)
    (package / "new_module.py").write_text("value = 3\n")
    with pytest.raises(ValueError, match="complete"):
        wheel_manifest(wheel, tmp_path, "a" * 40)


def test_wheel_manifest_allows_only_build_version_difference(tmp_path: Path) -> None:
    package = tmp_path / "src" / "databricks" / "labs" / "dqx"
    package.mkdir(parents=True)
    (package / "__version__.py").write_text('__version__ = "1.0"\n')
    wheel = tmp_path / "test.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("databricks/labs/dqx/__version__.py", '__version__ = "1.0+ci"\n')
    assert wheel_manifest(wheel, tmp_path, "a" * 40)["files"]
    with pytest.raises(ValueError, match="revision"):
        wheel_manifest(wheel, tmp_path, "branch-name")


@pytest.mark.parametrize("serverless", [False, True])
def test_packaged_tasks_share_artifact_and_reload_in_new_task(serverless: bool) -> None:
    parameters = {"wheel_sha256": "a" * 64}
    tasks = packaged_anomaly_tasks("/test/notebook", parameters, serverless=serverless, cluster_id="cluster")
    assert [task.task_key for task in tasks] == ["train", "score"]
    assert tasks[1].depends_on and tasks[1].depends_on[0].task_key == "train"
    for task in tasks:
        assert task.notebook_task and task.notebook_task.base_parameters
        assert task.notebook_task.base_parameters["wheel_sha256"] == parameters["wheel_sha256"]
        assert task.notebook_task.base_parameters["phase"] == task.task_key
        assert task.existing_cluster_id == (None if serverless else "cluster")
        assert task.new_cluster is None and task.max_retries == 0
    assert "phase" not in parameters


def test_classic_packaged_tasks_require_explicit_configured_compute() -> None:
    with pytest.raises(ValueError, match="configured cluster"):
        packaged_anomaly_tasks("/test/notebook", {}, serverless=False, cluster_id=None)


def test_remote_provenance_is_verified_and_payload_free() -> None:
    case = PackagedAnomalyCase(1, "a" * 64, "b" * 40, True)
    result = {
        "status": "passed",
        "phase": "score",
        "completed_cases": sorted(
            f"{name}:{mode}:{flag}"
            for name, _, _ in PACKAGED_MODEL_CONFIGS
            for mode in ("driver", "distributed")
            for flag in ("off", "on")
        ),
        "wheel_sha256": case.wheel_sha256,
        "revision": case.revision,
        "spark": "4.2.0",
        "untrusted": "SECRET",
        "environments": [
            {
                "source": source,
                "sources_verified": True,
                "python": "3.12.3",
                "packages": {"shap": "0.49.1", "untrusted": "SECRET"},
                "hostname": "SECRET",
            }
            for source in ("installed", "worker")
        ],
    }
    safe = safe_packaged_result(json.dumps(result), case, "score")
    assert safe["requested_compute"] == "serverless" and safe["spark"] == "4.2.0"
    assert "SECRET" not in json.dumps(safe)
    completed = result.pop("completed_cases")
    with pytest.raises(ValueError, match="scoring matrix"):
        safe_packaged_result(json.dumps(result), case, "score")
    result["completed_cases"] = completed[:-1]
    with pytest.raises(ValueError, match="scoring matrix"):
        safe_packaged_result(json.dumps(result), case, "score")
    result["completed_cases"] = completed
    result["wheel_sha256"] = "c" * 64
    with pytest.raises(ValueError, match="artifact"):
        safe_packaged_result(json.dumps(result), case, "score")


@pytest.mark.parametrize("payload", ["not json", "[]", "null", "{}"])
def test_invalid_remote_provenance_fails_safely(payload: str) -> None:
    with pytest.raises(ValueError, match="Packaged anomaly"):
        safe_packaged_result(payload, PackagedAnomalyCase(1, "a" * 64, "b" * 40, False), "score")


@pytest.mark.parametrize("source,verified", [("installed", True), ("worker", False), ("arbitrary", True)])
def test_both_verified_driver_and_worker_evidence_are_required(source: str, verified: bool) -> None:
    case = PackagedAnomalyCase(1, "a" * 64, "b" * 40, False)
    environment = {"source": source, "sources_verified": verified, "python": "3.12.3", "packages": {}}
    result = {
        "status": "passed",
        "phase": "score",
        "completed_cases": sorted(
            f"{name}:{mode}:{flag}"
            for name, _, _ in PACKAGED_MODEL_CONFIGS
            for mode in ("driver", "distributed")
            for flag in ("off", "on")
        ),
        "wheel_sha256": case.wheel_sha256,
        "revision": case.revision,
        "environments": [environment, environment],
    }
    with pytest.raises(ValueError, match="verify"):
        safe_packaged_result(json.dumps(result), case, "score")


@pytest.mark.parametrize("absent_resource", ["version", "model", "all"])
def test_packaged_model_factory_cleans_remaining_resources_after_notfound(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, absent_resource: str
) -> None:
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    pytester.makeconftest(
        rf'''
from pathlib import Path
from unittest.mock import create_autospec
import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import ModelVersionInfo, SchemaInfo
from tests.e2e.conftest import make_anomaly_model_intent

@pytest.fixture
def packaged_anomaly_schema() -> SchemaInfo:
    return SchemaInfo(full_name="catalog.schema")

@pytest.fixture
def ws():
    workspace = create_autospec(WorkspaceClient, instance=True)
    if {absent_resource!r} == "all":
        workspace.model_versions.list.side_effect = NotFound("synthetic absent model")
    else:
        workspace.model_versions.list.return_value = [ModelVersionInfo(version=1), ModelVersionInfo(version=2)]

    def delete_version(*, full_name: str, version: int) -> None:
        with Path("cleanup-attempts").open("a") as output:
            output.write(f"version-{{version}}\n")
        if {absent_resource!r} == "version" and version == 1:
            raise NotFound("synthetic absent version")

    def delete_model(*, full_name: str) -> None:
        with Path("cleanup-attempts").open("a") as output:
            output.write("model\n")
        if {absent_resource!r} in ("model", "all"):
            raise NotFound("synthetic absent model")

    workspace.model_versions.delete.side_effect = delete_version
    workspace.registered_models.delete.side_effect = delete_model
    return workspace
'''
    )
    pytester.makepyfile(
        """
def test_train(make_anomaly_model_intent):
    make_anomaly_model_intent()
    raise RuntimeError("synthetic training failure")
"""
    )

    result = pytester.runpytest("-q", "--tb=line", "-p", "no:cacheprovider", "-p", "no:rerunfailures")

    result.assert_outcomes(failed=1)
    attempts = pytester.path / "cleanup-attempts"
    if absent_resource == "all":
        # The shared cleanup helper still attempts idempotent parent deletion when
        # listing versions reports NotFound, and safely tolerates its absence.
        assert attempts.read_text().splitlines() == ["model"]
    else:
        assert attempts.read_text().splitlines() == ["version-1", "version-2", "model"]
    assert "synthetic training failure" in result.stdout.str()


@pytest.mark.parametrize("reverse_order,unreadable", [(False, False), (True, False), (True, True)])
def test_packaged_failures_report_both_tasks_and_message_fallback(
    caplog: pytest.LogCaptureFixture, reverse_order: bool, unreadable: bool
) -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.jobs.run_now.return_value.run_id = 100
    workspace.jobs.run_now.return_value.result.side_effect = OperationFailed("synthetic failed run")
    tasks = [
        RunTask(
            task_key="train",
            run_id=101,
            state=RunState(
                life_cycle_state=RunLifeCycleState.TERMINATED,
                result_state=RunResultState.FAILED,
                state_message="ModuleNotFoundError: No module named 'SENSITIVE_MODULE'",
            ),
        ),
        RunTask(
            task_key="score",
            run_id=102,
            state=RunState(
                life_cycle_state=RunLifeCycleState.SKIPPED,
                result_state=RunResultState.UPSTREAM_FAILED,
            ),
        ),
    ]
    workspace.jobs.get_run.return_value = Run(tasks=list(reversed(tasks)) if reverse_order else tasks)
    outputs = {
        101: RunOutput(error="ModuleNotFoundError: No module named 'SENSITIVE_MODULE'"),
        102: RunOutput(),
    }

    def read_output(run_id: int) -> RunOutput:
        if unreadable and run_id == 101:
            raise PermissionDenied("SENSITIVE_PERMISSION_PAYLOAD")
        return outputs[run_id]

    workspace.jobs.get_run_output.side_effect = read_output
    with pytest.raises(AssertionError, match="Packaged anomaly task failed"):
        run_packaged_anomaly(workspace, PackagedAnomalyCase(1, "a" * 64, "b" * 40, False, "cluster"))
    records = [
        json.loads(record.message.split("Packaged anomaly task failed: ", 1)[1])
        for record in caplog.records
        if "Packaged anomaly task failed: " in record.message
    ]
    assert len(records) == 2
    failures = {record["phase"]: record for record in records}
    assert failures["train"]["result_state"] == "FAILED"
    assert failures["score"]["result_state"] == "UPSTREAM_FAILED"
    assert failures["train"]["diagnostics"]["error_types"] == ["ModuleNotFoundError"]
    assert failures["train"]["diagnostics"]["hints"] == ["missing_python_dependency"]
    assert failures["train"]["output_error_type"] == ("PermissionDenied" if unreadable else None)
    assert "SENSITIVE_MODULE" not in caplog.text and "SENSITIVE_PERMISSION_PAYLOAD" not in caplog.text
    workspace.jobs.cancel_run.assert_not_called()


@pytest.mark.parametrize("channel", ["error", "state", "termination"])
def test_task_failure_uses_available_message_channels_without_payloads(channel: str) -> None:
    message = "ImportError: cannot import name 'SENSITIVE_NAME'"
    task = RunTask(
        task_key="train",
        state=RunState(
            life_cycle_state=RunLifeCycleState.INTERNAL_ERROR,
            result_state=RunResultState.FAILED,
            state_message=message if channel == "state" else None,
        ),
        status=RunStatus(
            termination_details=TerminationDetails(
                code=TerminationCodeCode.LIBRARY_INSTALLATION_ERROR,
                message=message if channel == "termination" else None,
            )
        ),
    )
    output = RunOutput(error=message) if channel == "error" else None
    failure = safe_packaged_task_failure(task, output)
    assert failure["termination_code"] == "LIBRARY_INSTALLATION_ERROR"
    diagnostics = failure["diagnostics"]
    assert isinstance(diagnostics, dict)
    assert diagnostics["error_types"] == ["ImportError"]
    assert diagnostics["hints"] == ["python_import_failure"]
    assert "SENSITIVE_NAME" not in json.dumps(failure)


@pytest.mark.parametrize(
    "failure",
    [
        DatabricksError("Synthetic polling failure"),
        TimeoutError("Synthetic polling timeout"),
        KeyboardInterrupt(),
        pytest.fail.Exception("Synthetic pytest timeout"),
    ],
    ids=["sdk_error", "timeout", "keyboard_interrupt", "pytest_timeout"],
)
def test_interrupted_packaged_polling_cancels_only_the_owned_run(failure: BaseException) -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.jobs.run_now.return_value.run_id = 123
    workspace.jobs.run_now.return_value.result.side_effect = failure
    with pytest.raises(type(failure)) as caught:
        run_packaged_anomaly(workspace, PackagedAnomalyCase(1, "a" * 64, "b" * 40, True))
    assert caught.value is failure
    workspace.jobs.cancel_run.assert_called_once_with(run_id=123)
    workspace.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once_with(
        run_id=123, timeout=timedelta(minutes=5)
    )
    workspace.jobs.get_run.assert_not_called()


def test_unreadable_failed_run_is_cancelled_before_propagating_polling_failure() -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.jobs.run_now.return_value.run_id = 123
    workspace.jobs.run_now.return_value.result.side_effect = OperationFailed("Synthetic failed run")
    failure = PermissionDenied("Synthetic unreadable run")
    workspace.jobs.get_run.side_effect = failure
    with pytest.raises(PermissionDenied) as caught:
        run_packaged_anomaly(workspace, PackagedAnomalyCase(1, "a" * 64, "b" * 40, True))
    assert caught.value is failure
    workspace.jobs.cancel_run.assert_called_once_with(run_id=123)
    workspace.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once_with(
        run_id=123, timeout=timedelta(minutes=5)
    )


@pytest.mark.parametrize(
    "stage,cleanup_failure",
    [
        ("cancel_run", DatabricksError("SENSITIVE_CLEANUP_DETAILS")),
        ("cancel_run", OperationFailed("SENSITIVE_CLEANUP_DETAILS")),
        ("wait_get_run_job_terminated_or_skipped", TimeoutError("SENSITIVE_CLEANUP_DETAILS")),
        ("cancel_run", RuntimeError("SENSITIVE_CLEANUP_DETAILS")),
        ("wait_get_run_job_terminated_or_skipped", pytest.fail.Exception("SENSITIVE_CLEANUP_DETAILS")),
    ],
    ids=[
        "cancel_sdk_error",
        "cancel_operation_failed",
        "termination_timeout",
        "unexpected_cleanup_error",
        "cleanup_pytest_timeout",
    ],
)
def test_cleanup_failure_preserves_original_polling_failure(stage: str, cleanup_failure: BaseException, caplog) -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.jobs.run_now.return_value.run_id = 123
    failure = DatabricksError("Synthetic original polling failure")
    workspace.jobs.run_now.return_value.result.side_effect = failure
    getattr(workspace.jobs, stage).side_effect = cleanup_failure
    with pytest.raises(DatabricksError) as caught:
        run_packaged_anomaly(workspace, PackagedAnomalyCase(1, "a" * 64, "b" * 40, True))
    assert caught.value is failure
    workspace.jobs.cancel_run.assert_called_once_with(run_id=123)
    assert "cancellation did not confirm termination" in caplog.text
    assert "SENSITIVE_CLEANUP_DETAILS" not in caplog.text


def test_cancelled_terminal_run_does_not_report_unresolved_cleanup(caplog) -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.jobs.run_now.return_value.run_id = 123
    failure = DatabricksError("Synthetic original polling failure")
    workspace.jobs.run_now.return_value.result.side_effect = failure
    workspace.jobs.wait_get_run_job_terminated_or_skipped.side_effect = OperationFailed("Synthetic terminal run")

    with pytest.raises(DatabricksError) as caught:
        run_packaged_anomaly(workspace, PackagedAnomalyCase(1, "a" * 64, "b" * 40, True))

    assert caught.value is failure
    workspace.jobs.cancel_run.assert_called_once_with(run_id=123)
    workspace.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once_with(
        run_id=123, timeout=timedelta(minutes=5)
    )
    assert "cancellation did not confirm termination" not in caplog.text
