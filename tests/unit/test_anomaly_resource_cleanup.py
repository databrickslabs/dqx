"""Cleanup survives partial setup and reports unresolved resources without raw payloads."""

from unittest.mock import Mock, call

import pytest
from mlflow.entities import Experiment
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import ModelVersionInfo, RegisteredModelInfo

from tests.anomaly_resources import (
    MLflowExperimentResource,
    ResourceCleanupError,
    delete_mlflow_experiment,
    drop_registered_models_in_schemas,
)

pytest_plugins = ("pytester",)


def test_experiment_deletion_uses_known_id(anomaly_experiment_boundaries: tuple[Mock, Mock]) -> None:
    lookup, delete = anomaly_experiment_boundaries
    resource = MLflowExperimentResource("/Users/synthetic/experiment", "123")

    delete_mlflow_experiment(resource, get_by_name=lookup, delete=delete)

    lookup.assert_not_called()
    delete.assert_called_once_with("123")
    assert "synthetic" not in repr(resource)
    assert "123" not in repr(resource)


def test_partially_created_experiment_is_resolved_by_name(anomaly_experiment_boundaries: tuple[Mock, Mock]) -> None:
    lookup, delete = anomaly_experiment_boundaries
    resource = MLflowExperimentResource("/Users/synthetic/experiment")
    lookup.return_value = Experiment("123", resource.path, "synthetic", "active")

    delete_mlflow_experiment(resource, get_by_name=lookup, delete=delete)

    lookup.assert_called_once_with(resource.path)
    delete.assert_called_once_with("123")


def test_uncreated_experiment_needs_no_deletion(anomaly_experiment_boundaries: tuple[Mock, Mock]) -> None:
    lookup, delete = anomaly_experiment_boundaries
    lookup.return_value = None
    delete_mlflow_experiment(MLflowExperimentResource("synthetic"), get_by_name=lookup, delete=delete)
    delete.assert_not_called()


@pytest.mark.parametrize("boundary", ["lookup", "delete"])
def test_experiment_cleanup_failure_suppresses_payload_and_exception_context(
    anomaly_experiment_boundaries: tuple[Mock, Mock], boundary: str
) -> None:
    lookup, delete = anomaly_experiment_boundaries
    lookup.return_value = Experiment("123", "synthetic", "synthetic", "active")
    failing = lookup if boundary == "lookup" else delete
    failing.side_effect = RuntimeError("private-resource-response")
    resource = MLflowExperimentResource("private-resource-path")

    with pytest.raises(ResourceCleanupError, match="error=RuntimeError") as error:
        delete_mlflow_experiment(resource, get_by_name=lookup, delete=delete)

    assert resource.fingerprint in str(error.value)
    assert "private-" not in str(error.value)
    assert error.value.__suppress_context__


def test_model_cleanup_deletes_versions_then_models(anomaly_cleanup_workspace: Mock) -> None:
    workspace = anomaly_cleanup_workspace
    workspace.registered_models.list.return_value = [RegisteredModelInfo(full_name="catalog.schema.model")]
    workspace.model_versions.list.return_value = [ModelVersionInfo(version=1), ModelVersionInfo(version=2)]

    drop_registered_models_in_schemas(workspace, [("catalog", "schema")])

    workspace.model_versions.delete.assert_has_calls(
        [call(full_name="catalog.schema.model", version=1), call(full_name="catalog.schema.model", version=2)]
    )
    workspace.registered_models.delete.assert_called_once_with(full_name="catalog.schema.model")


def test_model_cleanup_attempts_remaining_resources_after_failure(anomaly_cleanup_workspace: Mock) -> None:
    workspace = anomaly_cleanup_workspace
    workspace.registered_models.list.side_effect = [
        RuntimeError("private-schema-response"),
        [RegisteredModelInfo(full_name="catalog.other.first"), RegisteredModelInfo(full_name="catalog.other.second")],
    ]
    workspace.model_versions.list.return_value = [ModelVersionInfo(version=1), ModelVersionInfo(version=2)]
    workspace.model_versions.delete.side_effect = [RuntimeError("private-version-response"), None, None, None]
    workspace.registered_models.delete.side_effect = [RuntimeError("private-model-response"), None]

    with pytest.raises(ResourceCleanupError) as error:
        drop_registered_models_in_schemas(workspace, [("catalog", "schema"), ("catalog", "other")])

    assert workspace.model_versions.delete.call_count == 4
    assert workspace.registered_models.delete.call_args_list == [
        call(full_name="catalog.other.first"),
        call(full_name="catalog.other.second"),
    ]
    assert "private" not in str(error.value)
    assert "catalog" not in str(error.value)
    assert "error=RuntimeError" in str(error.value)
    assert error.value.__suppress_context__


def test_already_removed_schema_is_safe(anomaly_cleanup_workspace: Mock) -> None:
    workspace = anomaly_cleanup_workspace
    workspace.registered_models.list.side_effect = NotFound("synthetic missing schema")
    drop_registered_models_in_schemas(workspace, [("catalog", "schema")])
    workspace.model_versions.list.assert_not_called()


@pytest.mark.parametrize("failure_phase", ["setup", "call", "teardown", "call_and_teardown"])
def test_factory_owns_experiment_before_creation_and_preserves_test_outcome(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, failure_phase: str
) -> None:
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    pytester.makeconftest(
        f'''
from pathlib import Path
from unittest.mock import create_autospec
import mlflow
import pytest
from databricks.sdk import WorkspaceClient
from mlflow.entities import Experiment
from tests.anomaly_resources import fixture_mlflow_worker_experiment, fixture_make_mlflow_experiment_resource

@pytest.fixture(scope="session", autouse=True)
def service_boundaries():
    def activate(path):
        Path("creation-requested").write_text("yes")
        if {failure_phase!r} == "setup":
            raise RuntimeError("forced setup failure")
        return Experiment("123", path, "synthetic", "active")

    def delete(experiment_id):
        Path("cleanup-attempted").write_text(experiment_id)
        if {failure_phase!r} in ("teardown", "call_and_teardown"):
            raise RuntimeError("private-cleanup-response")

    with pytest.MonkeyPatch.context() as patches:
        patches.setattr(mlflow, "set_tracking_uri", lambda uri: None)
        patches.setattr(mlflow, "set_registry_uri", lambda uri: None)
        patches.setattr(mlflow, "set_experiment", activate)
        patches.setattr(mlflow, "get_experiment_by_name", lambda path: Experiment("123", path, "synthetic", "active"))
        patches.setattr(mlflow, "delete_experiment", delete)
        yield

@pytest.fixture
def ws():
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.current_user.me.return_value.user_name = "synthetic-user"
    return workspace
'''
    )
    pytester.makepyfile(
        f'''
def test_train(mlflow_worker_experiment):
    if {failure_phase!r} in ("call", "call_and_teardown"):
        raise RuntimeError("forced training failure")
    assert mlflow_worker_experiment[0] == "123"
'''
    )

    result = pytester.runpytest("-q", "--tb=line", "-p", "no:cacheprovider", "-p", "no:rerunfailures")

    result.assert_outcomes(
        passed=int(failure_phase == "teardown"),
        failed=int(failure_phase in {"call", "call_and_teardown"}),
        errors=int(failure_phase != "call"),
    )
    assert (pytester.path / "creation-requested").read_text() == "yes"
    assert (pytester.path / "cleanup-attempted").read_text() == "123"
    if failure_phase in {"teardown", "call_and_teardown"}:
        assert "ResourceCleanupError" in result.stdout.str()
        assert "private-cleanup-response" not in result.stdout.str()
    if failure_phase in {"call", "call_and_teardown"}:
        assert "forced training failure" in result.stdout.str()
