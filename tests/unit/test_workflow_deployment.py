"""Dependency installation contracts for deployed workflows."""

import importlib.metadata
from collections.abc import Callable
from unittest.mock import Mock, patch

import pytest
from packaging.requirements import Requirement

from databricks.sdk.service import compute, jobs
from databricks.labs.dqx.config import RunConfig, WorkspaceConfig
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.labs.dqx.installer.workflow_installer import WorkflowDeployment
from databricks.labs.dqx.installer.workflow_task import Task


@pytest.mark.parametrize("override_cluster", [False, True])
def test_classic_anomaly_workflow_installs_declared_extra(
    workflow_deployment_factory: Callable[[WorkspaceConfig, list[Task]], tuple[WorkflowDeployment, Mock, Mock]],
    override_cluster: bool,
) -> None:
    config = WorkspaceConfig([RunConfig()], serverless_clusters=override_cluster)
    task = Task(
        "anomaly-trainer",
        "train_model",
        "Train anomaly model",
        lambda *_: None,
        override_clusters={"default": "existing-cluster"} if override_cluster else None,
    )
    deployment, ws, _ = workflow_deployment_factory(config, [task])
    # Metadata is a third-party boundary; exercise selection without coupling the installer to a fixed version list.
    requires = [
        "databricks-sdk~=0.73",
        "mlflow[databricks]>=3.13.0,<4.0; extra == 'anomaly'",
        "shap>=0.42.0,<0.50; extra == 'anomaly'",
        "shap>=0.42.0,<0.50; extra == 'anomaly'",
        "dspy~=3.1.3; extra == 'llm'",
        "databricks-connect>=15.4,<18; extra == 'cli'",
    ]
    with patch.object(importlib.metadata, "requires", return_value=requires):
        deployment.create_jobs()

    submitted_task = ws.jobs.create.call_args.kwargs["tasks"][0]
    assert submitted_task.libraries == [
        compute.Library(whl="/Workspace/test/wheels/databricks_labs_dqx-0.16.0-py3-none-any.whl"),
        compute.Library(pypi=compute.PythonPyPiLibrary(package="mlflow[databricks]<4.0,>=3.13.0")),
        compute.Library(pypi=compute.PythonPyPiLibrary(package="shap<0.50,>=0.42.0")),
    ]
    assert submitted_task.python_wheel_task.package_name == "databricks_labs_dqx"
    assert submitted_task.existing_cluster_id == ("existing-cluster" if override_cluster else None)


def test_classic_anomaly_dependencies_follow_installed_package_metadata(
    workflow_deployment_factory: Callable[[WorkspaceConfig, list[Task]], tuple[WorkflowDeployment, Mock, Mock]],
) -> None:
    deployment, ws, _ = workflow_deployment_factory(
        WorkspaceConfig([RunConfig()], serverless_clusters=False),
        [Task("anomaly-trainer", "train_model", "Train anomaly model", lambda *_: None)],
    )

    deployment.create_jobs()

    submitted_task = ws.jobs.create.call_args.kwargs["tasks"][0]
    installed = {
        requirement.name: requirement
        for library in submitted_task.libraries
        if library.pypi
        for requirement in (Requirement(library.pypi.package),)
    }
    assert set(installed) == {"mlflow", "scikit-learn", "threadpoolctl", "cloudpickle", "shap"}
    declared_mlflow = next(
        Requirement(requirement)
        for requirement in importlib.metadata.requires("databricks-labs-dqx") or []
        if requirement.startswith("mlflow[")
    )
    assert installed["mlflow"].specifier == declared_mlflow.specifier
    assert installed["mlflow"].extras == {"databricks"}
    assert all(requirement.marker is None for requirement in installed.values())


@pytest.mark.parametrize("workflow", ["anomaly-trainer", "quality-checker"])
@pytest.mark.parametrize("serverless", [False, True])
@pytest.mark.parametrize("offline", [False, True])
def test_other_dependency_installation_paths_are_preserved(
    workflow_deployment_factory: Callable[[WorkspaceConfig, list[Task]], tuple[WorkflowDeployment, Mock, Mock]],
    workflow: str,
    serverless: bool,
    offline: bool,
) -> None:
    config = WorkspaceConfig([RunConfig()], serverless_clusters=serverless, upload_dependencies=offline)
    deployment, ws, wheels = workflow_deployment_factory(
        config, [Task(workflow, "task", "Workflow task", lambda *_: None)]
    )

    deployment.create_jobs()

    settings = ws.jobs.create.call_args.kwargs
    task = settings["tasks"][0]
    wheel_paths = ["/Workspace/test/wheels/databricks_labs_dqx-0.16.0-py3-none-any.whl"]
    if offline:
        wheel_paths.insert(0, "/Workspace/test/wheels/packaging-24.2-py3-none-any.whl")
        wheels.upload_wheel_dependencies.assert_called_once()
    else:
        wheels.upload_wheel_dependencies.assert_not_called()
    if serverless:
        assert task.libraries is None
        assert settings["environments"] == [
            jobs.JobEnvironment(
                environment_key="default",
                spec=compute.Environment(
                    client="1", dependencies=[f"{wheel}[llm,pii,anomaly]" for wheel in wheel_paths]
                ),
            )
        ]
    elif offline or workflow != "anomaly-trainer":
        assert task.libraries == [compute.Library(whl=wheel) for wheel in wheel_paths]
    else:
        assert any(library.pypi for library in task.libraries)


@pytest.mark.parametrize("requires", [None, [], ["databricks-sdk~=0.73"], ["invalid requirement !"]])
def test_missing_anomaly_metadata_does_not_silently_use_bundled_libraries(
    workflow_deployment_factory: Callable[[WorkspaceConfig, list[Task]], tuple[WorkflowDeployment, Mock, Mock]],
    requires: list[str] | None,
) -> None:
    deployment, ws, _ = workflow_deployment_factory(
        WorkspaceConfig([RunConfig()], serverless_clusters=False),
        [Task("anomaly-trainer", "train_model", "Train anomaly model", lambda *_: None)],
    )
    with patch.object(importlib.metadata, "requires", return_value=requires):
        with pytest.raises(InvalidConfigError, match="package metadata"):
            deployment.create_jobs()
    ws.jobs.create.assert_not_called()


def test_missing_package_metadata_does_not_silently_use_bundled_libraries(
    workflow_deployment_factory: Callable[[WorkspaceConfig, list[Task]], tuple[WorkflowDeployment, Mock, Mock]],
) -> None:
    deployment, ws, _ = workflow_deployment_factory(
        WorkspaceConfig([RunConfig()], serverless_clusters=False),
        [Task("anomaly-trainer", "train_model", "Train anomaly model", lambda *_: None)],
    )
    with patch.object(importlib.metadata, "requires", side_effect=importlib.metadata.PackageNotFoundError):
        with pytest.raises(InvalidConfigError, match="package metadata"):
            deployment.create_jobs()
    ws.jobs.create.assert_not_called()
