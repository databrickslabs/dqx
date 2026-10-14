"""Factory-owned anomaly test resources with payload-free teardown errors."""

import hashlib
import logging
import os
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from uuid import uuid4

import mlflow
import pytest

from mlflow.entities import Experiment

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.labs.pytester.fixtures.baseline import factory

logger = logging.getLogger(__name__)

# Process-scoped cache for MLflow experiment (one per xdist worker); lifecycle cleared by
# make_mlflow_experiment_resource at session end. Not thread-safe.
_MLFLOW_WORKER_EXPERIMENT_CACHE: dict[str, str | None] = {"id": None, "path": None}


class ResourceCleanupError(RuntimeError):
    """A test resource remains unresolved after all cleanup attempts."""


def resource_fingerprint(identifier: str) -> str:
    """Return a stable identifier that does not disclose a resource name or path."""
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()[:16]


@dataclass
class MLflowExperimentResource:
    """Register cleanup intent before requesting an experiment from the service."""

    path: str = field(repr=False)
    experiment_id: str | None = field(default=None, repr=False)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        self.fingerprint = resource_fingerprint(self.path)


def delete_mlflow_experiment(
    resource: MLflowExperimentResource,
    *,
    get_by_name: Callable[[str], Experiment | None],
    delete: Callable[[str], None],
) -> None:
    """Delete an experiment by recorded ID or resolve a partially created resource."""
    try:
        experiment_id = resource.experiment_id
        if experiment_id is None:
            experiment = get_by_name(resource.path)
            if experiment is None:
                return
            experiment_id = experiment.experiment_id
        delete(experiment_id)
    except Exception as exc:
        raise ResourceCleanupError(
            f"MLflow experiment cleanup failed: resource={resource.fingerprint}, error={type(exc).__name__}"
        ) from None


def _drop_model_versions(ws: WorkspaceClient, model_name: str) -> list[str]:
    model_key = resource_fingerprint(model_name)
    try:
        versions = list(ws.model_versions.list(full_name=model_name))
    except NotFound:
        return []
    except Exception as exc:
        return [f"model={model_key}, operation=list_versions, error={type(exc).__name__}"]
    failures = []
    for version in versions:
        if version.version is None:
            failures.append(f"model={model_key}, error=MissingModelVersion")
            continue
        try:
            ws.model_versions.delete(full_name=model_name, version=version.version)
        except NotFound:
            continue
        except Exception as exc:
            failures.append(f"model={model_key}, operation=delete_version, error={type(exc).__name__}")
    return failures


def drop_registered_model(ws: WorkspaceClient, model_name: str) -> list[str]:
    """Attempt every version and its owned model, returning payload-free failures."""
    failures = _drop_model_versions(ws, model_name)
    try:
        ws.registered_models.delete(full_name=model_name)
    except NotFound:
        pass
    except Exception as exc:
        failures.append(f"model={resource_fingerprint(model_name)}, operation=delete, error={type(exc).__name__}")
    return failures


def drop_registered_models_in_schemas(ws: WorkspaceClient, schemas: list[tuple[str, str]]) -> None:
    """Attempt every test-owned model cleanup and report unresolved resources safely."""
    failures = []
    for catalog_name, schema_name in schemas:
        schema_key = resource_fingerprint(f"{catalog_name}.{schema_name}")
        try:
            models = list(ws.registered_models.list(catalog_name=catalog_name, schema_name=schema_name))
        except NotFound:
            continue
        except Exception as exc:
            failures.append(f"schema={schema_key}, operation=list, error={type(exc).__name__}")
            continue
        for model in models:
            if not model.full_name:
                failures.append(f"schema={schema_key}, error=MissingModelName")
                continue
            failures.extend(drop_registered_model(ws, model.full_name))
    if failures:
        raise ResourceCleanupError("Registered model cleanup failed: " + "; ".join(failures)) from None


@pytest.fixture(name="make_mlflow_experiment_resource", scope="session")
def fixture_make_mlflow_experiment_resource() -> Generator[Callable[..., MLflowExperimentResource], None, None]:
    """Own experiment cleanup before remote creation, including partial setup failures."""
    failures: list[str] = []

    def cleanup(resource: MLflowExperimentResource) -> None:
        try:
            delete_mlflow_experiment(
                resource, get_by_name=mlflow.get_experiment_by_name, delete=mlflow.delete_experiment
            )
        except ResourceCleanupError as exc:
            failures.append(str(exc))

    try:
        yield from factory("MLflow experiment", MLflowExperimentResource, cleanup)
    finally:
        os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
        os.environ.pop("MLFLOW_EXPERIMENT_NAME", None)
        _MLFLOW_WORKER_EXPERIMENT_CACHE.update(id=None, path=None)
        if failures:
            raise ResourceCleanupError("; ".join(failures)) from None


@pytest.fixture(name="mlflow_worker_experiment")
def fixture_mlflow_worker_experiment(
    ws: WorkspaceClient, make_mlflow_experiment_resource: Callable[..., MLflowExperimentResource]
) -> tuple[str | None, str]:
    """Create one MLflow experiment per xdist worker (via module cache); reuse across tests.
    Cleanup is owned by make_mlflow_experiment_resource at session end. Function-scoped so
    we can depend on ws (pytester provides ws as function-scoped)."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        local_mlflow_db = os.environ.get("MLFLOW_LOCAL_DB")
        if not local_mlflow_db:
            worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
            local_mlflow_db = f"/tmp/dqx-mlflow-{worker_id}.db"
        tracking_uri = f"sqlite:///{local_mlflow_db}"
        os.environ.setdefault("MLFLOW_TRACKING_URI", tracking_uri)
        os.environ.setdefault("MLFLOW_REGISTRY_URI", tracking_uri)
    registry_uri = os.environ.get("MLFLOW_REGISTRY_URI", "databricks-uc")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_registry_uri(registry_uri)

    cached_path = _MLFLOW_WORKER_EXPERIMENT_CACHE["path"]
    if cached_path is not None:
        return (_MLFLOW_WORKER_EXPERIMENT_CACHE["id"], cached_path)

    os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
    user_name = ws.current_user.me().user_name
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    suffix = uuid4().hex[:8]
    experiment_path = f"/Users/{user_name}/dqx_integration_tests_{worker_id}_{suffix}"
    resource = make_mlflow_experiment_resource(path=experiment_path)
    experiment = mlflow.set_experiment(experiment_path)
    experiment_id = getattr(experiment, "experiment_id", None) if experiment else None
    resource.experiment_id = experiment_id
    logger.debug(f"Created MLflow experiment resource={resource.fingerprint}")
    _MLFLOW_WORKER_EXPERIMENT_CACHE["id"] = experiment_id
    _MLFLOW_WORKER_EXPERIMENT_CACHE["path"] = experiment_path
    return (experiment_id, experiment_path)
