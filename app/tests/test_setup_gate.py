"""Tests for the API readiness boundary."""

from collections.abc import Iterator

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
import pytest

from databricks_labs_dqx_app.backend.setup.gate import SetupGateMiddleware
from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupState
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime


@pytest.fixture
def gated_app() -> tuple[FastAPI, SetupRuntime, list[str]]:
    runtime = SetupRuntime()
    dependency_calls: list[str] = []
    app = FastAPI()
    app.add_middleware(SetupGateMiddleware, runtime=runtime)

    def database_dependency() -> None:
        dependency_calls.append("database")

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/version")
    def version() -> dict[str, str]:
        return {"version": "test"}

    @app.get("/api/v1/current-user")
    def current_user() -> dict[str, str]:
        return {"user_name": "admin@example.com"}

    @app.get("/api/v1/current-user/role", dependencies=[Depends(database_dependency)])
    def current_user_role() -> dict[str, str]:
        return {"role": "admin"}

    @app.get("/api/v1/setup/status")
    def setup_status() -> dict[str, str]:
        return {"state": runtime.report().state}

    @app.post("/api/v1/setup/reconcile")
    def reconcile() -> dict[str, str]:
        return {"state": runtime.report().state}

    @app.get("/api/v1/config/workspace-host")
    def workspace_host() -> dict[str, str]:
        return {"workspace_host": "https://workspace.example.com"}

    @app.get("/api/v1/home/stats", dependencies=[Depends(database_dependency)])
    def home_stats() -> dict[str, int]:
        return {"rules": 0}

    @app.get("/")
    def static_ui() -> dict[str, str]:
        return {"surface": "setup"}

    return app, runtime, dependency_calls


@pytest.fixture
def gated_client(gated_app: tuple[FastAPI, SetupRuntime, list[str]]) -> Iterator[TestClient]:
    app, _, _ = gated_app
    with TestClient(app) as client:
        yield client


def test_gate_blocks_normal_api_without_invoking_dependencies(
    gated_app: tuple[FastAPI, SetupRuntime, list[str]], gated_client: TestClient
) -> None:
    _, runtime, dependency_calls = gated_app
    runtime.publish(SetupReport(state=SetupState.SETUP_REQUIRED, steps=()))

    response = gated_client.get("/api/v1/home/stats")

    assert response.status_code == 503
    assert response.json()["detail"] == "DQX Studio setup is required."
    assert dependency_calls == []


def test_gate_allows_exact_setup_authentication_and_health_routes(gated_client: TestClient) -> None:
    assert gated_client.get("/api/health").status_code == 200
    assert gated_client.get("/api/v1/version").status_code == 200
    assert gated_client.get("/api/v1/current-user").status_code == 200
    assert gated_client.get("/api/v1/setup/status").status_code == 200
    assert gated_client.post("/api/v1/setup/reconcile").status_code == 200
    assert gated_client.get("/api/v1/config/workspace-host").status_code == 200


def test_gate_blocks_database_backed_current_user_role(
    gated_app: tuple[FastAPI, SetupRuntime, list[str]], gated_client: TestClient
) -> None:
    _, _, dependency_calls = gated_app

    response = gated_client.get("/api/v1/current-user/role")

    assert response.status_code == 503
    assert dependency_calls == []


def test_gate_does_not_prefix_match_allowed_routes(gated_client: TestClient) -> None:
    assert gated_client.get("/api/v1/setup/status/extra").status_code == 503
    assert gated_client.post("/api/v1/setup/status").status_code == 503


def test_gate_keeps_static_ui_available(gated_client: TestClient) -> None:
    assert gated_client.get("/").status_code == 200


def test_gate_passes_normal_api_when_ready(
    gated_app: tuple[FastAPI, SetupRuntime, list[str]], gated_client: TestClient
) -> None:
    _, runtime, dependency_calls = gated_app
    runtime.publish(SetupReport(state=SetupState.READY, steps=()))

    response = gated_client.get("/api/v1/home/stats")

    assert response.status_code == 200
    assert dependency_calls == ["database"]
