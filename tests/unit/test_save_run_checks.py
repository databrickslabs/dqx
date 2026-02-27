"""Unit tests for save_run_checks error handling in the backend router."""

import pytest
from fastapi import HTTPException

from databricks.labs.dqx.errors import InvalidCheckError
from databricks_labs_dqx_app.backend import router
from databricks_labs_dqx_app.backend.models import ChecksIn


class _FakeRunConfig:
    """Minimal fake run config object with only the attributes used by save_run_checks."""

    def __init__(self, name: str):
        self.name = name


class _FakeSerializer:
    """Fake ConfigSerializer that returns a simple run config."""

    def __init__(self, _ws):
        pass

    def load_run_config(self, run_config_name: str, install_folder: str):
        return _FakeRunConfig(run_config_name)


class _FakeChecksStorageConfig:
    """Fake InstallationChecksStorageConfig used only for type compatibility."""

    def __init__(self, run_config_name: str, install_folder: str):
        self.run_config_name = run_config_name
        self.install_folder = install_folder


class _FakeDQEngine:
    """Fake DQEngine that always raises InvalidCheckError when saving checks."""

    def save_checks(self, checks, config):
        raise InvalidCheckError("invalid check structure")


def test_save_run_checks_maps_invalid_check_error_to_http_400(monkeypatch, mock_workspace_client):
    """When engine.save_checks raises InvalidCheckError, the route should map it to HTTP 400."""

    # Arrange: isolate save_run_checks from external systems
    monkeypatch.setattr(router, "get_install_folder", lambda _ws, _path: "/dummy/install/folder")
    monkeypatch.setattr(router, "ConfigSerializer", _FakeSerializer)
    monkeypatch.setattr(router, "InstallationChecksStorageConfig", _FakeChecksStorageConfig)

    body = ChecksIn(checks=[{"this_is": "not_a_valid_check"}])

    # Act & Assert: InvalidCheckError must be translated to HTTP 400
    with pytest.raises(HTTPException) as exc_info:
        router.save_run_checks(
            name="test_run",
            body=body,
            obo_ws=mock_workspace_client,
            engine=_FakeDQEngine(),
            path=None,
        )

    assert exc_info.value.status_code == 400
    assert "Invalid checks format" in exc_info.value.detail

