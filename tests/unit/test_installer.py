from unittest.mock import patch, MagicMock
import pytest
from databricks.labs.blueprint.parallel import ManyError

from databricks.labs.dqx.installer.version_checker import VersionChecker
from databricks.labs.dqx.installer.install import WorkspaceInstaller


def test_installer_executed_outside_workspace(mock_workspace_client):
    with pytest.raises(SystemExit) as exc_info:
        WorkspaceInstaller(mock_workspace_client, environ={"DATABRICKS_RUNTIME_VERSION": "7.3"})
    assert str(exc_info.value) == "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"


def test_configure_raises_timeout_error(mock_workspace_client):
    mock_configure = MagicMock(side_effect=TimeoutError("Mocked timeout error"))
    installer = WorkspaceInstaller(mock_workspace_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(TimeoutError) as exc_info:
            installer.configure()

    assert str(exc_info.value) == "Mocked timeout error"


def test_configure_raises_single_error(mock_workspace_client):
    single_error = ValueError("Single error")
    mock_configure = MagicMock(side_effect=ManyError([single_error]))
    installer = WorkspaceInstaller(mock_workspace_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(ManyError) as exc_info:
            installer.configure()

    assert exc_info.value.errs == [single_error]


def test_configure_raises_many_errors(mock_workspace_client):
    first_error = ValueError("First error")
    second_error = ValueError("Second error")
    errors = [first_error, second_error]
    mock_configure = MagicMock(side_effect=ManyError(errors))
    installer = WorkspaceInstaller(mock_workspace_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(ManyError) as exc_info:
            installer.configure()

    assert exc_info.value.errs == errors


def test_extract_major_minor():
    assert VersionChecker.extract_major_minor("1.2.3") == "1.2"
    assert VersionChecker.extract_major_minor("10.20.30") == "10.20"
    assert VersionChecker.extract_major_minor("v1.2.3") == "1.2"
    assert VersionChecker.extract_major_minor("version 1.2.3") == "1.2"
    assert VersionChecker.extract_major_minor("1.2") == "1.2"
    assert VersionChecker.extract_major_minor("1.2.3.4") == "1.2"
    assert VersionChecker.extract_major_minor("no version") is None
    assert VersionChecker.extract_major_minor("") is None
    assert VersionChecker.extract_major_minor("1") is None
