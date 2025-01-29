from unittest.mock import patch, MagicMock
import pytest
from databricks.labs.dqx.installer.install import WorkspaceInstaller, ManyError
from databricks.sdk import WorkspaceClient


def test_installer_executed_outside_workspace():
    mock_ws_client = MagicMock(spec=WorkspaceClient)
    with pytest.raises(SystemExit) as exc_info:
        WorkspaceInstaller(mock_ws_client, environ={"DATABRICKS_RUNTIME_VERSION": "7.3"})
    assert str(exc_info.value) == "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"


def test_configure_raises_timeout_error():
    mock_configure = MagicMock(side_effect=TimeoutError("Mocked timeout error"))
    mock_ws_client = MagicMock(spec=WorkspaceClient)
    installer = WorkspaceInstaller(mock_ws_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(TimeoutError) as exc_info:
            installer.configure()

    assert str(exc_info.value) == "Mocked timeout error"


def test_configure_raises_single_error():
    single_error = ValueError("Single error")
    mock_configure = MagicMock(side_effect=ManyError([single_error]))
    mock_ws_client = MagicMock(spec=WorkspaceClient)
    installer = WorkspaceInstaller(mock_ws_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(ManyError) as exc_info:
            installer.configure()

    assert exc_info.value.errs == [single_error]


def test_configure_raises_many_errors():
    first_error = ValueError("First error")
    second_error = ValueError("Second error")
    errors = [first_error, second_error]
    mock_configure = MagicMock(side_effect=ManyError(errors))
    mock_ws_client = MagicMock(spec=WorkspaceClient)
    installer = WorkspaceInstaller(mock_ws_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(ManyError) as exc_info:
            installer.configure()

    assert exc_info.value.errs == errors


def test_extract_major_minor():
    assert WorkspaceInstaller.extract_major_minor("1.2.3") == "1.2"
    assert WorkspaceInstaller.extract_major_minor("10.20.30") == "10.20"
    assert WorkspaceInstaller.extract_major_minor("v1.2.3") == "1.2"
    assert WorkspaceInstaller.extract_major_minor("version 1.2.3") == "1.2"
    assert WorkspaceInstaller.extract_major_minor("1.2") == "1.2"
    assert WorkspaceInstaller.extract_major_minor("1.2.3.4") == "1.2"
    assert WorkspaceInstaller.extract_major_minor("no version") is None
    assert WorkspaceInstaller.extract_major_minor("") is None
    assert WorkspaceInstaller.extract_major_minor("1") is None