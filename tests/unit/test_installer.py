from unittest.mock import patch, MagicMock, create_autospec
import pytest
from databricks.labs.dqx.installer.install import WorkspaceInstaller, ManyError
from databricks.sdk import WorkspaceClient


def test_installer_executed_outside_workspace():
    ws_client = create_autospec(WorkspaceClient(), instance=True)
    with pytest.raises(SystemExit) as exc_info:
        WorkspaceInstaller(ws_client, environ={"DATABRICKS_RUNTIME_VERSION": "7.3"})
    assert str(exc_info.value) == "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"


def test_configure_raises_timeout_error():
    mock_configure = MagicMock(side_effect=TimeoutError("Mocked timeout error"))

    ws_client = create_autospec(WorkspaceClient(), instance=True)
    installer = WorkspaceInstaller(ws_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(TimeoutError) as exc_info:
            installer.configure()

    assert str(exc_info.value) == "Mocked timeout error"


def test_configure_raises_single_error():
    single_error = ValueError("Single error")
    mock_configure = MagicMock(side_effect=ManyError([single_error]))

    ws_client = create_autospec(WorkspaceClient(), instance=True)
    installer = WorkspaceInstaller(ws_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(ManyError) as exc_info:
            installer.configure()

    assert exc_info.value.errs == [single_error]


def test_configure_raises_many_errors():
    first_error = ValueError("First error")
    second_error = ValueError("Second error")
    errors = [first_error, second_error]
    mock_configure = MagicMock(side_effect=ManyError(errors))

    ws_client = create_autospec(WorkspaceClient(), instance=True)
    installer = WorkspaceInstaller(ws_client)

    with patch.object(installer, 'configure', mock_configure):
        with pytest.raises(ManyError) as exc_info:
            installer.configure()

    assert exc_info.value.errs == errors
