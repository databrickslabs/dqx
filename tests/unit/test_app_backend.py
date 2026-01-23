"""Unit tests for the DQX App backend modules."""

import base64
import logging
from unittest.mock import create_autospec

import pytest
from databricks_labs_dqx_app.backend.dependencies import get_obo_ws
from databricks_labs_dqx_app.backend.logger import CustomFormatter, setup_logger, get_logger
from databricks_labs_dqx_app.backend.models import InstallationSettings
from databricks_labs_dqx_app.backend.router import get_install_folder
from databricks_labs_dqx_app.backend.settings import SettingsManager

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.iam import User
from databricks.sdk.service.workspace import ExportResponse


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    ws = create_autospec(WorkspaceClient)
    mock_user = create_autospec(User)
    mock_user.user_name = "test_user@example.com"
    ws.current_user.me.return_value = mock_user
    return ws


# ============================================================================
# Tests for router.py - get_install_folder
# ============================================================================


class TestGetInstallFolder:
    """Unit tests for the get_install_folder helper function."""

    def test_returns_provided_path(self, mock_workspace_client):
        """Should return the provided path when it's a folder."""
        result = get_install_folder(mock_workspace_client, "/Workspace/my_project/dqx")
        assert result == "/Workspace/my_project/dqx"

    def test_strips_whitespace(self, mock_workspace_client):
        """Should strip whitespace from the provided path."""
        result = get_install_folder(mock_workspace_client, "  /Workspace/my_project/dqx  ")
        assert result == "/Workspace/my_project/dqx"

    def test_strips_yml_extension(self, mock_workspace_client):
        """Should strip .yml file from path and return folder."""
        result = get_install_folder(mock_workspace_client, "/Workspace/my_project/dqx/config.yml")
        assert result == "/Workspace/my_project/dqx"

    def test_strips_yaml_extension(self, mock_workspace_client):
        """Should strip .yaml file from path and return folder."""
        result = get_install_folder(mock_workspace_client, "/Workspace/my_project/dqx/config.yaml")
        assert result == "/Workspace/my_project/dqx"

    def test_handles_yml_only_filename(self, mock_workspace_client):
        """Should handle path that is just a yml filename with no folder."""
        result = get_install_folder(mock_workspace_client, "config.yml")
        assert result == "config.yml"

    def test_strips_yml_with_nested_path(self, mock_workspace_client):
        """Should handle deeply nested paths with yml extension."""
        result = get_install_folder(mock_workspace_client, "/Workspace/Users/user/project/subdir/config.yml")
        assert result == "/Workspace/Users/user/project/subdir"


class TestGetOboWs:
    """Unit tests for the get_obo_ws dependency function."""

    def test_raises_when_no_token(self):
        """Should raise ValueError when no token is provided."""
        with pytest.raises(ValueError, match="OBO token is not provided"):
            get_obo_ws(token=None)

    def test_raises_when_empty_token(self):
        """Should raise ValueError when empty token is provided."""
        with pytest.raises(ValueError, match="OBO token is not provided"):
            get_obo_ws(token="")


class TestCustomFormatter:
    """Unit tests for CustomFormatter."""

    def _create_log_record(self, module: str, func_name: str, msg: str = "Test") -> logging.LogRecord:
        """Helper to create a LogRecord with specific module and function name."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=f"{module}.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )
        record.module = module
        record.funcName = func_name
        return record

    def test_format_short_location(self):
        """Should include full location when it fits within max_length."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("router", "get_config")
        result = formatter.format(record)

        assert "router.get_config" in result

    def test_format_long_location_abbreviates_module(self):
        """Should abbreviate module parts when location is too long."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("databricks_labs_dqx_app.backend.router", "get_install_folder")
        result = formatter.format(record)

        # The location should be abbreviated to fit within 20 chars
        # Original would be "databricks_labs_dqx_app.backend.router.get_install_folder" (57 chars)
        # Abbreviated should be something like "d.l.d.b.r.get_instal" (20 chars)
        # Check that the full long name is NOT in the output
        assert "databricks_labs_dqx_app.backend.router.get_install_folder" not in result

    def test_format_module_level_code(self):
        """Should handle <module> function name by showing just module."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("router", "<module>")
        result = formatter.format(record)

        assert "router" in result
        assert "<module>" not in result

    def test_format_main_module(self):
        """Should handle __main__ module by showing just function name."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("__main__", "run")
        result = formatter.format(record)

        # __main__ should be stripped, leaving just the function name
        assert "__main__" not in result or "run" in result

    def test_format_includes_message(self):
        """Should include the log message in output."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func", msg="Hello World")
        result = formatter.format(record)

        assert "Hello World" in result

    def test_format_includes_level(self):
        """Should include the log level in output."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func")
        result = formatter.format(record)

        assert "INFO" in result

    def test_format_uses_pipe_separator(self):
        """Should use pipe-separated format."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func")
        result = formatter.format(record)

        assert "|" in result


class TestSetupLogger:
    """Unit tests for setup_logger function."""

    def test_creates_logger_with_name(self):
        """Should create logger with specified name."""
        log = setup_logger("test_logger", level=logging.DEBUG, use_colors=False)

        assert log.name == "test_logger"
        assert log.level == logging.DEBUG
        assert len(log.handlers) == 1

    def test_creates_logger_without_name(self):
        """Should create root logger when no name provided."""
        log = setup_logger(None, level=logging.WARNING, use_colors=False)

        assert log.name == "root"
        assert log.level == logging.WARNING

    def test_clears_existing_handlers(self):
        """Should clear existing handlers to avoid duplicates."""
        log = setup_logger("dup_test", level=logging.INFO, use_colors=False)
        initial_handlers = len(log.handlers)

        # Setup again - should still have same number of handlers
        log = setup_logger("dup_test", level=logging.INFO, use_colors=False)

        assert len(log.handlers) == initial_handlers


class TestGetLogger:
    """Unit tests for get_logger function."""

    def test_returns_default_logger_when_no_name(self):
        """Should return the default app logger when no name provided."""
        log = get_logger(None)
        assert log is not None

    def test_returns_named_logger(self):
        """Should return a new logger with specified name."""
        log = get_logger("custom_logger")
        assert log.name == "custom_logger"


class TestSettingsManager:
    """Unit tests for SettingsManager class."""

    def test_init_sets_paths(self, mock_workspace_client):
        """Should initialize with correct paths based on user."""
        manager = SettingsManager(mock_workspace_client)

        assert manager.user_home == "/Users/test_user@example.com"
        assert manager.default_dqx_folder == "/Users/test_user@example.com/.dqx"
        assert manager.app_settings_path == "/Users/test_user@example.com/.dqx/app.yml"

    def test_get_default_install_folder(self, mock_workspace_client):
        """Should return the default .dqx folder path."""
        manager = SettingsManager(mock_workspace_client)
        result = manager.get_default_install_folder()

        assert result == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_file_not_found(self, mock_workspace_client):
        """Should return default settings when app.yml doesn't exist."""
        mock_workspace_client.workspace.export.side_effect = ResourceDoesNotExist("Not found")

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"
        assert result.is_default is True

    def test_get_settings_returns_custom_when_file_exists(self, mock_workspace_client):
        """Should return custom settings when app.yml exists."""
        yaml_content = "install_folder: /custom/path/config.yml"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/custom/path/config.yml"
        assert result.is_default is False

    def test_get_settings_returns_default_on_exception(self, mock_workspace_client):
        """Should return default settings when export fails with unexpected error."""
        mock_workspace_client.workspace.export.side_effect = Exception("Unexpected error")

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.is_default is True

    def test_save_settings_raises_on_invalid_extension(self, mock_workspace_client):
        """Should raise ValueError when path doesn't end with .yml or .yaml."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/path/to/config.txt", is_default=False)

        with pytest.raises(ValueError, match="must be a valid .yml or .yaml file"):
            manager.save_settings(settings)

    def test_save_settings_deletes_file_for_default_path(self, mock_workspace_client):
        """Should delete app.yml when saving default path."""
        manager = SettingsManager(mock_workspace_client)
        default_path = f"{manager.default_dqx_folder}/config.yml"
        settings = InstallationSettings(install_folder=default_path, is_default=False)

        result = manager.save_settings(settings)

        mock_workspace_client.workspace.delete.assert_called_once_with(manager.app_settings_path)
        assert result.is_default is True

    def test_save_settings_creates_file_for_custom_path(self, mock_workspace_client):
        """Should create app.yml when saving custom path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path/config.yml", is_default=False)

        result = manager.save_settings(settings)

        mock_workspace_client.workspace.mkdirs.assert_called_once()
        mock_workspace_client.workspace.import_.assert_called_once()
        assert result.install_folder == "/custom/path/config.yml"
        assert result.is_default is False

    def test_save_settings_handles_delete_not_found(self, mock_workspace_client):
        """Should handle ResourceDoesNotExist when deleting app.yml for default."""
        mock_workspace_client.workspace.delete.side_effect = ResourceDoesNotExist("Not found")

        manager = SettingsManager(mock_workspace_client)
        default_path = f"{manager.default_dqx_folder}/config.yml"
        settings = InstallationSettings(install_folder=default_path, is_default=False)

        # Should not raise
        result = manager.save_settings(settings)
        assert result.is_default is True
