"""Integration tests for the DQX App backend."""

import pytest
from databricks.sdk.errors import ResourceDoesNotExist

from databricks_labs_dqx_app.backend.models import InstallationSettings
from databricks_labs_dqx_app.backend.settings import SettingsManager


@pytest.fixture
def settings_manager(ws):
    """Fixture that provides a SettingsManager instance."""
    return SettingsManager(ws)


@pytest.fixture
def installation_folder(ws, make_random):
    """Fixture that creates a temporary workspace folder and cleans it up automatically."""
    folders = []

    def create_folder(folder_path=None):
        """Create a folder in workspace and track it for cleanup."""
        if folder_path is None:
            user_home = f"/Users/{ws.current_user.me().user_name}"
            folder_path = f"{user_home}/.dqx_test_{make_random(8)}"

        folders.append(folder_path)
        return folder_path

    yield create_folder

    # Cleanup all created folders
    for folder in folders:
        try:
            ws.workspace.delete(folder, recursive=True)
        except ResourceDoesNotExist:
            pass


@pytest.fixture
def app_settings_file(ws, settings_manager):
    """Fixture that ensures app.yml is cleaned up after test."""
    yield settings_manager.app_settings_path

    try:
        ws.workspace.delete(settings_manager.app_settings_path, recursive=True)
    except ResourceDoesNotExist:
        pass


class TestSettingsManagerIntegration:
    """Integration tests for SettingsManager with real Databricks workspace."""

    def test_get_settings_returns_default_when_app_yml_does_not_exist(self, ws, settings_manager):
        """Should return default settings when app.yml doesn't exist in workspace."""
        settings = settings_manager.get_settings()

        # Should return default folder path
        assert settings.install_folder.endswith("/.dqx")
        assert ws.current_user.me().user_name in settings.install_folder

    def test_save_and_get_settings_roundtrip_custom_folder(
        self, settings_manager, installation_folder, app_settings_file
    ):
        """Should successfully save and retrieve custom install folder."""
        custom_folder = installation_folder()

        # Save custom settings
        settings_to_save = InstallationSettings(install_folder=custom_folder)
        saved_settings = settings_manager.save_settings(settings_to_save)

        assert saved_settings.install_folder == custom_folder

        # Retrieve and verify
        retrieved_settings = settings_manager.get_settings()
        assert retrieved_settings.install_folder == custom_folder

    def test_save_and_get_settings_roundtrip_default_folder(self, settings_manager, app_settings_file):
        """Should successfully save and retrieve default install folder."""
        default_folder = settings_manager.get_default_install_folder()

        # Save default settings
        settings_to_save = InstallationSettings(install_folder=default_folder)
        saved_settings = settings_manager.save_settings(settings_to_save)

        assert saved_settings.install_folder == default_folder

        # Retrieve and verify
        retrieved_settings = settings_manager.get_settings()
        assert retrieved_settings.install_folder == default_folder

    def test_save_settings_creates_install_folder_if_not_exists(
        self, ws, settings_manager, installation_folder, app_settings_file
    ):
        """Should create install folder if it doesn't exist."""
        custom_folder = installation_folder()

        # Ensure folder doesn't exist
        try:
            ws.workspace.delete(custom_folder, recursive=True)
        except ResourceDoesNotExist:
            pass

        # Save settings should create install folder
        settings_to_save = InstallationSettings(install_folder=custom_folder)
        settings_manager.save_settings(settings_to_save)

        # Verify install folder exists
        folder = ws.workspace.get_status(custom_folder)
        assert folder is not None

    def test_save_settings_overwrites_existing_app_yml(self, settings_manager, installation_folder, app_settings_file):
        """Should overwrite existing app.yml with new settings."""
        folder1 = installation_folder()
        folder2 = installation_folder()

        # Save first settings
        settings1 = InstallationSettings(install_folder=folder1)
        settings_manager.save_settings(settings1)

        # Verify first settings
        retrieved1 = settings_manager.get_settings()
        assert retrieved1.install_folder == folder1

        # Save second settings (should overwrite)
        settings2 = InstallationSettings(install_folder=folder2)
        settings_manager.save_settings(settings2)

        # Verify second settings overwrote first
        retrieved2 = settings_manager.get_settings()
        assert retrieved2.install_folder == folder2

    def test_save_settings_strips_whitespace_from_path(self, settings_manager, installation_folder, app_settings_file):
        """Should strip whitespace from install folder path."""
        custom_folder = installation_folder()
        folder_with_whitespace = f"  {custom_folder}  "

        # Save with whitespace
        settings_to_save = InstallationSettings(install_folder=folder_with_whitespace)
        saved_settings = settings_manager.save_settings(settings_to_save)

        # Should have stripped whitespace
        assert saved_settings.install_folder == custom_folder
        assert saved_settings.install_folder.strip() == saved_settings.install_folder

        # Retrieved should also be stripped
        retrieved_settings = settings_manager.get_settings()
        assert retrieved_settings.install_folder == custom_folder

    def test_save_settings_with_nested_folder_structure(
        self, ws, settings_manager, installation_folder, app_settings_file, make_random
    ):
        """Should handle nested folder structures correctly."""
        user_home = f"/Users/{ws.current_user.me().user_name}"
        nested_folder = f"{user_home}/test/nested/dqx_{make_random(8)}"

        # Register both the nested folder and its parent for cleanup
        installation_folder(nested_folder)
        installation_folder(f"{user_home}/test")

        # Save settings with nested path
        settings_to_save = InstallationSettings(install_folder=nested_folder)
        saved_settings = settings_manager.save_settings(settings_to_save)

        assert saved_settings.install_folder == nested_folder

        # Verify folder hierarchy was created
        folder = ws.workspace.get_status(nested_folder)
        assert folder is not None

        # Verify can retrieve
        retrieved_settings = settings_manager.get_settings()
        assert retrieved_settings.install_folder == nested_folder
