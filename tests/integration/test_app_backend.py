"""Integration tests for the DQX App backend."""

import pytest
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.dependencies import get_obo_ws, get_dqx_engine
from databricks_labs_dqx_app.backend.models import InstallationSettings
from databricks_labs_dqx_app.backend.settings import SettingsManager
from databricks.labs.dqx.config import RunConfig, InputConfig, OutputConfig, WorkspaceConfig
from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist


@pytest.fixture
def test_app_folder(ws, make_random):
    """Fixture that provides a unique test folder shared by settings_manager and api_client.

    This ensures both fixtures use the same folder to avoid conflicts.
    """
    user = ws.current_user.me()
    user_home = f"/Users/{user.user_name}"
    test_folder = f"{user_home}/.dqx_test_{make_random(8)}"

    yield test_folder

    # Cleanup: delete the test folder
    try:
        ws.workspace.delete(test_folder, recursive=True)
    except ResourceDoesNotExist:
        pass


@pytest.fixture
def settings_manager(ws, test_app_folder):
    """Fixture that provides a SettingsManager instance with isolated test folder."""
    test_app_yml = f"{test_app_folder}/app.yml"

    # Create SettingsManager and override its paths
    manager = SettingsManager(ws)
    manager.default_dqx_folder = test_app_folder
    manager.app_settings_path = test_app_yml

    return manager


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
def api_client(ws, spark, test_app_folder, monkeypatch):
    """Fixture that provides a FastAPI test client with dependency overrides."""
    # Import app lazy here to avoid module-level initialization during fixture collection
    from databricks_labs_dqx_app.backend.app import app

    test_app_yml = f"{test_app_folder}/app.yml"

    # Store original SettingsManager.__init__
    original_init = SettingsManager.__init__

    def patched_init(self, ws_client):
        """Patched SettingsManager init that uses test folders."""
        original_init(self, ws_client)
        # Override paths to use test folders (same as settings_manager fixture)
        self.default_dqx_folder = test_app_folder
        self.app_settings_path = test_app_yml

    # Patch SettingsManager to use test folders
    monkeypatch.setattr(SettingsManager, "__init__", patched_init)

    def override_get_obo_ws() -> WorkspaceClient:
        """Override OBO workspace client to use the test workspace client."""
        return ws

    def override_get_dqx_engine() -> DQEngine:
        """Override DQX engine to use test spark session."""
        return DQEngine(workspace_client=ws, spark=spark)

    app.dependency_overrides[get_obo_ws] = override_get_obo_ws
    app.dependency_overrides[get_dqx_engine] = override_get_dqx_engine

    client = TestClient(app)
    yield client

    # Clean up dependency overrides
    app.dependency_overrides.clear()


@pytest.fixture
def config_file(ws, installation_folder):
    """Fixture that tracks and cleans up config.yml files."""
    config_paths = []

    def track_config(folder_path):
        """Track a config.yml for cleanup."""
        config_path = f"{folder_path}/config.yml"
        config_paths.append(config_path)
        return config_path

    yield track_config

    # Cleanup all created config files
    for config_path in config_paths:
        try:
            ws.workspace.delete(config_path, recursive=False)
        except ResourceDoesNotExist:
            pass


class TestSettingsManagerIntegration:
    """Integration tests for SettingsManager with real Databricks workspace."""

    def test_get_settings_returns_default_when_app_yml_does_not_exist(self, ws, settings_manager):
        settings = settings_manager.get_settings()
        assert ws.current_user.me().user_name in settings.install_folder

    def test_save_and_get_settings_roundtrip_custom_folder(self, settings_manager, installation_folder):
        """Should successfully save and retrieve custom install folder."""
        custom_folder = installation_folder()

        # Save custom settings
        settings_to_save = InstallationSettings(install_folder=custom_folder)
        saved_settings = settings_manager.save_settings(settings_to_save)

        assert saved_settings.install_folder == custom_folder

        # Retrieve and verify
        retrieved_settings = settings_manager.get_settings()
        assert retrieved_settings.install_folder == custom_folder

    def test_save_settings_creates_install_folder_if_not_exists(self, ws, settings_manager, installation_folder):
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

    def test_save_settings_overwrites_existing_app_yml(self, settings_manager, installation_folder):
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

    def test_save_settings_strips_whitespace_from_path(self, settings_manager, installation_folder):
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

    def test_save_settings_with_nested_folder_structure(self, ws, settings_manager, installation_folder, make_random):
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

    def test_save_settings_creates_default_config_yml(self, ws, settings_manager, installation_folder):
        """Should create a default config.yml when saving settings if it doesn't exist."""
        custom_folder = installation_folder()

        # Save settings - should create both app.yml and config.yml
        settings_to_save = InstallationSettings(install_folder=custom_folder)
        settings_manager.save_settings(settings_to_save)

        # Verify config.yml was created
        config_path = f"{custom_folder}/config.yml"
        config_status = ws.workspace.get_status(config_path)
        assert config_status is not None
        assert config_status.path == config_path

        # Verify it's a valid empty config
        serializer = ConfigSerializer(ws)
        config = serializer.load_config(install_folder=custom_folder)
        assert len(config.run_configs) == 0

    def test_save_settings_does_not_overwrite_existing_config_yml(self, ws, settings_manager, installation_folder):
        """Should not overwrite existing config.yml when saving settings."""
        custom_folder = installation_folder()

        # Create a config with some data
        serializer = ConfigSerializer(ws)
        existing_config = WorkspaceConfig(
            run_configs=[
                RunConfig(
                    name="existing_run",
                    input_config=InputConfig(location="main.default.input"),
                    output_config=OutputConfig(location="main.default.output"),
                    checks_location="checks.yml",
                )
            ]
        )
        serializer.save_config(existing_config, install_folder=custom_folder)

        # Now save settings - should NOT overwrite the existing config
        settings_to_save = InstallationSettings(install_folder=custom_folder)
        settings_manager.save_settings(settings_to_save)

        # Verify the existing config is still there
        loaded_config = serializer.load_config(install_folder=custom_folder)
        assert len(loaded_config.run_configs) == 1
        assert loaded_config.run_configs[0].name == "existing_run"


class TestRouterIntegration:
    """Integration tests for API router endpoints."""

    # ============= Version Endpoint =============

    def test_get_version(self, api_client):
        """Should return application version information."""
        response = api_client.get("/api/version")

        assert response.status_code == 200
        data = response.json()
        assert "version" in data
        assert len(data["version"]) > 0

    # ============= Current User Endpoint =============

    def test_get_current_user(self, api_client, ws):
        """Should return current user information."""
        response = api_client.get("/api/current-user")

        assert response.status_code == 200
        data = response.json()
        assert "user_name" in data
        assert data["user_name"] == ws.current_user.me().user_name

    # ============= Settings Endpoints =============

    def test_get_settings_default(self, api_client, ws):
        """Should return default settings when no custom settings exist."""
        response = api_client.get("/api/settings")

        assert response.status_code == 200
        data = response.json()
        assert "install_folder" in data
        # Should return a path under the user's home directory with .dqx in it
        user_name = ws.current_user.me().user_name
        assert user_name in data["install_folder"]
        assert ".dqx" in data["install_folder"]

    def test_save_and_get_settings_roundtrip(self, api_client, installation_folder):
        """Should save and retrieve custom settings via API."""
        custom_folder = installation_folder()

        # Save settings
        response = api_client.post("/api/settings", json={"install_folder": custom_folder})

        assert response.status_code == 200
        assert response.json()["install_folder"] == custom_folder

        # Retrieve settings
        response = api_client.get("/api/settings")
        assert response.status_code == 200
        assert response.json()["install_folder"] == custom_folder

    def test_save_settings_with_whitespace(self, api_client, installation_folder):
        """Should strip whitespace from install folder path."""
        custom_folder = installation_folder()
        folder_with_whitespace = f"  {custom_folder}  "

        response = api_client.post("/api/settings", json={"install_folder": folder_with_whitespace})

        assert response.status_code == 200
        assert response.json()["install_folder"] == custom_folder

    def test_save_settings_creates_default_config_via_api(self, api_client, ws, installation_folder):
        """Should create default config.yml when saving settings via API."""
        custom_folder = installation_folder()

        # Save settings via API
        response = api_client.post("/api/settings", json={"install_folder": custom_folder})
        assert response.status_code == 200

        # Verify config.yml was created
        config_path = f"{custom_folder}/config.yml"
        config_status = ws.workspace.get_status(config_path)
        assert config_status is not None
        assert config_status.path == config_path

        # Verify it's a valid empty config
        serializer = ConfigSerializer(ws)
        config = serializer.load_config(install_folder=custom_folder)
        assert len(config.run_configs) == 0

    # ============= Config Endpoints =============

    def test_get_config_not_found(self, api_client, installation_folder):
        """Should return 404 when config doesn't exist."""
        non_existent_folder = installation_folder()

        response = api_client.get("/api/config", params={"path": non_existent_folder})

        assert response.status_code == 404
        assert "Configuration not found" in response.json()["detail"]

    def test_save_and_get_config_roundtrip(self, api_client, ws, installation_folder, config_file):
        """Should save and retrieve workspace configuration via API."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create test config
        config_data = {
            "run_configs": [
                {
                    "name": "test_run",
                    "input_config": {"location": "main.default.test_input"},
                    "output_config": {"location": "main.default.test_output"},
                    "checks_location": "checks.yml",
                }
            ]
        }

        # Save config
        response = api_client.post("/api/config", params={"path": install_folder}, json={"config": config_data})

        assert response.status_code == 200
        saved_config = response.json()["config"]
        assert len(saved_config["run_configs"]) == 1

        # Retrieve config
        response = api_client.get("/api/config", params={"path": install_folder})

        assert response.status_code == 200
        retrieved_config = response.json()["config"]
        assert retrieved_config["run_configs"][0]["name"] == "test_run"

    def test_get_config_uses_default_path_from_settings(
        self, api_client, ws, settings_manager, installation_folder, config_file
    ):
        """Should use install folder from settings when path not provided."""
        # Save settings with custom folder
        custom_folder = installation_folder()
        config_file(custom_folder)
        settings_manager.save_settings(InstallationSettings(install_folder=custom_folder))

        # Save a config to that folder
        serializer = ConfigSerializer(ws)
        test_config = WorkspaceConfig(
            run_configs=[
                RunConfig(
                    name="test_run",
                    input_config=InputConfig(location="main.default.input"),
                    output_config=OutputConfig(location="main.default.output"),
                    checks_location="checks.yml",
                )
            ]
        )
        serializer.save_config(test_config, install_folder=custom_folder)

        # Get config without path parameter - should use settings
        response = api_client.get("/api/config")

        assert response.status_code == 200
        assert len(response.json()["config"]["run_configs"]) == 1

    # ============= Run Config Endpoints =============

    def test_get_run_config_not_found(self, api_client, installation_folder, config_file):
        """Should return 404 when run config doesn't exist."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        response = api_client.get("/api/config/run/nonexistent", params={"path": install_folder})

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_save_and_get_run_config_roundtrip(self, api_client, installation_folder, config_file):
        """Should save and retrieve individual run configuration via API."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        # Save run config
        run_config = {
            "name": "test_run",
            "input_config": {"location": "main.default.test_input"},
            "output_config": {"location": "main.default.test_output"},
            "checks_location": "checks.yml",
        }

        response = api_client.post("/api/config/run", params={"path": install_folder}, json={"config": run_config})

        assert response.status_code == 200
        assert response.json()["config"]["name"] == "test_run"

        # Retrieve run config
        response = api_client.get("/api/config/run/test_run", params={"path": install_folder})

        assert response.status_code == 200
        assert response.json()["config"]["name"] == "test_run"

    def test_update_existing_run_config(self, api_client, installation_folder, config_file):
        """Should update an existing run configuration."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        # Save initial run config
        run_config = {
            "name": "test_run",
            "input_config": {"location": "main.default.input_v1"},
            "output_config": {"location": "main.default.output_v1"},
            "checks_location": "checks.yml",
        }
        api_client.post("/api/config/run", params={"path": install_folder}, json={"config": run_config})

        # Update run config
        updated_config = {
            "name": "test_run",
            "input_config": {"location": "main.default.input_v2"},
            "output_config": {"location": "main.default.output_v2"},
            "checks_location": "checks_v2.yml",
        }
        response = api_client.post("/api/config/run", params={"path": install_folder}, json={"config": updated_config})

        assert response.status_code == 200

        # Verify update
        response = api_client.get("/api/config/run/test_run", params={"path": install_folder})
        retrieved = response.json()["config"]
        assert retrieved["input_config"]["location"] == "main.default.input_v2"
        assert retrieved["checks_location"] == "checks_v2.yml"

    def test_delete_run_config(self, api_client, installation_folder, config_file):
        """Should delete a run configuration via API."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        # Create run config
        run_config = {
            "name": "test_run",
            "input_config": {"location": "main.default.test_input"},
            "output_config": {"location": "main.default.test_output"},
            "checks_location": "checks.yml",
        }
        api_client.post("/api/config/run", params={"path": install_folder}, json={"config": run_config})

        # Delete the run config
        response = api_client.delete("/api/config/run/test_run", params={"path": install_folder})

        assert response.status_code == 200
        remaining_configs = response.json()["config"]["run_configs"]
        assert len(remaining_configs) == 0

        # Verify it's deleted
        response = api_client.get("/api/config/run/test_run", params={"path": install_folder})
        assert response.status_code == 404

    def test_delete_nonexistent_run_config(self, api_client, installation_folder, config_file):
        """Should return 404 when deleting non-existent run config."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        # Try to delete non-existent run config
        response = api_client.delete("/api/config/run/nonexistent", params={"path": install_folder})

        assert response.status_code == 404

    def test_delete_one_of_multiple_run_configs(self, api_client, installation_folder, config_file):
        """Should delete only the specified run config when multiple exist."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        # Create two run configs
        run_config1 = {
            "name": "run1",
            "input_config": {"location": "main.default.input1"},
            "output_config": {"location": "main.default.output1"},
            "checks_location": "checks1.yml",
        }
        run_config2 = {
            "name": "run2",
            "input_config": {"location": "main.default.input2"},
            "output_config": {"location": "main.default.output2"},
            "checks_location": "checks2.yml",
        }

        api_client.post("/api/config/run", params={"path": install_folder}, json={"config": run_config1})
        api_client.post("/api/config/run", params={"path": install_folder}, json={"config": run_config2})

        # Delete run1
        response = api_client.delete("/api/config/run/run1", params={"path": install_folder})

        assert response.status_code == 200
        remaining = response.json()["config"]["run_configs"]
        assert len(remaining) == 1
        assert remaining[0]["name"] == "run2"

    # ============= Checks Endpoints =============

    def test_save_and_get_checks_roundtrip(self, api_client, ws, installation_folder, config_file):
        """Should save and retrieve checks for a run configuration via API."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        # First create run config
        run_config = {
            "name": "test_run",
            "input_config": {"location": "main.default.test_input"},
            "output_config": {"location": "main.default.test_output"},
            "checks_location": "checks.yml",
        }
        api_client.post("/api/config/run", params={"path": install_folder}, json={"config": run_config})

        # Save checks
        checks = [
            {
                "name": "test_check",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            }
        ]

        response = api_client.post(
            "/api/config/run/test_run/checks", params={"path": install_folder}, json={"checks": checks}
        )

        assert response.status_code == 200
        assert len(response.json()["checks"]) == 1

        # Retrieve checks
        response = api_client.get("/api/config/run/test_run/checks", params={"path": install_folder})

        assert response.status_code == 200
        retrieved_checks = response.json()["checks"]
        assert len(retrieved_checks) == 1
        assert retrieved_checks[0]["name"] == "test_check"

    def test_get_checks_for_nonexistent_run_config(self, api_client, installation_folder, config_file):
        """Should return 404 when getting checks for non-existent run config."""
        install_folder = installation_folder()
        config_file(install_folder)

        response = api_client.get("/api/config/run/nonexistent/checks", params={"path": install_folder})

        assert response.status_code == 404

    def test_save_checks_for_nonexistent_run_config(self, api_client, installation_folder, config_file):
        """Should return 404 when saving checks for non-existent run config."""
        install_folder = installation_folder()
        config_file(install_folder)

        response = api_client.post(
            "/api/config/run/nonexistent/checks", params={"path": install_folder}, json={"checks": []}
        )

        assert response.status_code == 404

    def test_update_existing_checks(self, api_client, ws, installation_folder, config_file):
        """Should update existing checks for a run configuration."""
        install_folder = installation_folder()
        config_file(install_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": install_folder}, json={"config": {"run_configs": []}})

        # Create run config
        run_config = {
            "name": "test_run",
            "input_config": {"location": "main.default.test_input"},
            "output_config": {"location": "main.default.test_output"},
            "checks_location": "checks.yml",
        }
        api_client.post("/api/config/run", params={"path": install_folder}, json={"config": run_config})

        # Save initial checks
        checks_v1 = [
            {
                "name": "check_v1",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            }
        ]
        api_client.post("/api/config/run/test_run/checks", params={"path": install_folder}, json={"checks": checks_v1})

        # Update checks
        checks_v2 = [
            {
                "name": "check_v2",
                "criticality": "warning",
                "check": {"function": "is_not_null", "arguments": {"column": "name"}},
            },
            {
                "name": "check_v3",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "email"}},
            },
        ]
        response = api_client.post(
            "/api/config/run/test_run/checks", params={"path": install_folder}, json={"checks": checks_v2}
        )

        assert response.status_code == 200
        assert len(response.json()["checks"]) == 2

        # Verify checks were updated
        response = api_client.get("/api/config/run/test_run/checks", params={"path": install_folder})
        retrieved = response.json()["checks"]
        assert len(retrieved) == 2
        assert retrieved[0]["name"] == "check_v2"
        assert retrieved[1]["name"] == "check_v3"

    # ============= Path Parameter Tests =============

    def test_config_endpoints_with_custom_path_parameter(self, api_client, installation_folder, config_file):
        """Should use custom path parameter instead of default settings."""
        custom_folder = installation_folder()
        config_file(custom_folder)

        config_data = {
            "run_configs": [
                {
                    "name": "test_run",
                    "input_config": {"location": "main.default.input"},
                    "output_config": {"location": "main.default.output"},
                    "checks_location": "checks.yml",
                }
            ]
        }

        # Save to custom path
        response = api_client.post("/api/config", params={"path": custom_folder}, json={"config": config_data})
        assert response.status_code == 200

        # Retrieve from same custom path
        response = api_client.get("/api/config", params={"path": custom_folder})
        assert response.status_code == 200
        assert len(response.json()["config"]["run_configs"]) == 1

    def test_run_config_endpoints_with_custom_path_parameter(self, api_client, installation_folder, config_file):
        """Should use custom path parameter for run config operations."""
        custom_folder = installation_folder()
        config_file(custom_folder)

        # Create empty config first
        api_client.post("/api/config", params={"path": custom_folder}, json={"config": {"run_configs": []}})

        run_config = {
            "name": "custom_path_run",
            "input_config": {"location": "main.default.input"},
            "output_config": {"location": "main.default.output"},
            "checks_location": "checks.yml",
        }

        # Save to custom path
        response = api_client.post("/api/config/run", params={"path": custom_folder}, json={"config": run_config})
        assert response.status_code == 200

        # Retrieve from same custom path
        response = api_client.get("/api/config/run/custom_path_run", params={"path": custom_folder})
        assert response.status_code == 200
        assert response.json()["config"]["name"] == "custom_path_run"

        # Delete from custom path
        response = api_client.delete("/api/config/run/custom_path_run", params={"path": custom_folder})
        assert response.status_code == 200
