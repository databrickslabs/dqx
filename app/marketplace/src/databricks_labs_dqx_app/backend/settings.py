import base64

import yaml
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.workspace import ExportFormat, ImportFormat

from .logger import logger
from .models import InstallationSettings


class SettingsManager:
    def __init__(self, ws: WorkspaceClient):
        self.ws = ws
        self.user = ws.current_user.me()
        self.user_home = f"/Users/{self.user.user_name}"
        self.default_dqx_folder = f"{self.user_home}/.dqx"
        self.app_settings_path = f"{self.default_dqx_folder}/app.yml"

    def get_default_install_folder(self) -> str:
        return self.default_dqx_folder

    def get_settings(self) -> InstallationSettings:
        """Read app settings from app.yml.

        The app.yml file stores app-specific settings, e.g. which DQX installation folder to use).
        This is separate from config.yml which contains DQX configuration.
        """
        try:
            resp = self.ws.workspace.export(self.app_settings_path, format=ExportFormat.SOURCE)
            if resp.content:
                decoded = base64.b64decode(resp.content).decode("utf-8")
                data = yaml.safe_load(decoded)
                if data and "install_folder" in data:
                    install_folder = data["install_folder"]
                    return InstallationSettings(install_folder=install_folder)
        except ResourceDoesNotExist:
            pass
        except Exception as e:
            logger.warning(f"Failed to read app settings from {self.app_settings_path}: {e}")

        # Return default if not found or error
        return InstallationSettings(install_folder=self.get_default_install_folder())

    def save_settings(self, settings: InstallationSettings) -> InstallationSettings:
        """Save app settings to app.yml and ensure default config exists.

        This method:
        1. Saves the install folder path to app.yml
        2. Creates the install folder if it doesn't exist
        3. Creates a default config.yml in the install folder if it doesn't exist

        The app.yml file stores app-specific configuration (which install folder to use).
        The config.yml file stores the actual DQX configuration (run configs, checks, etc.).
        """
        install_folder = settings.install_folder.strip()

        # Ensure the .dqx folder exists (where app.yml lives)
        try:
            self.ws.workspace.mkdirs(self.default_dqx_folder)
        except Exception as e:
            logger.error(f"Failed to create .dqx folder {self.default_dqx_folder}: {e}")
            raise ValueError(f"Could not create .dqx folder: {self.default_dqx_folder}") from e

        if install_folder != self.default_dqx_folder:
            # Ensure the install folder exists (create if needed)
            try:
                self.ws.workspace.mkdirs(install_folder)
            except Exception as e:
                logger.error(f"Failed to create install folder {install_folder}: {e}")
                raise ValueError(f"Could not create install folder: {install_folder}") from e

        # Save app.yml with info about install folder
        content = yaml.dump({"install_folder": install_folder})
        content_bytes = content.encode("utf-8")

        try:
            self.ws.workspace.upload(self.app_settings_path, content_bytes, format=ImportFormat.AUTO, overwrite=True)
            logger.info(f"Saved app settings to {self.app_settings_path}: install_folder={install_folder}")
        except Exception as e:
            logger.error(f"Failed to save app settings to {self.app_settings_path}: {e}", exc_info=True)
            raise ValueError(f"Could not save app settings to: {self.app_settings_path}") from e

        # Create default config.yml if it doesn't exist
        self._ensure_default_config_exists(install_folder)

        return InstallationSettings(install_folder=install_folder)

    def _ensure_default_config_exists(self, install_folder: str) -> None:
        """Create a default config.yml in the install folder if it doesn't exist.

        Args:
            install_folder: The folder where config.yml should be located.
        """
        config_path = f"{install_folder}/config.yml"

        # Check if config.yml already exists
        try:
            self.ws.workspace.get_status(config_path)
            logger.info(f"Config file already exists at {config_path}")
            return  # Config already exists, nothing to do
        except ResourceDoesNotExist:
            # Config doesn't exist, create a default one
            logger.info(f"Config file not found at {config_path}, creating default config")
        except Exception as e:
            logger.warning(f"Error checking for config file at {config_path}: {e}")
            return  # Don't fail the save_settings operation if we can't check

        # Create a default empty config
        try:
            default_config = WorkspaceConfig(run_configs=[])
            serializer = ConfigSerializer(self.ws)
            serializer.save_config(default_config, install_folder=install_folder)
            logger.info(f"Created default config at {config_path}")
        except Exception as e:
            # Log the error but don't fail the save_settings operation
            logger.warning(f"Failed to create default config at {config_path}: {e}")
