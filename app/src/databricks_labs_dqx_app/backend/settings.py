import base64

import yaml
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
        """Save app settings to app.yml.

        The app.yml file stores app-specific configuration, e.g. which install folder to use for DQX.
        This is separate from config.yml which contains the actual DQX configuration.
        """
        install_folder = settings.install_folder.strip()

        # Ensure the .dqx folder exists (where app.yml lives)
        try:
            self.ws.workspace.mkdirs(self.default_dqx_folder)
        except Exception as e:
            logger.error(f"Failed to create .dqx folder {self.default_dqx_folder}: {e}")
            raise ValueError(f"Could not create .dqx folder: {self.default_dqx_folder}") from e

        # Ensure the install folder exists (create if needed)
        try:
            self.ws.workspace.mkdirs(install_folder)
        except Exception as e:
            logger.error(f"Failed to create install folder {install_folder}: {e}")
            raise ValueError(f"Could not create install folder: {install_folder}") from e

        # Save app.yml with the install folder
        content = yaml.dump({"install_folder": install_folder})
        content_bytes = content.encode("utf-8")

        try:
            self.ws.workspace.upload(self.app_settings_path, content_bytes, format=ImportFormat.AUTO, overwrite=True)
            logger.info(f"Saved app settings to {self.app_settings_path}: install_folder={install_folder}")
        except Exception as e:
            logger.error(f"Failed to save app settings to {self.app_settings_path}: {e}", exc_info=True)
            raise ValueError(f"Could not save app settings to: {self.app_settings_path}") from e

        return InstallationSettings(install_folder=install_folder)
