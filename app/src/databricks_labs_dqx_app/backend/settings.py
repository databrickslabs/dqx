import yaml
import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.workspace import ExportFormat, ImportFormat

from .models import InstallationSettings
from .logger import logger

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
        try:
            # Try to read app.yml using Workspace API (export)
            # Export returns response with content field (base64 encoded usually if not direct download, 
            # but SDK helper .download might handle it?
            # ws.workspace.export returns ExportResponse(content=..., file_type=...)
            
            # Using download() context manager helper from SDK mixin if available,
            # but SDK documentation says 'export' returns the content.
            # Let's use the standard export method.
            
            resp = self.ws.workspace.export(self.app_settings_path, format=ExportFormat.SOURCE)
            if resp.content:
                # Content is base64 encoded string in the response object
                decoded = base64.b64decode(resp.content).decode("utf-8")
                data = yaml.safe_load(decoded)
                if data and "install_folder" in data:
                    return InstallationSettings(
                        install_folder=data["install_folder"],
                        is_default=False
                    )
        except ResourceDoesNotExist:
            pass
        except Exception as e:
            logger.warning(f"Failed to read app settings: {e}")

        # Return default if not found or error
        return InstallationSettings(
            install_folder=self.get_default_install_folder(),
            is_default=True
        )

    def save_settings(self, settings: InstallationSettings) -> InstallationSettings:
        default_folder = self.get_default_install_folder()
        
        path_str = settings.install_folder.strip()
        
        if not path_str.endswith(".yml") and not path_str.endswith(".yaml"):
             raise ValueError("Configuration path must be a valid .yml or .yaml file")

        # Normalize for comparison
        # If the user provided path is exactly the default folder (without config.yml), 
        # or default folder + /config.yml, we treat it as default.
        
        is_default = False
        if path_str == default_folder or path_str == f"{default_folder}/config.yml":
             is_default = True

        if is_default:
            # It is default, remove app.yml if exists
            try:
                self.ws.workspace.delete(self.app_settings_path)
            except ResourceDoesNotExist:
                pass
            except Exception as e:
                logger.warning(f"Failed to delete app settings: {e}")
            
            return InstallationSettings(
                install_folder=default_folder, # Return the folder by default? Or explicit path?
                is_default=True
            )
        else:
            # Ensure .dqx folder exists (where app.yml lives)
            # Workspace API 'mkdirs'
            try:
                self.ws.workspace.mkdirs(self.default_dqx_folder)
            except Exception:
                pass 

            # Save app.yml using Workspace API import
            content = yaml.dump({"install_folder": path_str})
            encoded_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
            
            self.ws.workspace.import_(
                self.app_settings_path, 
                content=encoded_content, 
                format=ImportFormat.SOURCE,
                overwrite=True
            )
            
            return InstallationSettings(
                install_folder=path_str,
                is_default=False
            )
