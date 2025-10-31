import logging
import os

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)


class InstallationMixin:
    def __init__(self, workspace_client: WorkspaceClient):
        self._workspace_client = workspace_client

    @staticmethod
    def _get_name(name: str, install_folder: str) -> str:
        """
        Current installation name
        """
        name_prefix = os.path.basename(install_folder).removeprefix('.').upper()
        return f"[{name_prefix}] {name}"

    @property
    def _my_username(self):
        """
        Current user
        """
        if not hasattr(self, "_me"):
            self._me = self._workspace_client.current_user.me()
        return self._me.user_name

    def _get_installation(
        self, product_name: str, assume_user: bool = True, install_folder: str | None = None
    ) -> Installation:
        """
        Get the installation for the given product name.

        Args:
            assume_user: if True, assume user installation
            product_name: name of the product
            install_folder: optional installation folder
        """

        if install_folder:
            installation = self._get_custom_installation(product_name, install_folder)
            return installation

        if assume_user:
            installation = Installation.assume_user_home(self._workspace_client, product_name)
        else:
            installation = Installation.assume_global(self._workspace_client, product_name)

        installation.current(self._workspace_client, product_name, assume_user=assume_user)
        return installation

    def _get_custom_installation(self, product_name: str, install_folder: str) -> Installation:
        """
        Creates an Installation instance for a custom installation folder, similar to assume_user_home and assume_global.
        This ensures the custom folder is created in the workspace when the installation is accessed.

        Args:
            product_name: The product name
            install_folder: The custom installation folder path

        Returns:
            An Installation instance for the custom folder
        """
        try:
            self._workspace_client.workspace.get_status(install_folder)
        except NotFound:
            self._workspace_client.workspace.mkdirs(install_folder)

        return Installation(self._workspace_client, product_name, install_folder=install_folder)
