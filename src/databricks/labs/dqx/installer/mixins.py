import logging
import os

from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx.mixins import PickleableMixin
from databricks.labs.dqx.utils import get_workspace_client
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class InstallationMixin(PickleableMixin):
    def __init__(self, ws: WorkspaceClient | None = None):
        self._ws = ws

    @property
    def ws(self) -> WorkspaceClient:
        return self._ws or get_workspace_client()

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
            self._me = self.ws.current_user.me()
        return self._me.user_name

    def _get_installation(
        self, product_name: str = "dqx", assume_user: bool = True, install_folder: str | None = None
    ) -> Installation:
        """
        Get the installation for the given product name.

        Args:
            product_name: name of the product.
            assume_user: if True, assume user installation.
            install_folder: optional installation folder.
        """

        if install_folder:
            return Installation(self.ws, product_name, install_folder=install_folder)

        if assume_user:
            installation = Installation.assume_user_home(self.ws, product_name)
        else:
            installation = Installation.assume_global(self.ws, product_name)

        installation.current(self.ws, product_name, assume_user=assume_user)
        return installation
