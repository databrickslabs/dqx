import logging
import os

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import WorkspaceConfig

logger = logging.getLogger(__name__)


class InstallationMixin:
    def __init__(self, config: WorkspaceConfig, installation: Installation, ws: WorkspaceClient):
        self._config = config
        self._installation = installation
        self._ws = ws

    def _name(self, name: str) -> str:
        prefix = os.path.basename(self._installation.install_folder()).removeprefix('.')
        return f"[{prefix.upper()}] {name}"

    @property
    def _my_username(self):
        if not hasattr(self, "_me"):
            self._me = self._ws.current_user.me()
        return self._me.user_name
