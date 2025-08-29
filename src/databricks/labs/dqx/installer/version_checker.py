import logging
import re

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.wheels import ProductInfo, Version
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)


class VersionChecker:
    """Encapsulates version detection and comparison logic for DQX installations."""

    def __init__(self, product_info: ProductInfo, installation: Installation, prompts: Prompts):
        self._product_info = product_info
        self._installation = installation
        self._prompts = prompts

    @staticmethod
    def extract_major_minor(version_string: str):
        """
        Extracts the major and minor version from a version string.

        Args:
            version_string: The version string to extract from.

        Returns:
            The major.minor version as a string, or None if not found.
        """
        match = re.search(r"(\d+\.\d+)", version_string)
        if match:
            return match.group(1)
        return None

    def compare_and_prompt_upgrade(self) -> None:
        """Compares released and installed versions and optionally asks user to update.

        Behavior matches previous inline implementation in `WorkspaceInstaller`.
        """
        try:
            local_version = self._product_info.released_version()
            remote_version = self._installation.load(Version).version
            if VersionChecker.extract_major_minor(remote_version) == VersionChecker.extract_major_minor(local_version):
                logger.info(f"DQX v{self._product_info.version()} is already installed on this workspace")
                msg = "Do you want to update the existing installation?"
                if not self._prompts.confirm(msg):
                    raise RuntimeWarning(
                        "DQX workspace remote and local install versions are same and no override is requested. "
                        "Exiting..."
                    )
        except NotFound as err:
            logger.warning(f"DQX workspace remote version not found: {err}")
