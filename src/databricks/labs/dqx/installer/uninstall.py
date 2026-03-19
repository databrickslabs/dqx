import logging

from databricks.labs.blueprint.entrypoint import is_in_debug
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.installer.install import InstallationService


if __name__ == "__main__":
    if is_in_debug():
        logging.getLogger("databricks").setLevel(logging.DEBUG)

    uninstaller_prompts = Prompts()
    custom_folder = uninstaller_prompts.question(
        "Enter the workspace path of the DQX installation to uninstall "
        "(leave empty to uninstall from user's home or global directory)",
        default="empty",
        valid_regex=r"^(/.*)?$",
    ).strip()

    custom_install_folder = custom_folder if custom_folder and custom_folder != "empty" else None

    ws = WorkspaceClient(product="dqx", product_version=__version__)
    installer = InstallationService.current(ws, install_folder=custom_install_folder)
    installer.uninstall()
