import json
import webbrowser

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.contexts.workspace_cli import WorkspaceContext

dqx = App(__file__)
logger = get_logger(__file__)

CANT_FIND_DQX_MSG = (
    "Couldn't find DQX configuration in the user's home folder. "
    "Make sure the current user has configured and installed DQX."
)


@dqx.command
def me(w: WorkspaceClient, greeting: str):
    """shows current installation username"""
    ctx = WorkspaceContext(w)
    print(f"{greeting} {ctx.installation.username()}")


@dqx.command
def open_remote_config(w: WorkspaceClient):
    """Opens remote configuration in the browser"""
    ctx = WorkspaceContext(w)
    workspace_link = ctx.installation.workspace_link("config.yml")
    webbrowser.open(workspace_link)


@dqx.command
def installations(w: WorkspaceClient):
    """Show installations by different users on the same workspace"""
    logger.info("Fetching installations...")
    all_users = []
    for installation in Installation.existing(w, "dqx"):
        try:
            config = installation.load(WorkspaceConfig)
            all_users.append(
                {
                    "input": config.input_location,
                    "version": config.__version__,
                    "path": installation.install_folder(),
                }
            )
        except NotFound:
            continue
        except SerdeError:
            continue
    print(json.dumps(all_users))


if __name__ == "__main__":
    dqx()
