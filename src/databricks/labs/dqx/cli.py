import json
import webbrowser

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.dqx.engine import DQEngine

dqx = App(__file__)
logger = get_logger(__file__)


@dqx.command
def open_remote_config(w: WorkspaceClient, *, ctx: WorkspaceContext | None = None):
    """
    Opens remote configuration in the browser.

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w)
    workspace_link = ctx.installation.workspace_link(WorkspaceConfig.__file__)
    webbrowser.open(workspace_link)


@dqx.command
def installations(w: WorkspaceClient, *, product_name: str = "dqx") -> list[dict]:
    """
    Show installations by different users on the same workspace.

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param product_name: The name of the product to search for in the installation folder.
    """
    logger.info("Fetching installations...")
    all_users = []
    for installation in Installation.existing(w, product_name):
        try:
            config = installation.load(WorkspaceConfig)
            all_users.append(
                {
                    "version": config.__version__,
                    "path": installation.install_folder(),
                }
            )
        except NotFound:
            continue
        except SerdeError:
            continue

    print(json.dumps(all_users))
    return all_users


@dqx.command
def validate_checks(
    w: WorkspaceClient, *, run_config: str = "default", ctx: WorkspaceContext | None = None
) -> list[dict]:
    """
    Validate checks stored in the installation directory as a file.

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param run_config: The name of the run configuration to use.
    :param ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w)
    config = ctx.installation.load(WorkspaceConfig)
    checks_file = f"{ctx.installation.install_folder()}/{config.get_run_config(run_config).checks_file}"
    dq_engine = DQEngine(w)
    checks = dq_engine.load_checks_from_workspace_file(checks_file)
    status = dq_engine.validate_checks(checks)

    errors_list = []
    if status.has_errors:
        errors_list = [{"error": error} for error in status.errors]

    print(json.dumps(errors_list))
    return errors_list


if __name__ == "__main__":
    dqx()
