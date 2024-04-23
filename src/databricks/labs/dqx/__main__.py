from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.sdk import AccountClient, WorkspaceClient

dqx = App(__file__)
logger = get_logger(__file__)


@dqx.command
def me(w: WorkspaceClient, greeting: str):
    """Shows current username"""
    logger.info(f"{greeting}, {w.current_user.me().user_name}!")


@dqx.command(is_account=True)
def workspaces(a: AccountClient):
    """Shows workspaces"""
    for ws in a.workspaces.list():
        logger.info(f"Workspace: {ws.workspace_name} ({ws.workspace_id})")


if __name__ == "__main__":
    dqx()
