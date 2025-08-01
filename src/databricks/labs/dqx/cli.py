import json
import webbrowser

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.checks_storage import WorkspaceFileChecksStorageHandler
from databricks.labs.dqx.config import WorkspaceConfig, WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.contexts.workspace import WorkspaceContext
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
def open_dashboards(w: WorkspaceClient, *, ctx: WorkspaceContext | None = None):
    """
    Opens remote dashboard directory in the browser.

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w)
    workspace_link = ctx.installation.workspace_link("")
    webbrowser.open(f"{workspace_link}dashboards/")


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
    w: WorkspaceClient,
    *,
    run_config: str = "default",
    validate_custom_check_functions: bool = True,
    ctx: WorkspaceContext | None = None,
) -> list[dict]:
    """
    Validate checks stored in the installation directory as a file.

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param run_config: The name of the run configuration to use.
    :param validate_custom_check_functions: Whether to validate custom check functions (default is True).
    :param ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w)
    config = ctx.installation.load(WorkspaceConfig)
    checks_file = f"{ctx.installation.install_folder()}/{config.get_run_config(run_config).checks_file}"
    # Not using the installation method because loading from a table requires a Spark session,
    # which isn't available when the CLI is invoked in the local user context.
    checks = WorkspaceFileChecksStorageHandler(w).load(config=WorkspaceFileChecksStorageConfig(location=checks_file))
    status = DQEngine.validate_checks(checks, validate_custom_check_functions=validate_custom_check_functions)

    errors_list = []
    if status.has_errors:
        errors_list = [{"error": error} for error in status.errors]

    print(json.dumps(errors_list))
    return errors_list


@dqx.command
def profile(w: WorkspaceClient, *, run_config: str = "default", ctx: WorkspaceContext | None = None) -> None:
    """
    Profile input data and generate quality rule (checks) candidates.

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param run_config: The name of the run configuration to use.
    :param ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w)
    ctx.deployed_workflows.run_workflow("profiler", run_config)


@dqx.command
def workflows(w: WorkspaceClient, *, ctx: WorkspaceContext | None = None):
    """
    Show deployed workflows and their state

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w)
    logger.info("Fetching deployed jobs...")
    latest_job_status = ctx.deployed_workflows.latest_job_status()
    print(json.dumps(latest_job_status))
    return latest_job_status


@dqx.command
def logs(w: WorkspaceClient, *, workflow: str | None = None, ctx: WorkspaceContext | None = None):
    """
    Show logs of the latest job run.

    :param w: The WorkspaceClient instance to use for accessing the workspace.
    :param workflow: The name of the workflow to show logs for.
    :param ctx: The WorkspaceContext instance to use for accessing the workspace
    """
    ctx = ctx or WorkspaceContext(w)
    ctx.deployed_workflows.relay_logs(workflow)


if __name__ == "__main__":
    dqx()
