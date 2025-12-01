import json
import webbrowser
from datetime import timedelta

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.checks_storage import WorkspaceFileChecksStorageHandler, VolumeFileChecksStorageHandler
from databricks.labs.dqx.config import (
    WorkspaceConfig,
    WorkspaceFileChecksStorageConfig,
    RunConfig,
    VolumeFileChecksStorageConfig,
)
from databricks.labs.dqx.contexts.workspace_context import WorkspaceContext
from databricks.labs.dqx.engine import DQEngine

dqx = App(__file__)
logger = get_logger(__file__)


@dqx.command
def open_remote_config(w: WorkspaceClient, *, install_folder: str = "", ctx: WorkspaceContext | None = None):
    """
    Opens remote configuration in the browser.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        install_folder: Optional custom installation folder path.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    workspace_link = ctx.installation.workspace_link(WorkspaceConfig.__file__)
    webbrowser.open(workspace_link)


@dqx.command
def open_dashboards(w: WorkspaceClient, *, install_folder: str = "", ctx: WorkspaceContext | None = None):
    """
    Opens remote dashboard directory in the browser.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        install_folder: Optional custom installation folder path.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    workspace_link = ctx.installation.workspace_link("")
    webbrowser.open(f"{workspace_link}dashboards/")


@dqx.command
def installations(w: WorkspaceClient, *, product_name: str = "dqx") -> list[dict]:
    """
    Show installations by different users on the same workspace.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        product_name: The name of the product to search for in the installation folder.
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
    run_config: str = "",
    validate_custom_check_functions: bool = True,
    install_folder: str = "",
    ctx: WorkspaceContext | None = None,
) -> list[dict]:
    """
    Validate checks stored in a workspace file or volume.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        run_config: The name of the run configuration to use. If not provided, run it for all run configs.
        validate_custom_check_functions: Whether to validate custom check functions (default is True).
        install_folder: Optional custom installation folder path.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    config = ctx.installation.load(WorkspaceConfig)

    errors_list = []
    if run_config:
        run_config_obj = config.get_run_config(run_config)
        status = _validate_checks(w, ctx, run_config_obj, validate_custom_check_functions)
        if status.has_errors:
            errors_list = [{"run_config": run_config_obj.name, "error": error} for error in status.errors]
    else:
        for run_config_obj in config.run_configs:
            status = _validate_checks(w, ctx, run_config_obj, validate_custom_check_functions)
            if status.has_errors:
                errors_list.extend([{"run_config": run_config_obj.name, "error": error} for error in status.errors])

    print(json.dumps(errors_list))
    return errors_list


def _validate_checks(
    w: WorkspaceClient, ctx: WorkspaceContext, run_config: RunConfig, validate_custom_check_functions: bool
):
    """
    Validate checks for a given run configuration.

    This handles checks stored in a workspace file or volume.
    Table storage is not supported for validation as it requires a Spark session
    which is not available when the CLI is invoked in the local user context.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
        run_config: The run configuration to validate checks for.
        validate_custom_check_functions: Whether to validate custom check functions.
    """
    checks_location = (
        run_config.checks_location
        if run_config.checks_location.startswith("/")
        else f"{ctx.installation.install_folder()}/{run_config.checks_location}"
    )

    if checks_location.startswith("/Volumes/"):
        checks = VolumeFileChecksStorageHandler(w).load(config=VolumeFileChecksStorageConfig(location=checks_location))
    else:
        checks = WorkspaceFileChecksStorageHandler(w).load(
            config=WorkspaceFileChecksStorageConfig(location=checks_location)
        )
    return DQEngine.validate_checks(checks, validate_custom_check_functions=validate_custom_check_functions)


@dqx.command
def profile(
    w: WorkspaceClient,
    *,
    run_config: str = "",
    patterns: str = "",
    exclude_patterns: str = "",
    timeout_minutes: int = 30,
    install_folder: str = "",
    ctx: WorkspaceContext | None = None,
) -> None:
    """
    Profile input data and generate quality rule (checks) candidates.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        run_config: The name of the run configuration to use. If not provided, run it for all run configs.
        patterns: Semicolon-separated list of location patterns (with wildcards) to profile.
            If provided, location fields in the run config are ignored.
            Requires a run config to be provided which is used as a template for other fields.
        exclude_patterns: Semicolon-separated list of location patterns to exclude.
            Useful to skip existing output and quarantine tables based on suffixes.
        timeout_minutes: The timeout for the workflow run in minutes (default is 30).
        install_folder: Optional custom installation folder path.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    timeout = timedelta(minutes=timeout_minutes)
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    ctx.deployed_workflows.run_workflow(
        workflow="profiler",
        run_config_name=run_config,
        patterns=patterns,
        exclude_patterns=exclude_patterns,
        max_wait=timeout,
    )


@dqx.command
def apply_checks(
    w: WorkspaceClient,
    *,
    run_config: str = "",
    patterns: str = "",
    exclude_patterns: str = "",
    output_table_suffix: str = "_dq_output",
    quarantine_table_suffix: str = "_dq_quarantine",
    timeout_minutes: int = 30,
    install_folder: str = "",
    ctx: WorkspaceContext | None = None,
) -> None:
    """
    Apply data quality checks to the input data and save the results.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        run_config: The name of the run configuration to use. If not provided, run it for all run configs.
        patterns: Semicolon-separated list of location patterns (with wildcards) to profile.
            If provided, location fields in the run config are ignored.
            Requires a run config to be provided which is used as a template for other fields.
        exclude_patterns: Semicolon-separated list of location patterns to exclude.
            Useful to skip existing output and quarantine tables based on suffixes.
        output_table_suffix: Suffix to append to the output table names (default is "_dq_output").
        quarantine_table_suffix: Suffix to append to the quarantine table names (default is "_dq_quarantine").
        timeout_minutes: The timeout for the workflow run in minutes (default is 30).
        install_folder: Optional custom installation folder path.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    timeout = timedelta(minutes=timeout_minutes)
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    ctx.deployed_workflows.run_workflow(
        workflow="quality-checker",
        run_config_name=run_config,
        patterns=patterns,
        exclude_patterns=exclude_patterns,
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
        max_wait=timeout,
    )


@dqx.command
def e2e(
    w: WorkspaceClient,
    *,
    run_config: str = "",
    patterns: str = "",
    exclude_patterns: str = "",
    output_table_suffix: str = "_dq_output",
    quarantine_table_suffix: str = "_dq_quarantine",
    timeout_minutes: int = 60,
    install_folder: str = "",
    ctx: WorkspaceContext | None = None,
) -> None:
    """
    Run end to end workflow to:
    - profile input data and generate quality checks candidates
    - apply the generated quality checks
    - save the results to the output table and optionally quarantine table (based on the run config)

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        run_config: The name of the run configuration to use. If not provided, run it for all run configs.
        patterns: Semicolon-separated list of location patterns (with wildcards) to profile.
            If provided, location fields in the run config are ignored.
            Requires a run config to be provided which is used as a template for other fields.
        exclude_patterns: Semicolon-separated list of location patterns to exclude.
            Useful to skip existing output and quarantine tables based on suffixes.
        output_table_suffix: Suffix to append to the output table names (default is "_dq_output").
        quarantine_table_suffix: Suffix to append to the quarantine table names (default is "_dq_quarantine").
        timeout_minutes: The timeout for the workflow run in minutes (default is 60).
        install_folder: Optional custom installation folder path.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
    """
    timeout = timedelta(minutes=timeout_minutes)
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    ctx.deployed_workflows.run_workflow(
        workflow="e2e",
        run_config_name=run_config,
        patterns=patterns,
        exclude_patterns=exclude_patterns,
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
        max_wait=timeout,
    )


@dqx.command
def workflows(w: WorkspaceClient, *, ctx: WorkspaceContext | None = None, install_folder: str = ""):
    """
    Show deployed workflows and their state

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        ctx: The WorkspaceContext instance to use for accessing the workspace.
        install_folder: Optional custom installation folder path.
    """
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    logger.info("Fetching deployed jobs...")
    latest_job_status = ctx.deployed_workflows.latest_job_status()
    print(json.dumps(latest_job_status))
    return latest_job_status


@dqx.command
def logs(
    w: WorkspaceClient, *, workflow: str | None = None, install_folder: str = "", ctx: WorkspaceContext | None = None
):
    """
    Show logs of the latest job run.

    Args:
        w: The WorkspaceClient instance to use for accessing the workspace.
        workflow: The name of the workflow to show logs for.
        install_folder: Optional custom installation folder path.
        ctx: The WorkspaceContext instance to use for accessing the workspace
    """
    ctx = ctx or WorkspaceContext(w, install_folder=install_folder or None)
    ctx.deployed_workflows.relay_logs(workflow)


if __name__ == "__main__":
    dqx()
