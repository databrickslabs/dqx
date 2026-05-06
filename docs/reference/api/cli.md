# databricks.labs.dqx.cli

### open\_remote\_config[​](#open_remote_config "Direct link to open_remote_config")

```python
@dqx.command
def open_remote_config(w: WorkspaceClient,
                       *,
                       install_folder: str = "",
                       ctx: WorkspaceContext | None = None)

```

Opens remote configuration in the browser.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.

### open\_dashboards[​](#open_dashboards "Direct link to open_dashboards")

```python
@dqx.command
def open_dashboards(w: WorkspaceClient,
                    *,
                    install_folder: str = "",
                    ctx: WorkspaceContext | None = None)

```

Opens remote dashboard directory in the browser.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.

### installations[​](#installations "Direct link to installations")

```python
@dqx.command
def installations(w: WorkspaceClient,
                  *,
                  product_name: str = "dqx") -> list[dict]

```

Show installations by different users on the same workspace.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `product_name` - The name of the product to search for in the installation folder.

### validate\_checks[​](#validate_checks "Direct link to validate_checks")

```python
@dqx.command
def validate_checks(w: WorkspaceClient,
                    *,
                    run_config: str = "",
                    validate_custom_check_functions: bool = True,
                    install_folder: str = "",
                    ctx: WorkspaceContext | None = None) -> list[dict]

```

Validate checks stored in a workspace file or volume.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `run_config` - The name of the run configuration to use. If not provided, run it for all run configs.
* `validate_custom_check_functions` - Whether to validate custom check functions (default is True).
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.

### profile[​](#profile "Direct link to profile")

```python
@dqx.command
def profile(w: WorkspaceClient,
            *,
            run_config: str = "",
            patterns: str = "",
            exclude_patterns: str = "",
            timeout_minutes: int = 30,
            install_folder: str = "",
            ctx: WorkspaceContext | None = None) -> None

```

Profile input data and generate quality rule (checks) candidates.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `run_config` - The name of the run configuration to use. If not provided, run it for all run configs.
* `patterns` - Semicolon-separated list of location patterns (with wildcards) to profile. If provided, location fields in the run config are ignored. Requires a run config to be provided which is used as a template for other fields.
* `exclude_patterns` - Semicolon-separated list of location patterns to exclude. Useful to skip existing output and quarantine tables based on suffixes.
* `timeout_minutes` - The timeout for the workflow run in minutes (default is 30).
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.

### apply\_checks[​](#apply_checks "Direct link to apply_checks")

```python
@dqx.command
def apply_checks(w: WorkspaceClient,
                 *,
                 run_config: str = "",
                 patterns: str = "",
                 exclude_patterns: str = "",
                 output_table_suffix: str = "_dq_output",
                 quarantine_table_suffix: str = "_dq_quarantine",
                 timeout_minutes: int = 30,
                 install_folder: str = "",
                 ctx: WorkspaceContext | None = None) -> None

```

Apply data quality checks to the input data and save the results.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `run_config` - The name of the run configuration to use. If not provided, run it for all run configs.
* `patterns` - Semicolon-separated list of location patterns (with wildcards) to profile. If provided, location fields in the run config are ignored. Requires a run config to be provided which is used as a template for other fields.
* `exclude_patterns` - Semicolon-separated list of location patterns to exclude. Useful to skip existing output and quarantine tables based on suffixes.
* `output_table_suffix` - Suffix to append to the output table names (default is "\_dq\_output").
* `quarantine_table_suffix` - Suffix to append to the quarantine table names (default is "\_dq\_quarantine").
* `timeout_minutes` - The timeout for the workflow run in minutes (default is 30).
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.

### e2e[​](#e2e "Direct link to e2e")

```python
@dqx.command
def e2e(w: WorkspaceClient,
        *,
        run_config: str = "",
        patterns: str = "",
        exclude_patterns: str = "",
        output_table_suffix: str = "_dq_output",
        quarantine_table_suffix: str = "_dq_quarantine",
        timeout_minutes: int = 60,
        install_folder: str = "",
        ctx: WorkspaceContext | None = None) -> None

```

Run end to end workflow to:

* profile input data and generate quality checks candidates
* apply the generated quality checks
* save the results to the output table and optionally quarantine table (based on the run config)

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `run_config` - The name of the run configuration to use. If not provided, run it for all run configs.
* `patterns` - Semicolon-separated list of location patterns (with wildcards) to profile. If provided, location fields in the run config are ignored. Requires a run config to be provided which is used as a template for other fields.
* `exclude_patterns` - Semicolon-separated list of location patterns to exclude. Useful to skip existing output and quarantine tables based on suffixes.
* `output_table_suffix` - Suffix to append to the output table names (default is "\_dq\_output").
* `quarantine_table_suffix` - Suffix to append to the quarantine table names (default is "\_dq\_quarantine").
* `timeout_minutes` - The timeout for the workflow run in minutes (default is 60).
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.

### train\_anomaly[​](#train_anomaly "Direct link to train_anomaly")

```python
@dqx.command
def train_anomaly(w: WorkspaceClient,
                  *,
                  run_config: str = "",
                  timeout_minutes: int = 60,
                  install_folder: str = "",
                  ctx: WorkspaceContext | None = None) -> None

```

Train anomaly detection model using the anomaly-trainer workflow.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `run_config` - The name of the run configuration to use. If not provided, run it for all run configs.
* `timeout_minutes` - The timeout for the workflow run in minutes (default is 60).
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.

### workflows[​](#workflows "Direct link to workflows")

```python
@dqx.command
def workflows(w: WorkspaceClient,
              *,
              ctx: WorkspaceContext | None = None,
              install_folder: str = "")

```

Show deployed workflows and their state

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace.
* `install_folder` - Optional custom installation folder path.

### logs[​](#logs "Direct link to logs")

```python
@dqx.command
def logs(w: WorkspaceClient,
         *,
         workflow: str | None = None,
         install_folder: str = "",
         ctx: WorkspaceContext | None = None)

```

Show logs of the latest job run.

**Arguments**:

* `w` - The WorkspaceClient instance to use for accessing the workspace.
* `workflow` - The name of the workflow to show logs for.
* `install_folder` - Optional custom installation folder path.
* `ctx` - The WorkspaceContext instance to use for accessing the workspace
