---
sidebar_label: cli
title: databricks.labs.dqx.cli
---

#### open\_remote\_config

```python
@dqx.command
def open_remote_config(w: WorkspaceClient,
                       *,
                       ctx: WorkspaceContext | None = None)
```

Opens remote configuration in the browser.

**Arguments**:

- `w`: The WorkspaceClient instance to use for accessing the workspace.
- `ctx`: The WorkspaceContext instance to use for accessing the workspace.

#### open\_dashboards

```python
@dqx.command
def open_dashboards(w: WorkspaceClient,
                    *,
                    ctx: WorkspaceContext | None = None)
```

Opens remote dashboard directory in the browser.

**Arguments**:

- `w`: The WorkspaceClient instance to use for accessing the workspace.
- `ctx`: The WorkspaceContext instance to use for accessing the workspace.

#### installations

```python
@dqx.command
def installations(w: WorkspaceClient,
                  *,
                  product_name: str = "dqx") -> list[dict]
```

Show installations by different users on the same workspace.

**Arguments**:

- `w`: The WorkspaceClient instance to use for accessing the workspace.
- `product_name`: The name of the product to search for in the installation folder.

#### validate\_checks

```python
@dqx.command
def validate_checks(w: WorkspaceClient,
                    *,
                    run_config: str = "default",
                    validate_custom_check_functions: bool = True,
                    ctx: WorkspaceContext | None = None) -> list[dict]
```

Validate checks stored in the installation directory as a file.

**Arguments**:

- `w`: The WorkspaceClient instance to use for accessing the workspace.
- `run_config`: The name of the run configuration to use.
- `validate_custom_check_functions`: Whether to validate custom check functions (default is True).
- `ctx`: The WorkspaceContext instance to use for accessing the workspace.

#### profile

```python
@dqx.command
def profile(w: WorkspaceClient,
            *,
            run_config: str = "default",
            ctx: WorkspaceContext | None = None) -> None
```

Profile input data and generate quality rule (checks) candidates.

**Arguments**:

- `w`: The WorkspaceClient instance to use for accessing the workspace.
- `run_config`: The name of the run configuration to use.
- `ctx`: The WorkspaceContext instance to use for accessing the workspace.

#### workflows

```python
@dqx.command
def workflows(w: WorkspaceClient, *, ctx: WorkspaceContext | None = None)
```

Show deployed workflows and their state

**Arguments**:

- `w`: The WorkspaceClient instance to use for accessing the workspace.
- `ctx`: The WorkspaceContext instance to use for accessing the workspace.

#### logs

```python
@dqx.command
def logs(w: WorkspaceClient,
         *,
         workflow: str | None = None,
         ctx: WorkspaceContext | None = None)
```

Show logs of the latest job run.

**Arguments**:

- `w`: The WorkspaceClient instance to use for accessing the workspace.
- `workflow`: The name of the workflow to show logs for.
- `ctx`: The WorkspaceContext instance to use for accessing the workspace

