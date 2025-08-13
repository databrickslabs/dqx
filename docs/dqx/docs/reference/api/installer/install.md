---
sidebar_label: install
title: databricks.labs.dqx.installer.install
---

## WorkspaceInstaller Objects

```python
class WorkspaceInstaller(WorkspaceContext)
```

Installer for DQX workspace.

**Arguments**:

- `ws`: The WorkspaceClient instance.
- `environ`: Optional dictionary of environment variables.

#### upgrades

```python
@cached_property
def upgrades()
```

Returns the Upgrades instance for the product.

**Returns**:

An Upgrades instance.

#### installation

```python
@cached_property
def installation()
```

Returns the current installation for the product.

**Raises**:

- `NotFound`: If the installation is not found.

**Returns**:

An Installation instance.

#### run

```python
def run(default_config: WorkspaceConfig | None = None) -> WorkspaceConfig
```

Runs the installation process.

**Arguments**:

- `default_config`: Optional default configuration.

**Raises**:

- `ManyError`: If multiple errors occur during installation.
- `TimeoutError`: If a timeout occurs during installation.

**Returns**:

The final WorkspaceConfig used for the installation.

#### extract\_major\_minor

```python
@staticmethod
def extract_major_minor(version_string: str)
```

Extracts the major and minor version from a version string.

**Arguments**:

- `version_string`: The version string to extract from.

**Returns**:

The major.minor version as a string, or None if not found.

#### configure

```python
def configure(
        default_config: WorkspaceConfig | None = None) -> WorkspaceConfig
```

Configures the workspace.

Notes:
1. Connection errors are not handled within this configure method.

**Arguments**:

- `default_config`: Optional default configuration.

**Raises**:

- `NotFound`: If the previous installation is not found.
- `RuntimeWarning`: If the existing installation is corrupted.

**Returns**:

The final WorkspaceConfig used for the installation.

#### replace\_config

```python
def replace_config(**changes: Any) -> WorkspaceConfig | None
```

Persist the list of workspaces where UCX is successfully installed in the config

## WorkspaceInstallation Objects

```python
class WorkspaceInstallation()
```

#### current

```python
@classmethod
def current(cls, ws: WorkspaceClient)
```

Creates a current WorkspaceInstallation instance based on the current workspace client.

**Arguments**:

- `ws`: The WorkspaceClient instance.

**Returns**:

A WorkspaceInstallation instance.

#### config

```python
@property
def config()
```

Returns the configuration of the workspace installation.

**Returns**:

The WorkspaceConfig instance.

#### folder

```python
@property
def folder()
```

Returns the installation folder path.

**Returns**:

The installation folder path as a string.

#### run

```python
def run() -> bool
```

Runs the workflow installation.

**Returns**:

True if the installation finished successfully, False otherwise.

#### uninstall

```python
def uninstall()
```

Uninstalls DQX from the workspace, including project folder, dashboards, and jobs.

