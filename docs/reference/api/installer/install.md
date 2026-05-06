# databricks.labs.dqx.installer.install

## WorkspaceInstaller Objects[​](#workspaceinstaller-objects "Direct link to WorkspaceInstaller Objects")

```python
class WorkspaceInstaller(WorkspaceContext, InstallationMixin)

```

Installer for DQX workspace. Orchestrates install flow (config, version checks, upgrades, dependency wiring).

**Arguments**:

* `environ` - Optional dictionary of environment variables.
* `ws` - The WorkspaceClient instance.
* `install_folder` - Optional custom workspace folder path for installation.

### upgrades[​](#upgrades "Direct link to upgrades")

```python
@cached_property
def upgrades()

```

Returns the Upgrades instance for the product.

**Returns**:

An Upgrades instance.

### installation[​](#installation "Direct link to installation")

```python
@cached_property
def installation()

```

Returns the current installation for the product.

**Returns**:

An Installation instance.

**Raises**:

* `NotFound` - If the installation is not found.

### run[​](#run "Direct link to run")

```python
def run(default_config: WorkspaceConfig | None = None) -> WorkspaceConfig

```

Runs the installation process.

**Arguments**:

* `default_config` - Optional default configuration.

**Returns**:

The final WorkspaceConfig used for the installation.

**Raises**:

* `ManyError` - If multiple errors occur during installation.
* `TimeoutError` - If a timeout occurs during installation.

### replace\_config[​](#replace_config "Direct link to replace_config")

```python
def replace_config(**changes: Any) -> WorkspaceConfig | None

```

Persist the list of workspaces where UCX is successfully installed in the config

### configure[​](#configure "Direct link to configure")

```python
def configure(
        default_config: WorkspaceConfig | None = None) -> WorkspaceConfig

```

Configures the workspace.

**Notes**:

* Connection errors are not handled within this configure method.

**Arguments**:

* `default_config` - Optional default configuration.

**Returns**:

The final WorkspaceConfig used for the installation.

**Raises**:

* `NotFound` - If the previous installation is not found.
* `RuntimeWarning` - If the existing installation is corrupted.

## InstallationService Objects[​](#installationservice-objects "Direct link to InstallationService Objects")

```python
class InstallationService()

```

Perform operations on a concrete installation instance (create jobs/dashboards, uninstall, cleanup).

### current[​](#current "Direct link to current")

```python
@classmethod
def current(cls, ws: WorkspaceClient, install_folder: str | None = None)

```

Creates a current InstallationService instance based on the current workspace client and install folder.

**Arguments**:

* `ws` - The WorkspaceClient instance.
* `install_folder` - Optional custom workspace folder path for the installation.

**Returns**:

An InstallationService instance.

### config[​](#config "Direct link to config")

```python
@property
def config()

```

Returns the configuration of the workspace installation.

**Returns**:

The WorkspaceConfig instance.

### install\_folder[​](#install_folder "Direct link to install_folder")

```python
@property
def install_folder()

```

Returns the installation install\_folder path.

**Returns**:

The installation install\_folder path as a string.

### run[​](#run-1 "Direct link to run")

```python
def run() -> bool

```

Runs the workflow installation.

**Returns**:

True if the installation finished successfully, False otherwise.

### uninstall[​](#uninstall "Direct link to uninstall")

```python
def uninstall()

```

Uninstalls DQX from the workspace, including project install\_folder, dashboards, and jobs.
