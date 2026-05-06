# databricks.labs.dqx.config\_serializer

## ConfigSerializer Objects[​](#configserializer-objects "Direct link to ConfigSerializer Objects")

```python
class ConfigSerializer(InstallationMixin)

```

Class to handle loading of configuration from the installation.

### load\_config[​](#load_config "Direct link to load_config")

```python
def load_config(product_name: str = "dqx",
                assume_user: bool = True,
                install_folder: str | None = None) -> WorkspaceConfig

```

Load workspace configuration from the installation. The workspace config contains all run configs.

**Arguments**:

* `product_name` - name of the product
* `assume_user` - if True, assume user installation
* `install_folder` - Custom workspace installation folder. Required if DQX is installed in a custom folder.

**Returns**:

* `WorkspaceConfig` - Loaded workspace configuration.

### save\_config[​](#save_config "Direct link to save_config")

```python
def save_config(config: WorkspaceConfig,
                product_name: str = "dqx",
                assume_user: bool = True,
                install_folder: str | None = None) -> None

```

Save workspace config in the installation.

**Arguments**:

* `config` - Workspace config object to save
* `product_name` - Product/installation identifier used to resolve installation paths (not used if install\_folder is provided)
* `assume_user` - Whether to assume a per-user installation when loading the run configuration (not used if install\_folder is provided)
* `install_folder` - Custom workspace installation folder. Required if DQX is installed in a custom folder.

**Returns**:

None

### load\_run\_config[​](#load_run_config "Direct link to load_run_config")

```python
def load_run_config(run_config_name: str | None,
                    product_name: str = "dqx",
                    assume_user: bool = True,
                    install_folder: str | None = None) -> RunConfig

```

Load run configuration from the installation.

**Arguments**:

* `run_config_name` - Name of the run configuration to use, e.g. input table or job name.
* `product_name` - Product/installation identifier used to resolve installation paths (not used if install\_folder is provided)
* `assume_user` - Whether to assume a per-user installation when loading the run configuration (not used if install\_folder is provided)
* `install_folder` - Custom workspace installation folder. Required if DQX is installed in a custom folder.

**Returns**:

* `RunConfig` - Loaded run configuration.

### save\_run\_config[​](#save_run_config "Direct link to save_run_config")

```python
def save_run_config(run_config: RunConfig,
                    product_name: str = "dqx",
                    assume_user: bool = True,
                    install_folder: str | None = None) -> None

```

Save run config in the workspace installation config.

**Arguments**:

* `run_config` - Run config object to save in the workspace config
* `product_name` - Product/installation identifier used to resolve installation paths (not used if install\_folder is provided)
* `assume_user` - Whether to assume a per-user installation when loading the run configuration (not used if install\_folder is provided)
* `install_folder` - Custom workspace installation folder. Required if DQX is installed in a custom folder.

**Returns**:

None
