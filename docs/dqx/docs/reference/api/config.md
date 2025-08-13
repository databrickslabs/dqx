---
sidebar_label: config
title: databricks.labs.dqx.config
---

## InputConfig Objects

```python
@dataclass
class InputConfig()
```

Configuration class for input data sources (e.g. tables or files).

## OutputConfig Objects

```python
@dataclass
class OutputConfig()
```

Configuration class for output data sinks (e.g. tables or files).

## ProfilerConfig Objects

```python
@dataclass
class ProfilerConfig()
```

Configuration class for profiler.

#### summary\_stats\_file

file containing profile summary statistics

#### sample\_fraction

fraction of data to sample (30%)

#### sample\_seed

seed for sampling

#### limit

limit the number of records to profile

## RunConfig Objects

```python
@dataclass
class RunConfig()
```

Configuration class for the data quality checks

#### name

name of the run configuration

#### quarantine\_config

quarantined data table

#### checks\_location

relative workspace file path or table containing quality rules / checks

#### warehouse\_id

warehouse id to use in the dashboard

## WorkspaceConfig Objects

```python
@dataclass
class WorkspaceConfig()
```

Configuration class for the workspace

#### get\_run\_config

```python
def get_run_config(run_config_name: str | None = "default") -> RunConfig
```

Get the run configuration for a given run name, or the default configuration if no run name is provided.

**Arguments**:

- `run_config_name`: The name of the run configuration to get.

**Raises**:

- `ValueError`: If no run configurations are available or if the specified run configuration name is
not found.

**Returns**:

The run configuration.

## BaseChecksStorageConfig Objects

```python
@dataclass
class BaseChecksStorageConfig(abc.ABC)
```

Marker base class for storage configuration.

## FileChecksStorageConfig Objects

```python
@dataclass
class FileChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a file.

**Arguments**:

- `location`: The file path where the checks are stored.

## WorkspaceFileChecksStorageConfig Objects

```python
@dataclass
class WorkspaceFileChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a workspace file.

**Arguments**:

- `location`: The workspace file path where the checks are stored.

## TableChecksStorageConfig Objects

```python
@dataclass
class TableChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a table.

**Arguments**:

- `location`: The table name where the checks are stored.
- `run_config_name`: The name of the run configuration to use for checks (default is &#x27;default&#x27;).
- `mode`: The mode for writing checks to a table (e.g., &#x27;append&#x27; or &#x27;overwrite&#x27;).
The `overwrite` mode will only replace checks for the specific run config and not all checks in the table.

#### run\_config\_name

to filter checks by run config

## VolumeFileChecksStorageConfig Objects

```python
@dataclass
class VolumeFileChecksStorageConfig(BaseChecksStorageConfig)
```

Configuration class for storing checks in a Unity Catalog volume file.

**Arguments**:

- `location`: The Unity Catalog volume file path where the checks are stored.

## InstallationChecksStorageConfig Objects

```python
@dataclass
class InstallationChecksStorageConfig(WorkspaceFileChecksStorageConfig,
                                      TableChecksStorageConfig,
                                      VolumeFileChecksStorageConfig)
```

Configuration class for storing checks in an installation.

**Arguments**:

- `location`: The installation path where the checks are stored (e.g., table name, file path).
Not used when using installation method, as it is retrieved from the installation config.
- `run_config_name`: The name of the run configuration to use for checks (default is &#x27;default&#x27;).
- `product_name`: The product name for retrieving checks from the installation (default is &#x27;dqx&#x27;).
- `assume_user`: Whether to assume the user is the owner of the checks (default is True).

#### location

retrieved from the installation config

#### run\_config\_name

to retrieve run config

