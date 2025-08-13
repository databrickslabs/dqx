---
sidebar_label: checks_storage
title: databricks.labs.dqx.checks_storage
---

## ChecksStorageHandler Objects

```python
class ChecksStorageHandler(ABC, Generic[T])
```

Abstract base class for handling storage of quality rules (checks).

#### load

```python
@abstractmethod
def load(config: T) -> list[dict]
```

Load quality rules from the source.

The returned checks can be used as input for `apply_checks_by_metadata` or
`apply_checks_by_metadata_and_split` functions.

**Arguments**:

- `config`: configuration for loading checks, including the table location and run configuration name.

**Returns**:

list of dq rules or raise an error if checks file is missing or is invalid.

#### save

```python
@abstractmethod
def save(checks: list[dict], config: T) -> None
```

Save quality rules to the target.

## TableChecksStorageHandler Objects

```python
class TableChecksStorageHandler(ChecksStorageHandler[TableChecksStorageConfig]
                                )
```

Handler for storing quality rules (checks) in a Delta table in the workspace.

#### load

```python
def load(config: TableChecksStorageConfig) -> list[dict]
```

Load checks (dq rules) from a Delta table in the workspace.

**Arguments**:

- `config`: configuration for loading checks, including the table location and run configuration name.

**Returns**:

list of dq rules or raise an error if checks table is missing or is invalid.

#### save

```python
def save(checks: list[dict], config: TableChecksStorageConfig) -> None
```

Save checks to a Delta table in the workspace.

**Arguments**:

- `checks`: list of dq rules to save
- `config`: configuration for saving checks, including the table location and run configuration name.

**Raises**:

- `ValueError`: if the table name is not provided

## WorkspaceFileChecksStorageHandler Objects

```python
class WorkspaceFileChecksStorageHandler(
        ChecksStorageHandler[WorkspaceFileChecksStorageConfig])
```

Handler for storing quality rules (checks) in a file (json or yaml) in the workspace.

#### load

```python
def load(config: WorkspaceFileChecksStorageConfig) -> list[dict]
```

Load checks (dq rules) from a file (json or yaml) in the workspace.

This does not require installation of DQX in the workspace.

**Arguments**:

- `config`: configuration for loading checks, including the file location and storage type.

**Returns**:

list of dq rules or raise an error if checks file is missing or is invalid.

#### save

```python
def save(checks: list[dict], config: WorkspaceFileChecksStorageConfig) -> None
```

Save checks (dq rules) to yaml file in the workspace.

This does not require installation of DQX in the workspace.

**Arguments**:

- `checks`: list of dq rules to save
- `config`: configuration for saving checks, including the file location and storage type.

## FileChecksStorageHandler Objects

```python
class FileChecksStorageHandler(ChecksStorageHandler[FileChecksStorageConfig])
```

Handler for storing quality rules (checks) in a file (json or yaml) in the local filesystem.

#### load

```python
def load(config: FileChecksStorageConfig) -> list[dict]
```

Load checks (dq rules) from a file (json or yaml) in the local filesystem.

**Arguments**:

- `config`: configuration for loading checks, including the file location.

**Raises**:

- `ValueError`: if the file path is not provided
- `FileNotFoundError`: if the file path does not exist

**Returns**:

list of dq rules or raise an error if checks file is missing or is invalid.

#### save

```python
def save(checks: list[dict], config: FileChecksStorageConfig) -> None
```

Save checks (dq rules) to a file (json or yaml) in the local filesystem.

**Arguments**:

- `checks`: list of dq rules to save
- `config`: configuration for saving checks, including the file location.

**Raises**:

- `ValueError`: if the file path is not provided
- `FileNotFoundError`: if the file path does not exist

## InstallationChecksStorageHandler Objects

```python
class InstallationChecksStorageHandler(
        ChecksStorageHandler[InstallationChecksStorageConfig])
```

Handler for storing quality rules (checks) defined in the installation configuration.

#### load

```python
def load(config: InstallationChecksStorageConfig) -> list[dict]
```

Load checks (dq rules) from the installation configuration.

**Arguments**:

- `config`: configuration for loading checks, including the run configuration name and method.

**Raises**:

- `NotFound`: if the checks file or table is not found in the installation.

**Returns**:

list of dq rules or raise an error if checks file is missing or is invalid.

#### save

```python
def save(checks: list[dict], config: InstallationChecksStorageConfig) -> None
```

Save checks (dq rules) to yaml file or table in the installation folder.

This will overwrite existing checks file or table.

**Arguments**:

- `checks`: list of dq rules to save
- `config`: configuration for saving checks, including the run configuration name, method, and table location.

## VolumeFileChecksStorageHandler Objects

```python
class VolumeFileChecksStorageHandler(
        ChecksStorageHandler[VolumeFileChecksStorageConfig])
```

Handler for storing quality rules (checks) in a file (json or yaml) in a Unity Catalog volume.

#### load

```python
def load(config: VolumeFileChecksStorageConfig) -> list[dict]
```

Load checks (dq rules) from a file (json or yaml) in a Unity Catalog volume.

**Arguments**:

- `config`: configuration for loading checks, including the file location and storage type.

**Returns**:

list of dq rules or raise an error if checks file is missing or is invalid.

#### save

```python
def save(checks: list[dict], config: VolumeFileChecksStorageConfig) -> None
```

Save checks (dq rules) to yaml file in a Unity Catalog volume.

This does not require installation of DQX in a Unity Catalog volume.

**Arguments**:

- `checks`: list of dq rules to save
- `config`: configuration for saving checks, including the file location and storage type.

## BaseChecksStorageHandlerFactory Objects

```python
class BaseChecksStorageHandlerFactory(ABC)
```

Abstract base class for factories that create storage handlers for checks.

#### create

```python
@abstractmethod
def create(config: BaseChecksStorageConfig) -> ChecksStorageHandler
```

Abstract method to create a handler based on the type of the provided configuration object.

**Arguments**:

- `config`: Configuration object for loading or saving checks.

**Returns**:

An instance of the corresponding BaseChecksStorageHandler.

## ChecksStorageHandlerFactory Objects

```python
class ChecksStorageHandlerFactory(BaseChecksStorageHandlerFactory)
```

#### create

```python
def create(config: BaseChecksStorageConfig) -> ChecksStorageHandler
```

Factory method to create a handler based on the type of the provided configuration object.

**Arguments**:

- `config`: Configuration object for loading or saving checks.

**Raises**:

- `ValueError`: If the configuration type is unsupported.

**Returns**:

An instance of the corresponding BaseChecksStorageHandler.

