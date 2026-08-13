# databricks.labs.dqx.actions.manager

DQActionManager — high-level entry point for persisting and loading *DQAction* definitions.

*DQActionManager* delegates to *ActionsStorageHandlerFactory* to pick the right backend (Unity Catalog Delta table or Lakebase PostgreSQL) based on the config type passed by the caller.

Typical usage::

from databricks.sdk import WorkspaceClient from pyspark.sql import SparkSession from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn from databricks.labs.dqx.actions.dq\_action import DQAction from databricks.labs.dqx.actions.destinations.slack import DQSlackAlertDestination from databricks.labs.dqx.actions.manager import DQActionManager from databricks.labs.dqx.config import TableActionsStorageConfig

ws = WorkspaceClient() manager = DQActionManager(ws=ws)

dest = DQSlackAlertDestination(name="slack", webhook\_url="[https://hooks.slack.com/…](https://hooks.slack.com/%E2%80%A6)") alert = DQAlert(destinations=\[dest]) actions = \[DQAction(action=alert, condition="error\_row\_count > 0")]

config = TableActionsStorageConfig(location="catalog.schema.dqx\_actions") manager.save\_actions(actions, config) loaded = manager.load\_actions(config)

## DQActionManager Objects[​](#dqactionmanager-objects "Direct link to DQActionManager Objects")

```python
class DQActionManager()

```

High-level manager for persisting and loading *DQAction* definitions.

Wraps *ActionsStorageHandlerFactory* to provide a single, dependency-injected entry point for saving and loading action definitions to/from either a Unity Catalog Delta table or a Lakebase PostgreSQL instance.

The *spark* parameter is optional. When *None*, the manager first tries *SparkSession.getActiveSession()* and falls back to *SparkSession.builder.getOrCreate()* so that it works transparently in both Databricks interactive clusters (active session exists) and job contexts (must build one).

**Arguments**:

* `ws` - Authenticated *WorkspaceClient*.
* `spark` - Active *SparkSession*, or *None* to auto-resolve.

### load\_actions\_from\_local\_file[​](#load_actions_from_local_file "Direct link to load_actions_from_local_file")

```python
@staticmethod
def load_actions_from_local_file(filepath: str) -> list[DQAction]

```

Load *DQAction* definitions from a local YAML or JSON file.

The file must contain a top-level list of action dicts, each matching the wire format produced by *save\_actions\_in\_local\_file*. YAML files use the *.yml* or *.yaml* extension; JSON files use *.json*.

Example YAML file:

```yaml
- action:
    type: alert
    destinations:
    - type: slack
      name: ops-channel
      webhook_url:
        secret: my_scope/slack_webhook
  condition: error_row_count > 0
- action:
    type: fail_pipeline
  condition: error_row_count > 10

```

**Arguments**:

* `filepath` - Path to a local *.yml*, *.yaml*, or *.json* file.

**Returns**:

List of *DQAction* instances loaded from the file.

**Raises**:

* `InvalidParameterError` - If the file does not exist or the extension is not *.yml*, *.yaml*, or *.json*.
* `InvalidConfigError` - If the file cannot be parsed or its content cannot be validated as a list of *DQAction* dicts.

### save\_actions\_in\_local\_file[​](#save_actions_in_local_file "Direct link to save_actions_in_local_file")

```python
@staticmethod
def save_actions_in_local_file(actions: list[DQAction], filepath: str) -> None

```

Save *DQAction* definitions to a local YAML or JSON file.

The file extension determines the output format: *.yml* / *.yaml* produces YAML; *.json* produces JSON.

**Arguments**:

* `actions` - List of *DQAction* instances to persist.
* `filepath` - Destination path. Must end with *.yml*, *.yaml*, or *.json*.

**Raises**:

* `InvalidParameterError` - If the file extension is not supported.
* `InvalidConfigError` - If the file cannot be written.

### save\_actions[​](#save_actions "Direct link to save_actions")

```python
def save_actions(
        actions: list[DQAction],
        config: TableActionsStorageConfig | LakebaseActionsStorageConfig
) -> None

```

Persist *actions* to the configured storage backend.

**Arguments**:

* `actions` - List of *DQAction* definitions to persist.
* `config` - Backend-specific configuration; either *TableActionsStorageConfig* (Unity Catalog Delta) or *LakebaseActionsStorageConfig* (Lakebase PostgreSQL).

### load\_actions[​](#load_actions "Direct link to load_actions")

```python
def load_actions(
    config: TableActionsStorageConfig | LakebaseActionsStorageConfig
) -> list[DQAction]

```

Load *DQAction* definitions from the configured storage backend.

**Arguments**:

* `config` - Backend-specific configuration; either *TableActionsStorageConfig* or *LakebaseActionsStorageConfig*.

**Returns**:

List of *DQAction* instances loaded from storage.
