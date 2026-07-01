"""DQActionManager — high-level entry point for persisting and loading *DQAction* definitions.

*DQActionManager* delegates to *ActionsStorageHandlerFactory* to pick the right
backend (Unity Catalog Delta table or Lakebase PostgreSQL) based on the config
type passed by the caller.

Typical usage::

    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
    from databricks.labs.dqx.actions.dq_action import DQAction
    from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
    from databricks.labs.dqx.actions.manager import DQActionManager
    from databricks.labs.dqx.config import TableActionsStorageConfig

    ws = WorkspaceClient()
    manager = DQActionManager(ws=ws)

    dest = SlackDQAlertDestination(name="slack", webhook_url="https://hooks.slack.com/…")
    alert = DQAlert(destinations=[dest])
    actions = [DQAction(action=alert, condition="error_row_count > 0")]

    config = TableActionsStorageConfig(location="catalog.schema.dqx_actions")
    manager.save_actions(actions, config)
    loaded = manager.load_actions(config)
"""

from __future__ import annotations

import json
import logging
import os

import yaml
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.definition_storage import ActionsStorageHandlerFactory
from databricks.labs.dqx.actions.serializer import deserialize_actions, serialize_actions
from databricks.labs.dqx.config import LakebaseActionsStorageConfig, TableActionsStorageConfig
from databricks.labs.dqx.errors import InvalidConfigError, InvalidParameterError

logger = logging.getLogger(__name__)


class DQActionManager:
    """High-level manager for persisting and loading *DQAction* definitions.

    Wraps *ActionsStorageHandlerFactory* to provide a single, dependency-injected
    entry point for saving and loading action definitions to/from either a Unity
    Catalog Delta table or a Lakebase PostgreSQL instance.

    The *spark* parameter is optional.  When *None*, the manager first tries
    *SparkSession.getActiveSession()* and falls back to
    *SparkSession.builder.getOrCreate()* so that it works transparently in both
    Databricks interactive clusters (active session exists) and job contexts
    (must build one).

    Args:
        ws: Authenticated *WorkspaceClient*.
        spark: Active *SparkSession*, or *None* to auto-resolve.
    """

    def __init__(self, ws: WorkspaceClient, spark: SparkSession | None = None) -> None:
        self._ws = ws
        self._spark = spark

    def _get_spark(self) -> SparkSession:
        """Return the injected or auto-resolved *SparkSession*.

        Resolution order:

        1. Use the *spark* passed to the constructor when provided.
        2. Fall back to *SparkSession.getActiveSession()*.
        3. Fall back to *SparkSession.builder.getOrCreate()*.

        Returns:
            An active *SparkSession*.
        """
        if self._spark is not None:
            return self._spark

        active = SparkSession.getActiveSession()
        if active is not None:
            return active
        return SparkSession.builder.getOrCreate()

    @staticmethod
    def load_actions_from_local_file(filepath: str) -> list[DQAction]:
        """Load *DQAction* definitions from a local YAML or JSON file.

        The file must contain a top-level list of action dicts, each matching
        the wire format produced by *save_actions_in_local_file*.  YAML files
        use the *.yml* or *.yaml* extension; JSON files use *.json*.

        Example YAML file:

        ```yaml
        - type: dq_alert
          destinations:
            - type: slack
              name: ops-channel
              webhook_url:
                secret: my_scope/slack_webhook
          condition: error_row_count > 0
        - type: fail_pipeline
          condition: error_row_count > 10
        ```

        Args:
            filepath: Path to a local *.yml*, *.yaml*, or *.json* file.

        Returns:
            List of *DQAction* instances loaded from the file.

        Raises:
            InvalidParameterError: If the file does not exist or the extension
                is not *.yml*, *.yaml*, or *.json*.
            InvalidConfigError: If the file cannot be parsed or its content
                cannot be validated as a list of *DQAction* dicts.
        """
        if not os.path.exists(filepath):
            raise InvalidParameterError(f"Actions file not found: {filepath}")

        _, ext = os.path.splitext(filepath)
        ext = ext.lower()
        if ext not in {".yml", ".yaml", ".json"}:
            raise InvalidParameterError(f"Unsupported file extension '{ext}'. Use .yml, .yaml, or .json.")

        try:
            with open(filepath, encoding="utf-8") as file:
                raw = file.read()
        except OSError as exc:
            raise InvalidConfigError(f"Cannot read actions file: {exc}") from exc

        try:
            if ext in {".yml", ".yaml"}:
                data = yaml.safe_load(raw)
            else:
                data = json.loads(raw)
        except Exception as exc:
            raise InvalidConfigError(f"Failed to parse actions file '{filepath}': {exc}") from exc

        if not isinstance(data, list):
            raise InvalidConfigError(f"Actions file must contain a top-level list; got {type(data).__name__}")

        return deserialize_actions(data)

    @staticmethod
    def save_actions_in_local_file(actions: list[DQAction], filepath: str) -> None:
        """Save *DQAction* definitions to a local YAML or JSON file.

        The file extension determines the output format: *.yml* / *.yaml*
        produces YAML; *.json* produces JSON.

        Args:
            actions: List of *DQAction* instances to persist.
            filepath: Destination path.  Must end with *.yml*, *.yaml*, or
                *.json*.

        Raises:
            InvalidParameterError: If the file extension is not supported.
            InvalidConfigError: If the file cannot be written.
        """
        _, ext = os.path.splitext(filepath)
        ext = ext.lower()
        if ext not in {".yml", ".yaml", ".json"}:
            raise InvalidParameterError(f"Unsupported file extension '{ext}'. Use .yml, .yaml, or .json.")

        data = serialize_actions(actions)

        try:
            with open(filepath, "w", encoding="utf-8") as file:
                if ext in {".yml", ".yaml"}:
                    yaml.safe_dump(data, file, sort_keys=False)
                else:
                    file.write(json.dumps(data, indent=2))
        except OSError as exc:
            raise InvalidConfigError(f"Cannot write actions file: {exc}") from exc

    def save_actions(
        self,
        actions: list[DQAction],
        config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
    ) -> None:
        """Persist *actions* to the configured storage backend.

        Args:
            actions: List of *DQAction* definitions to persist.
            config: Backend-specific configuration; either
                *TableActionsStorageConfig* (Unity Catalog Delta) or
                *LakebaseActionsStorageConfig* (Lakebase PostgreSQL).
        """
        spark = self._get_spark()
        ActionsStorageHandlerFactory.save(actions, config, spark, self._ws)

    def load_actions(
        self,
        config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
    ) -> list[DQAction]:
        """Load *DQAction* definitions from the configured storage backend.

        Args:
            config: Backend-specific configuration; either
                *TableActionsStorageConfig* or *LakebaseActionsStorageConfig*.

        Returns:
            List of *DQAction* instances loaded from storage.
        """
        spark = self._get_spark()
        return ActionsStorageHandlerFactory.load(config, spark, self._ws)


__all__ = ["DQActionManager"]
