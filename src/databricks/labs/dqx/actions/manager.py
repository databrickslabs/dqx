"""DQActionManager — high-level entry point for persisting and loading *DQAction* definitions.

*DQActionManager* delegates to *ActionsStorageHandlerFactory* to pick the right
backend (Unity Catalog Delta table or Lakebase PostgreSQL) based on the config
type passed by the caller.

Typical usage::

    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
    from databricks.labs.dqx.actions.base import DQAction
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

import logging

from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.base import DQAction
from databricks.labs.dqx.actions.definition_storage import ActionsStorageHandlerFactory
from databricks.labs.dqx.config import LakebaseActionsStorageConfig, TableActionsStorageConfig

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
