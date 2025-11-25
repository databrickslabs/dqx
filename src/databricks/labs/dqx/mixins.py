from unittest.mock import MagicMock
from databricks.sdk import WorkspaceClient


class PicklableMixin:
    """
    Provides serialization support for classes containing WorkspaceClient.

    This mixin is designed to make DQX classes compatible with Spark Connect by properly
    handling serialization and deserialization. When a class is pickled (e.g., in ``foreachBatch``),
    non-serializable objects like ``WorkspaceClient`` and ``SparkSession`` are removed. When the class is
    unpickled on a Spark executor, these objects are recreated with default or mock instances.

    Classes using this mixin must define:
    1. `_get_serialization_exclusions()` to specify which attributes to exclude
    2. `_restore_after_deserialization()` to recreate excluded objects
    """

    def __getstate__(self):
        """
        Removes non-serializable objects when pickling for Spark Connect.

        This method is called when the object is serialized (e.g., in ``foreachBatch``).
        It creates a copy of the object's state and removes attributes specified by
        ``_get_serialization_exclusions()``.

        Returns:
            A state dictionary with non-serializable objects removed.
        """
        state = self.__dict__.copy()
        exclusions = self._get_serialization_exclusions()

        for attr in exclusions:
            if attr in state:
                state[attr] = None

        for attr in list(state.keys()):
            if attr == 'ws' and '_workspace_client' in exclusions:
                del state[attr]

        return state

    def __setstate__(self, state):
        """
        Restores state and re-creates necessary objects when unpickling.

        This method is called when the object is deserialized on a Spark executor.
        It restores the state and calls ``_restore_after_deserialization()`` to re-create
        any objects that were excluded.

        Args:
            state: The state dictionary to restore.
        """
        self.__dict__.update(state)
        if hasattr(self, '_workspace_client') and self.__getattribute__("_workspace_client") is None:
            self._workspace_client = MagicMock(spec=WorkspaceClient)

        self._restore_after_deserialization(state)

    def _get_serialization_exclusions(self) -> set[str]:
        """
        Returns a set of attribute names to exclude during serialization.

        This method must be overridden by subclasses to specify which attributes should be
        removed before pickling. Common exclusions include ``WorkspaceClient``,
        ``SparkSession``, and other non-serializable objects.

        Returns:
            A set of attribute names to exclude from serialization.
        """
        return set()

    def _restore_after_deserialization(self, state: dict) -> None:
        """
        Restores or re-creates objects after deserialization.

        This method must be overridden by subclasses to re-create any objects that were
        excluded during serialization. This is where ``SparkSession`` and ``WorkspaceClient`` should be
        re-created.

        Args:
            state: A state dictionary to restore.
        """
