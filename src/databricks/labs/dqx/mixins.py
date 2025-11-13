class WorkspaceClientSerDeMixin:
    """Mixin to make classes with WorkspaceClient picklable."""

    def __getstate__(self):
        """Removes the WorkspaceClient when the object is pickled."""
        state = self.__dict__.copy()
        state['_ws'] = None
        state.pop('ws', None)
        return state

    def __setstate__(self, state):
        """Recreates the WorkspaceClient when the object is unpickled."""
        self.__dict__.update(state)
