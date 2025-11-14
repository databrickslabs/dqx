class PickleableMixin:
    """
    Mixin to make classes with WorkspaceClient pickleable. Used to broadcast objects to Spark executors.
    
    This mixin removes WorkspaceClient instances during pickling to avoid serialization of thread locks and network
    connections. The WorkspaceClient is recreated lazily on first access after unpickling.
    """

    def __getstate__(self):
        """Removes the WorkspaceClient when the object is pickled."""
        state = self.__dict__.copy()
        if '_ws' in state:
            state['_ws'] = None
        if '_workspace_client' in state:
            state['_workspace_client'] = None
        state.pop('ws', None)
        state.pop('workspace_client', None)
        state.pop('_me', None)
        return state

    def __setstate__(self, state):
        """Recreates the WorkspaceClient when the object is unpickled."""
        self.__dict__.update(state)
