"""Secret resolution for DQX action destinations.

Destination configs may carry credentials as either plain strings (suitable
for local development) or *DQSecret*
references that are resolved at delivery time via the Databricks secrets API.
"""

import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.errors import InvalidParameterError

logger = logging.getLogger(__name__)

__all__ = ["SecretResolver"]


class SecretResolver:
    """Resolves credential values from plain strings or Databricks secret references.

    Plain strings are returned unchanged.  *DQSecret* references are resolved
    via *ws.dbutils.secrets.get* so that sensitive values are never stored in
    DQX configuration files.

    Args:
        ws: An authenticated *WorkspaceClient* instance.  The client is
            injected rather than constructed internally to keep this class
            testable and to respect the workspace context established by the
            caller.
    """

    def __init__(self, ws: WorkspaceClient) -> None:
        self._ws = ws

    def resolve(self, value: str | DQSecret) -> str:
        """Resolve *value* to a plaintext string.

        If *value* is already a plain *str* it is returned as-is without
        contacting the secrets API.  If it is a *DQSecret* the secret is
        fetched from the Databricks secret scope identified by *value.scope*
        and *value.key*.

        The resolved secret is **never** logged or included in exception
        messages.

        Args:
            value: Either a plain string credential or a *DQSecret* scope/key
                reference.

        Returns:
            The plaintext credential string.

        Raises:
            InvalidParameterError: If *value* is a *DQSecret* and the secrets
                API call fails.  The error message names the *scope* and *key*
                but never contains the resolved secret value.
        """
        if isinstance(value, str):
            return value

        scope = value.scope
        key = value.key
        try:
            return self._ws.dbutils.secrets.get(scope, key)
        except Exception:
            # Raise 'from None' so the original cause (which could embed the secret value in
            # its message) is never chained onto the user-facing error. The message names only
            # the scope and key, never the resolved value.
            raise InvalidParameterError(f"Failed to resolve secret for scope={scope!r}, key={key!r}.") from None
