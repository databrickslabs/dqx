"""Unit tests for SecretResolver."""

from unittest.mock import create_autospec

import pytest

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.errors import InvalidParameterError


def test_resolve_plain_string_returns_unchanged() -> None:
    """resolve() must return a plain string unchanged without calling the secrets API."""
    ws = create_autospec(WorkspaceClient)
    resolver = SecretResolver(ws)

    result = resolver.resolve("plain-value")

    assert result == "plain-value"
    ws.dbutils.secrets.get.assert_not_called()


def test_resolve_dq_secret_calls_secrets_api_and_returns_value() -> None:
    """resolve() must fetch the secret via ws.dbutils.secrets.get and return its value."""
    ws = create_autospec(WorkspaceClient)
    ws.dbutils.secrets.get.return_value = "supersecret"
    resolver = SecretResolver(ws)

    result = resolver.resolve(DQSecret("my-scope", "my-key"))

    ws.dbutils.secrets.get.assert_called_once_with("my-scope", "my-key")
    assert result == "supersecret"


def test_resolve_dq_secret_wraps_api_error_in_invalid_parameter_error() -> None:
    """A failure from the secrets API must be raised as InvalidParameterError."""
    ws = create_autospec(WorkspaceClient)
    ws.dbutils.secrets.get.side_effect = Exception("Secret not found")
    resolver = SecretResolver(ws)

    with pytest.raises(InvalidParameterError) as exc_info:
        resolver.resolve(DQSecret("bad-scope", "bad-key"))

    msg = str(exc_info.value)
    assert "bad-scope" in msg
    assert "bad-key" in msg


def test_resolve_error_message_does_not_contain_secret_value() -> None:
    """The InvalidParameterError message must never include the resolved secret value."""
    ws = create_autospec(WorkspaceClient)
    secret_value = "very-sensitive-password-12345"
    ws.dbutils.secrets.get.side_effect = Exception(f"hint: {secret_value}")
    resolver = SecretResolver(ws)

    with pytest.raises(InvalidParameterError) as exc_info:
        resolver.resolve(DQSecret("scope", "key"))

    assert secret_value not in str(exc_info.value)
