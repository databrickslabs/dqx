from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.__main__ import me


def test_me(caplog):
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me.return_value.user_name = "Serge"
    me(ws, "Hello")
    assert "Hello, Serge!" in caplog.messages
