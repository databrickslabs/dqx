import uuid
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

from sqlalchemy import Engine

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseCredential

from databricks.labs.dqx.lakebase_engine import create_lakebase_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ws() -> MagicMock:
    """Return a *WorkspaceClient* mock whose credential call yields a fixed token."""
    ws = MagicMock(spec=WorkspaceClient)
    cred = MagicMock(spec=DatabaseCredential)
    cred.token = "tok-abc"
    ws.database.generate_database_credential.return_value = cred
    return ws


def _dispatch_listeners(engine: Engine) -> list[Callable[..., Any]]:
    """Return the registered *do_connect* listeners for *engine*.

    The *do_connect* event belongs to *DialectEvents*, so it is accessible
    via *engine.dialect.dispatch.do_connect*, not *engine.dispatch*.
    """
    return list(engine.dialect.dispatch.do_connect)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# create_lakebase_engine — return value, URL, and pool settings
# ---------------------------------------------------------------------------


def test_create_lakebase_engine_returns_engine() -> None:
    """*create_lakebase_engine* returns a SQLAlchemy *Engine* instance."""
    ws = _make_ws()
    engine = create_lakebase_engine(
        ws=ws,
        instance_name="inst",
        host="db.example.com",
        user="alice@example.com",
        port="5432",
        database="mydb",
    )
    assert isinstance(engine, Engine)


def test_create_lakebase_engine_connection_url_components() -> None:
    """The engine URL encodes the correct driver, host, port, user, and database."""
    ws = _make_ws()
    engine = create_lakebase_engine(
        ws=ws,
        instance_name="inst",
        host="pg.host.example",
        user="user@example.com",
        port="5433",
        database="testdb",
    )
    url = engine.url
    assert url.drivername == "postgresql+psycopg2"
    assert url.host == "pg.host.example"
    assert url.port == 5433
    assert url.database == "testdb"
    assert url.username == "user@example.com"


def test_create_lakebase_engine_registers_do_connect_listener() -> None:
    """A *do_connect* listener is registered so credentials are injected per connection."""
    ws = _make_ws()
    engine = create_lakebase_engine(
        ws=ws,
        instance_name="inst",
        host="h",
        user="u",
        port="5432",
        database="d",
    )
    assert len(_dispatch_listeners(engine)) >= 1


# ---------------------------------------------------------------------------
# do_connect listener — credential injection behaviour
# ---------------------------------------------------------------------------


def test_do_connect_listener_injects_fresh_token() -> None:
    """The registered listener injects a freshly generated token into *cparams*."""
    ws = _make_ws()
    cred = MagicMock(spec=DatabaseCredential)
    cred.token = "fresh-token"
    ws.database.generate_database_credential.return_value = cred

    engine = create_lakebase_engine(
        ws=ws,
        instance_name="my-inst",
        host="h",
        user="u",
        port="5432",
        database="d",
    )

    cparams: dict[str, object] = {}
    listener = _dispatch_listeners(engine)[0]
    listener(None, None, None, cparams)

    assert cparams["password"] == "fresh-token"
    ws.database.generate_database_credential.assert_called_once()
    call = ws.database.generate_database_credential.call_args
    assert call.kwargs["instance_names"] == ["my-inst"]


def test_do_connect_listener_uses_unique_request_id_per_call() -> None:
    """Each invocation of the listener requests a credential with a distinct UUID."""
    ws = _make_ws()
    engine = create_lakebase_engine(ws=ws, instance_name="inst", host="h", user="u", port="5432", database="d")
    listener = _dispatch_listeners(engine)[0]

    listener(None, None, None, {})
    listener(None, None, None, {})

    calls = ws.database.generate_database_credential.call_args_list
    assert len(calls) == 2
    ids = [c.kwargs["request_id"] for c in calls]
    for rid in ids:
        uuid.UUID(rid)  # raises if not a valid UUID string
    assert ids[0] != ids[1]
