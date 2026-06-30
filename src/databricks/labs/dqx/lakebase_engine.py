"""Shared helpers for creating SQLAlchemy engines connected to Databricks Lakebase (PostgreSQL).

This module centralises two concerns:

1. *create_lakebase_engine* — a pure function that builds a SQLAlchemy *Engine*
   with the standard DQX pool settings (pool_recycle, sslmode, pool_size) and a
   *do_connect* event listener that injects a freshly generated Databricks
   credential token before every connection attempt.

2. *LakebaseConnectionMixin* — a lightweight mixin that supplies a lazily
   created, cached *Engine* to any class that stores a *WorkspaceClient*, a
   *LakebaseActionsStorageConfig*, and an optional pre-built engine.  The mixin
   eliminates duplicated ``_get_engine`` / ``_create_engine`` logic across
   *LakebaseActionsStorageHandler* and *LakebaseActionEventStore*.

Security
--------
Credential tokens are injected only into the *cparams* dict immediately before
each connection attempt and are never logged (CWE-532).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable

from pyspark.sql import SparkSession
from sqlalchemy import Engine, create_engine, event

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import LakebaseActionsStorageConfig
from databricks.labs.dqx.errors import InvalidConfigError


def create_lakebase_engine(
    ws: WorkspaceClient,
    instance_name: str,
    host: str,
    user: str,
    port: str,
    database: str,
) -> Engine:
    """Build a SQLAlchemy engine for a Databricks Lakebase PostgreSQL instance.

    The returned engine is configured with:

    - *pool_recycle=45*60* so connections are recycled before the Lakebase
      idle-connection timeout.
    - *sslmode=require* via *connect_args* for encrypted transport.
    - *pool_size=4* to limit concurrent connections.
    - A *do_connect* listener that injects a freshly generated Databricks
      database credential token before every connection attempt, so short-lived
      credentials are never stale.

    Args:
        ws: Authenticated *WorkspaceClient* used to call
            *database.generate_database_credential*.
        instance_name: Lakebase instance identifier, e.g. ``"my-lakebase"``.
            Used in the credential request.
        host: Read-write DNS hostname of the Lakebase instance (obtained via
            *ws.database.get_database_instance(instance_name).read_write_dns*).
        user: PostgreSQL user name; typically the service-principal client ID
            or the current user's e-mail address.
        port: TCP port string, e.g. ``"5432"``.
        database: Name of the PostgreSQL database to connect to.

    Returns:
        A configured SQLAlchemy *Engine* instance ready for use.
    """
    url = f"postgresql+psycopg2://{user}@{host}:{port}/{database}"
    engine = create_engine(
        url,
        pool_recycle=45 * 60,
        connect_args={"sslmode": "require"},
        pool_size=4,
    )

    def _before_connect(_dialect: object, _conn_rec: object, _cargs: object, cparams: dict[str, object]) -> None:
        cred = ws.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])
        cparams["password"] = cred.token

    listener: Callable[..., None] = _before_connect
    event.listen(engine, "do_connect", listener)
    return engine


class LakebaseConnectionMixin:
    """Mixin that supplies a lazily created, cached SQLAlchemy *Engine*.

    Any class that stores *_ws*, *_spark*, *_config*, and *_engine* can inherit
    from this mixin to avoid duplicating the ``_get_engine`` / ``_create_engine``
    boilerplate.

    Concrete classes must call ``super().__init__(spark, ws, config, engine)`` to
    initialise the shared attributes.

    Args:
        spark: Active *SparkSession* (kept for interface symmetry; not used for
            PostgreSQL queries).
        ws: Authenticated *WorkspaceClient* used to resolve the Lakebase DNS
            and generate short-lived credentials.
        config: *LakebaseActionsStorageConfig* with instance and table details.
        engine: Optional pre-built SQLAlchemy *Engine* (useful for testing
            without a real Lakebase instance).
    """

    def __init__(
        self,
        spark: SparkSession,
        ws: WorkspaceClient,
        config: LakebaseActionsStorageConfig,
        engine: Engine | None = None,
    ) -> None:
        self._spark = spark
        self._ws = ws
        self._config = config
        self._engine = engine

    def _get_engine(self) -> Engine:
        """Return the cached SQLAlchemy engine, creating it on first call.

        Returns:
            SQLAlchemy *Engine* configured for Lakebase (PostgreSQL).
        """
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    def _create_engine(self) -> Engine:
        """Build a SQLAlchemy engine with a credential-refresh listener.

        Returns:
            A newly created SQLAlchemy *Engine*.
        """
        instance_name = self._config.instance_name
        if not instance_name:
            # The config's model validator guarantees this at construction; guard defensively for
            # direct/mutated use and to satisfy the str | None type.
            raise InvalidConfigError("Lakebase 'instance_name' must be set to create a connection.")
        instance = self._ws.database.get_database_instance(instance_name)
        host = instance.read_write_dns or ""
        user = self._config.client_id if self._config.client_id else (self._ws.current_user.me().user_name or "")
        return create_lakebase_engine(
            ws=self._ws,
            instance_name=instance_name,
            host=host,
            user=user,
            port=self._config.port,
            database=self._config.database_name,
        )


__all__ = ["LakebaseConnectionMixin", "create_lakebase_engine"]
