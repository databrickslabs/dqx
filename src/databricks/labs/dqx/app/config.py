from __future__ import annotations

from functools import cached_property
import logging
from logging import Logger
from pathlib import Path
import uuid

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Engine
from sqlmodel import Session, create_engine

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.app.utils import TimedCachedProperty, configure_logging

configure_logging()

logger = logging.getLogger("dqx.app")
logger.setLevel(logging.DEBUG)


# Load environment variables from .env file in the root directory of the project
project_root = Path(__file__).parent.parent.parent.parent.parent.parent
env_file = project_root / ".env"

if env_file.exists():
    logger.info(f"Loading environment variables from {env_file}")
    load_dotenv(dotenv_path=env_file)
else:
    logger.info(f"Env file {env_file} not found, continuing with current environment variables")


class DatabaseConfig(BaseModel):
    instance_name: str = Field(default="dqx")
    port: int = Field(default=5432)
    database: str = Field(default="databricks_postgres")


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_file, env_prefix="DQX_", extra="allow")

    static_assets_path: Path = Field(
        default=Path(__file__).parent / "static" / "dist",
        description="Path to the static assets directory",
    )

    dev_token: SecretStr | None = Field(
        default=None,
        description="Token for local development",
    )

    db: DatabaseConfig = Field(default_factory=DatabaseConfig)


class ConnectionInfo(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str

    def to_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode=require"


class DatabaseManager(BaseModel):
    rt: Runtime

    model_config = ConfigDict(ignored_types=(TimedCachedProperty,))

    def get_connection_info(self) -> ConnectionInfo:
        """
        Returns the SQLAlchemy engine URL used for database operations.
        This URL is initialized with the database URL and can be used to create sessions.
        The URL is cached for 30 minutes to improve performance while ensuring
        credentials are refreshed periodically.
        """
        instance = self.rt.ws.database.get_database_instance(name=self.rt.conf.db.instance_name)
        cred = self.rt.ws.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[self.rt.conf.db.instance_name]
        )
        user = self.rt.ws.current_user.me().user_name
        pwd = cred.token
        host = instance.read_write_dns
        assert host is not None, "Host is not found"
        assert user is not None, "User is not found"
        assert pwd is not None, "Password is not found"

        return ConnectionInfo(
            host=host,
            port=self.rt.conf.db.port,
            user=user,
            password=pwd,
            database=self.rt.conf.db.database,
        )

    @TimedCachedProperty[Engine](ttl_seconds=30 * 60)  # 30 minutes
    def engine(self) -> Engine:
        """
        Returns the SQLAlchemy engine used for database operations.
        This engine is initialized with the database URL and can be used to create sessions.
        The engine is cached for 30 minutes to improve performance while ensuring
        credentials are refreshed periodically.
        """
        self.rt.logger.info("Creating new SQLAlchemy engine (cache expired or first time)")
        return create_engine(
            self.get_connection_info().to_url(),
            pool_size=2,
            max_overflow=0,
        )

    def session(self) -> Session:
        """
        Returns the SQLModel session used for database operations.
        This session is initialized with the engine and can be used to create transactions.
        The session is cached for 30 minutes to improve performance while ensuring
        credentials are refreshed periodically.
        """
        return Session(self.engine)


class Runtime(BaseModel):
    conf: AppConfig

    model_config = ConfigDict(ignored_types=(TimedCachedProperty,))

    @cached_property
    def logger(self) -> Logger:
        return logger

    @cached_property
    def ws(self) -> WorkspaceClient:
        """
        Returns the service principal client.
        """
        return WorkspaceClient()

    @model_validator(mode="after")
    def validate_conf(self) -> Runtime:
        try:
            self.ws.current_user.me()
        except Exception as e:
            self.logger.error("Cannot connect to Databricks API using service principal")
            raise e

        logger.info(f"App initialized with config: {self.conf}")
        return self


conf = AppConfig()
rt = Runtime(conf=conf)
