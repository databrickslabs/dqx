import os
from importlib import resources
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .._metadata import app_name, app_slug

# project root is the parent of the src folder
project_root = Path(__file__).parent.parent.parent.parent
env_file = project_root / ".env"

if env_file.exists():
    load_dotenv(dotenv_path=env_file)


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=env_file,
        env_prefix="DQX_",
        extra="ignore",
        env_nested_delimiter="__",
        populate_by_name=True,
    )
    app_name: str = Field(default=app_name)
    api_prefix: str = Field(default="/api")
    catalog: str = Field(default="dqx")
    schema_name: str = Field(default="dqx_studio", validation_alias="DQX_SCHEMA")
    tmp_schema_name: str = Field(default="dqx_studio_tmp", validation_alias="DQX_TMP_SCHEMA")
    job_id: str = Field(default="", validation_alias="DQX_JOB_ID")
    wheels_volume: str = Field(default="", validation_alias="DQX_WHEELS_VOLUME")
    llm_endpoint: str = Field(default="databricks-claude-sonnet-4-5", validation_alias="DQX_LLM_ENDPOINT")
    admin_group: str | None = Field(
        default=None,
        validation_alias="DQX_ADMIN_GROUP",
        description="Databricks workspace group name for bootstrap Admin access",
    )
    profiler_max_sample_limit: int = Field(default=100_000)
    profiler_default_sample_limit: int = Field(default=50_000)
    dryrun_max_sample_size: int = Field(default=10_000)
    dryrun_default_sample_size: int = Field(default=1_000)

    # ------------------------------------------------------------------
    # Lakebase (Postgres) backend
    # ------------------------------------------------------------------
    # When ``lakebase_instance_name`` is set the OLTP-style tables
    # (rules catalog, app settings, RBAC, comments, schedule configs,
    # scheduler bookkeeping) are routed to a Lakebase Postgres instance
    # instead of Delta. Bulk/append-only tables (validation runs,
    # profiling results, metrics, quarantine records) always stay in
    # Delta because they are written by the Spark task runner.
    #
    # Leaving these empty keeps the legacy "everything on Delta"
    # behaviour, so existing deployments continue to work without
    # changes.  See ``app/databricks.yml`` for the deploy-time toggle.
    lakebase_instance_name: str = Field(
        default="",
        validation_alias="DQX_LAKEBASE_INSTANCE_NAME",
        description="Lakebase instance name. Empty disables Lakebase routing.",
    )
    # Default must match the ``lakebase_database_name`` bundle var in
    # ``app/databricks.yml`` (``databricks_postgres`` — the always-present
    # admin database every Lakebase instance ships with). The bundle
    # intentionally does NOT create a per-app logical Postgres database;
    # per-app isolation lives in ``lakebase_schema_name`` below. Keep
    # the two defaults in sync so contexts where the DABs env-var
    # injection isn't in effect — local dev, unit tests, debug shells —
    # don't try to connect to a logical DB that was never created.
    lakebase_database_name: str = Field(
        default="databricks_postgres",
        validation_alias="DQX_LAKEBASE_DATABASE_NAME",
        description="Database within the Lakebase instance the app connects to.",
    )
    lakebase_schema_name: str = Field(
        default="dqx_studio",
        validation_alias="DQX_LAKEBASE_SCHEMA",
        description="Postgres schema for app tables. Created at startup if missing.",
    )
    lakebase_pool_min_size: int = Field(default=1, validation_alias="DQX_LAKEBASE_POOL_MIN_SIZE")
    lakebase_pool_max_size: int = Field(default=10, validation_alias="DQX_LAKEBASE_POOL_MAX_SIZE")
    # Lakebase OAuth tokens currently expire after one hour; refresh
    # well before that so in-flight queries never see a 401.
    lakebase_token_refresh_minutes: int = Field(
        default=50,
        validation_alias="DQX_LAKEBASE_TOKEN_REFRESH_MINUTES",
    )
    # ------------------------------------------------------------------
    # Token-refresh resilience (see PgExecutor._token_refresh_loop).
    # ------------------------------------------------------------------
    # When a refresh attempt fails we sleep ``retry_seconds`` ± jitter
    # and try again *fast* — NOT the full 50-minute scheduled interval
    # — because the pool's ``max_lifetime`` is also 50 minutes. If we
    # only retried at the scheduled cadence, a refresh failure window
    # of 50-60 minutes would silently drain the pool: every in-pool
    # connection dies at ``max_lifetime``, no valid replacement is
    # ever minted, and the app stops accepting work without any
    # explicit "token rotation failed" error visible to operators.
    #
    # After ``max_failures`` consecutive failures (default 12 × 10s ≈
    # 2 minutes of sustained failure) the executor escalates by
    # exiting the process so the supervisor (uvicorn / Databricks
    # Apps) restarts the worker. Crashing loud at 2 minutes is much
    # better than silently degrading at 50 minutes.
    lakebase_token_refresh_retry_seconds: int = Field(
        default=10,
        validation_alias="DQX_LAKEBASE_TOKEN_REFRESH_RETRY_SECONDS",
        description=(
            "Base back-off between failed token-refresh attempts. " "Jittered by ±retry_jitter on each retry."
        ),
    )
    lakebase_token_refresh_retry_jitter: float = Field(
        default=0.3,
        validation_alias="DQX_LAKEBASE_TOKEN_REFRESH_RETRY_JITTER",
        description=(
            "Fractional jitter applied to retry back-off so multiple "
            "workers don't thunder-herd against the SDK on the same "
            "second after a shared transient failure."
        ),
    )
    lakebase_token_refresh_max_failures: int = Field(
        default=12,
        validation_alias="DQX_LAKEBASE_TOKEN_REFRESH_MAX_FAILURES",
        description=(
            "Consecutive refresh failures before the executor escalates "
            "by exiting the process so the supervisor restarts the "
            "worker. Default 12 × 10s ≈ 2 minutes of sustained "
            "failure, which leaves ~48 minutes of pool buffer before "
            "max_lifetime would otherwise drain the pool silently."
        ),
    )

    @property
    def static_assets_path(self) -> Path:
        return Path(str(resources.files(app_slug))).joinpath("__dist__")

    @property
    def lakebase_enabled(self) -> bool:
        """``True`` when the deployment was provisioned with Lakebase.

        Falls back to ``False`` (legacy UC-only mode) when the
        instance name is empty so existing tests and dev setups keep
        working with no Postgres dependency.
        """
        return bool(self.lakebase_instance_name.strip())


conf = AppConfig()


def get_sql_warehouse_path() -> str:
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if not wh_id:
        raise ValueError("SQL warehouse not configured. Set DATABRICKS_WAREHOUSE_ID.")
    return f"/sql/1.0/warehouses/{wh_id}"
