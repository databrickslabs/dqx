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
    # Production deploys bind ``job_id`` and ``wheels_volume`` from
    # bundle resources, so missing values there indicate a misconfigured
    # deploy that would otherwise silently break profiler / dry-run /
    # schedules at first use. Setting ``DQX_REQUIRE_TASK_RUNNER=1``
    # promotes the missing-env warnings in :func:`backend.app.lifespan`
    # to fail-fast startup errors. Leave unset (default) for local dev
    # so the API can still boot without the task-runner wheels.
    require_task_runner: bool = Field(
        default=False,
        validation_alias="DQX_REQUIRE_TASK_RUNNER",
        description="Require DQX_JOB_ID and DQX_WHEELS_VOLUME at startup (production deploys).",
    )
    llm_endpoint: str = Field(default="databricks-claude-sonnet-4-5", validation_alias="DQX_LLM_ENDPOINT")
    # Hard cap on tokens generated per LLM call. Bounds cost/latency and
    # mitigates LLM denial-of-service from pathological prompts (OWASP LLM04);
    # rule-generation responses are small JSON payloads, so this is generous.
    llm_max_tokens: int = Field(
        default=4096,
        validation_alias="DQX_LLM_MAX_TOKENS",
        gt=0,
        description="Maximum output tokens per ChatDatabricks call (LLM budget cap).",
    )
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
    # When ``lakebase_endpoint`` is set the OLTP-style tables
    # (rules catalog, app settings, RBAC, comments, schedule configs,
    # scheduler bookkeeping) are routed to a Lakebase Postgres instance
    # instead of Delta. Bulk/append-only tables (validation runs,
    # profiling results, metrics, quarantine records) always stay in
    # Delta because they are written by the Spark task runner.
    #
    # Leaving these empty keeps the legacy "everything on Delta"
    # behaviour, so existing deployments continue to work without
    # changes.  See ``app/databricks.yml`` for the deploy-time toggle.
    lakebase_endpoint: str = Field(
        default="",
        validation_alias="DQX_LAKEBASE_ENDPOINT",
        description=(
            "Lakebase endpoint resource path (Postgres *projects* model), "
            "e.g. ``projects/dqx-studio-db/branches/dev/endpoints/primary``. "
            "The single connection input: it drives both host resolution "
            "(``postgres.get_endpoint``) and OAuth credential issuance "
            "(``postgres.generate_database_credential``). Empty — or any of "
            "the sentinel values ``-`` / ``disabled`` / ``off`` / ``none`` "
            "(case-insensitive) — disables Lakebase routing and falls back "
            "to Delta. The sentinel form exists because Databricks Apps "
            "rejects env vars with an empty ``value`` string, so deployments "
            "that want to disable Lakebase must pass a non-empty placeholder."
        ),
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

    # Sentinel values that explicitly disable Lakebase routing even
    # though the env var has to be non-empty (Databricks Apps rejects
    # ``value: ""``). Comparison is case-insensitive after stripping.
    _LAKEBASE_DISABLED_SENTINELS = frozenset({"", "-", "disabled", "off", "none"})

    @property
    def lakebase_enabled(self) -> bool:
        """``True`` when the deployment was provisioned with Lakebase.

        Falls back to ``False`` (legacy UC-only mode) when the
        endpoint path is empty or set to a recognised "disabled"
        sentinel so existing tests, dev setups, and Lakebase-less
        Databricks Apps deployments keep working with no Postgres
        dependency.
        """
        return self.lakebase_endpoint.strip().lower() not in self._LAKEBASE_DISABLED_SENTINELS


conf = AppConfig()


# Maximum number of sample table rows fed into an AI/LLM prompt (e.g. the AI
# rule generator's optional sample-row context). Mirrors the 500-row sample the
# "ask a question about this data" path already uses
# (services.table_data_service.TableDataService.PREVIEW_LIMIT), so every place
# that samples table data for an AI/LLM question caps at the same 500 rows.
# Still a hard, finite bound (OWASP LLM04/LLM06): it limits prompt size and the
# volume of raw data echoed into a model call.
AI_SAMPLE_ROW_LIMIT = 500


def get_sql_warehouse_path() -> str:
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if not wh_id:
        raise ValueError("SQL warehouse not configured. Set DATABRICKS_WAREHOUSE_ID.")
    return f"/sql/1.0/warehouses/{wh_id}"
