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
        env_prefix=f"{app_name.upper()}_",
        extra="ignore",
        env_nested_delimiter="__",
        populate_by_name=True,
    )
    app_name: str = Field(default=app_name)
    api_prefix: str = Field(default="/api")
    catalog: str = Field(default="dqx")
    schema_name: str = Field(default="dqx_app", validation_alias="DQX_SCHEMA")
    tmp_schema_name: str = Field(default="dqx_app_tmp", validation_alias="DQX_TMP_SCHEMA")
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

    @property
    def static_assets_path(self) -> Path:
        return Path(str(resources.files(app_slug))).joinpath("__dist__")


conf = AppConfig()


def get_sql_warehouse_path() -> str:
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if not wh_id:
        raise ValueError("SQL warehouse not configured. Set DATABRICKS_WAREHOUSE_ID.")
    return f"/sql/1.0/warehouses/{wh_id}"
