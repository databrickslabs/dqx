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


# Delta table names for the DQX app (as per PROFILER_VIEW_BASED_APPROACH.md)
DQ_APP_SETTINGS_TABLE = "dq_app_settings"
DQ_PROFILING_RESULTS_TABLE = "dq_profiling_results"
DQ_PROFILING_SUGGESTIONS_TABLE = "dq_profiling_suggestions"
DQ_QUALITY_RULES_TABLE = "dq_quality_rules"
DQ_VALIDATION_RUNS_TABLE = "dq_validation_runs"


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=env_file,
        env_prefix=f"{app_name.upper()}_",
        extra="ignore",
        env_nested_delimiter="__",
    )
    app_name: str = Field(default=app_name)
    api_prefix: str = Field(default="/api")
    
    # Default schema name for DQX app tables
    default_schema: str = Field(default="dqx_app")
    
    # Maximum sample limits for profiling
    max_sample_limit: int = Field(default=100000)
    default_sample_limit: int = Field(default=50000)
    default_sample_fraction: float = Field(default=0.1)

    @property
    def static_assets_path(self) -> Path:
        return Path(str(resources.files(app_slug))).joinpath("__dist__")


conf = AppConfig()
