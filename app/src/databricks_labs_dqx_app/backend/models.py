from typing import Any

from databricks.labs.dqx.config import RunConfig, WorkspaceConfig
from pydantic import BaseModel, Field

from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


class ConfigOut(BaseModel):
    config: WorkspaceConfig


class ConfigIn(BaseModel):
    config: WorkspaceConfig


class RunConfigOut(BaseModel):
    config: RunConfig


class RunConfigIn(BaseModel):
    config: RunConfig


class ChecksOut(BaseModel):
    checks: list[dict[str, Any]]


class ChecksIn(BaseModel):
    checks: list[dict[str, Any]]


class GenerateChecksIn(BaseModel):
    user_input: str = Field(description="Natural language description of data quality requirements")
    table_fqn: str | None = Field(default=None, description="Optional fully qualified table name for schema context")


class GenerateChecksOut(BaseModel):
    yaml_output: str = Field(description="Generated checks in YAML format")
    checks: list[dict[str, Any]] = Field(description="Generated checks as a list of dictionaries")
    validation_errors: list[str] = Field(default_factory=list, description="Validation errors if any")


class RuleCatalogEntryOut(BaseModel):
    table_fqn: str
    checks: list[dict[str, Any]]
    version: int
    status: str
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None


class SaveRulesIn(BaseModel):
    table_fqn: str = Field(description="Fully qualified table name (catalog.schema.table)")
    checks: list[dict[str, Any]] = Field(description="List of check metadata dictionaries")


class SetStatusIn(BaseModel):
    status: str = Field(description="New status: draft | pending_approval | approved | rejected")
    expected_version: int | None = Field(
        default=None,
        description="If provided, the update is rejected when the current version does not match (optimistic concurrency).",
    )


class DryRunIn(BaseModel):
    table_fqn: str = Field(description="Fully qualified table name to run checks against")
    checks: list[dict[str, Any]] = Field(description="List of check metadata dictionaries")
    sample_size: int = Field(default=1000, le=10_000, description="Number of rows to sample")


class DryRunSubmitOut(BaseModel):
    run_id: str
    job_run_id: int


class DryRunOut(BaseModel):
    total_rows: int
    valid_rows: int
    invalid_rows: int
    error_summary: list[dict[str, Any]]
    sample_invalid: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Profiler models
# ---------------------------------------------------------------------------


class ProfileRunIn(BaseModel):
    table_fqn: str = Field(description="Fully qualified table name to profile")
    sample_limit: int = Field(default=50_000, le=100_000, description="Max rows to sample")
    columns: list[str] | None = Field(default=None, description="Specific columns to profile (all if None)")
    profile_options: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Advanced profiler options: filter (SQL WHERE), max_null_ratio, max_empty_ratio, "
            "max_in_count, distinct_ratio, remove_outliers, num_sigmas, llm_primary_key_detection"
        ),
    )


class ProfileRunOut(BaseModel):
    run_id: str
    job_run_id: int


class RunStatusOut(BaseModel):
    run_id: str
    state: str  # PENDING, RUNNING, TERMINATED, etc.
    result_state: str | None = None  # SUCCESS, FAILED, etc.
    message: str | None = None


class ProfileResultsOut(BaseModel):
    run_id: str
    source_table_fqn: str
    rows_profiled: int | None = None
    columns_profiled: int | None = None
    duration_seconds: float | None = None
    generated_rules: list[dict[str, Any]] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)


class ProfileRunSummaryOut(BaseModel):
    run_id: str
    source_table_fqn: str
    status: str | None = None
    rows_profiled: int | None = None
    columns_profiled: int | None = None
    duration_seconds: float | None = None
    requesting_user: str | None = None
    created_at: str | None = None


class DryRunResultsOut(BaseModel):
    run_id: str
    source_table_fqn: str
    total_rows: int | None = None
    valid_rows: int | None = None
    invalid_rows: int | None = None
    error_summary: list[dict[str, Any]] = Field(default_factory=list)
    sample_invalid: list[dict[str, Any]] = Field(default_factory=list)


class CatalogOut(BaseModel):
    name: str
    comment: str | None = None


class SchemaOut(BaseModel):
    name: str
    catalog_name: str
    comment: str | None = None


class TableOut(BaseModel):
    name: str
    catalog_name: str
    schema_name: str
    table_type: str | None = None
    comment: str | None = None


class ColumnOut(BaseModel):
    name: str
    type_name: str
    comment: str | None = None
    nullable: bool = True
    position: int = 0


class UserRoleOut(BaseModel):
    email: str
    role: str


class InstallationSettings(BaseModel):
    install_folder: str
