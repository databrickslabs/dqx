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
    sample_size: int = Field(default=100, le=1000, description="Number of rows to sample")


class DryRunOut(BaseModel):
    total_rows: int
    valid_rows: int
    invalid_rows: int
    error_summary: list[dict[str, Any]]
    sample_invalid: list[dict[str, Any]]


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
