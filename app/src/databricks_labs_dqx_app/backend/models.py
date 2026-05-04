from typing import Any

from databricks.labs.dqx.config import RunConfig, WorkspaceConfig
from pydantic import BaseModel, Field

from .. import __version__


class VersionOut(BaseModel):
    version: str
    core_version: str

    @classmethod
    def from_metadata(cls):
        try:
            from importlib.metadata import version as pkg_version

            core = pkg_version("databricks-labs-dqx")
        except Exception:
            core = "unknown"
        return cls(version=__version__, core_version=core)


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
    display_name: str = ""
    checks: list[dict[str, Any]]
    version: int
    status: str
    source: str = "ui"
    rule_id: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None


class SaveRulesIn(BaseModel):
    table_fqn: str = Field(description="Fully qualified table name (catalog.schema.table)")
    checks: list[dict[str, Any]] = Field(description="List of check metadata dictionaries")
    source: str = Field(default="ui", description="Origin of the rules: ui, imported, or ai")
    rule_id: str | None = Field(default=None, description="If set, update existing rule instead of creating")


class BatchSaveRulesIn(BaseModel):
    table_fqns: list[str] = Field(description="Fully qualified table names to apply the checks to")
    checks: list[dict[str, Any]] = Field(description="List of check metadata dictionaries")
    source: str = Field(default="ui", description="Origin of the rules: ui, imported, or ai")


class BatchSaveRulesOut(BaseModel):
    saved: list[RuleCatalogEntryOut] = Field(description="Successfully saved rule sets")
    failed: list[dict[str, str]] = Field(
        default_factory=list,
        description="Tables that failed: [{table_fqn, error}]",
    )


class CheckDuplicatesIn(BaseModel):
    table_fqn: str = Field(description="Fully qualified table name")
    checks: list[dict[str, Any]] = Field(description="Checks to test for duplicates")
    exclude_rule_id: str | None = Field(
        default=None, description="Exclude this rule_id from duplicate check (for edits)"
    )
    exclude_rule_ids: list[str] = Field(
        default_factory=list, description="Exclude multiple rule_ids from duplicate check (for edits)"
    )


class CheckDuplicatesOut(BaseModel):
    duplicates: list[dict[str, Any]] = Field(description="Checks that already exist for this table")


class FilterTablesByColumnsIn(BaseModel):
    required_columns: list[str] = Field(description="Column names that must exist in the table")
    table_fqns: list[str] = Field(description="Fully qualified table names to check")


class FilterTablesByColumnsOut(BaseModel):
    matching: list[str] = Field(description="Table FQNs that contain all required columns")
    not_matching: list[str] = Field(default_factory=list, description="Table FQNs missing one or more columns")
    errors: list[dict[str, str]] = Field(default_factory=list, description="Tables that failed column lookup")


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
    skip_history: bool = Field(default=False, description="If true, do not record this run in the history table")


class DryRunSubmitOut(BaseModel):
    run_id: str
    job_run_id: int
    view_fqn: str = Field(description="Temporary view FQN for cleanup tracking")


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
    view_fqn: str = Field(description="Temporary view FQN for cleanup tracking")


class RunStatusOut(BaseModel):
    run_id: str
    state: str  # PENDING, RUNNING, TERMINATED, etc.
    result_state: str | None = None  # SUCCESS, FAILED, etc.
    message: str | None = None
    view_cleaned_up: bool = Field(default=False, description="Whether the temporary view was cleaned up")


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
    canceled_by: str | None = None
    updated_at: str | None = None
    created_at: str | None = None


class BatchProfileRunIn(BaseModel):
    table_fqns: list[str] = Field(description="List of fully qualified table names to profile")
    sample_limit: int = Field(default=50_000, le=100_000, description="Max rows to sample per table")
    profile_options: dict[str, Any] | None = Field(
        default=None,
        description="Advanced profiler options applied to all tables",
    )


class BatchProfileRunFailure(BaseModel):
    """One per-table failure inside a partially-successful batch profile run.

    The route still returns 2xx when at least one table submitted
    successfully so the frontend can navigate to the runs list, but it
    surfaces individual per-table failures here so the UI can show the
    user *exactly* which tables failed and why (e.g. ``USE SCHEMA``
    permission missing on a specific catalog/schema).
    """

    table_fqn: str = Field(description="Fully qualified name of the table that failed to submit")
    error: str = Field(description="Human-readable error message (often the underlying SQL error)")
    error_code: str | None = Field(
        default=None,
        description=(
            "Stable identifier for known error classes — currently one of "
            "``INSUFFICIENT_PERMISSIONS``, ``TABLE_OR_VIEW_NOT_FOUND``, or "
            "``UNKNOWN``. The UI uses this to surface a friendlier headline."
        ),
    )


class BatchProfileRunOut(BaseModel):
    runs: list[ProfileRunOut] = Field(description="One entry per table with run_id, job_run_id, view_fqn")
    errors: list[BatchProfileRunFailure] = Field(
        default_factory=list,
        description=(
            "Per-table failures encountered during batch submission. Empty "
            "when every table submitted successfully. The route still returns "
            "2xx as long as at least one table submitted; clients should always "
            "check ``errors`` and surface them to the user."
        ),
    )


class BatchRunFromCatalogIn(BaseModel):
    table_fqns: list[str] = Field(description="Approved table FQNs whose rules should be executed")
    sample_size: int = Field(default=1000, le=10_000, description="Number of rows to sample per table")


class BatchRunFromCatalogOut(BaseModel):
    submitted: list[DryRunSubmitOut] = Field(default_factory=list, description="Successfully submitted runs")
    errors: list[str] = Field(default_factory=list, description="Tables that failed to submit")


class DryRunResultsOut(BaseModel):
    run_id: str
    source_table_fqn: str
    total_rows: int | None = None
    valid_rows: int | None = None
    invalid_rows: int | None = None
    error_summary: list[dict[str, Any]] = Field(default_factory=list)
    sample_invalid: list[dict[str, Any]] = Field(default_factory=list)


class ValidationRunSummaryOut(BaseModel):
    run_id: str
    source_table_fqn: str
    status: str | None = None
    requesting_user: str | None = None
    canceled_by: str | None = None
    updated_at: str | None = None
    sample_size: int | None = None
    total_rows: int | None = None
    run_type: str | None = None
    valid_rows: int | None = None
    invalid_rows: int | None = None
    created_at: str | None = None
    error_message: str | None = None
    checks: list[dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Import rules models
# ---------------------------------------------------------------------------


class ValidateChecksIn(BaseModel):
    checks: list[dict[str, Any]] = Field(description="List of check metadata dictionaries to validate")


class ValidateChecksOut(BaseModel):
    valid: bool = Field(description="Whether all checks passed validation")
    errors: list[str] = Field(default_factory=list, description="Validation error messages")


# ---------------------------------------------------------------------------
# Comments models
# ---------------------------------------------------------------------------


class AddCommentIn(BaseModel):
    entity_type: str = Field(description="Entity type: 'run' or 'rule'")
    entity_id: str = Field(description="Entity identifier: run_id or table_fqn")
    comment: str = Field(description="Comment text")


class CommentOut(BaseModel):
    comment_id: str
    entity_type: str
    entity_id: str
    user_email: str
    comment: str
    created_at: str | None = None


# ---------------------------------------------------------------------------
# Quarantine models
# ---------------------------------------------------------------------------


class QuarantineRecordOut(BaseModel):
    quarantine_id: str
    run_id: str
    source_table_fqn: str
    requesting_user: str | None = None
    row_data: dict[str, Any] | None = None
    errors: list[Any] | None = None
    created_at: str | None = None


class QuarantineListOut(BaseModel):
    records: list[QuarantineRecordOut] = Field(default_factory=list)
    total_count: int = 0
    offset: int = 0
    limit: int = 50


# ---------------------------------------------------------------------------
# Metrics models
# ---------------------------------------------------------------------------


class CheckMetricBreakdown(BaseModel):
    """Per-check error/warning breakdown produced by ``DQMetricsObserver``."""

    check_name: str
    error_count: int = 0
    warning_count: int = 0


class MetricSnapshotOut(BaseModel):
    """One row in the validation trend chart for a given source table.

    Pivoted from the long-format ``dq_metrics`` table by the metrics
    route so existing UI components keep working unchanged. Some
    fields (``error_row_count``, ``warning_row_count``, ``check_metrics``,
    ``custom_metrics``) are new additions exposed by ``DQMetricsObserver``;
    older snapshots predating the migration leave them ``None``.
    """

    metric_id: str
    run_id: str
    source_table_fqn: str
    run_type: str | None = None
    total_rows: int | None = None
    valid_rows: int | None = None
    invalid_rows: int | None = None
    error_row_count: int | None = None
    warning_row_count: int | None = None
    pass_rate: float | None = None
    error_breakdown: list[dict[str, Any]] | None = None
    check_metrics: list[CheckMetricBreakdown] | None = None
    custom_metrics: dict[str, Any] | None = None
    rule_set_fingerprint: str | None = None
    requesting_user: str | None = None
    created_at: str | None = None


class MetricsSummaryOut(BaseModel):
    source_table_fqn: str
    latest_pass_rate: float | None = None
    latest_run_id: str | None = None
    latest_run_type: str | None = None
    latest_created_at: str | None = None


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
    permissions: list[str] = Field(default_factory=list, description="List of permissions granted to this role")
    is_runner: bool = Field(
        default=False,
        description=(
            "Whether the user holds the orthogonal RUNNER role. Admins are "
            "always runners. Other roles only become runners when their "
            "group is explicitly mapped to RUNNER."
        ),
    )


class InstallationSettings(BaseModel):
    install_folder: str


# ---------------------------------------------------------------------------
# Role management models
# ---------------------------------------------------------------------------


class RoleMappingOut(BaseModel):
    role: str = Field(description="Role name (admin, rule_approver, rule_author, viewer)")
    group_name: str = Field(description="Databricks workspace group name")
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None


class CreateRoleMappingIn(BaseModel):
    role: str = Field(description="Role name (admin, rule_approver, rule_author, viewer)")
    group_name: str = Field(description="Databricks workspace group name")


class GroupOut(BaseModel):
    display_name: str = Field(description="Group display name")
    id: str | None = Field(default=None, description="Group ID")


# ---------------------------------------------------------------------------
# Unity Catalog tags models
# ---------------------------------------------------------------------------


class TableTagsOut(BaseModel):
    table_fqn: str = Field(description="Fully qualified table name")
    table_tags: list[str] = Field(default_factory=list, description="Tags assigned to the table")
    column_tags: dict[str, list[str]] = Field(default_factory=dict, description="Column name to list of tags mapping")


# ---------------------------------------------------------------------------
# Schedule config models
# ---------------------------------------------------------------------------


class ScheduleConfigOut(BaseModel):
    schedule_name: str
    config: dict[str, Any]
    version: int = 1
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None


class ScheduleConfigIn(BaseModel):
    schedule_name: str = Field(
        description="Unique name for this schedule",
        pattern=r"^[a-zA-Z0-9_\-]{1,64}$",
    )
    config: dict[str, Any] = Field(description="Schedule configuration (frequency, scope, sample_size, etc.)")


class ScheduleConfigHistoryOut(BaseModel):
    schedule_name: str
    config: dict[str, Any]
    version: int = 0
    action: str
    changed_by: str | None = None
    changed_at: str | None = None
