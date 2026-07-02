from typing import Any

from databricks.labs.dqx.config import RunConfig, WorkspaceConfig
from pydantic import BaseModel, Field

from .. import __version__
from .registry_models import AuthorKind as RegistryAuthorKind
from .registry_models import Polarity as RegistryPolarity
from .registry_models import RegistryRule as RegistryRuleDomain
from .registry_models import RuleDefinition as RegistryRuleDefinition
from .registry_models import RuleMode as RegistryRuleMode
from .registry_models import RuleStatus as RegistryRuleStatus
from .registry_models import RuleVersion as RegistryRuleVersionDomain
from .registry_models import MonitoredTable as MonitoredTableDomain
from .registry_models import MonitoredTableStatus as MonitoredTableStatusDomain
from .services.monitored_table_service import (
    AppliedRuleSummary,
    LatestProfile,
    MonitoredTableDetail,
    MonitoredTableSummary,
)


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


class GenerateRulesFromContractIn(BaseModel):
    """Request body for generating DQX rules from an ODCS v3.x contract."""

    # Bound the raw payload so a single request can't carry a pathologically
    # large contract. 1 MiB is far larger than any realistic ODCS contract
    # while still capping parse cost and the upstream LLM fan-out from
    # ``type: text`` expectations (OWASP LLM04 — see AGENTS.md).
    contract_text: str = Field(
        max_length=1_048_576,
        description="Raw ODCS contract YAML or JSON content",
    )
    generate_predefined_rules: bool = Field(
        default=True,
        description="Generate rules from schema property constraints (required, pattern, min/max, etc.)",
    )
    process_text_rules: bool = Field(
        default=False,
        description="Process natural-language quality expectations via LLM (requires [llm] extras)",
    )
    generate_schema_validation: bool = Field(
        default=True,
        description="Emit a has_valid_schema dataset rule per ODCS schema",
    )
    strict_schema_validation: bool = Field(
        default=True,
        description="If true, schema check requires exact columns/order/types",
    )
    default_criticality: str = Field(
        default="error",
        description="Default criticality for predefined rules: 'error' or 'warn'",
    )


class ContractMetadataOut(BaseModel):
    """Top-level metadata extracted from the source contract for UI display."""

    contract_id: str | None = None
    name: str | None = None
    version: str | None = None
    odcs_api_version: str | None = None
    status: str | None = None
    owner: str | None = None
    domain: str | None = None
    description: str | None = None


class ContractSchemaRulesOut(BaseModel):
    """Generated rules for one ODCS schema, plus the suggested UC target if any."""

    schema_name: str = Field(description="ODCS schema name (typically the logical table name)")
    physical_name: str | None = Field(
        default=None,
        description="physicalName from the contract, if present — a suggested UC target",
    )
    property_count: int = Field(default=0, description="Number of columns/properties in the schema")
    rules: list[dict[str, Any]] = Field(description="Generated DQX rules belonging to this schema")


class GenerateRulesFromContractOut(BaseModel):
    metadata: ContractMetadataOut
    schemas: list[ContractSchemaRulesOut]
    unassigned_rules: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Rules with no schema metadata (rare; surfaced rather than dropped)",
    )
    total_rules: int = 0
    warnings: list[str] = Field(default_factory=list)
    validation_errors: list[str] = Field(
        default_factory=list,
        description=(
            "DQEngine.validate_checks errors for the generated rules. Non-blocking — "
            "surfaced so the UI can flag rules that would fail at execution time "
            "(mirrors the AI-assisted generation endpoint)."
        ),
    )


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


class TableSchemaDdlOut(BaseModel):
    """Spark-DDL snapshot of a UC table's current column list.

    Used as a starting point for the schema-validation rule builder so
    the user can fine-tune (drop extra columns, widen types) rather than
    type everything from scratch.
    """

    table_fqn: str = Field(description="Fully qualified UC table name")
    ddl: str = Field(description="Comma-separated 'col TYPE' string suitable for has_valid_schema")
    column_count: int = Field(default=0, description="Number of columns included in the DDL")


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


class CreateRegistryRuleIn(BaseModel):
    """Request body for creating a new draft Rules Registry rule."""

    mode: RegistryRuleMode = Field(description="Authoring type: dqx_native | lowcode | sql")
    definition: RegistryRuleDefinition = Field(description="Mode-specific body plus typed slots/parameters")
    polarity: RegistryPolarity | None = Field(default=None, description="pass|fail — meaningful for lowcode/sql only")
    author_kind: RegistryAuthorKind = Field(default="human", description="human | ai_generated | ai_assisted")
    user_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Reserved tag keys (name/description/dimension/severity) + free-text tags",
    )
    steward: str | None = Field(default=None, description="Owning steward's email/username")


class UpdateRegistryRuleIn(BaseModel):
    """Request body for updating a draft Rules Registry rule. Only draft rules are editable."""

    mode: RegistryRuleMode | None = None
    definition: RegistryRuleDefinition | None = None
    polarity: RegistryPolarity | None = None
    user_metadata: dict[str, Any] | None = None
    steward: str | None = None


class RegistryRuleOut(BaseModel):
    """A ``dq_rules`` row as returned to the frontend."""

    rule_id: str
    mode: RegistryRuleMode
    status: RegistryRuleStatus
    version: int
    polarity: RegistryPolarity | None = None
    author_kind: RegistryAuthorKind | None = None
    definition: RegistryRuleDefinition
    user_metadata: dict[str, Any] = Field(default_factory=dict)
    fingerprint: str | None = None
    steward: str | None = None
    is_builtin: bool = False
    source: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None

    @classmethod
    def from_domain(cls, rule: RegistryRuleDomain) -> "RegistryRuleOut":
        return cls(
            rule_id=rule.rule_id,
            mode=rule.mode,
            status=rule.status,
            version=rule.version,
            polarity=rule.polarity,
            author_kind=rule.author_kind,
            definition=rule.definition,
            user_metadata=rule.user_metadata,
            fingerprint=rule.fingerprint,
            steward=rule.steward,
            is_builtin=rule.is_builtin,
            source=rule.source,
            created_by=rule.created_by,
            created_at=rule.created_at.isoformat() if rule.created_at else None,
            updated_by=rule.updated_by,
            updated_at=rule.updated_at.isoformat() if rule.updated_at else None,
        )


class RegistryRuleVersionOut(BaseModel):
    """A frozen ``dq_rule_versions`` snapshot as returned to the frontend."""

    rule_id: str
    version: int
    definition: RegistryRuleDefinition
    polarity: RegistryPolarity | None = None
    user_metadata: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    created_at: str | None = None

    @classmethod
    def from_domain(cls, version: RegistryRuleVersionDomain) -> "RegistryRuleVersionOut":
        return cls(
            rule_id=version.rule_id,
            version=version.version,
            definition=version.definition,
            polarity=version.polarity,
            user_metadata=version.user_metadata,
            created_by=version.created_by,
            created_at=version.created_at.isoformat() if version.created_at else None,
        )


class CreateRegistryRuleOut(BaseModel):
    """Response for a successful create — includes a non-blocking dedup warning, if any."""

    rule: RegistryRuleOut
    dedup_warning: str | None = Field(
        default=None, description="Non-blocking warning when a published rule shares this fingerprint"
    )


class RegistryRuleDetailOut(BaseModel):
    """A registry rule plus its current published snapshot (None if never published)."""

    rule: RegistryRuleOut
    current_version: RegistryRuleVersionOut | None = None


class RegisterMonitoredTableIn(BaseModel):
    """Request body for registering a table under Rules Registry governance."""

    table_fqn: str = Field(description="Fully qualified table name (catalog.schema.table)")
    steward: str | None = Field(default=None, description="Owning steward's email/username")


class AppliedRuleOut(BaseModel):
    """A ``dq_applied_rules`` row, denormalized with its registry rule's descriptive tags."""

    id: str | None = None
    binding_id: str
    rule_id: str
    pinned_version: int | None = None
    severity_override: str | None = None
    column_mapping: list[dict[str, str]] = Field(default_factory=list)
    user_metadata: dict[str, Any] = Field(default_factory=dict)
    mapping_hash: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    rule_name: str | None = None
    rule_dimension: str | None = None
    rule_severity: str | None = None

    @classmethod
    def from_summary(cls, summary: AppliedRuleSummary) -> "AppliedRuleOut":
        applied_rule = summary.applied_rule
        return cls(
            id=applied_rule.id,
            binding_id=applied_rule.binding_id,
            rule_id=applied_rule.rule_id,
            pinned_version=applied_rule.pinned_version,
            severity_override=applied_rule.severity_override,
            column_mapping=applied_rule.column_mapping,
            user_metadata=applied_rule.user_metadata,
            mapping_hash=applied_rule.mapping_hash,
            created_by=applied_rule.created_by,
            created_at=applied_rule.created_at.isoformat() if applied_rule.created_at else None,
            rule_name=summary.rule_name,
            rule_dimension=summary.rule_dimension,
            rule_severity=summary.rule_severity,
        )


class MonitoredTableOut(BaseModel):
    """A ``dq_monitored_tables`` row as returned to the frontend."""

    binding_id: str
    table_fqn: str
    steward: str | None = None
    status: MonitoredTableStatusDomain
    last_profiled_at: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None

    @classmethod
    def from_domain(cls, table: MonitoredTableDomain) -> "MonitoredTableOut":
        return cls(
            binding_id=table.binding_id,
            table_fqn=table.table_fqn,
            steward=table.steward,
            status=table.status,
            last_profiled_at=table.last_profiled_at.isoformat() if table.last_profiled_at else None,
            created_by=table.created_by,
            created_at=table.created_at.isoformat() if table.created_at else None,
            updated_by=table.updated_by,
            updated_at=table.updated_at.isoformat() if table.updated_at else None,
        )


class MonitoredTableSummaryOut(BaseModel):
    """A monitored table plus a lightweight list-view counter, for ``listMonitoredTables``."""

    table: MonitoredTableOut
    applied_rule_count: int = 0

    @classmethod
    def from_domain(cls, summary: MonitoredTableSummary) -> "MonitoredTableSummaryOut":
        return cls(
            table=MonitoredTableOut.from_domain(summary.table),
            applied_rule_count=summary.applied_rule_count,
        )


class MonitoredTableDetailOut(BaseModel):
    """A monitored table plus its applied rules, for ``getMonitoredTable``."""

    table: MonitoredTableOut
    applied_rules: list[AppliedRuleOut] = Field(default_factory=list)

    @classmethod
    def from_domain(cls, detail: MonitoredTableDetail) -> "MonitoredTableDetailOut":
        return cls(
            table=MonitoredTableOut.from_domain(detail.table),
            applied_rules=[AppliedRuleOut.from_summary(s) for s in detail.applied_rules],
        )


class MonitoredTableProfileOut(BaseModel):
    """A read-only projection of the latest ``dq_profiling_results`` row for a monitored table."""

    run_id: str
    source_table_fqn: str
    status: str | None = None
    rows_profiled: int | None = None
    columns_profiled: int | None = None
    duration_seconds: float | None = None
    summary: dict[str, Any] = Field(default_factory=dict)
    generated_rules: list[dict[str, Any]] = Field(default_factory=list)
    profiled_at: str | None = None

    @classmethod
    def from_domain(cls, profile: LatestProfile) -> "MonitoredTableProfileOut":
        return cls(
            run_id=profile.run_id,
            source_table_fqn=profile.source_table_fqn,
            status=profile.status,
            rows_profiled=profile.rows_profiled,
            columns_profiled=profile.columns_profiled,
            duration_seconds=profile.duration_seconds,
            summary=profile.summary,
            generated_rules=profile.generated_rules,
            profiled_at=profile.profiled_at,
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
    # ``invalid_rows`` is kept for backwards compatibility but is no longer
    # the primary count surfaced in the UI — see ``error_rows`` below.
    invalid_rows: int
    error_rows: int = 0
    warning_rows: int = 0
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
    # ``error_rows`` / ``warning_rows`` are the authoritative DQX observer
    # counts; ``invalid_rows`` is kept for backwards compatibility only.
    error_rows: int | None = None
    warning_rows: int | None = None
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
    error_rows: int | None = None
    warning_rows: int | None = None
    created_at: str | None = None
    error_message: str | None = None
    checks: list[dict[str, Any]] = Field(default_factory=list)
    # Per-run review status — set by reviewers on the Runs detail page,
    # filterable on the Runs History page. ``review_status`` is the
    # effective value (catalogue default for unreviewed runs, persisted
    # value otherwise); ``review_status_is_default`` lets the History
    # table render unreviewed rows distinctly (e.g. lighter badge) so
    # they're not visually indistinguishable from rows where someone
    # explicitly selected "Pending review".
    review_status: str | None = None
    review_status_is_default: bool = False
    review_status_updated_by: str | None = None
    review_status_updated_at: str | None = None


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
    warnings: list[Any] | None = None
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


class RoleMappingHistoryOut(BaseModel):
    """One row from the role-mapping audit log.

    Read-only — populated only by service-side ``RoleService`` mutations
    against ``dq_role_mappings_history``. Returned by ``GET /v1/roles/history``
    (Admin only) for the admin Settings page audit panel.
    """

    role: str = Field(description="Role name affected by the change")
    group_name: str = Field(description="Databricks workspace group name affected by the change")
    action: str = Field(description="Mutation kind: 'create' or 'delete'")
    changed_by: str | None = Field(
        default=None,
        description=(
            "Email of the admin who performed the change. ``null`` for legacy "
            "delete rows recorded before the deleter email was wired through."
        ),
    )
    changed_at: str | None = Field(
        default=None,
        description="ISO-8601 timestamp of when the change was recorded.",
    )


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


# ---------------------------------------------------------------------------
# DQX check function registry models
# ---------------------------------------------------------------------------


class CheckFunctionParam(BaseModel):
    """A single parameter on a DQX check function as exposed to the UI.

    The shape is intentionally permissive: ``kind`` is a coarse,
    UI-friendly classification derived from ``inspect.signature`` while
    ``annotation`` preserves the verbatim Python type hint for advanced
    consumers (e.g. validation tooling, AI prompts).
    """

    name: str = Field(description="Parameter name as defined on the DQX function")
    kind: str = Field(
        description=("UI input kind: 'column', 'columns', 'boolean', 'number', " "'list', or 'string'."),
    )
    required: bool = Field(description="True iff the parameter has no default")
    default: str | None = Field(
        default=None,
        description="String-rendered default value (omitted when required)",
    )
    annotation: str = Field(
        default="",
        description="Verbatim Python type annotation (best-effort string repr)",
    )


class CheckFunctionDef(BaseModel):
    """A DQX check function as advertised by the backend to the UI."""

    name: str = Field(description="Function name as registered in CHECK_FUNC_REGISTRY")
    rule_type: str = Field(description="'row' or 'dataset'")
    category: str = Field(
        description=(
            "UX grouping bucket (e.g. 'Null & Empty', 'Numeric & Comparable', "
            "'Aggregates'). Used to group entries in the UI dropdown."
        ),
    )
    doc: str = Field(default="", description="First line of the function docstring")
    params: list[CheckFunctionParam] = Field(default_factory=list)


class CheckFunctionsOut(BaseModel):
    """Response wrapper for ``GET /api/v1/check-functions``."""

    functions: list[CheckFunctionDef] = Field(default_factory=list)
