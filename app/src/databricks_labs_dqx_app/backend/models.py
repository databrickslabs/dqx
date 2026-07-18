from typing import TYPE_CHECKING, Any, Literal

from databricks.labs.dqx.config import RunConfig, WorkspaceConfig
from pydantic import BaseModel, Field

from .. import __version__
from .config import AI_SAMPLE_ROW_LIMIT
from .registry_models import AuthorKind as RegistryAuthorKind
from .registry_models import Polarity as RegistryPolarity
from .registry_models import RegistryRule as RegistryRuleDomain
from .registry_models import RuleDefinition as RegistryRuleDefinition
from .registry_models import RuleMode as RegistryRuleMode
from .registry_models import RuleStatus as RegistryRuleStatus
from .registry_models import RuleVersion as RegistryRuleVersionDomain
from .registry_models import RuleDisplayStatus as RegistryRuleStatusDisplay
from .registry_models import registry_display_status
from .registry_models import AppliedRule as AppliedRuleDomain
from .registry_models import ColumnMappingGroup
from .registry_models import MonitoredTable as MonitoredTableDomain
from .registry_models import MonitoredTableStatus as MonitoredTableStatusDomain
from .registry_models import ScheduleKind as RegistryScheduleKind
from .registry_models import SCHEDULE_KIND_DEFAULT as REGISTRY_SCHEDULE_KIND_DEFAULT
from .registry_models import MonitoredTableVersion as MonitoredTableVersionDomain
from .registry_models import RuleSlot as RegistryRuleSlot
from .registry_models import RunSetSource as RegistryRunSetSource
from .registry_models import RunSetTrigger as RegistryRunSetTrigger
from .registry_models import DataProductStatus as RegistryDataProductStatus
from .services.data_product_service import (
    DataProductDetail,
    DataProductMemberDetail,
    DataProductRunResult,
    DataProductRunSubmission,
)
from .services.data_product_service import display_status as data_product_display_status
from .services.monitored_table_service import (
    AppliedRuleSummary,
    BulkRegisterResult,
    LatestProfile,
    MonitoredTableDetail,
    MonitoredTableSummary,
)
from .services.rule_suggester import RuleSuggestion, SuggestRulesResult
from .services.tag_suggestion_service import TagRuleSuggestion

if TYPE_CHECKING:
    # Imported for typing only: a runtime import would form a cycle
    # (models -> profiling_suggestion_service -> profiling_rule_builder -> models,
    # whose ``CheckFunctionDef`` is defined far below this line).
    from .services.profiling_suggestion_service import BatchApplyResult, ProfilingSuggestion


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


class AiGenerateRuleIn(BaseModel):
    """Request body for AI-generating a full Rules Registry rule proposal."""

    description: str = Field(
        max_length=4000,
        description="Natural language description of the data quality requirement",
    )
    table_fqn: str | None = Field(default=None, description="Optional fully qualified table name for schema context")
    columns: list[str] | None = Field(default=None, max_length=200, description="Optional candidate column names")
    sample_rows: list[dict[str, Any]] | None = Field(
        default=None,
        max_length=AI_SAMPLE_ROW_LIMIT,
        description="Optional sample rows for context; up to AI_SAMPLE_ROW_LIMIT (500) are forwarded to the model",
    )


class AiGenerateRuleOut(BaseModel):
    """A validated, AI-generated Rules Registry rule proposal, ready to prefill the create form."""

    name: str
    description: str
    mode: str = Field(description="lowcode | dqx_native | sql")
    dimension: str | None = None
    severity: str | None = None
    polarity: str | None = None
    definition: dict[str, Any] = Field(
        description=(
            "Mode-specific body: {function, arguments} (dqx_native), {sql_query} (sql), or "
            "{lowcode_ast, group_by?, predicate | sql_query, merge_columns?} (lowcode)"
        )
    )
    slots: list[RegistryRuleSlot] | None = Field(
        default=None,
        description=(
            "Typed column slots. For a dqx_native proposal, one per column the rule targets, "
            "named from the model's column references with the family locked to the check "
            "function's semantics. For a lowcode proposal, one per {{slot}} placeholder in the "
            "compiled body. None/empty for sql proposals."
        ),
    )
    author_kind: str = Field(default="ai_generated")


class AiSuggestFieldIn(BaseModel):
    """Request body for an AI per-field suggestion (name/description/dimension/severity)."""

    field: str = Field(description="Field being suggested, e.g. 'name', 'description', 'dimension', 'severity'")
    context: str = Field(max_length=4000, description="Rule context (description + any known fields) as free text")


class AiSuggestFieldOut(BaseModel):
    """A single suggested value for one rule field."""

    value: str


class AiWriteSqlIn(BaseModel):
    """Request body for AI-writing a SQL predicate for a rule from a natural-language description."""

    description: str = Field(
        min_length=1,
        max_length=2000,
        description="Natural language description of what the SQL predicate should check",
    )
    columns: list[str] | None = Field(
        default=None,
        max_length=200,
        description="Declared reusable slot names ({{slot}}) the predicate may reference",
    )
    table_fqn: str | None = Field(default=None, description="Optional fully qualified table name for schema context")


class AiImproveSqlIn(BaseModel):
    """Request body for AI-improving an existing SQL predicate per a free-text instruction."""

    predicate: str = Field(min_length=1, max_length=4000, description="The current SQL boolean predicate to refine")
    instruction: str = Field(
        min_length=1,
        max_length=500,
        description="How the predicate should be refined (e.g. 'tighten the null handling')",
    )
    columns: list[str] | None = Field(
        default=None,
        max_length=200,
        description="Declared reusable slot names ({{slot}}) the predicate may reference",
    )


class AiSqlOut(BaseModel):
    """An AI-written or -improved SQL predicate, validated safe before it leaves the server."""

    predicate: str = Field(description="The SQL boolean predicate, referencing slots as {{slot}} placeholders")
    polarity: str | None = Field(default=None, description="pass | fail — whether a TRUE predicate is a pass or fail")


class AiExplainSqlIn(BaseModel):
    """Request body for an AI plain-language explanation of a SQL predicate."""

    predicate: str = Field(min_length=1, max_length=4000, description="The SQL boolean predicate to explain")


class AiExplainSqlOut(BaseModel):
    """A short, plain-language explanation of what a SQL predicate checks."""

    explanation: str


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


class RuleHistoryEntryOut(BaseModel):
    """One recorded change from the ``dq_quality_rules_history`` audit log.

    Backs ``getRuleHistory`` — the per-rule change trail that lets Drafts &
    Review show a previous-vs-proposed diff for a per-table rule draft. Each
    row carries the post-state ``check`` payload plus the status transition,
    so the UI can reconstruct what changed without walking the whole log.
    """

    rule_id: str | None = None
    table_fqn: str
    check: dict[str, Any] | None = Field(
        default=None, description="Post-state DQX check payload recorded at this change (None if not captured)"
    )
    version: int | None = None
    source: str | None = None
    action: str
    prev_status: str | None = None
    new_status: str | None = None
    changed_by: str | None = None
    changed_at: str | None = None


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
    author_kind: RegistryAuthorKind | None = Field(
        default=None,
        description=(
            "Re-stamp AI provenance during an edit-in-place session (e.g. a human accepts an "
            "AI-suggested field on an otherwise human-authored draft). Omit to leave unchanged."
        ),
    )


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
    modified_since_publish: bool = Field(
        default=False,
        description=(
            "True when this approved (or in-review revision of an) already-published rule carries "
            "unpublished live edits — its definition/tags differ from the current published snapshot "
            "('Modified since vN'). Only meaningful on the list / detail read paths."
        ),
    )
    display_status: RegistryRuleStatusDisplay = Field(
        description="UI-facing status: raw status, or 'modified' for an edited approved rule."
    )

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
            modified_since_publish=rule.modified_since_publish,
            display_status=registry_display_status(rule.status, rule.version, rule.modified_since_publish),
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


# Upper bound on a single batch import. The endpoint runs a SYNCHRONOUS
# per-rule loop of DB writes on one worker thread + connection, so an
# unbounded payload would let a single request block a worker and hold a DB
# connection for an arbitrarily long time (DoS). A YAML file / data contract
# import is a handful-to-hundreds of rules in practice; anything larger should
# be split client-side. Requests over this cap are rejected at validation
# (422) before any DB work starts.
BATCH_IMPORT_MAX_RULES = 500


class BatchImportRegistryRulesIn(BaseModel):
    """Bulk-create registry drafts from imported check dicts (YAML, data contract, …)."""

    rules: list[CreateRegistryRuleIn] = Field(min_length=1, max_length=BATCH_IMPORT_MAX_RULES)
    also_submit: bool = Field(
        default=False,
        description="When true, transition each successfully created draft to pending_approval.",
    )
    skip_duplicates: bool = Field(
        default=False,
        description=(
            "When true, reuse an existing structurally-identical ACTIVE rule "
            "(draft/pending_approval/approved) instead of creating a duplicate, "
            "and dedupe repeated rules within this batch. Reused rules are "
            "returned in ``reused`` and are neither re-created nor re-submitted. "
            "Makes re-importing the same contract bundle idempotent."
        ),
    )


class BatchImportRegistryRulesFailure(BaseModel):
    """One rule that failed during a batch import."""

    index: int
    error: str


class BatchImportRegistryRulesOut(BaseModel):
    """Result of a bulk registry import — partial success is allowed."""

    created: list[CreateRegistryRuleOut] = Field(default_factory=list)
    reused: list[CreateRegistryRuleOut] = Field(
        default_factory=list,
        description="Rules matched to an existing active rule by fingerprint (skip_duplicates) — not created.",
    )
    saved: int = 0
    submitted: int = 0
    submit_failed: int = 0
    failed: list[BatchImportRegistryRulesFailure] = Field(default_factory=list)


# Cap the number of pending applications a single batch-record call accepts.
# Mirrors ``BATCH_IMPORT_MAX_RULES``: the endpoint runs a synchronous
# per-entry DB write loop, so an unbounded payload would block the uvicorn
# worker and hold DB connections. Bulk Contract Import chunks to this limit.
BATCH_RECORD_PENDING_MAX = 500


class RecordPendingApplicationIn(BaseModel):
    """One staged (binding, rule, mapping) application awaiting the rule's approval."""

    binding_id: str = Field(description="The monitored table binding this application will attach to")
    rule_id: str = Field(description="The registry rule (not yet approved) to apply on publish")
    column_mapping: list[ColumnMappingGroup] = Field(
        default_factory=list,
        description="One slot-name -> column-name mapping group per materialized check; may be "
        "empty for whole-table rules (no slots).",
    )


class BatchRecordPendingApplicationsIn(BaseModel):
    """Bulk-record pending applications for rules that landed ``pending_approval``.

    Used by Bulk Contract Import when auto-approve is off: rules are created +
    submitted but stay pending, so their intended table bindings + column
    mappings are staged here and activated by ``_publish_registry_rule`` when
    the rule is later approved.
    """

    applications: list[RecordPendingApplicationIn] = Field(min_length=1, max_length=BATCH_RECORD_PENDING_MAX)


class BatchRecordPendingApplicationsFailure(BaseModel):
    """One pending application that failed to record during a batch call."""

    index: int
    error: str


class BatchRecordPendingApplicationsOut(BaseModel):
    """Result of a batch pending-application record — partial success is allowed."""

    recorded: int = 0
    failed: list[BatchRecordPendingApplicationsFailure] = Field(default_factory=list)


class PendingApplicationOut(BaseModel):
    """A staged (approval-gated) application, enriched with its rule's display fields.

    Surfaced read-only on the Apply Rules tab so an application staged by Bulk
    Contract Import (recorded while the rule was still ``pending_approval``) is
    visible instead of the table looking empty. It is NOT a real applied rule:
    no ``dq_applied_rules`` row exists and no checks are materialized until the
    rule is approved and the approval hook drains it. ``rule_name``/
    ``rule_status`` are ``None`` when the referenced rule has since vanished.
    """

    id: str
    binding_id: str
    rule_id: str
    rule_name: str | None = None
    rule_status: str | None = None
    column_mapping: list[ColumnMappingGroup] = Field(default_factory=list)
    created_by: str | None = None
    created_at: str | None = None


class RegistryRuleDetailOut(BaseModel):
    """A registry rule plus its current published snapshot (None if never published)."""

    rule: RegistryRuleOut
    current_version: RegistryRuleVersionOut | None = None


class RegisterMonitoredTableIn(BaseModel):
    """Request body for registering a table under Rules Registry governance."""

    table_fqn: str = Field(description="Fully qualified table name (catalog.schema.table)")
    steward: str | None = Field(default=None, description="Owning steward's email/username")


class UpdateMonitoredTableScheduleIn(BaseModel):
    """Request body for setting/clearing a monitored table's run schedule (P21 item 14).

    ``schedule_cron=None`` clears the schedule. When a cron is present the caller
    should supply ``schedule_tz`` (defaults to UTC service-side when omitted).
    """

    schedule_cron: str | None = Field(default=None, description="5-field POSIX cron; None clears the schedule")
    schedule_tz: str | None = Field(default=None, description="IANA zone the cron is evaluated in; None = UTC")
    schedule_kind: RegistryScheduleKind = Field(
        default=REGISTRY_SCHEDULE_KIND_DEFAULT,
        description="What the scheduled run does: profiling only, DQ only, or both (default both)",
    )




class BulkRegisterMonitoredTablesIn(BaseModel):
    """Request body for bulk-registering many tables under Rules Registry governance."""

    table_fqns: list[str] = Field(description="Fully qualified table names (catalog.schema.table) to register")
    steward: str | None = Field(default=None, description="Owning steward's email/username applied to all")


class BulkRegisterMonitoredTablesOut(BaseModel):
    """Response for ``bulkRegisterMonitoredTables`` — a partitioned summary of the batch."""

    registered: list[str] = Field(default_factory=list, description="Newly registered table FQNs")
    skipped_existing: list[str] = Field(
        default_factory=list, description="Table FQNs already monitored — left untouched"
    )
    invalid: list[str] = Field(default_factory=list, description="Table FQNs that failed FQN validation")

    @classmethod
    def from_domain(cls, result: BulkRegisterResult) -> "BulkRegisterMonitoredTablesOut":
        return cls(registered=result.registered, skipped_existing=result.skipped_existing, invalid=result.invalid)


class AppliedRuleOut(BaseModel):
    """A ``dq_applied_rules`` row, denormalized with its registry rule's descriptive tags."""

    id: str | None = None
    binding_id: str
    rule_id: str
    pinned_version: int | None = None
    severity_override: str | None = None
    row_filter: str | None = Field(
        default=None,
        description="Per-rule SQL WHERE predicate scoping which rows this rule's check validates; "
        "None/blank = every row.",
    )
    pass_threshold: int | None = Field(
        default=None,
        description="Per-rule minimum % of rows that must pass; None = no per-rule threshold.",
    )
    column_mapping: list[dict[str, str]] = Field(default_factory=list)
    user_metadata: dict[str, Any] = Field(default_factory=dict)
    mapping_hash: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    rule_name: str | None = None
    rule_dimension: str | None = None
    rule_severity: str | None = None
    rule_source: str | None = None

    @classmethod
    def from_summary(cls, summary: AppliedRuleSummary) -> "AppliedRuleOut":
        applied_rule = summary.applied_rule
        return cls(
            id=applied_rule.id,
            binding_id=applied_rule.binding_id,
            rule_id=applied_rule.rule_id,
            pinned_version=applied_rule.pinned_version,
            severity_override=applied_rule.severity_override,
            row_filter=applied_rule.row_filter,
            pass_threshold=applied_rule.pass_threshold,
            column_mapping=applied_rule.column_mapping,
            user_metadata=applied_rule.user_metadata,
            mapping_hash=applied_rule.mapping_hash,
            created_by=applied_rule.created_by,
            created_at=applied_rule.created_at.isoformat() if applied_rule.created_at else None,
            rule_name=summary.rule_name,
            rule_dimension=summary.rule_dimension,
            rule_severity=summary.rule_severity,
            rule_source=summary.rule_source,
        )

    @classmethod
    def from_domain(cls, applied: AppliedRuleDomain) -> "AppliedRuleOut":
        """Build from a bare ``AppliedRule`` (no joined registry tags) — the shape
        returned directly by ``ApplyRulesService`` (apply/pin/severity-override),
        as opposed to :meth:`from_summary`'s ``MonitoredTableService``-joined shape.
        """
        return cls(
            id=applied.id,
            binding_id=applied.binding_id,
            rule_id=applied.rule_id,
            pinned_version=applied.pinned_version,
            severity_override=applied.severity_override,
            row_filter=applied.row_filter,
            pass_threshold=applied.pass_threshold,
            column_mapping=applied.column_mapping,
            user_metadata=applied.user_metadata,
            mapping_hash=applied.mapping_hash,
            created_by=applied.created_by,
            created_at=applied.created_at.isoformat() if applied.created_at else None,
        )


class ApplyRuleIn(BaseModel):
    """Request body for applying a published registry rule to a monitored table."""

    rule_id: str = Field(description="The published (approved) dq_rules row to apply")
    column_mapping: list[ColumnMappingGroup] = Field(
        description="One slot-name -> column-name mapping group per materialized check; "
        "every group's keys must exactly match the rule's slot names. May be an empty list "
        "to stage the application with no mapping yet (nothing is materialized until a "
        "follow-up call supplies a fully-covering group)."
    )
    pinned_version: int | None = Field(
        default=None, description="None = follow latest published version; a number freezes to that snapshot"
    )
    severity_override: str | None = Field(
        default=None, description="Overrides the rule's tagged severity for this application only"
    )
    row_filter: str | None = Field(
        default=None,
        description="Per-rule SQL WHERE predicate scoping which rows this rule's check validates; "
        "None/blank = every row. Validated for SQL safety before persistence.",
    )
    pass_threshold: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Per-rule minimum % of rows that must pass; None = no per-rule threshold.",
    )
    tags: dict[str, Any] = Field(default_factory=dict, description="Per-application free-text tags")


class DesiredAppliedRuleIn(BaseModel):
    """One entry in the full desired set of applications for ``saveAppliedRules``."""

    rule_id: str = Field(description="The published (approved) dq_rules row to apply")
    column_mapping: list[ColumnMappingGroup] = Field(
        default_factory=list,
        description="One slot-name -> column-name mapping group per materialized check; may be "
        "empty to stage the application with no mapping yet.",
    )
    pinned_version: int | None = Field(
        default=None, description="None = follow latest published version; a number freezes to that snapshot"
    )
    severity_override: str | None = Field(
        default=None, description="Overrides the rule's tagged severity for this application only"
    )
    row_filter: str | None = Field(
        default=None,
        description="Per-rule SQL WHERE predicate scoping which rows this rule's check validates; "
        "None/blank = every row. Validated for SQL safety before persistence.",
    )
    pass_threshold: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Per-rule minimum % of rows that must pass; None = no per-rule threshold.",
    )
    tags: dict[str, Any] = Field(default_factory=dict, description="Per-application free-text tags")


class SaveAppliedRulesIn(BaseModel):
    """Request body for ``saveAppliedRules`` — the FULL desired set of applications for a binding.

    Anything currently applied to the binding that isn't (re)supplied here is
    removed; see ``ApplyRulesService.save_applied_rules`` for reconcile semantics.
    """

    applications: list[DesiredAppliedRuleIn] = Field(default_factory=list)


class SetAppliedRulePinIn(BaseModel):
    """Request body for pinning/unpinning an applied rule's version."""

    pinned_version: int | None = Field(default=None, description="None clears the pin (follow latest published)")


class SetAppliedRuleSeverityOverrideIn(BaseModel):
    """Request body for setting/clearing an applied rule's severity override."""

    severity: str | None = Field(default=None, description="None clears the override")


class MonitoredTableOut(BaseModel):
    """A ``dq_monitored_tables`` row as returned to the frontend."""

    binding_id: str
    table_fqn: str
    steward: str | None = None
    status: MonitoredTableStatusDomain
    version: int = Field(default=0, description="0 = never approved; bumped on each table approval")
    schedule_cron: str | None = Field(default=None, description="5-field POSIX cron; None = not scheduled")
    schedule_tz: str | None = Field(default=None, description="IANA zone the cron runs in; None = UTC")
    schedule_kind: RegistryScheduleKind = Field(
        default=REGISTRY_SCHEDULE_KIND_DEFAULT,
        description="What the scheduled run does: profiling only, DQ only, or both (default both)",
    )
    last_profiled_at: str | None = None
    last_run_at: str | None = Field(
        default=None,
        description="Newest terminal validation-run instant for this table (either trigger surface); "
        "drives the overview 'Last run' column.",
    )
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
            version=table.version,
            schedule_cron=table.schedule_cron,
            schedule_tz=table.schedule_tz,
            schedule_kind=table.schedule_kind,
            last_profiled_at=table.last_profiled_at.isoformat() if table.last_profiled_at else None,
            last_run_at=table.last_run_at.isoformat() if table.last_run_at else None,
            created_by=table.created_by,
            created_at=table.created_at.isoformat() if table.created_at else None,
            updated_by=table.updated_by,
            updated_at=table.updated_at.isoformat() if table.updated_at else None,
        )


class MonitoredTableReviewOut(BaseModel):
    """Response for the submit/approve/reject monitored-table lifecycle routes.

    ``table`` carries the binding with its new roll-up status; ``affected_check_count``
    is how many materialized ``dq_quality_rules`` rows changed status in this
    transition (submitted, approved, or rejected respectively).
    """

    table: MonitoredTableOut
    affected_check_count: int = 0
    new_version: int | None = Field(
        default=None,
        description="On approve: the newly frozen monitored-table version. None for submit/reject.",
    )


class MonitoredTableVersionOut(BaseModel):
    """A ``dq_monitored_table_versions`` row (metadata only; ``checks_json`` omitted).

    Backs ``listMonitoredTableVersions`` — the frozen-checks payload is
    resolved separately at run time, so this listing carries only the audit
    + display metadata (``state_json``) the version picker needs.
    """

    id: str | None = None
    binding_id: str
    version: int
    state_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    created_at: str | None = None
    refrozen_at: str | None = None

    @classmethod
    def from_domain(cls, version: "MonitoredTableVersionDomain") -> "MonitoredTableVersionOut":
        return cls(
            id=version.id,
            binding_id=version.binding_id,
            version=version.version,
            state_json=version.state_json,
            created_by=version.created_by,
            created_at=version.created_at.isoformat() if version.created_at else None,
            refrozen_at=version.refrozen_at.isoformat() if version.refrozen_at else None,
        )


class MonitoredTableVersionChecksOut(BaseModel):
    """Frozen ``checks_json`` for one monitored-table version snapshot.

    Backs ``getMonitoredTableVersionChecks`` — the heavy per-version payload
    that ``listMonitoredTableVersions`` deliberately omits. Lets Drafts &
    Review diff a binding's previously frozen checks (vN-1) against the
    proposed (current / vN) rule set.
    """

    binding_id: str
    version: int
    checks: list[dict[str, Any]] = Field(default_factory=list)


class MonitoredTableSummaryOut(BaseModel):
    """A monitored table plus lightweight list-view counters, for ``listMonitoredTables``.

    The ``score*`` fields are LEFT-JOINed from the ``dq_score_cache`` OLTP
    table in the same round-trip (P3.4) — the cached row-weighted DQ score
    of the table's latest PUBLISHED run. All None when the table has never
    been scored (no cache row yet).
    """

    table: MonitoredTableOut
    applied_rule_count: int = 0
    check_count: int = 0
    score: float | None = Field(default=None, description="Cached DQ score in [0, 1]; None = never computed")
    failed_tests: int | None = None
    total_tests: int | None = None
    score_computed_at: str | None = Field(default=None, description="When the cached score was last recomputed")

    @classmethod
    def from_domain(cls, summary: MonitoredTableSummary) -> "MonitoredTableSummaryOut":
        return cls(
            table=MonitoredTableOut.from_domain(summary.table),
            applied_rule_count=summary.applied_rule_count,
            check_count=summary.check_count,
            score=summary.score,
            failed_tests=summary.failed_tests,
            total_tests=summary.total_tests,
            score_computed_at=summary.score_computed_at,
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


class BackfillRuleEmbeddingsOut(BaseModel):
    """Result of a manual re-embed pass over every published registry rule (Rules Registry Phase 4B)."""

    total_published: int
    embedded: int


class SuggestedRuleMappingOut(BaseModel):
    """One validated, complete slot->column mapping suggestion (Rules Registry Phase 4C)."""

    rule_id: str
    rule_name: str | None = None
    dimension: str | None = None
    severity: str | None = None
    column_mapping: ColumnMappingGroup
    explanation: str = ""

    @classmethod
    def from_domain(cls, suggestion: RuleSuggestion) -> "SuggestedRuleMappingOut":
        return cls(
            rule_id=suggestion.rule_id,
            rule_name=suggestion.rule_name,
            dimension=suggestion.dimension,
            severity=suggestion.severity,
            column_mapping=suggestion.column_mapping,
            explanation=suggestion.explanation,
        )


class SuggestRulesOut(BaseModel):
    """Response of ``POST /monitored-tables/{binding_id}/suggest-rules``.

    ``available=False`` (with a human-readable ``reason``) covers every
    degraded path — Vector Search/embedding/AI not configured, retrieval or
    judge failure — and is always returned with HTTP 200, never a 500.
    """

    available: bool
    suggestions: list[SuggestedRuleMappingOut] = Field(default_factory=list)
    reason: str = ""

    @classmethod
    def from_domain(cls, result: SuggestRulesResult) -> "SuggestRulesOut":
        return cls(
            available=result.available,
            reason=result.reason,
            suggestions=[SuggestedRuleMappingOut.from_domain(s) for s in result.suggestions],
        )


class TagRuleSuggestionOut(BaseModel):
    """One tag-matched, accept-to-attach rule suggestion for a monitored table (apply-on-tag).

    The OFF-path counterpart to auto-apply: surfaced on a table's Apply Rules
    screen when ``tag_auto_apply`` is off. ``column_mapping`` is the single
    representative slot->column group; ``explanation`` names the matched tags.
    """

    rule_id: str
    rule_name: str | None = None
    dimension: str | None = None
    severity: str | None = None
    column_mapping: ColumnMappingGroup
    explanation: str = ""

    @classmethod
    def from_domain(cls, suggestion: "TagRuleSuggestion") -> "TagRuleSuggestionOut":
        return cls(
            rule_id=suggestion.rule_id,
            rule_name=suggestion.rule_name,
            dimension=suggestion.dimension,
            severity=suggestion.severity,
            column_mapping=suggestion.column_mapping,
            explanation=suggestion.explanation,
        )


class TagSuggestionsOut(BaseModel):
    """Response of ``GET /monitored-tables/{binding_id}/tag-suggestions``.

    Best-effort: on any read/service failure the route returns an empty list
    with HTTP 200, never a 500 (mirroring the suggest-rules contract).
    """

    suggestions: list[TagRuleSuggestionOut] = Field(default_factory=list)

    @classmethod
    def from_domain(cls, suggestions: list["TagRuleSuggestion"]) -> "TagSuggestionsOut":
        return cls(suggestions=[TagRuleSuggestionOut.from_domain(s) for s in suggestions])


class ProfilingSuggestionOut(BaseModel):
    """One applicable profiler-derived rule suggestion shown on the Profile page (B2-82).

    Read-only: listing these has NO side effects — no registry rule is created
    or approved until the user explicitly applies the suggestion (which resolves
    or creates + approves the rule and binds it via ``applyProfilingSuggestion``).
    """

    index: int = Field(description="Position of the source check in the latest profile's generated_rules")
    function: str
    rule_name: str | None = None
    description: str | None = None
    dimension: str | None = None
    severity: str | None = None
    column_mapping: ColumnMappingGroup = Field(default_factory=dict)

    @classmethod
    def from_domain(cls, suggestion: "ProfilingSuggestion") -> "ProfilingSuggestionOut":
        return cls(
            index=suggestion.index,
            function=suggestion.function,
            rule_name=suggestion.rule_name,
            description=suggestion.description,
            dimension=suggestion.dimension,
            severity=suggestion.severity,
            column_mapping=suggestion.column_mapping,
        )


class ApplyProfilingSuggestionsIn(BaseModel):
    """Request body for applying a batch of profiler suggestions to a monitored table (B2-109).

    Each entry is the ``index`` of a suggestion from ``listProfilingSuggestions``.
    Applying is the ONLY path that resolves-or-creates + approves the underlying
    registry rules — selecting/showing suggestions creates nothing.
    """

    indices: list[int] = Field(
        min_length=1,
        description="Indices of the profiler suggestions to apply (from listProfilingSuggestions).",
    )


class ProfilingSuggestionApplyFailureOut(BaseModel):
    """One profiler suggestion that could not be applied during a batch apply."""

    index: int
    reason: str


class ApplyProfilingSuggestionsOut(BaseModel):
    """Result of a batch profiler-suggestion apply (B2-109).

    Reports partial success explicitly: ``applied`` holds the rules bound to the
    table and ``failed`` the per-index failures, so one unapplicable suggestion
    never aborts the rest.
    """

    applied: list[AppliedRuleOut] = Field(default_factory=list)
    failed: list[ProfilingSuggestionApplyFailureOut] = Field(default_factory=list)

    @classmethod
    def from_domain(cls, result: "BatchApplyResult") -> "ApplyProfilingSuggestionsOut":
        return cls(
            applied=[AppliedRuleOut.from_domain(a) for a in result.applied],
            failed=[ProfilingSuggestionApplyFailureOut(index=f.index, reason=f.reason) for f in result.failed],
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
    # Optional because the single-table submit endpoint's caller already
    # knows which table it asked for. The batch endpoint always populates
    # this so callers can associate each submitted run with its source
    # table by value instead of by list position — batch submission skips
    # tables that fail validation, which shifts `submitted` out of index
    # alignment with the request's `table_fqns` (see runs.tsx regression
    # this field fixes).
    table_fqn: str | None = Field(default=None, description="Source table FQN this run was submitted for")


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
    # "scheduled" for scheduler-launched profiling runs, "manual" otherwise.
    # Derived in the read path from ``requesting_user`` provenance because
    # ``dq_profiling_results`` has no ``run_type`` column (a durable column
    # would need a Delta migration). Drives the Runs History Manual/Scheduled
    # sub-label, mirroring ``ValidationRunSummaryOut.run_type``.
    run_type: str | None = None
    canceled_by: str | None = None
    updated_at: str | None = None
    created_at: str | None = None
    # Databricks task-runner job run id (``dq_profiling_results.job_run_id``).
    # Combined with the workspace host + task-runner ``job_id`` (see
    # ``GET /config/workspace-host``) the UI builds a deep link to the run
    # page: ``{host}/jobs/{job_id}/runs/{job_run_id}``. None for runs that
    # predate job-run tracking or never submitted a job.
    job_run_id: int | None = None


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
    # Real wall-clock run duration in seconds, computed server-side from the
    # RUNNING-placeholder → terminal-row span (see JobService.list_dryrun_rows).
    # Mirrors ``ProfileRunSummaryOut.duration_seconds`` so the Runs History
    # "Time" column matches the linked Databricks job. None when the run is
    # still RUNNING or its true start can't be recovered (old runs).
    duration_seconds: float | None = None
    # Databricks task-runner job run id (``dq_validation_runs.job_run_id``).
    # Combined with the workspace host + task-runner ``job_id`` (see
    # ``GET /config/workspace-host``) the UI builds a deep link to the run
    # page: ``{host}/jobs/{job_id}/runs/{job_run_id}``. None for runs that
    # predate job-run tracking or never submitted a job.
    job_run_id: int | None = None
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
# Data Products Task 3 — run sets + monitored-table run endpoint
# ---------------------------------------------------------------------------


class RunMonitoredTableIn(BaseModel):
    """Body of ``POST /monitored-tables/{binding_id}/run`` (``runMonitoredTable``)."""

    source: RegistryRunSetSource = Field(
        description="'approved' resolves a frozen snapshot; 'draft' renders live state"
    )
    version: int | None = Field(
        default=None,
        description="Pin to a specific approved snapshot version. Ignored when source='draft'.",
    )
    rule_ids: list[str] | None = Field(
        default=None,
        min_length=1,
        description="Optional registry rule ids to run. Omit to run every applied rule on the binding.",
    )
    # Deliberately NO sample_size field: approved/published runs always
    # check the whole table (never sample), and draft runs are capped by
    # the admin setting ``draft_run_sample_limit`` — sampling is not a
    # per-request choice. See BindingRunService.run_binding.


class RunMonitoredTableOut(BaseModel):
    """Response of ``POST /monitored-tables/{binding_id}/run``."""

    run_set_id: str
    run_id: str
    job_run_id: int
    view_fqn: str


class RunSetMemberDetailOut(BaseModel):
    """A single member row inside ``GET /run-sets/{run_set_id}`` (``getRunSet``)."""

    run_id: str
    binding_id: str
    table_fqn: str | None = None
    binding_version: int | None = Field(default=None, description="None for draft-source members")
    status: str | None = None
    total_rows: int | None = None
    valid_rows: int | None = None
    invalid_rows: int | None = None
    error_rows: int | None = None
    warning_rows: int | None = None


class RunSetSummaryOut(BaseModel):
    """A ``dq_run_sets`` row + aggregated status, as returned by ``listRunSets``."""

    run_set_id: str
    product_id: str | None = None
    product_version: int | None = None
    source: RegistryRunSetSource
    trigger: RegistryRunSetTrigger
    created_by: str | None = None
    created_at: str | None = None
    member_count: int
    status: str = Field(description="Aggregated across members: running > failed > canceled > success")


class RunSetDetailOut(BaseModel):
    """Response of ``GET /run-sets/{run_set_id}`` (``getRunSet``)."""

    run_set_id: str
    product_id: str | None = None
    product_version: int | None = None
    source: RegistryRunSetSource
    trigger: RegistryRunSetTrigger
    created_by: str | None = None
    created_at: str | None = None
    status: str = Field(description="Aggregated across members: running > failed > canceled > success")
    members: list[RunSetMemberDetailOut] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Data Products Task 4 — products CRUD, publish, member management, run fan-out
# ---------------------------------------------------------------------------


class CreateDataProductIn(BaseModel):
    """Body of ``POST /data-products`` (``createDataProduct``)."""

    name: str
    description: str | None = None
    steward: str | None = Field(default=None, description="Defaults to the creator's email when omitted")


class UpdateDataProductIn(BaseModel):
    """Body of ``PATCH /data-products/{id}`` (``updateDataProduct``).

    Every field is optional; the route uses ``model_dump(exclude_unset=True)``
    so an omitted field is left untouched while an explicit ``null`` (e.g.
    clearing the schedule) is honored. ANY successful PATCH flips the space
    back to ``draft`` without bumping ``version`` (P21 item 30).
    """

    name: str | None = None
    description: str | None = None
    steward: str | None = None
    schedule_cron: str | None = None
    schedule_tz: str | None = None
    schedule_kind: RegistryScheduleKind | None = None


class AddDataProductMemberIn(BaseModel):
    """Body of ``POST /data-products/{id}/members`` (``addDataProductMember``).

    Upserts by ``binding_id`` — calling again for a binding already a member
    updates its pin in place rather than duplicating a row.
    """

    binding_id: str
    pinned_version: int | None = Field(default=None, description="None = follow latest approved")


class RunDataProductIn(BaseModel):
    """Body of ``POST /data-products/{id}/run`` (``runDataProduct``)."""

    source: RegistryRunSetSource = Field(
        description="'approved' resolves pinned/latest frozen snapshots; 'draft' renders every member's live state"
    )


class DataProductMemberOut(BaseModel):
    """A ``dq_data_product_members`` row joined with its binding's live state.

    The ``score*`` fields carry the binding's cached table-scope DQ score
    from ``dq_score_cache`` (P5.3) — same round-trip as the member
    counters, never a warehouse recompute. All None when the table has
    never been scored.
    """

    id: str
    binding_id: str
    table_fqn: str
    binding_status: str
    binding_version: int
    pinned_version: int | None = Field(default=None, description="None = follow latest approved")
    rules_count: int
    checks_count: int
    runnable: bool = Field(description="binding status == 'approved' AND binding_version > 0")
    score: float | None = Field(default=None, description="Cached DQ score in [0, 1]; None = never computed")
    failed_tests: int | None = None
    total_tests: int | None = None
    score_computed_at: str | None = Field(default=None, description="When the cached score was last recomputed")

    @classmethod
    def from_domain(cls, member: DataProductMemberDetail) -> "DataProductMemberOut":
        return cls(
            id=member.id,
            binding_id=member.binding_id,
            table_fqn=member.table_fqn,
            binding_status=member.binding_status,
            binding_version=member.binding_version,
            pinned_version=member.pinned_version,
            rules_count=member.rules_count,
            checks_count=member.checks_count,
            runnable=member.runnable,
            score=member.score,
            failed_tests=member.failed_tests,
            total_tests=member.total_tests,
            score_computed_at=member.score_computed_at,
        )


class DataProductOut(BaseModel):
    """A ``dq_data_products`` row plus resolved members and list-view counters."""

    product_id: str
    name: str
    description: str | None = None
    steward: str | None = None
    schedule_cron: str | None = None
    schedule_tz: str | None = None
    schedule_kind: RegistryScheduleKind = REGISTRY_SCHEDULE_KIND_DEFAULT
    status: RegistryDataProductStatus
    version: int
    display_status: str = Field(
        description="'approved' | 'pending_approval' | 'rejected' | 'modified' | 'draft' — review lifecycle display"
    )
    members: list[DataProductMemberOut] = Field(default_factory=list)
    member_count: int = 0
    runnable_count: int = 0
    last_run_at: str | None = None
    # LEFT-JOINed from the dq_score_cache OLTP table in the same round-trip
    # (P3.4): the cached unweighted mean of member tables' latest published
    # scores. All None when the product has never been scored.
    score: float | None = Field(default=None, description="Cached DQ score in [0, 1]; None = never computed")
    failed_tests: int | None = None
    total_tests: int | None = None
    score_computed_at: str | None = Field(default=None, description="When the cached score was last recomputed")
    created_by: str | None = None
    created_at: str | None = None
    updated_by: str | None = None
    updated_at: str | None = None

    @classmethod
    def from_domain(cls, detail: DataProductDetail) -> "DataProductOut":
        product = detail.product
        return cls(
            product_id=product.product_id,
            name=product.name,
            description=product.description,
            steward=product.steward,
            schedule_cron=product.schedule_cron,
            schedule_tz=product.schedule_tz,
            schedule_kind=product.schedule_kind,
            status=product.status,
            version=product.version,
            display_status=data_product_display_status(product),
            members=[DataProductMemberOut.from_domain(m) for m in detail.members],
            member_count=detail.member_count,
            runnable_count=detail.runnable_count,
            last_run_at=detail.last_run_at.isoformat() if detail.last_run_at else None,
            score=detail.score,
            failed_tests=detail.failed_tests,
            total_tests=detail.total_tests,
            score_computed_at=detail.score_computed_at,
            created_by=product.created_by,
            created_at=product.created_at.isoformat() if product.created_at else None,
            updated_by=product.updated_by,
            updated_at=product.updated_at.isoformat() if product.updated_at else None,
        )


class DataProductReviewMemberOut(BaseModel):
    """One member of a Table Space under review, with its governed checks.

    Table Spaces have no per-version snapshot store, so the only prior state
    recoverable for a review diff is each member binding's currently frozen
    (pinned, else latest-approved) rule set. Backs ``getDataProductReviewChanges``.
    """

    binding_id: str
    table_fqn: str
    pinned_version: int | None = None
    binding_version: int = 0
    checks: list[dict[str, Any]] = Field(default_factory=list)


class DataProductReviewChangesOut(BaseModel):
    """Recoverable prior/proposed state for a Table Space pending approval.

    NOTE (documented limitation): the app does not persist a per-version
    snapshot of a Table Space's membership/definition, so there is no true
    "previous product version" to diff against. What is recoverable is the
    CURRENT proposed definition — the members being approved and each
    member's governed (frozen) checks. The UI presents this with a note that
    no prior product snapshot exists, rather than fabricating a diff.
    """

    product_id: str
    name: str
    version: int
    members: list[DataProductReviewMemberOut] = Field(default_factory=list)


class DataProductRunSubmissionOut(BaseModel):
    """One successfully submitted member run inside ``runDataProduct``'s response."""

    binding_id: str
    table_fqn: str
    run_id: str
    job_run_id: int
    view_fqn: str
    binding_version: int | None = Field(default=None, description="None for draft-source submissions")

    @classmethod
    def from_domain(cls, submission: DataProductRunSubmission) -> "DataProductRunSubmissionOut":
        return cls(
            binding_id=submission.binding_id,
            table_fqn=submission.table_fqn,
            run_id=submission.run_id,
            job_run_id=submission.job_run_id,
            view_fqn=submission.view_fqn,
            binding_version=submission.binding_version,
        )


class RunDataProductOut(BaseModel):
    """Response of ``POST /data-products/{id}/run``."""

    run_set_id: str
    submitted: list[DataProductRunSubmissionOut] = Field(default_factory=list)
    skipped: list[str] = Field(
        default_factory=list, description="'{table_fqn}: {reason}' entries for members that were skipped or failed"
    )

    @classmethod
    def from_domain(cls, result: DataProductRunResult) -> "RunDataProductOut":
        return cls(
            run_set_id=result.run_set_id,
            submitted=[DataProductRunSubmissionOut.from_domain(s) for s in result.submitted],
            skipped=result.skipped,
        )


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


class FailingRecordFailureOut(BaseModel):
    """One rule failure attached to a quarantined row.

    *columns* lists the source columns the failed check inspected (DQX's
    result-struct *columns* field) — the UI uses it for per-cell
    highlighting. Empty for legacy rows without column attribution.
    """

    rule_name: str | None = None
    message: str | None = None
    columns: list[str] = Field(default_factory=list)


class FailingRecordOut(BaseModel):
    """One quarantined source row, shaped for per-cell failure highlighting."""

    record_key: str
    row_values: dict[str, str | None] = Field(default_factory=dict)
    failed_columns: list[str] = Field(default_factory=list)
    failures: list[FailingRecordFailureOut] = Field(default_factory=list)


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


class TableScoreOut(BaseModel):
    """Row-weighted DQ score for one table, computed from its latest run.

    *score* is None when the latest run has no rows or no per-check
    breakdown (e.g. runs predating the observer's *check_metrics*
    emission).
    """

    source_table_fqn: str
    score: float | None = None
    latest_run_id: str | None = None
    total_tests: int = 0
    failed_tests: int = 0


class RuleScoreOut(BaseModel):
    """Aggregate DQ score for a registry rule, across every table it is applied to.

    *applied_to_count* is the TOTAL number of applications of the rule
    (across all bindings), independent of the requesting viewer's catalog
    access — the frontend disables the rule Results view on
    ``applied_to_count == 0``, and a rule applied only to tables the
    viewer cannot see is still applied. *per_table* IS filtered to the
    viewer's accessible catalogs (deduplicated by table), and
    *overall_score* is the unweighted mean over the scored entries of
    *per_table* — None when none are scored.
    """

    rule_id: str
    applied_to_count: int = 0
    overall_score: float | None = None
    per_table: list[TableScoreOut] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# DQ results models (dqlake-shape port — see routes/v1/dq_results.py).
# Field names/shapes deliberately mirror dqlake's routers/dq_results.py
# response models so the ported results UI consumes them nearly verbatim.
# ---------------------------------------------------------------------------


class GroupRowOut(BaseModel):
    """One breakdown row (by dimension / severity / rule / column / table).

    *label* is None for checks whose rule carries no tag on the grouped
    axis (dqlake parity: the UI renders an em-dash). *check_count* is
    None on the by-column breakdown, matching dqlake's by_column query
    which does not compute it. *binding_id* is filled on the by_table
    axis only (additive — the monitored-table binding for the row's
    table, so the UI can link the row; None when the table is not
    monitored or on every other axis). *rule_id* is filled on the
    by_rule axis only (additive — the frozen registry rule id the group
    is keyed on, so the UI can facet-filter by rule IDENTITY across
    renames; None for legacy/untagged name-keyed groups and on every
    other axis).
    """

    label: str | None = None
    binding_id: str | None = None
    rule_id: str | None = None
    pass_rate: float | None = None
    failed_tests: int | None = None
    rule_count: int | None = None
    check_count: int | None = None
    total_tests: int | None = None


class TrendPointOut(BaseModel):
    """One over-time point; *series* is set on grouped trends only.

    *version* is the monitored-table binding version active at this run
    instant (the highest approved version whose freeze time is at/-before
    the run); 0 before the first approval, None when not applicable
    (grouped trends, or scopes without a single binding). Only the
    single-table overall trend populates it — the UI marks the runs where
    it increments.

    *is_draft* marks a point whose contributing run(s) were DRAFT (not
    published) so the over-time tooltip can badge it (B2-136). When a point
    collapses several runs onto one instant (multi-table pooling, or the
    as-of carry-forward), it is draft if ANY contributing run was a draft —
    the conservative choice so a mixed instant is never silently shown as
    fully published.
    """

    run_date: str | None = None
    series: str | None = None
    pass_rate: float | None = None
    rule_count: int | None = None
    total_tests: int | None = None
    version: int | None = None
    is_draft: bool = False


class TrendCountPointOut(BaseModel):
    """Per-run count axes: distinct rules, checks (rows), and tests
    (record-level evaluations). Feeds the "Number of Rules, Checks & Tests"
    chart."""

    run_date: str | None = None
    rule_count: int | None = None
    check_count: int | None = None
    test_count: int | None = None


class TrendFailurePointOut(BaseModel):
    """Per-run failure count axes. A failed check = a check row with >=1
    failed test; a failed rule = a distinct rule with any failed test.
    *failed_records* is the run's distinct failing-row count (derived from
    the observer's input/valid row counts); None when underivable."""

    run_date: str | None = None
    failed_rule_count: int | None = None
    failed_check_count: int | None = None
    failed_test_count: int | None = None
    failed_records: int | None = None


class EntityResultsOut(BaseModel):
    """Breakdowns + trends for one results entity (table / product / rule / global).

    The table endpoint fills *tables*; the product/global/rule endpoints
    fill *by_table* and *trend_by_table* (dqlake parity). Keys outside the
    requested *axes* slice are returned empty so the shape is stable.
    """

    by_dimension: list[GroupRowOut] = Field(default_factory=list)
    by_severity: list[GroupRowOut] = Field(default_factory=list)
    by_column: list[GroupRowOut] = Field(default_factory=list)
    by_table: list[GroupRowOut] = Field(default_factory=list)
    by_rule: list[GroupRowOut] = Field(default_factory=list)
    trend: list[TrendPointOut] = Field(default_factory=list)
    trend_by_dimension: list[TrendPointOut] = Field(default_factory=list)
    trend_by_severity: list[TrendPointOut] = Field(default_factory=list)
    trend_by_table: list[TrendPointOut] = Field(default_factory=list)
    trend_counts: list[TrendCountPointOut] = Field(default_factory=list)
    trend_failures: list[TrendFailurePointOut] = Field(default_factory=list)
    tables: list[GroupRowOut] = Field(default_factory=list)


class RunRowOut(BaseModel):
    """One run's rollup for the run picker (newest first).

    *run_mode* is the run's provenance ('draft' | 'published') — the
    stamped run-level tag, with untagged legacy runs resolved to
    'published' (in the shaping view). Only meaningful to display when the
    caller requested ``include_drafts=true``; the default filter already
    restricts rows to published runs.
    """

    run_id: str | None = None
    run_ts: str | None = None
    pass_rate: float | None = None
    failed_tests: int | None = None
    total_tests: int | None = None
    run_mode: str | None = None


class RunsOut(BaseModel):
    rows: list[RunRowOut] = Field(default_factory=list)


class FailedRowFailureOut(BaseModel):
    """One rule failure attached to a failing row, enriched with the
    applied-rule metadata (registry rule id, severity tag, quality
    dimension) joined via the check name. Enrichment fields are None for
    checks not attributable to a registry rule application."""

    rule_id: str | None = None
    rule_name: str | None = None
    quality_dimension: str | None = None
    severity: str | None = None
    message: str | None = None
    columns: list[str] = Field(default_factory=list)


class FailedRowOut(BaseModel):
    """One failing source row shaped for per-cell failure highlighting."""

    record_key: str | None = None
    row_values: dict[str, str | None] = Field(default_factory=dict)
    failed_columns: list[str] = Field(default_factory=list)
    failures: list[FailedRowFailureOut] = Field(default_factory=list)
    run_ts: str | None = None


class FailedRowsOut(BaseModel):
    """Filtered failing-rows sample (dqlake shape plus *suppressed*).

    *total* is the number of matching rows found within the scanned
    window — it can exceed ``len(rows)`` when capped by *limit*.
    *suppressed* is True when the source table carries fine-grained
    access controls (Task 7 semantics); an empty non-suppressed response
    is also what a caller without SELECT on the source table receives.
    """

    rows: list[FailedRowOut] = Field(default_factory=list)
    total: int = 0
    suppressed: bool = False


class SeverityOut(BaseModel):
    """One severity registry entry derived from the reserved label definition."""

    name: str
    color: str
    rank: int


class DimensionOut(BaseModel):
    """One quality-dimension registry entry derived from the reserved label definition."""

    name: str
    color: str
    rank: int


# Guard on the run-completion refresh trigger: the frontend only ever knows
# a handful of just-finished tables, so a longer list signals a misuse (or
# an attempt to turn the endpoint into a full-cache recompute).
REFRESH_SCORES_MAX_TABLES = 100


class RefreshScoresIn(BaseModel):
    """Body of ``POST /dq-results/refresh-scores`` (``refreshDqScores``)."""

    table_fqns: list[str] = Field(
        min_length=1,
        max_length=REFRESH_SCORES_MAX_TABLES,
        description="Three-part FQNs of the tables whose runs just completed",
    )


class RefreshScoresOut(BaseModel):
    """Summary of one score-cache recompute pass."""

    refreshed_tables: int = 0
    refreshed_products: int = 0
    global_refreshed: bool = True


class ScoreTrendPointOut(BaseModel):
    """One homepage trend point from ``dq_score_history`` (P3.5).

    *ts* is the point's ``computed_at`` instant (ISO-ish string, same
    projection the score-cache reads use); *score* is the 0..1 fraction.
    """

    ts: str
    score: float


class HomeStatsOut(BaseModel):
    """Homepage "at a glance" stats (dqlake's ``HomeStatsOut``, adapted).

    Counts come from cheap app-DB COUNT(*) queries; *score* (plus the
    *failed_tests* / *total_tests* counters behind it) is the cached
    org-wide aggregate from the ``dq_score_cache`` 'global' row (P3.4) —
    the endpoint never touches the warehouse. *computed_at* is when that
    global row was last recomputed (dqlake's *refreshed_at* analogue);
    None until the first run-completion refresh populates the cache.

    *score_trend* is the last ~30 global points from ``dq_score_history``
    (oldest first — dqlake's home trend, re-sourced from the OLTP store);
    *score_delta* is the change between the trend's last two points (a
    0..1 fraction, e.g. +0.05 = +5 percentage points), None until there
    are at least two points.
    """

    rule_count: int = 0
    monitored_table_count: int = 0
    table_space_count: int = 0
    score: float | None = None
    failed_tests: int | None = None
    total_tests: int | None = None
    computed_at: str | None = None
    score_trend: list[ScoreTrendPointOut] = Field(default_factory=list)
    score_delta: float | None = None


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


class PrivilegedPrincipalOut(BaseModel):
    """A principal that holds elevated access — either a workspace admin or an app CAN_MANAGE holder."""

    principal: str = Field(description="Display name or email of the privileged principal")
    kind: Literal["workspace_admin", "app_owner"] = Field(
        description="Why this principal is privileged: 'workspace_admin' (member of the SCIM admins group) or 'app_owner' (CAN_MANAGE on the Databricks App)"
    )


# ---------------------------------------------------------------------------
# Unity Catalog tags models
# ---------------------------------------------------------------------------


class TableTagsOut(BaseModel):
    table_fqn: str = Field(description="Fully qualified table name")
    table_tags: list[str] = Field(default_factory=list, description="Tags assigned to the table")
    column_tags: dict[str, list[str]] = Field(default_factory=dict, description="Column name to list of tags mapping")


class GovernedTagOut(BaseModel):
    tag: str = Field(description="Governed tag key, or key=value")
    description: str | None = Field(default=None, description="Governed tag description, if any")


class GovernedTagsOut(BaseModel):
    tags: list[GovernedTagOut] = Field(default_factory=list, description="Governed tags visible to the caller")


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
        description=("UI input kind: 'column', 'columns', 'boolean', 'number', 'list', or 'string'."),
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
    family: str | None = Field(
        default=None,
        description=(
            "For a column-kind parameter ('column' / 'columns'), the slot family the "
            "check's semantics imply ('numeric', 'text', 'temporal', 'boolean', 'array', "
            "or 'any'). A specific (non-'any') family is locked in the authoring UI and "
            "narrows the apply-time column picker. None for non-column parameters."
        ),
    )


class CheckFunctionDef(BaseModel):
    """A DQX check function as advertised by the backend to the UI."""

    name: str = Field(description="Function name as registered in CHECK_FUNC_REGISTRY")
    label: str = Field(description="Human-readable display name for the UI (e.g. 'Is Not Null')")
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


# ---------------------------------------------------------------------------
# Object permissions (UC-style grants) — P22-D item 10
# ---------------------------------------------------------------------------


class PrincipalSearchOut(BaseModel):
    """A workspace principal (user or group) returned by the principal picker."""

    kind: str = Field(description="'user' or 'group'")
    workspace_principal_id: str = Field(description="Workspace SCIM id of the principal")
    display_name: str = Field(description="Human-readable name for display")
    secondary: str | None = Field(default=None, description="Secondary label (username or member count)")


class ObjectGrantOut(BaseModel):
    """One principal's grant on a securable object (direct, inherited, or the users-group default)."""

    principal_id: str = Field(description="Workspace SCIM id; 'users' for the workspace users group")
    principal_type: str = Field(description="'user' or 'group'")
    principal_name: str | None = Field(default=None, description="Human-readable principal name")
    privileges: list[str] = Field(
        default_factory=list, description="Granted privileges (SELECT/MODIFY/APPLY or ALL_PRIVILEGES)"
    )
    inherit: bool = Field(default=False, description="Whether this grant flows down to child objects")
    grantor: str | None = Field(default=None, description="Who granted this")
    updated_at: str | None = Field(default=None, description="When the grant was last set (ISO8601)")
    inherited: bool = Field(default=False, description="True when surfaced from a parent object via inheritance")
    inherited_from_type: str | None = Field(default=None, description="Parent object type an inherited grant came from")
    inherited_from_id: str | None = Field(default=None, description="Parent object id an inherited grant came from")
    is_default: bool = Field(
        default=False,
        description="True on the synthetic users-group default row (implicit SELECT+APPLY, not yet materialized)",
    )


class ObjectGrantsOut(BaseModel):
    """Response for the Permissions tab: grants (incl. the users-group default) + caller capability."""

    object_type: str = Field(description="Securable object type")
    object_id: str = Field(description="Securable object id")
    grants: list[ObjectGrantOut] = Field(default_factory=list)
    can_manage: bool = Field(default=False, description="Whether the caller may add/remove grants on this object")
    default_inherit: bool = Field(
        default=False, description="Admin default for the per-grant inheritance toggle on new grants"
    )


class SetObjectGrantIn(BaseModel):
    """Create-or-replace one principal's grant on a securable object."""

    principal_id: str = Field(description="Workspace SCIM id; 'users' for the workspace users group")
    principal_type: str = Field(description="'user' or 'group'")
    principal_name: str | None = Field(default=None, description="Human-readable principal name")
    privileges: list[str] = Field(
        default_factory=list,
        description="Privileges to grant (empty removes the grant, or revokes the users-group default)",
    )
    inherit: bool = Field(default=False, description="Whether the grant flows down to child objects")


class EffectivePermissionsOut(BaseModel):
    """The caller's effective privileges on a single object (drives UI gating)."""

    object_type: str
    object_id: str
    privileges: list[str] = Field(default_factory=list)
    can_modify: bool = Field(default=False)
    can_apply: bool = Field(default=False)
    can_manage_grants: bool = Field(default=False)
    is_owner: bool = Field(default=False)


class PermissionsDefaultInheritOut(BaseModel):
    """Admin setting: default state of the per-grant inheritance toggle."""

    enabled: bool = Field(description="When true, new grants default to inheriting down the hierarchy")


class SetPermissionsDefaultInheritIn(BaseModel):
    """Request body for updating the default-inheritance admin setting."""

    enabled: bool


# ---------------------------------------------------------------------------
# Genie chat (Ask Genie over the DQ score views) — dqlake-parity shapes
# ---------------------------------------------------------------------------

# Genie conversation/message ids are opaque workspace identifiers (hex-ish).
# The charset constraint keeps them safe to echo into URL paths and logs.
_GENIE_ID_PATTERN = r"^[A-Za-z0-9_\-]{1,128}$"


class GenieAskIn(BaseModel):
    """Ask (or continue) a Genie conversation. The question may carry a
    context preamble — ``(Table: <fqn>)`` or
    ``(Data product: <name> — tables: ...)`` — that the space instructions
    route on."""

    question: str = Field(min_length=1, max_length=4000)
    conversation_id: str | None = Field(default=None, pattern=_GENIE_ID_PATTERN)


class GenieAnswerOut(BaseModel):
    """Partial-or-final state of one Genie message (shared by ask/start/poll)."""

    available: bool = Field(description="False when no Genie space is provisioned")
    conversation_id: str | None = None
    message_id: str | None = None
    answer_text: str | None = None
    sql: str | None = None
    sql_description: str | None = None
    # Executed query result for a query-answer: column names + row cells.
    # None when the answer has no query attachment or the result fetch failed.
    # Capped server-side (see genie_chat_service) so a large table can't
    # bloat the response.
    result_columns: list[str] | None = None
    result_rows: list[list[str | None]] | None = None
    status: str | None = None
    # Short human label for the current step ("Writing SQL", "Running query",
    # "Summarising results", "Done"), so the chat UI can show live progress
    # while polling instead of one undifferentiated spinner.
    stage: str | None = None
    error: str | None = None


class GeniePollIn(BaseModel):
    """Poll one in-flight Genie message."""

    conversation_id: str = Field(pattern=_GENIE_ID_PATTERN)
    message_id: str = Field(pattern=_GENIE_ID_PATTERN)


class GenieSpaceOut(BaseModel):
    """Genie space availability + metadata for the chat UI."""

    available: bool
    space_id: str | None = None
    sample_questions: list[str] = Field(default_factory=list)
    # Provisioning lifecycle: "provisioning" | "ready" | "error" | None.
    # Lets the UI show a calm "getting ready…" state and poll until ready.
    status: str | None = None
    # Deep link to the full Genie space in the workspace, when both the space
    # id and the workspace host are known ("open in new tab").
    space_url: str | None = None


class GenieFeedbackIn(BaseModel):
    """Thumbs up/down on one Genie answer."""

    message_id: str = Field(pattern=_GENIE_ID_PATTERN)
    vote: str = Field(pattern=r"^(up|down)$")


class GenieFeedbackOut(BaseModel):
    ok: bool


class GenieVerifyEntitlementsIn(BaseModel):
    """Pre-verify row-level (failing-rows) access for a batch of tables.

    The cap matches ``entitlement_service.VERIFY_ENTITLEMENTS_MAX_FQNS`` —
    together with the probe semaphore it bounds the worst-case OBO work one
    request can trigger. FQN syntax is validated per entry by the service
    (malformed names get an ``error`` outcome, never a probe).
    """

    table_fqns: list[str] = Field(min_length=1, max_length=50)


class GenieVerifyEntitlementsOut(BaseModel):
    """Per-FQN verification outcome.

    ``verified`` | ``denied`` (no SELECT) | ``suppressed`` (SELECT passed
    but the table carries fine-grained access controls, mirroring the
    failed-rows endpoint's suppression) | ``error``.
    """

    results: dict[str, str] = Field(default_factory=dict)


class ResetDatabaseIn(BaseModel):
    """Request body for the admin "Reset database" endpoint.

    Defense-in-depth on top of the ``require_role(ADMIN)`` route gate: the
    caller must echo back the exact confirmation phrase
    (:data:`~backend.services.database_reset_service.RESET_CONFIRMATION_PHRASE`).
    The server rejects any mismatch with a 400, so a stray/replayed request
    that lacks the phrase cannot trigger the wipe.
    """

    confirmation_phrase: str = Field(
        min_length=1,
        max_length=200,
        description="Must exactly match the expected reset confirmation phrase.",
    )


class ResetDatabaseOut(BaseModel):
    """Result of a database reset — what was cleared, kept, and by whom."""

    status: str
    performed_by: str
    performed_at: str
    cleared_tables: list[str] = Field(default_factory=list)
    failed_tables: dict[str, str] = Field(default_factory=dict)
    preserved_note: str = ""


class ExportOut(BaseModel):
    """A rendered YAML export the client downloads as a file.

    ``format`` is ``dqx`` (a DQX check-list YAML, re-importable into the
    registry) or ``odcs`` (an ODCS v3 DataContract). ``filename`` is a
    suggested download name; ``content`` is the raw YAML text.
    """

    filename: str = Field(description="Suggested download filename, e.g. 'registry_rules.dqx.yaml'.")
    content: str = Field(description="The rendered YAML document.")
    format: str = Field(description="The export format: 'dqx' or 'odcs'.")


class DeployDemoContentIn(BaseModel):
    """Request body for the admin "Deploy demo content" endpoint.

    Args:
        wipe_first: When ``True``, the seed clears existing DQX Studio-managed
            data before seeding so the demo lands on a clean slate.
    """

    wipe_first: bool = False


class DeployDemoContentOut(BaseModel):
    """Acknowledgement that a demo-content seed was launched.

    The seed runs for ~30min on a background daemon thread, so this returns
    immediately with the initial ``running`` state; progress is polled via the
    demo-content status endpoint.
    """

    status: str
    started_at: str


class DemoContentStatusOut(BaseModel):
    """Current state of the long-running demo-content seed job."""

    state: str
    phase: str
    message: str
    started_at: str
    updated_at: str
