"""Rules Registry domain model (Phase 2A — data + domain layer).

The registry is the authoring/governance layer described in
``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §3 — reusable,
versioned, table-agnostic rule *templates* that are later applied to a
monitored table (mapping slots to real columns) and materialized into
``dq_quality_rules`` (unchanged runner-facing table).

Descriptive metadata — ``name``, ``description``, ``dimension``, ``severity``
— is intentionally **not** a column on any of these models. It lives as
reserved keys inside ``user_metadata``, exactly like the Phase 1
``LabelDefinition`` tags (``routes.v1.config.LabelDefinition``), alongside
arbitrary free-text tags. The reserved-tag-key helpers at the bottom of this
module are the single place that reads/writes those keys so callers never
hand-roll ``user_metadata["dimension"]`` lookups.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

# ---------------------------------------------------------------------------
# Type aliases (mirrors the CHECK constraints on dq_rules / dq_rule_versions)
# ---------------------------------------------------------------------------

RuleMode = Literal["dqx_native", "lowcode", "sql"]
RuleStatus = Literal["draft", "pending_approval", "approved", "rejected", "deprecated"]
Polarity = Literal["pass", "fail"]
AuthorKind = Literal["human", "ai_generated", "ai_assisted"]

SlotFamily = Literal["numeric", "text", "temporal", "boolean", "array", "any"]
SlotCardinality = Literal["one", "many"]

ParamType = Literal["number", "string", "list", "boolean", "regex", "ref_table", "ref_column"]
# JSON-compatible parameter value. ``Any`` is deliberately avoided per
# AGENTS.md — a registry-rule parameter can only ever be one of these
# primitive/JSON shapes once it round-trips through ``dq_rules.definition``.
RuleParamValue = str | float | int | bool | list[str] | None


# ---------------------------------------------------------------------------
# Slots & parameters (§3.2 — slot family drives the column picker; param type
# drives the value input)
# ---------------------------------------------------------------------------


class RuleSlot(BaseModel):
    """A ``{{name}}`` placeholder declared on a registry rule's definition.

    ``name`` is author-editable and arbitrary (e.g. ``user_email``) — it no
    longer has to match the DQX check function's parameter name for a
    ``dqx_native`` rule. ``family`` drives the family-filtered column picker
    when a rule is applied to a monitored table. ``position`` fixes a stable
    display/substitution order; ``cardinality`` distinguishes a single-column
    slot (``one``) from a composite/multi-column slot (``many``, e.g.
    ``is_unique`` over a list of columns).
    """

    name: str = Field(description="Slot placeholder name, e.g. 'column'")
    family: SlotFamily = Field(description="Column family the slot accepts")
    position: int = Field(default=0, description="Stable ordering position among a rule's slots")
    cardinality: SlotCardinality = Field(default="one", description="Whether the slot binds one or many columns")
    arg_key: str | None = Field(
        default=None,
        description=(
            "For a dqx_native column slot, the DQX check function's real parameter name "
            "(e.g. 'column') that this slot's '{{name}}' placeholder fills as a VALUE inside "
            "body.arguments[arg_key]. None for sql/lowcode slots (no function parameter to key "
            "by) and for legacy/back-compat slots where name already equals the parameter name."
        ),
    )


class RuleParameter(BaseModel):
    """A non-column argument on a registry rule's definition.

    ``type`` drives which value-input widget the authoring UI renders;
    ``value`` is the concrete value (or default) supplied at authoring or
    apply time.
    """

    name: str = Field(description="Parameter name as it appears in the check-function signature")
    type: ParamType = Field(description="UI-facing value type")
    value: RuleParamValue = Field(default=None, description="Concrete value or default")


class RuleDefinition(BaseModel):
    """Mode-specific rule body plus its typed slots/params.

    ``body`` holds the mode-specific payload (native: ``{function,
    arguments}`` with ``{{slot}}`` placeholders; lowcode: ``{lowcode_ast,
    predicate}``; sql: ``{predicate}`` or ``{sql_query}``). It is kept as a
    permissive JSON-shaped dict — like ``ChecksOut.checks`` elsewhere in this
    backend — because the three authoring modes have genuinely different
    shapes and validating each one is the ``RegistryService``'s job (a later
    phase), not the domain model's.
    """

    body: dict[str, Any] = Field(default_factory=dict)
    slots: list[RuleSlot] = Field(default_factory=list)
    parameters: list[RuleParameter] = Field(default_factory=list)
    error_message: str | None = Field(
        default=None,
        description=(
            "Optional custom failure message (a Spark SQL expression string), mirroring "
            "DQRule.message_expr. Threaded through create/update and frozen into each "
            "dq_rule_versions snapshot as part of the definition. Materialized as a "
            "top-level 'message_expr' key on the rendered dq_quality_rules check when set; "
            "omitted entirely when None or empty."
        ),
    )


# ---------------------------------------------------------------------------
# Registry rule (dq_rules) & frozen publish snapshot (dq_rule_versions)
# ---------------------------------------------------------------------------


class RegistryRule(BaseModel):
    """Domain model for a ``dq_rules`` row — the LIVE registry template.

    Deliberately has no ``name``/``description``/``dimension``/``severity``
    fields: those are reserved tag keys inside ``user_metadata`` (see the
    helpers below), not columns.
    """

    rule_id: str
    mode: RuleMode
    status: RuleStatus
    version: int = Field(default=0, description="0 until first publish")
    polarity: Polarity | None = Field(default=None, description="Meaningful for lowcode/sql only")
    author_kind: AuthorKind | None = None
    definition: RuleDefinition
    user_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Reserved tag keys (name/description/dimension/severity) + free-text tags",
    )
    fingerprint: str | None = Field(default=None, description="Dedup hash over canonical definition + slots")
    steward: str | None = None
    is_builtin: bool = False
    source: str | None = None
    created_by: str | None = None
    created_at: datetime | None = None
    updated_by: str | None = None
    updated_at: datetime | None = None
    modified_since_publish: bool = Field(
        default=False,
        description=(
            "Transient (never persisted): True when the LIVE definition/polarity/tags differ from the "
            "current published dq_rule_versions snapshot — i.e. this approved (or in-review revision of an) "
            "already-published rule has unpublished edits ('Modified since vN'). Computed by "
            "RegistryService in the list / get-with-version read paths; left False elsewhere (e.g. the "
            "materializer, which resolves the frozen snapshot and does not care)."
        ),
    )


class RuleVersion(BaseModel):
    """Domain model for a ``dq_rule_versions`` row — a FROZEN publish snapshot.

    Written once per publish; never mutated afterward. ``user_metadata`` is a
    full frozen copy of the tags (including dimension/severity) at publish
    time, independent of subsequent edits to the live ``dq_rules`` row.
    """

    id: str | None = Field(default=None, description="None until persisted")
    rule_id: str
    version: int
    mode: RuleMode | None = Field(
        default=None,
        description=(
            "Authoring mode frozen at publish time (dqx_native/lowcode/sql). Frozen alongside the "
            "definition so a later in-place mode switch on the still-editable approved rule cannot "
            "change how this served snapshot is rendered. ``None`` only for legacy rows written "
            "before mode was frozen — the materializer falls back to the live rule's mode for those."
        ),
    )
    definition: RuleDefinition
    polarity: Polarity | None = None
    user_metadata: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    created_at: datetime | None = None


# ---------------------------------------------------------------------------
# Monitored tables + applied rules (Layer 2, Phase 3A — §3.1/§7)
# ---------------------------------------------------------------------------

MonitoredTableStatus = Literal["draft", "pending_approval", "approved", "rejected"]

# One mapping GROUP is ``{slot_name: column_name}`` — the slot→column binding
# for exactly one materialized check. ``column_mapping`` on an applied rule is
# a list of such groups so a single rule can be applied to a table more than
# once with different column bindings (e.g. the same range check on two
# different numeric columns) under one ``dq_applied_rules`` row.
ColumnMappingGroup = dict[str, str]


class MonitoredTable(BaseModel):
    """Domain model for a ``dq_monitored_tables`` row.

    A thin binding recording that *table_fqn* is under active Rules Registry
    governance. Profiling data itself lives in the existing
    ``dq_profiling_results`` Delta table (reused, not duplicated here) —
    ``last_profiled_at`` is just a pointer so the UI can show "profiled 3
    days ago" without a join.
    """

    binding_id: str
    table_fqn: str
    steward: str | None = None
    status: MonitoredTableStatus = "draft"
    version: int = Field(default=0, description="0 = never approved; bumped on each table approval")
    schedule_cron: str | None = Field(
        default=None,
        description="5-field POSIX cron; None = not scheduled. Approved tables with a cron fire on the scheduler.",
    )
    schedule_tz: str | None = Field(default=None, description="IANA zone the cron is evaluated in; None = UTC")
    last_profiled_at: datetime | None = None
    created_by: str | None = None
    created_at: datetime | None = None
    updated_by: str | None = None
    updated_at: datetime | None = None


class AppliedRule(BaseModel):
    """Domain model for a ``dq_applied_rules`` row — the LIVE LINK between a
    published registry rule and a monitored table's column mapping.

    ``pinned_version`` ``None`` means "follow latest published" (the
    materializer re-renders this application whenever the rule is
    republished); a concrete version number freezes it to that
    ``dq_rule_versions`` snapshot. ``severity_override`` overrides the rule's
    tagged severity for this application only, without mutating the registry
    rule. ``mapping_hash`` is populated via :func:`compute_mapping_hash` —
    never hand-computed by callers — so uniqueness on
    ``(binding_id, rule_id, mapping_hash)`` is enforced consistently.
    """

    id: str | None = Field(default=None, description="None until persisted")
    binding_id: str
    rule_id: str
    pinned_version: int | None = Field(default=None, description="None = follow latest published")
    severity_override: str | None = None
    column_mapping: list[ColumnMappingGroup] = Field(
        default_factory=list,
        description="One entry per materialized check: a slot-name -> column-name mapping group",
    )
    user_metadata: dict[str, Any] = Field(default_factory=dict, description="Per-application free-text tags")
    mapping_hash: str | None = Field(default=None, description="Computed via compute_mapping_hash; dedup key")
    created_by: str | None = None
    created_at: datetime | None = None


def compute_mapping_hash(column_mapping: list[ColumnMappingGroup]) -> str:
    """Compute a deterministic dedup hash for an applied rule's *column_mapping*.

    Order-insensitive at two levels, so re-submitting a semantically
    identical mapping never slips past the ``(binding_id, rule_id,
    mapping_hash)`` uniqueness guard just because the caller listed things in
    a different order:

    - **Within a group**: ``{"column": "id"}`` and a group built by inserting
      keys in a different order hash identically (dicts compare by sorted
      items, not insertion order).
    - **Across groups**: ``[{"column": "a"}, {"column": "b"}]`` and
      ``[{"column": "b"}, {"column": "a"}]`` hash identically — each group
      independently maps to one materialized check, so the list order carries
      no semantic meaning.

    Args:
        column_mapping: List of slot-name -> column-name mapping groups.

    Returns:
        A hex-encoded SHA-256 hash string.
    """
    normalized_groups = sorted(tuple(sorted(group.items())) for group in column_mapping)
    combined = json.dumps(normalized_groups, sort_keys=True)
    return hashlib.sha256(combined.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Data Products — versioned monitored-table snapshots, product groupings,
# and run sets (docs/superpowers/plans/2026-07-07-data-products.md Task 1;
# design spec docs/superpowers/specs/2026-07-07-data-products-design.md §3).
# ---------------------------------------------------------------------------

DataProductStatus = Literal["draft", "pending_approval", "approved", "rejected"]
RunSetSource = Literal["approved", "draft"]
RunSetTrigger = Literal["manual", "scheduled"]


class MonitoredTableVersion(BaseModel):
    """Domain model for a ``dq_monitored_table_versions`` row.

    A FROZEN snapshot of a monitored table's approved rule set (design spec
    §3.2). ``checks_json`` is the exact list of DQX check dicts the runner
    consumes (same shape as ``RulesCatalogService.get_approved_checks_for_table``
    output); ``state_json`` is display-only metadata (applied rules with
    registry ids/versions/pins/severities/mappings at freeze time).
    ``refrozen_at`` is set when this version's content is rewritten in place
    without a version bump (auto-upgrade or a per-rule approval/rejection
    affecting this binding) — never mutated at initial freeze time.
    """

    id: str | None = Field(default=None, description="None until persisted")
    binding_id: str
    version: int
    checks_json: list[dict[str, Any]] = Field(default_factory=list)
    state_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    created_at: datetime | None = None
    refrozen_at: datetime | None = Field(default=None, description="Set on re-freeze without a version bump")


class DataProduct(BaseModel):
    """Domain model for a ``dq_data_products`` row — the grouping GUID.

    A Table Space carries its own review lifecycle
    (draft -> pending_approval -> approved/rejected), mirroring registry
    rules and monitored tables. ``version`` is bumped ONLY on approve; member
    or metadata edits flip the space back to ``draft`` ("Modified since
    approval" display state) without touching it.
    """

    product_id: str
    name: str
    description: str | None = None
    steward: str | None = None
    schedule_cron: str | None = None
    schedule_tz: str | None = None
    status: DataProductStatus = "draft"
    version: int = Field(default=0, description="0 until first approval; bumped ONLY on approve")
    created_by: str | None = None
    created_at: datetime | None = None
    updated_by: str | None = None
    updated_at: datetime | None = None


class DataProductMember(BaseModel):
    """Domain model for a ``dq_data_product_members`` row.

    ``pinned_version`` ``None`` means "follow latest approved" for this
    binding; a concrete version number REALLY executes that frozen
    ``dq_monitored_table_versions`` snapshot (a deliberate upgrade over
    dqlake's display-only pin). ``binding_id`` references
    ``dq_monitored_tables`` (service-enforced, no FK — matching every other
    cross-table reference in this schema).
    """

    id: str | None = Field(default=None, description="None until persisted")
    product_id: str
    binding_id: str
    pinned_version: int | None = Field(default=None, description="None = follow latest approved")


class RunSet(BaseModel):
    """Domain model for a ``dq_run_sets`` row.

    Every run submission (product, single table, scheduled product) mints
    one run set — a run set of one for single-table runs. ``product_id`` /
    ``product_version`` are ``None`` for single-table runs.
    """

    run_set_id: str
    product_id: str | None = None
    product_version: int | None = None
    source: RunSetSource
    trigger: RunSetTrigger
    created_by: str | None = None
    created_at: datetime | None = None


class RunSetMember(BaseModel):
    """Domain model for a ``dq_run_set_members`` row.

    ``binding_version`` records the frozen snapshot version actually run
    for this member — ``None`` for draft-source runs where no frozen
    snapshot was used.
    """

    id: str | None = Field(default=None, description="None until persisted")
    run_set_id: str
    run_id: str
    binding_id: str
    binding_version: int | None = Field(default=None, description="None for draft-source runs")


# ---------------------------------------------------------------------------
# Reserved tag-key helpers
# ---------------------------------------------------------------------------
#
# name/description/dimension/severity are reserved keys inside
# ``user_metadata`` — never native columns. These helpers are the single
# choke point for reading/writing them so callers never hand-roll
# ``user_metadata["dimension"]`` lookups (which would silently break if the
# key were ever renamed or the value were a non-string).

RESERVED_NAME_KEY = "name"
RESERVED_DESCRIPTION_KEY = "description"
RESERVED_DIMENSION_KEY = "dimension"
RESERVED_SEVERITY_KEY = "severity"

RESERVED_RULE_METADATA_KEYS: frozenset[str] = frozenset(
    {
        RESERVED_NAME_KEY,
        RESERVED_DESCRIPTION_KEY,
        RESERVED_DIMENSION_KEY,
        RESERVED_SEVERITY_KEY,
    }
)


def get_reserved_tag(user_metadata: dict[str, Any], key: str) -> str | None:
    """Read a reserved tag key from *user_metadata*, ignoring non-string/empty values.

    Args:
        user_metadata: The rule's (or version's) ``user_metadata`` dict.
        key: One of the reserved keys in :data:`RESERVED_RULE_METADATA_KEYS`.

    Returns:
        The tag value if present and a non-empty string, otherwise ``None``.
    """
    value = user_metadata.get(key)
    return value if isinstance(value, str) and value else None


def set_reserved_tag(user_metadata: dict[str, Any], key: str, value: str | None) -> dict[str, Any]:
    """Return a *new* ``user_metadata`` dict with *key* set to *value* (or removed).

    Never mutates *user_metadata* in place — callers hold the returned dict.

    Args:
        user_metadata: The current ``user_metadata`` dict.
        key: One of the reserved keys in :data:`RESERVED_RULE_METADATA_KEYS`.
        value: The new value; ``None`` (or empty string) removes the key.

    Returns:
        A new dict with the update applied.
    """
    updated = dict(user_metadata)
    if value:
        updated[key] = value
    else:
        updated.pop(key, None)
    return updated


def get_rule_name(user_metadata: dict[str, Any]) -> str | None:
    """Read the reserved ``name`` tag."""
    return get_reserved_tag(user_metadata, RESERVED_NAME_KEY)


def get_rule_description(user_metadata: dict[str, Any]) -> str | None:
    """Read the reserved ``description`` tag."""
    return get_reserved_tag(user_metadata, RESERVED_DESCRIPTION_KEY)


def get_rule_dimension(user_metadata: dict[str, Any]) -> str | None:
    """Read the reserved ``dimension`` tag."""
    return get_reserved_tag(user_metadata, RESERVED_DIMENSION_KEY)


def get_rule_severity(user_metadata: dict[str, Any]) -> str | None:
    """Read the reserved ``severity`` tag."""
    return get_reserved_tag(user_metadata, RESERVED_SEVERITY_KEY)


# UI-facing status union: every raw lifecycle status plus the derived
# "modified" state (an approved rule carrying unpublished edits).
RuleDisplayStatus = Literal["draft", "pending_approval", "approved", "rejected", "deprecated", "modified"]


def registry_display_status(status: str, version: int, modified_since_publish: bool) -> RuleDisplayStatus:
    """Compute the UI-facing display status for a registry rule.

    Mirrors the Monitored Tables / Data Products "Modified since publish"
    display convention (:func:`data_product_service.display_status`), applied
    to a registry rule's own edit-in-place lifecycle: an ``approved`` rule
    that has been published at least once (``version > 0``) but carries
    unpublished live edits reads as ``"modified"`` ("Modified since vN"),
    while every other state passes its raw ``status`` through unchanged.

    Args:
        status: The rule's persisted lifecycle status.
        version: The rule's current version (``0`` until first publish).
        modified_since_publish: Whether the live definition/tags differ from
            the current published snapshot (see
            ``RegistryRule.modified_since_publish``).

    Returns:
        One of the raw statuses, or ``"modified"`` for an edited approved rule.
    """
    if status == "approved" and version > 0 and modified_since_publish:
        return "modified"
    return cast(RuleDisplayStatus, status)


# ---------------------------------------------------------------------------
# Severity -> DQX criticality mapping (§9 / materializer)
# ---------------------------------------------------------------------------
#
# DQX ``criticality`` (warn/error) is the separate execution-facing field
# that decides which output DataFrame a failing row lands in — it is NOT
# the same axis as the registry's ``severity`` tag (Low/Medium/High/
# Critical), but the materializer has to pick *some* concrete criticality
# when it renders a ``dq_quality_rules`` row, so this is the single place
# that conversion happens. The mapping is admin-editable: it lives in the
# ``value_criticality`` map on the reserved ``severity`` label definition
# (``dq_app_settings`` / ``label_definitions``), with
# :data:`SEVERITY_TO_CRITICALITY` as the built-in default for installs
# whose stored definition predates the field. The defaults match the
# per-function severity seed map's own implicit scale
# (``builtin_rules_seed._SEVERITY_SEED_MAP``: High for integrity/
# consistency checks, Low for informational geo checks).

DEFAULT_CRITICALITY = "warn"

SEVERITY_TO_CRITICALITY: dict[str, str] = {
    "Low": "warn",
    "Medium": "warn",
    "High": "error",
    "Critical": "error",
}

_SEVERITY_LABEL_KEY = "severity"


def resolve_criticality(severity: str | None, app_settings_service: AppSettingsService) -> str:
    """Map a registry ``severity`` tag value to a DQX ``criticality`` value.

    Reads the admin-editable ``value_criticality`` map on the reserved
    ``severity`` label definition. Resolution order for a non-``None``
    *severity*: the stored ``value_criticality`` entry if present, then the
    built-in :data:`SEVERITY_TO_CRITICALITY` default (so pre-existing
    installs whose stored definition predates ``value_criticality`` keep
    the historical behavior), then :data:`DEFAULT_CRITICALITY` (e.g. a
    custom severity value with no explicit mapping).

    Args:
        severity: The effective severity tag value (already resolved from
            ``severity_override`` or the rule's own tag by the caller).
        app_settings_service: Settings service used to read the stored
            label definitions.

    Returns:
        ``"error"`` or ``"warn"``.
    """
    if severity is None:
        return DEFAULT_CRITICALITY
    for definition in app_settings_service.get_label_definitions():
        if definition.get("key") == _SEVERITY_LABEL_KEY:
            mapping = definition.get("value_criticality")
            if isinstance(mapping, dict) and severity in mapping:
                return str(mapping[severity])
            break
    return SEVERITY_TO_CRITICALITY.get(severity, DEFAULT_CRITICALITY)
