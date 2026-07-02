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

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Type aliases (mirrors the CHECK constraints on dq_rules / dq_rule_versions)
# ---------------------------------------------------------------------------

RuleMode = Literal["dqx_native", "lowcode", "sql"]
RuleStatus = Literal["draft", "pending_approval", "approved", "rejected", "deprecated"]
Polarity = Literal["pass", "fail"]
AuthorKind = Literal["human", "ai_generated", "ai_assisted"]

SlotFamily = Literal["numeric", "text", "temporal", "boolean", "any"]
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
    """A ``{{column}}`` placeholder declared on a registry rule's definition.

    ``family`` drives the family-filtered column picker when a rule is
    applied to a monitored table. ``position`` fixes a stable display/
    substitution order; ``cardinality`` distinguishes a single-column slot
    (``one``) from a composite/multi-column slot (``many``, e.g. ``is_unique``
    over a list of columns).
    """

    name: str = Field(description="Slot placeholder name, e.g. 'column'")
    family: SlotFamily = Field(description="Column family the slot accepts")
    position: int = Field(default=0, description="Stable ordering position among a rule's slots")
    cardinality: SlotCardinality = Field(default="one", description="Whether the slot binds one or many columns")


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


class RuleVersion(BaseModel):
    """Domain model for a ``dq_rule_versions`` row — a FROZEN publish snapshot.

    Written once per publish; never mutated afterward. ``user_metadata`` is a
    full frozen copy of the tags (including dimension/severity) at publish
    time, independent of subsequent edits to the live ``dq_rules`` row.
    """

    id: str | None = Field(default=None, description="None until persisted")
    rule_id: str
    version: int
    definition: RuleDefinition
    polarity: Polarity | None = None
    user_metadata: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    created_at: datetime | None = None


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
