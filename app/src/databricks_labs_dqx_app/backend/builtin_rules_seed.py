"""Seed all built-in DQX check functions as Rules Registry rules (Phase 2C).

Plan reference: ``docs/superpowers/plans/2026-07-02-rules-registry.md``
§PHASE 2 bullet 2.4; design spec §6 (seeding), §3.2 (slots/typing), §4
(``dqx_native`` mode).

Every function DQX's own ``listCheckFunctions`` endpoint would offer to the
single-table editor (see
``routes.v1.check_functions._introspect_check_functions``) is seeded here as
a pre-published (``status='approved'``, ``version=1``), ``is_builtin=true``
registry rule in ``dqx_native`` mode:

- Column-bearing arguments become ``{{slot}}`` placeholders in the frozen
  ``definition.body['arguments']`` (per §4, a ``dqx_native`` rule serializes
  directly to ``{function, arguments}``); every other argument is declared as
  a :class:`~databricks_labs_dqx_app.backend.registry_models.RuleParameter`
  but deliberately left **out** of ``arguments`` — it's an apply-time value,
  not something a table-agnostic template can freeze.
- Descriptive metadata (name/description/dimension/severity) is written as
  reserved ``user_metadata`` tag keys (§3.1), never as columns — see
  ``registry_models.RESERVED_RULE_METADATA_KEYS``.

Idempotent: :func:`seed_builtin_rules_if_absent` looks up each candidate rule
by its structural fingerprint (:func:`compute_registry_rule_fingerprint`)
before inserting. Because a built-in rule's definition never varies between
runs (same function, same slots/params, no per-value data), an existing row
with the same fingerprint IS that built-in rule regardless of what tags an
admin may have since edited onto it — so a match always means "already
seeded", never "coincidentally identical", and edits are never overwritten.
"""

from __future__ import annotations

import logging
from typing import Any

from .models import CheckFunctionDef
from .registry_fingerprint import compute_registry_rule_fingerprint
from .registry_models import (
    RESERVED_DESCRIPTION_KEY,
    RESERVED_DIMENSION_KEY,
    RESERVED_NAME_KEY,
    RESERVED_SEVERITY_KEY,
    RegistryRule,
    RuleDefinition,
    set_reserved_tag,
)
from .registry_seed_map import derive_slots_and_parameters
from .routes.v1.check_functions import _introspect_check_functions
from .services.registry_service import RegistryService

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_SEVERITY",
    "DEFAULT_DIMENSION",
    "build_builtin_definition",
    "build_builtin_metadata",
    "humanize_function_name",
    "resolve_dimension",
    "resolve_severity",
    "seed_builtin_rules_if_absent",
]

# Fallback severity for every seeded built-in not covered by
# ``_SEVERITY_SEED_MAP`` below — admins can retag after seeding (§6); DQX
# ``criticality`` (warn/error) stays the separate execution field this
# doesn't touch.
DEFAULT_SEVERITY = "Medium"

# Fallback dimension for any introspected function not covered by
# ``_DIMENSION_SEED_MAP`` below (design spec §6 / task brief: "default to
# Validity if unclear"). This also transparently covers the ~25
# ``geo.check_funcs`` entries and any future check functions.
DEFAULT_DIMENSION = "Validity"

# Hand-curated function -> dimension seed map (admin-editable after seeding
# via the reserved ``dimension`` tag). Anything not listed here defaults to
# ``DEFAULT_DIMENSION`` — see the task brief's category examples.
_DIMENSION_SEED_MAP: dict[str, str] = {
    # --- Completeness --------------------------------------------------
    "is_not_null": "Completeness",
    "is_null": "Completeness",
    "is_not_null_and_not_empty": "Completeness",
    "is_not_empty": "Completeness",
    "is_empty": "Completeness",
    "is_null_or_empty": "Completeness",
    "is_not_null_and_not_empty_array": "Completeness",
    "is_not_null_and_is_in_list": "Completeness",
    # --- Uniqueness ------------------------------------------------------
    "is_unique": "Uniqueness",
    # --- Consistency -----------------------------------------------------
    "foreign_key": "Consistency",
    "has_valid_schema": "Consistency",
    "compare_datasets": "Consistency",
    # --- Timeliness --------------------------------------------------------
    "is_data_fresh": "Timeliness",
    "is_data_fresh_per_time_window": "Timeliness",
    "is_older_than_n_days": "Timeliness",
    "is_older_than_col2_for_n_days": "Timeliness",
    "is_not_in_future": "Timeliness",
    "is_not_in_near_future": "Timeliness",
    # --- Validity (explicit examples from the task brief; everything else
    # not listed here also defaults to Validity) --------------------------
    "regex_match": "Validity",
    "is_valid_email": "Validity",
    "is_valid_ipv4_address": "Validity",
    "is_valid_ipv6_address": "Validity",
    "is_ipv4_address_in_cidr": "Validity",
    "is_ipv6_address_in_cidr": "Validity",
    "is_in_list": "Validity",
    "is_not_in_list": "Validity",
    "is_in_range": "Validity",
    "is_not_in_range": "Validity",
    "is_valid_date": "Validity",
    "is_valid_timestamp": "Validity",
    "is_valid_json": "Validity",
    "has_json_keys": "Validity",
    "has_valid_json_schema": "Validity",
    "sql_expression": "Validity",
    "sql_query": "Validity",
}

# Hand-curated function -> severity seed map (admin-editable after seeding
# via the reserved ``severity`` tag). Anything not listed here defaults to
# ``DEFAULT_SEVERITY`` ("Medium") — see the task brief's category examples:
# completeness/most validity/format/freshness checks stay at the Medium
# default, integrity/consistency/uniqueness checks are bumped to High, and
# purely informational geo geometry-shape checks are lowered to Low.
_SEVERITY_SEED_MAP: dict[str, str] = {
    # --- High: integrity / consistency / uniqueness checks -----------------
    "is_unique": "High",
    "foreign_key": "High",
    "compare_datasets": "High",
    "sql_query": "High",
    "has_valid_schema": "High",
    "has_valid_json_schema": "High",
    # --- Low: informational geo geometry-shape checks -----------------------
    "has_dimension": "Low",
    "has_x_coordinate_between": "Low",
    "has_y_coordinate_between": "Low",
    "is_area_equal_to": "Low",
    "is_area_not_equal_to": "Low",
    "is_area_not_greater_than": "Low",
    "is_area_not_less_than": "Low",
    "is_geo_contains": "Low",
    "is_geo_covers": "Low",
    "is_geo_intersects": "Low",
    "is_geo_touches": "Low",
    "is_geo_within": "Low",
    "is_geography": "Low",
    "is_geometry": "Low",
    "is_geometrycollection": "Low",
    "is_latitude": "Low",
    "is_linestring": "Low",
    "is_longitude": "Low",
    "is_multilinestring": "Low",
    "is_multipoint": "Low",
    "is_multipolygon": "Low",
    "is_non_empty_geometry": "Low",
    "is_not_null_island": "Low",
    "is_num_points_equal_to": "Low",
    "is_num_points_not_equal_to": "Low",
    "is_num_points_not_greater_than": "Low",
    "is_num_points_not_less_than": "Low",
    "is_ogc_valid": "Low",
    "is_point": "Low",
    "is_polygon": "Low",
    "are_polygons_mutually_disjoint": "Low",
    # Everything else (is_not_null, regex_match, is_valid_email, is_in_range,
    # is_in_list, is_data_fresh, date/timestamp/json checks, ...) is
    # genuinely Medium-severity and stays at the default — no entry needed
    # here.
}


def humanize_function_name(name: str) -> str:
    """Turn a check-function name into a readable display name.

    ``is_not_null`` -> ``"Is not null"``. Underscore-joined words become
    space-joined, and only the first word is capitalized (matching normal
    sentence-case UI copy, not Title Case).
    """
    if not name:
        return ""
    words = name.replace("_", " ")
    return words[0].upper() + words[1:]


def resolve_dimension(function_name: str) -> str:
    """Resolve the seed dimension tag for *function_name*.

    Falls back to :data:`DEFAULT_DIMENSION` for any function not covered by
    the seed map — this is what makes the mapping exhaustive over the full
    introspected function list (including geo/anomaly/PII checks) without
    having to enumerate every one individually.
    """
    return _DIMENSION_SEED_MAP.get(function_name, DEFAULT_DIMENSION)


def resolve_severity(function_name: str) -> str:
    """Resolve the seed severity tag for *function_name*.

    Falls back to :data:`DEFAULT_SEVERITY` ("Medium") for any function not
    covered by the seed map — this keeps the mapping exhaustive over the
    full introspected function list (including geo/anomaly/PII checks)
    without having to enumerate every one individually.
    """
    return _SEVERITY_SEED_MAP.get(function_name, DEFAULT_SEVERITY)


def build_builtin_definition(check_function: CheckFunctionDef) -> RuleDefinition:
    """Build the frozen ``dqx_native`` :class:`RuleDefinition` for *check_function*.

    Column-bearing arguments are declared as ``{{slot}}`` placeholders in
    ``body['arguments']``; every other argument is declared as a
    :class:`RuleParameter` but left out of ``arguments`` entirely — it's an
    apply-time value that a monitored-table application (Phase 3) fills in,
    not something the table-agnostic template can freeze.
    """
    slots, parameters = derive_slots_and_parameters(check_function)
    arguments: dict[str, Any] = {slot.name: f"{{{{{slot.name}}}}}" for slot in slots}
    body = {"function": check_function.name, "arguments": arguments}
    return RuleDefinition(body=body, slots=slots, parameters=parameters)


def build_builtin_metadata(check_function: CheckFunctionDef) -> dict[str, Any]:
    """Build the reserved ``user_metadata`` tags for a seeded built-in rule.

    Only the four reserved keys are set here — arbitrary free-text tags are
    left for admins to add after seeding.
    """
    metadata: dict[str, Any] = {}
    metadata = set_reserved_tag(metadata, RESERVED_NAME_KEY, humanize_function_name(check_function.name))
    metadata = set_reserved_tag(metadata, RESERVED_DESCRIPTION_KEY, check_function.doc or None)
    metadata = set_reserved_tag(metadata, RESERVED_DIMENSION_KEY, resolve_dimension(check_function.name))
    metadata = set_reserved_tag(metadata, RESERVED_SEVERITY_KEY, resolve_severity(check_function.name))
    return metadata


def seed_builtin_rules_if_absent(registry: RegistryService, *, user_email: str = "system") -> int:
    """Seed every introspected DQX check function as a built-in registry rule.

    Idempotent: for each function, computes the definition's structural
    fingerprint and skips seeding if a rule with that fingerprint already
    exists (see the module docstring for why that's a safe identity check).
    Never updates or overwrites an existing row.

    Args:
        registry: The :class:`RegistryService` to seed into.
        user_email: Attributed as ``created_by``/``updated_by`` on seeded
            rows and in the ``dq_rules_history`` audit trail.

    Returns:
        The number of new built-in rules created (0 on a fully-idempotent
        repeat run).
    """
    created = 0
    for check_function in _introspect_check_functions():
        definition = build_builtin_definition(check_function)
        probe = RegistryRule(
            rule_id="__seed_probe__",
            mode="dqx_native",
            status="draft",
            version=0,
            definition=definition,
        )
        fingerprint = compute_registry_rule_fingerprint(probe)
        if registry.get_rule_by_fingerprint(fingerprint) is not None:
            continue
        metadata = build_builtin_metadata(check_function)
        registry.seed_builtin_rule(definition=definition, user_metadata=metadata, user_email=user_email)
        created += 1
    if created:
        logger.info("Seeded %d built-in registry rule(s)", created)
    return created
