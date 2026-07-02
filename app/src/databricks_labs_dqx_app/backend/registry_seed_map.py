"""Slot family + parameter type derivation for DQX check functions.

Phase 2A (task 2.2/2.4 prep): given a check function's metadata as exposed by
``listCheckFunctions`` (see ``routes.v1.check_functions``), split its
parameters into typed :class:`~databricks_labs_dqx_app.backend.registry_models.RuleSlot`
entries (column-bearing arguments — the ``{{placeholders}}`` a monitored-table
mapping later binds to real columns) and
:class:`~databricks_labs_dqx_app.backend.registry_models.RuleParameter`
entries (everything else).

DQX check-function signatures type column arguments as ``str | Column``,
which says nothing about what *kind* of column the check expects. A small
seed map (:data:`_FUNCTION_FAMILY_SEED_MAP`) refines the slot family for
functions where that matters (e.g. ``is_valid_email`` only makes sense on a
text column) — everything not in the map defaults to ``"any"``, which is
correct for genuinely type-agnostic checks like ``is_not_null``.
"""

from __future__ import annotations

from .models import CheckFunctionDef
from .registry_models import ParamType, RuleParameter, RuleSlot, SlotFamily

__all__ = ["derive_slots_and_parameters", "resolve_slot_family"]


# ``CheckFunctionParam.kind`` values (see
# ``routes.v1.check_functions._classify_param_kind``) that bind to the
# target table's own columns rather than being a scalar/argument value.
# These become RuleSlot entries instead of RuleParameter entries.
_COLUMN_KINDS = frozenset({"column", "columns"})

# Map every remaining ``CheckFunctionParam.kind`` to a registry ParamType.
# "ref_columns" (a CSV of columns on the *reference* table, e.g.
# ``foreign_key``) collapses onto the singular "ref_column" ParamType —
# the registry's parameter vocabulary (design spec §3.2) has no separate
# plural, since it still identifies reference-table column(s), not a
# distinct value type. Any kind not listed here (there is none today)
# falls back to "string" in :func:`derive_slots_and_parameters`.
_PARAM_KIND_TO_TYPE: dict[str, ParamType] = {
    "boolean": "boolean",
    "number": "number",
    "list": "list",
    "string": "string",
    "ref_table": "ref_table",
    "ref_columns": "ref_column",
}

_DEFAULT_SLOT_FAMILY: SlotFamily = "any"

# Seed map refining slot family beyond the "any" default where DQX's
# ``str | Column`` signature is too loose to say anything useful about the
# expected column type (design spec §3.2 / §141). Keyed by check-function
# name — every column slot on that function shares the resolved family
# (none of today's built-ins mix families across two column slots of the
# same function).
#
# ``is_in_range``'s ``min_limit``/``max_limit`` bounds can be numeric OR
# temporal (date/datetime), so its column slot genuinely spans both
# families. "numeric" is picked as the practical default — numeric range
# checks vastly outnumber date-range ones in practice. A future per-
# argument refinement could special-case a temporal-typed limit.
_FUNCTION_FAMILY_SEED_MAP: dict[str, SlotFamily] = {
    # --- Text / string-shaped -------------------------------------------------
    "is_valid_email": "text",
    "is_not_null_and_not_empty": "text",
    "is_not_empty": "text",
    "is_empty": "text",
    "is_null_or_empty": "text",
    "regex_match": "text",
    "is_valid_ipv4_address": "text",
    "is_valid_ipv6_address": "text",
    "is_ipv4_address_in_cidr": "text",
    "is_ipv6_address_in_cidr": "text",
    "is_valid_json": "text",
    "has_json_keys": "text",
    "has_valid_json_schema": "text",
    # --- Numeric ---------------------------------------------------------------
    "is_in_range": "numeric",
    "is_not_in_range": "numeric",
    "is_not_less_than": "numeric",
    "is_not_greater_than": "numeric",
    "is_equal_to": "numeric",
    "is_not_equal_to": "numeric",
    "has_no_outliers": "numeric",
    "has_no_aggr_outliers": "numeric",
    "is_aggr_not_greater_than": "numeric",
    "is_aggr_not_less_than": "numeric",
    "is_aggr_equal": "numeric",
    "is_aggr_not_equal": "numeric",
    # --- Temporal ----------------------------------------------------------
    "is_valid_date": "temporal",
    "is_valid_timestamp": "temporal",
    "is_data_fresh": "temporal",
    "is_data_fresh_per_time_window": "temporal",
    "is_older_than_n_days": "temporal",
    "is_older_than_col2_for_n_days": "temporal",
    "is_not_in_future": "temporal",
    "is_not_in_near_future": "temporal",
    # Everything else (is_not_null, is_in_list, is_unique, foreign_key, ...)
    # is genuinely type-agnostic and stays at the "any" default — no entry
    # needed here.
}


def resolve_slot_family(function_name: str) -> SlotFamily:
    """Resolve the slot family for a check function's column slot(s).

    Falls back to ``"any"`` (correct for genuinely type-agnostic checks)
    when *function_name* has no seed-map override.

    Args:
        function_name: The DQX check-function name (``CHECK_FUNC_REGISTRY``
            key), e.g. ``"is_valid_email"``.

    Returns:
        The resolved :data:`SlotFamily`.
    """
    return _FUNCTION_FAMILY_SEED_MAP.get(function_name, _DEFAULT_SLOT_FAMILY)


def derive_slots_and_parameters(check_function: CheckFunctionDef) -> tuple[list[RuleSlot], list[RuleParameter]]:
    """Split a DQX check function's parameters into typed slots + parameters.

    Column-bearing parameters (``kind`` in ``{"column", "columns"}``) become
    :class:`RuleSlot` entries. Every other parameter becomes a
    :class:`RuleParameter` with its type derived from the UI ``kind`` via
    :data:`_PARAM_KIND_TO_TYPE` (defaulting to ``"string"`` for any future
    kind this module doesn't yet know about, rather than raising — the
    registry should never hard-fail on a new DQX check function).

    Args:
        check_function: A check-function definition as returned by
            ``listCheckFunctions`` (``routes.v1.check_functions``).

    Returns:
        A ``(slots, parameters)`` tuple, each in the same relative order as
        ``check_function.params``. Slot ``position`` is a dense 0-based
        index over the column-bearing parameters only.
    """
    slots: list[RuleSlot] = []
    parameters: list[RuleParameter] = []
    slot_position = 0
    for param in check_function.params:
        if param.kind in _COLUMN_KINDS:
            slots.append(
                RuleSlot(
                    name=param.name,
                    family=resolve_slot_family(check_function.name),
                    position=slot_position,
                    cardinality="many" if param.kind == "columns" else "one",
                )
            )
            slot_position += 1
        else:
            parameters.append(
                RuleParameter(
                    name=param.name,
                    type=_PARAM_KIND_TO_TYPE.get(param.kind, "string"),
                )
            )
    return slots, parameters
