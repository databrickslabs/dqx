"""Slot family + parameter type derivation for DQX check functions.

Phase 2A (task 2.2/2.4 prep): given a check function's metadata as exposed by
``listCheckFunctions`` (see ``routes.v1.check_functions``), split its
parameters into typed :class:`~databricks_labs_dqx_app.backend.registry_models.RuleSlot`
entries (column-bearing arguments — the ``{{placeholders}}`` a monitored-table
mapping later binds to real columns) and
:class:`~databricks_labs_dqx_app.backend.registry_models.RuleParameter`
entries (everything else).

DQX check-function signatures type column arguments as ``str | Column``,
which says nothing about what *kind* of column the check expects. The slot
family for each check's column argument(s) is resolved by
``routes.v1.check_functions._family_for_column_param`` (item 10 — typed
slots) — that module is the single source of truth for check-function
semantics (it already owns ``_CATEGORIES`` and the param-kind classifier),
so :func:`resolve_slot_family` here simply delegates to it rather than
keeping a second, independently-maintained family table. Everything not
covered by that map defaults to ``"any"``, which is correct for genuinely
type-agnostic checks like ``is_not_null``.
"""

from __future__ import annotations

from typing import cast

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


def resolve_slot_family(function_name: str) -> SlotFamily:
    """Resolve the slot family for a check function's column slot(s).

    Delegates to ``routes.v1.check_functions._family_for_column_param`` — the
    single source of truth for a check's column-argument semantics (see the
    ``_COLUMN_FAMILIES`` map there, which also backs the ``family`` exposed
    on each ``CheckFunctionParam`` via ``listCheckFunctions``). Falls back to
    ``"any"`` (correct for genuinely type-agnostic checks) when
    *function_name* has no override there.

    The import is local to avoid a module-load-order dependency between
    ``registry_seed_map`` and the FastAPI route module; there is no circular
    import risk since ``check_functions`` never imports this module.

    Args:
        function_name: The DQX check-function name (``CHECK_FUNC_REGISTRY``
            key), e.g. ``"is_valid_email"``.

    Returns:
        The resolved :data:`SlotFamily`.
    """
    from .routes.v1.check_functions import _family_for_column_param  # noqa: PLC0415

    # `_family_for_column_param` returns `str`, but its `_COLUMN_FAMILIES`
    # map is hand-authored with only valid SlotFamily literal values (plus
    # the "any" default), so this cast is safe.
    return cast(SlotFamily, _family_for_column_param(function_name))


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
            # Prefer the family already resolved onto the param (set by
            # ``_build_param`` via ``_family_for_column_param`` for every
            # real ``CheckFunctionDef``); fall back to re-resolving by
            # function name for synthetic/hand-built fixtures (e.g. unit
            # tests) that construct a ``CheckFunctionParam`` without it.
            family = cast(SlotFamily, param.family) if param.family else resolve_slot_family(check_function.name)
            slots.append(
                RuleSlot(
                    name=param.name,
                    family=family,
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
