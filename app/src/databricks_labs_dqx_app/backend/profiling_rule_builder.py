"""Turn a DQX profiler-generated check into a registry-rule candidate.

The profiler (``GET /monitored-tables/{binding_id}/profile`` ->
``LatestProfile.generated_rules``) emits checks in the standard DQX metadata
shape — ``{"check": {"function": ..., "arguments": {...}}, ...}`` — with
concrete column names and concrete parameter values baked in. The Rules
Registry, by contrast, stores *table-agnostic* templates: column-bearing
arguments are ``{{slot}}`` placeholders and non-column arguments are declared
:class:`RuleParameter` entries whose ``value`` is frozen at authoring time.

:func:`build_profiling_rule` bridges the two. Given one profiler check it
produces a :class:`ProfilingRuleCandidate`:

* a :class:`RuleDefinition` in ``dqx_native`` mode whose body mirrors what the
  built-in seeder (:mod:`builtin_rules_seed`) would produce for the same
  function — column args become ``{{slot}}`` placeholders — but with every
  non-column parameter's concrete profiler value *frozen* onto it, so the
  registry rule reproduces exactly the check the profiler proposed;
* the ``{slot -> column}`` mapping group that binds those slots back to the
  profiled table's real columns;
* the reserved ``user_metadata`` tags (name/description/dimension/severity)
  the seeder assigns to that function.

Structural equality between two profiler checks (same function, same slots,
same frozen parameter values) yields an identical
:func:`compute_registry_rule_fingerprint`, so
:meth:`RegistryService.match_or_create_approved_rule` can dedupe them and never
spawns a duplicate registry rule on re-run.

Security: the function name must resolve through DQX's
``CHECK_FUNC_REGISTRY`` (via :func:`_introspect_check_functions`) or the check
is skipped, and any SQL-bearing argument is validated with
:func:`is_sql_query_safe` before it is frozen — an unsafe query is skipped
rather than persisted.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from databricks.labs.dqx.utils import is_sql_query_safe

from .builtin_rules_seed import build_builtin_metadata
from .models import CheckFunctionDef
from .registry_models import ColumnMappingGroup, RuleDefinition, RuleParameter, RuleSlot
from .registry_seed_map import derive_slots_and_parameters
from .routes.v1.check_functions import _introspect_check_functions
from .sql_utils import strip_sql_line_comments

logger = logging.getLogger(__name__)

__all__ = ["ProfilingRuleCandidate", "build_profiling_rule"]

# Argument keys that carry a raw SQL fragment for the SQL-based dqx_native
# checks (``sql_query`` / ``sql_expression``). Validated with
# ``is_sql_query_safe`` before the value is frozen onto the registry rule.
_SQL_ARGUMENT_KEYS = frozenset({"query", "expression", "sql_query"})


@dataclass
class ProfilingRuleCandidate:
    """A profiler check resolved into a registry-rule template + column binding."""

    function: str
    definition: RuleDefinition
    mapping: ColumnMappingGroup
    metadata: dict[str, Any]


def _extract_check(check: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Pull ``(function, arguments)`` out of a profiler-generated check dict.

    Accepts both the full DQX metadata shape (``{"check": {"function": ...,
    "arguments": {...}}}``) and a bare inner ``{"function": ..., "arguments":
    {...}}`` dict. Returns ``None`` when the shape is unusable.
    """
    inner = check.get("check") if isinstance(check.get("check"), dict) else check
    if not isinstance(inner, dict):
        return None
    function = inner.get("function")
    arguments = inner.get("arguments", {})
    if not isinstance(function, str) or not function:
        return None
    if not isinstance(arguments, dict):
        return None
    return function, arguments


def _mapping_value(slot: RuleSlot, raw: object) -> str | None:
    """Resolve one column slot's profiler argument into a mapping-group value.

    A ``one`` slot binds a single column name; a ``many`` slot binds a
    comma-separated column list (mirroring how the materializer renders a
    ``many`` slot). Returns ``None`` when *raw* is missing or not column-shaped,
    which makes the whole candidate unmappable (an incomplete mapping is never
    suggested).
    """
    if slot.cardinality == "many":
        if isinstance(raw, list) and raw and all(isinstance(c, str) and c for c in raw):
            return ",".join(raw)
        if isinstance(raw, str) and raw:
            return raw
        return None
    if isinstance(raw, str) and raw:
        return raw
    return None


def _sql_arguments_safe(function: str, arguments: dict[str, Any]) -> bool:
    """Reject a profiler check whose SQL-bearing argument fails the safety scan.

    Only the SQL-based native checks (``sql_query`` / ``sql_expression``) carry
    a raw query/expression; everything else has no SQL surface and passes
    trivially. Comments are stripped before the scan (a check may carry an
    explanatory ``-- ...`` prefix) exactly as ``RegistryService`` does.
    """
    if function not in ("sql_query", "sql_expression"):
        return True
    for key in _SQL_ARGUMENT_KEYS:
        value = arguments.get(key)
        if isinstance(value, str) and value and not is_sql_query_safe(strip_sql_line_comments(value)):
            return False
    return True


def _function_def(function: str) -> CheckFunctionDef | None:
    """Resolve a check-function name to its introspected definition.

    Backed by ``CHECK_FUNC_REGISTRY`` (see ``_introspect_check_functions``), so
    an unknown / unregistered / editor-hidden function returns ``None`` and the
    profiler check is skipped rather than trusted.
    """
    for candidate in _introspect_check_functions():
        if candidate.name == function:
            return candidate
    return None


def build_profiling_rule(check: dict[str, Any]) -> ProfilingRuleCandidate | None:
    """Build a :class:`ProfilingRuleCandidate` from one profiler-generated check.

    Returns ``None`` (the check is silently skipped) when the check shape is
    unusable, its function is not a registered DQX check, a column slot has no
    usable column argument, or a SQL argument fails :func:`is_sql_query_safe`.

    Args:
        check: One entry from ``LatestProfile.generated_rules`` — a DQX check
            in metadata form.

    Returns:
        The resolved candidate, or ``None`` when the check can't be mapped
        safely and completely to a registry rule.
    """
    extracted = _extract_check(check)
    if extracted is None:
        return None
    function, arguments = extracted

    cfd = _function_def(function)
    if cfd is None:
        logger.info("Skipping profiler check for unregistered function %r", function.replace("\n", " "))
        return None

    if not _sql_arguments_safe(function, arguments):
        logger.warning("Skipping profiler check %r: SQL argument failed the safety scan", function.replace("\n", " "))
        return None

    slots, parameters = derive_slots_and_parameters(cfd)

    mapping: ColumnMappingGroup = {}
    for slot in slots:
        value = _mapping_value(slot, arguments.get(slot.name))
        if value is None:
            return None
        mapping[slot.name] = value
    if not mapping:
        # A rule with no column slots (e.g. a table-level SQL check) has no
        # slot->column binding to suggest against a monitored table's columns.
        return None

    frozen_parameters = [_freeze_parameter(param, arguments.get(param.name)) for param in parameters]

    body: dict[str, Any] = {
        "function": function,
        "arguments": {slot.name: f"{{{{{slot.name}}}}}" for slot in slots},
    }
    definition = RuleDefinition(body=body, slots=slots, parameters=frozen_parameters)
    metadata = build_builtin_metadata(cfd)
    return ProfilingRuleCandidate(function=function, definition=definition, mapping=mapping, metadata=metadata)


def _freeze_parameter(param: RuleParameter, raw: object) -> RuleParameter:
    """Return a copy of *param* with the profiler's concrete value frozen on.

    A value the profiler didn't supply (``None``/absent) leaves the parameter
    at its unset default, keeping the template identical to the built-in for
    parameter-free checks (so those dedupe onto the seeded built-in rule).
    Values are coerced to the registry's ``RuleParamValue`` union — anything
    outside it is dropped to ``None`` rather than persisted raw.
    """
    value: Any = None
    if isinstance(raw, (str, int, float, bool)):
        value = raw
    elif isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        value = list(raw)
    return RuleParameter(name=param.name, type=param.type, value=value)
