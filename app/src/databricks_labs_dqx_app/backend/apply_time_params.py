"""Apply-time parameter values for parameterized registry rules.

A registry (marketplace) rule stays generic: non-column arguments may be
unbound (``None``) or a ``{{placeholder}}`` copied from YAML. Concrete
values are supplied when the rule is applied to a column, stored per
mapping group on the applied rule, and substituted at materialization so
placeholders never reach ``dq_resolved_rules`` / results.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from databricks_labs_dqx_app.backend.registry_models import (
    ColumnMappingGroup,
    RuleParameter,
    RuleParamValue,
    RuleSlot,
    is_sql_param_placeholder_name,
)

RESERVED_PARAMETER_VALUES_KEY = "parameter_values"

PLACEHOLDER_RE = re.compile(r"^\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$")
ANY_PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")

_POSITIVE_INT_NAMES = frozenset(
    {
        "limit",
        "max_length",
        "min_length",
        "column_max_length",
    }
)
_NONNEG_INT_NAMES = frozenset({"scale", "decimal_scale"})
_RANGE_MIN_NAMES = ("min_limit", "min_value", "min")
_RANGE_MAX_NAMES = ("max_limit", "max_value", "max")

# DQX substitutes this itself; leftover-placeholder scans must ignore it.
_RESERVED_PLACEHOLDERS = frozenset({"input_view"})


class InvalidParameterValueError(ValueError):
    """Raised when an apply-time parameter is missing or the wrong type."""


class UnboundParameterError(ValueError):
    """Raised when a rendered check still contains ``{{placeholder}}`` tokens."""


def is_placeholder_string(value: object) -> bool:
    return isinstance(value, str) and PLACEHOLDER_RE.match(value.strip()) is not None


def is_unbound_parameter_value(value: object) -> bool:
    if value is None:
        return True
    return is_placeholder_string(value)


_MUST_BIND_NAMES = frozenset(
    {
        "limit",
        "regex",
        "pattern",
        "allowed",
        "min_limit",
        "max_limit",
        "min_value",
        "max_value",
        "scale",
        "decimal_scale",
        "column_max_length",
        "max_length",
        "min_length",
        "ref_table",
        "ref_columns",
    }
)


def binds_every_parameter(mode: str | None) -> bool:
    """SQL text references each parameter as a ``{{token}}``, so none may stay unbound."""
    return mode in ("sql", "lowcode")


def must_bind_parameter(param: RuleParameter, *, bind_all: bool = False) -> bool:
    """Whether a concrete apply-time value is required to materialize the check."""
    if bind_all or is_placeholder_string(param.value):
        return True
    if param.type in ("integer", "decimal", "date"):
        return True
    if is_sql_param_placeholder_name(param.name):
        return True
    lowered = param.name.lower()
    if lowered in _MUST_BIND_NAMES:
        return True
    if (
        _looks_like_length_param(param.name)
        or _looks_like_scale_param(param.name)
        or _looks_like_rate_param(param.name)
    ):
        return True
    return False


def mapping_group_key(group: ColumnMappingGroup) -> str:
    """Stable identity for one mapping group, matching the frontend ``mappingGroupKey``."""
    return json.dumps({str(k): str(v) for k, v in group.items()}, sort_keys=True, separators=(",", ":"))


def get_applied_parameter_values(user_metadata: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """``{mappingGroupKey: {paramName: value}}`` stored on an applied rule."""
    raw = (user_metadata or {}).get(RESERVED_PARAMETER_VALUES_KEY)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, vals in raw.items():
        if not isinstance(key, str) or not isinstance(vals, dict):
            continue
        out[key] = dict(vals)
    return out


def parameter_values_for_group(user_metadata: dict[str, Any] | None, group: ColumnMappingGroup) -> dict[str, Any]:
    return dict(get_applied_parameter_values(user_metadata).get(mapping_group_key(group), {}))


def drop_parameter_keys(
    group: ColumnMappingGroup, parameters: list[RuleParameter], slots: list[RuleSlot]
) -> ColumnMappingGroup:
    """Remove mapping entries for names that are parameters, not column slots.

    Applications saved while a value token (e.g. ``allowed_scale``) was still
    a column slot carry a stale column for it.
    """
    stale = {p.name for p in parameters} - {s.name for s in slots}
    return {key: value for key, value in group.items() if key not in stale}


def prune_parameter_mapping(
    column_mapping: list[ColumnMappingGroup],
    parameters: list[RuleParameter],
    slots: list[RuleSlot],
    stored: dict[str, dict[str, Any]],
) -> tuple[list[ColumnMappingGroup], dict[str, dict[str, Any]]]:
    """Drop stale parameter keys from every group, re-keying stored values to the pruned group."""
    pruned_mapping: list[ColumnMappingGroup] = []
    rekeyed = dict(stored)
    for group in column_mapping:
        pruned = drop_parameter_keys(group, parameters, slots)
        pruned_mapping.append(pruned)
        if pruned == group:
            continue
        old_key, new_key = mapping_group_key(group), mapping_group_key(pruned)
        if old_key in rekeyed:
            values = rekeyed.pop(old_key)
            rekeyed.setdefault(new_key, values)
    return pruned_mapping, rekeyed


def overlay_parameters(parameters: list[RuleParameter], apply_values: dict[str, Any] | None) -> list[RuleParameter]:
    """Copy definition parameters with apply-time values taking precedence.

    Unbound definition values (``None`` / ``{{placeholder}}``) are replaced.
    A concrete authored default is kept when the apply map has no entry.
    """
    applied = apply_values or {}
    overlaid: list[RuleParameter] = []
    for param in parameters:
        if param.name in applied:
            overlaid.append(param.model_copy(update={"value": applied[param.name]}))
        elif is_unbound_parameter_value(param.value):
            overlaid.append(param.model_copy(update={"value": None}))
        else:
            overlaid.append(param)
    return overlaid


def _looks_like_length_param(name: str) -> bool:
    lowered = name.lower()
    return lowered in _POSITIVE_INT_NAMES or "max_length" in lowered or "min_length" in lowered


def _looks_like_scale_param(name: str) -> bool:
    return name.lower() in _NONNEG_INT_NAMES or name.lower().endswith("_scale")


def _looks_like_rate_param(name: str) -> bool:
    return "rate" in name.lower()


def _as_number(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise InvalidParameterValueError(f"{name} must be a number.")
    if isinstance(value, str) and not value.strip():
        raise InvalidParameterValueError(f"{name} is required.")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidParameterValueError(f"{name} must be a number.") from exc
    if number != number or number in (float("inf"), float("-inf")):  # noqa: PLR0124
        raise InvalidParameterValueError(f"{name} must be a number.")
    return number


def _as_int(name: str, value: object, *, minimum: int | None) -> int:
    number = _as_number(name, value)
    if number != int(number):
        raise InvalidParameterValueError(f"{name} must be a whole number.")
    integer = int(number)
    if minimum is not None and integer < minimum:
        if minimum <= 0:
            raise InvalidParameterValueError(f"{name} must be zero or a positive whole number.")
        raise InvalidParameterValueError(f"{name} must be a positive whole number.")
    return integer


def coerce_parameter_value(param: RuleParameter, raw: object) -> RuleParamValue:
    """Validate and coerce one apply-time value. Empty / placeholder is rejected."""
    if is_unbound_parameter_value(raw):
        raise InvalidParameterValueError(f"{param.name} is required.")
    if isinstance(raw, str) and not raw.strip() and param.type != "boolean":
        raise InvalidParameterValueError(f"{param.name} is required.")

    if param.type == "boolean":
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str) and raw.strip().lower() in {"true", "false"}:
            return raw.strip().lower() == "true"
        raise InvalidParameterValueError(f"{param.name} must be true or false.")

    if param.type in {"list", "ref_column"}:
        if isinstance(raw, list):
            items = [str(item).strip() for item in raw if str(item).strip()]
        elif isinstance(raw, str):
            items = [part.strip() for part in raw.split(",") if part.strip()]
        else:
            raise InvalidParameterValueError(f"{param.name} must be a list of values.")
        if not items:
            raise InvalidParameterValueError(f"{param.name} is required.")
        if any(is_placeholder_string(item) for item in items):
            raise InvalidParameterValueError(f"{param.name} cannot be a placeholder.")
        return items

    if param.type in ("number", "integer", "decimal"):
        if _looks_like_length_param(param.name):
            return _as_int(param.name, raw, minimum=1)
        if _looks_like_scale_param(param.name):
            return _as_int(param.name, raw, minimum=0)
        if param.type == "integer":
            return _as_int(param.name, raw, minimum=None)
        number = _as_number(param.name, raw)
        if _looks_like_rate_param(param.name) and not 0 <= number <= 100:
            raise InvalidParameterValueError(f"{param.name} must be between 0 and 100.")
        return int(number) if number == int(number) else number

    if param.type == "date":
        text = str(raw).strip()
        try:
            return date.fromisoformat(text[:10]).isoformat()
        except ValueError as exc:
            raise InvalidParameterValueError(f"{param.name} must be a date (YYYY-MM-DD).") from exc

    text = str(raw).strip()
    if not text:
        raise InvalidParameterValueError(f"{param.name} is required.")
    if is_placeholder_string(text):
        raise InvalidParameterValueError(f"{param.name} cannot be a placeholder.")
    return text


def _range_pairs(values: dict[str, Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for min_name, max_name in zip(_RANGE_MIN_NAMES, _RANGE_MAX_NAMES, strict=True):
        if min_name in values and max_name in values:
            pairs.append((min_name, max_name))
    return pairs


def validate_group_parameter_values(
    parameters: list[RuleParameter],
    raw_values: dict[str, Any] | None,
    *,
    require_all: bool,
    bind_all: bool = False,
) -> dict[str, Any]:
    """Validate apply-time values for one mapping group.

    When *require_all* is true (the group is mapped and will materialize),
    every declared parameter must be present and valid. Empty mapping groups
    (staged, not yet mapped) skip this. *bind_all* (SQL rules) makes every
    parameter required, not only the ones :func:`must_bind_parameter` flags.
    """
    if not parameters:
        return {}
    raw_values = raw_values or {}
    if not require_all and not raw_values:
        return {}
    coerced: dict[str, Any] = {}
    for param in parameters:
        if param.name not in raw_values:
            if not is_unbound_parameter_value(param.value):
                coerced[param.name] = coerce_parameter_value(param, param.value)
                continue
            if require_all and must_bind_parameter(param, bind_all=bind_all):
                raise InvalidParameterValueError(f"{param.name} is required.")
            continue
        coerced[param.name] = coerce_parameter_value(param, raw_values[param.name])
    for min_name, max_name in _range_pairs(coerced):
        try:
            minimum = float(coerced[min_name])
            maximum = float(coerced[max_name])
        except (TypeError, ValueError):
            continue
        if minimum > maximum:
            raise InvalidParameterValueError(f"{min_name} ({minimum}) must not be greater than {max_name} ({maximum}).")
    return coerced


def validate_applied_parameter_values(
    parameters: list[RuleParameter],
    column_mapping: list[ColumnMappingGroup],
    stored: dict[str, dict[str, Any]] | None,
    *,
    bind_all: bool = False,
) -> dict[str, dict[str, Any]]:
    """Validate every mapped group's apply-time parameters. Empty mapping is allowed."""
    stored = stored or {}
    if not parameters:
        return {}
    validated: dict[str, dict[str, Any]] = {}
    for group in column_mapping:
        if not group:
            continue
        key = mapping_group_key(group)
        validated[key] = validate_group_parameter_values(
            parameters, stored.get(key), require_all=True, bind_all=bind_all
        )
    return validated


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_parameter_declarations(parameters: list[RuleParameter], slots: list[RuleSlot]) -> None:
    """Reject parameter names that are not ``{{token}}``-safe, repeat, or collide with a column slot."""
    slot_names = {slot.name for slot in slots}
    seen: set[str] = set()
    for param in parameters:
        if not _IDENTIFIER_RE.match(param.name):
            raise ValueError(
                f"Parameter name '{param.name}' must start with a letter or underscore "
                "and contain only letters, digits and underscores."
            )
        if param.name in _RESERVED_PLACEHOLDERS:
            raise ValueError(f"'{param.name}' is reserved and cannot be used as a parameter name.")
        if param.name in seen:
            raise ValueError(f"Parameter '{param.name}' is declared more than once.")
        if param.name in slot_names:
            raise ValueError(f"'{param.name}' is declared as both a column and a parameter.")
        seen.add(param.name)


def _escape_sql_string(text: str) -> str:
    return text.replace("\\", "\\\\").replace("'", "\\'")


def _sql_number(param: RuleParameter, value: object) -> str:
    number = _as_number(param.name, value)
    return str(int(number)) if number == int(number) else repr(number)


def sql_literal(param: RuleParameter, value: object, *, quoted: bool) -> str:
    """Render *value* for a ``{{token}}`` in SQL text.

    *quoted* means the author already wrapped the token in single quotes
    (``'{{name}}'``), so only the escaped content is returned.
    """
    if param.type == "boolean" or isinstance(value, bool):
        flag = value if isinstance(value, bool) else str(value).strip().lower() == "true"
        return ("true" if flag else "false") if quoted else ("TRUE" if flag else "FALSE")
    if param.type in ("number", "integer", "decimal"):
        return _sql_number(param, value)
    if isinstance(value, list):
        items = [_escape_sql_string(str(item)) for item in value]
        return ", ".join(items) if quoted else ", ".join(f"'{item}'" for item in items)
    text = _escape_sql_string(str(value))
    if quoted:
        return text
    if param.type == "date":
        return f"DATE'{text}'"
    return f"'{text}'"


def substitute_sql_parameters(text: str, parameters: list[RuleParameter]) -> str:
    """Replace bound ``{{param}}`` tokens in SQL text with literals formatted by parameter type."""
    for param in parameters:
        if is_unbound_parameter_value(param.value):
            continue
        token_re = re.compile(r"(')?\{\{\s*" + re.escape(param.name) + r"\s*\}\}(')?")

        def render(match: re.Match[str], param: RuleParameter = param) -> str:
            before, after = match.group(1) or "", match.group(2) or ""
            if before and after:
                return f"'{sql_literal(param, param.value, quoted=True)}'"
            return before + sql_literal(param, param.value, quoted=False) + after

        text = token_re.sub(render, text)
    return text


def leftover_placeholders(value: Any, *, ignore: frozenset[str] = _RESERVED_PLACEHOLDERS) -> list[str]:
    """Return leftover ``{{token}}`` names in a rendered argument tree."""
    found: list[str] = []
    seen: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for item in node.values():
                walk(item)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, str):
            return
        for match in ANY_PLACEHOLDER_RE.finditer(node):
            name = match.group(1)
            if name in ignore or name in seen:
                continue
            seen.add(name)
            found.append(name)

    walk(value)
    return found


def assert_no_placeholders(value: Any) -> None:
    leftover = leftover_placeholders(value)
    if leftover:
        raise UnboundParameterError(
            "Unresolved parameter placeholder(s): "
            + ", ".join(f"{{{{{name}}}}}" for name in leftover)
            + ". Enter a value for each parameter before publishing."
        )


def substitute_param_placeholders(value: Any, values: dict[str, Any]) -> Any:
    """Replace ``{{param}}`` tokens inside strings using *values* (JSON-scalar)."""
    if isinstance(value, list):
        return [substitute_param_placeholders(item, values) for item in value]
    if isinstance(value, dict):
        return {key: substitute_param_placeholders(item, values) for key, item in value.items()}
    if not isinstance(value, str):
        return value
    match = PLACEHOLDER_RE.match(value.strip())
    if match and match.group(1) in values:
        return values[match.group(1)]
    result = value
    for name, replacement in values.items():
        token = "{{" + name + "}}"
        if token not in result:
            continue
        result = result.replace(token, str(replacement))
    return result
