"""Compile a ``dqx_native`` registry rule into a row-level SQL test predicate.

The Rules Registry "Test" tab evaluates a SQL boolean expression per row on
the configured SQL warehouse. Native rules materialize as DQX check functions
(PySpark), but every *row*-level check whose semantics map to a single-row
SQL expression can be tested by compiling that expression here — with
``{{slot}}`` placeholders preserved for :func:`rule_test_sql.substitute_slots`.

Dataset-level checks (``is_unique``, ``foreign_key``, aggregates, …) and row
checks that rely on UDFs / geospatial builtins are rejected with
:class:`NativeTestNotSupportedError`.
"""

from __future__ import annotations

import re
from typing import Any

import databricks.labs.dqx.check_funcs  # noqa: F401 — populate CHECK_FUNC_REGISTRY
import databricks.labs.dqx.geo.check_funcs  # noqa: F401
from databricks.labs.dqx.check_funcs import DQPattern
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

_SLOT_RE = re.compile(r"^\{\{\s*(.+?)\s*\}\}$")

# Row checks that cannot be faithfully row-tested via warehouse SQL today.
_ROW_UNSUPPORTED: frozenset[str] = frozenset(
    {
        # UDF / pandas-backed
        "is_valid_ipv6_address",
        "is_ipv6_address_in_cidr",
        # Bitwise CIDR membership — no faithful single-row SQL without UDFs
        "is_ipv4_address_in_cidr",
        # JSON schema / key checks need richer JSON parsing than a predicate grid
        "has_valid_json_schema",
        "has_json_keys",
    }
)

# Every geo-registered check (side-effect import above).
_GEO_UNSUPPORTED: frozenset[str] = frozenset(
    name for name in CHECK_FUNC_REGISTRY if name.startswith(("is_geo", "is_geom", "is_point", "is_line", "is_polygon", "is_multi", "is_area", "is_num_points", "is_latitude", "is_longitude", "is_ogc", "is_non_empty", "is_not_null_island", "has_dimension", "has_x_coordinate", "has_y_coordinate", "are_polygons"))
)


class NativeTestNotSupportedError(ValueError):
    """Raised when a native check cannot be compiled for the Test tab."""


class NativeTestCompileError(ValueError):
    """Raised when native arguments are incomplete or malformed for compilation."""


def is_native_rule_testable(function: str) -> bool:
    """Return whether *function* can be exercised on the Test tab."""
    rule_type = CHECK_FUNC_REGISTRY.get(function)
    if rule_type != "row":
        return False
    if function in _ROW_UNSUPPORTED or function in _GEO_UNSUPPORTED:
        return False
    return function in _COMPILERS


def compile_native_test_predicate(function: str, arguments: dict[str, Any]) -> str:
    """Compile *function* + frozen ``arguments`` into a SQL pass predicate.

    The returned expression is TRUE when a row satisfies the rule under
    ``pass`` polarity (``passed_expr`` in :mod:`rule_test_sql` handles
  ``fail`` polarity). ``{{slot}}`` placeholders are kept verbatim.

    Raises:
        NativeTestNotSupportedError: check is dataset-level or unsupported.
        NativeTestCompileError: required arguments are missing.
    """
    if CHECK_FUNC_REGISTRY.get(function) != "row":
        raise NativeTestNotSupportedError(f"Rule tests aren't available for the '{function}' check.")
    if function in _ROW_UNSUPPORTED or function in _GEO_UNSUPPORTED:
        raise NativeTestNotSupportedError(f"Rule tests aren't available for the '{function}' check yet.")
    compiler = _COMPILERS.get(function)
    if compiler is None:
        raise NativeTestNotSupportedError(f"Rule tests aren't available for the '{function}' check yet.")
    # Polarity is applied by passed_expr — never bake negate into the predicate.
    args = {k: v for k, v in arguments.items() if k != "negate"}
    return compiler(args)


def _slot(value: Any, *, param: str) -> str:
    if not isinstance(value, str):
        raise NativeTestCompileError(f"Expected a column slot for '{param}'.")
    text = value.strip()
    if not _SLOT_RE.match(text):
        raise NativeTestCompileError(f"Expected a {{{{slot}}}} placeholder for '{param}'.")
    return text


def _sql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _sql_scalar(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return _sql_string(value)
    raise NativeTestCompileError(f"Unsupported scalar argument value: {value!r}")


def _sql_in_list(values: list[Any], *, case_sensitive: bool, col: str) -> str:
    if not values:
        raise NativeTestCompileError("List argument must not be empty.")
    items = ", ".join(_sql_scalar(v) for v in values)
    if case_sensitive:
        return f"({col} IN ({items}))"
    return f"(LOWER(CAST({col} AS STRING)) IN ({', '.join(_sql_scalar(str(v).lower()) for v in values)}))"


def _string_col(col: str, *, trim: bool = False) -> str:
    inner = f"CAST({col} AS STRING)"
    return f"TRIM({inner})" if trim else inner


def _col_arg(arguments: dict[str, Any], key: str = "column") -> str:
    return _slot(arguments.get(key), param=key)


def _compile_is_not_null(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    return f"({col} IS NOT NULL)"


def _compile_is_null(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    return f"({col} IS NULL)"


def _compile_is_empty(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    trim = bool(args.get("trim_strings") or False)
    return f"({_string_col(col, trim=trim)} = '')"


def _compile_is_not_empty(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    trim = bool(args.get("trim_strings") or False)
    return f"({_string_col(col, trim=trim)} <> '')"


def _compile_is_null_or_empty(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    trim = bool(args.get("trim_strings") or False)
    s = _string_col(col, trim=trim)
    return f"({col} IS NULL OR {s} = '')"


def _compile_is_not_null_and_not_empty(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    trim = bool(args.get("trim_strings") or False)
    s = _string_col(col, trim=trim)
    return f"({col} IS NOT NULL AND {s} <> '')"


def _compile_is_not_null_and_not_empty_array(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    return f"({col} IS NOT NULL AND SIZE({col}) > 0)"


def _compile_is_in_range(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    min_limit = args.get("min_limit")
    max_limit = args.get("max_limit")
    parts: list[str] = []
    if min_limit is not None:
        parts.append(f"{col} >= {_sql_scalar(min_limit)}")
    if max_limit is not None:
        parts.append(f"{col} <= {_sql_scalar(max_limit)}")
    if not parts:
        raise NativeTestCompileError("is_in_range requires min_limit and/or max_limit.")
    return "(" + " AND ".join(parts) + ")"


def _compile_is_not_in_range(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    min_limit = args.get("min_limit")
    max_limit = args.get("max_limit")
    parts: list[str] = []
    if min_limit is not None:
        parts.append(f"{col} < {_sql_scalar(min_limit)}")
    if max_limit is not None:
        parts.append(f"{col} > {_sql_scalar(max_limit)}")
    if not parts:
        raise NativeTestCompileError("is_not_in_range requires min_limit and/or max_limit.")
    return "(" + " OR ".join(parts) + ")"


def _compile_is_not_less_than(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    limit = args.get("limit")
    if limit is None:
        raise NativeTestCompileError("is_not_less_than requires limit.")
    return f"({col} >= {_sql_scalar(limit)})"


def _compile_is_not_greater_than(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    limit = args.get("limit")
    if limit is None:
        raise NativeTestCompileError("is_not_greater_than requires limit.")
    return f"({col} <= {_sql_scalar(limit)})"


def _tolerance_pass(col: str, value: Any, abs_tol: Any, rel_tol: Any) -> str:
    abs_tol = 0.0 if abs_tol is None else float(abs_tol)
    rel_tol = 0.0 if rel_tol is None else float(rel_tol)
    val_sql = _sql_scalar(value)
    if abs_tol > 0 or rel_tol > 0:
        abs_part = f"ABS({col} - {val_sql}) <= {abs_tol}"
        rel_part = f"ABS({col} - {val_sql}) <= {rel_tol} * GREATEST(ABS({col}), ABS({val_sql}))"
        if abs_tol > 0 and rel_tol > 0:
            return f"(({abs_part}) OR ({rel_part}))"
        return f"({abs_part if abs_tol > 0 else rel_part})"
    return f"({col} = {val_sql})"


def _compile_is_equal_to(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    value = args.get("value")
    if value is None:
        raise NativeTestCompileError("is_equal_to requires value.")
    return _tolerance_pass(col, value, args.get("abs_tolerance"), args.get("rel_tolerance"))


def _compile_is_not_equal_to(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    value = args.get("value")
    if value is None:
        raise NativeTestCompileError("is_not_equal_to requires value.")
    inner = _tolerance_pass(col, value, args.get("abs_tolerance"), args.get("rel_tolerance"))
    return f"(NOT {inner})"


def _compile_is_in_list(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    allowed = args.get("allowed")
    if not isinstance(allowed, list) or not allowed:
        raise NativeTestCompileError("is_in_list requires a non-empty allowed list.")
    return _sql_in_list(allowed, case_sensitive=bool(args.get("case_sensitive", True)), col=col)


def _compile_is_not_in_list(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    forbidden = args.get("forbidden")
    if not isinstance(forbidden, list) or not forbidden:
        raise NativeTestCompileError("is_not_in_list requires a non-empty forbidden list.")
    inner = _sql_in_list(forbidden, case_sensitive=bool(args.get("case_sensitive", True)), col=col)
    return f"(NOT {inner})"


def _compile_is_not_null_and_is_in_list(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    allowed = args.get("allowed")
    if not isinstance(allowed, list) or not allowed:
        raise NativeTestCompileError("is_not_null_and_is_in_list requires a non-empty allowed list.")
    in_list = _sql_in_list(allowed, case_sensitive=bool(args.get("case_sensitive", True)), col=col)
    return f"({col} IS NOT NULL AND {in_list})"


def _compile_regex_match(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    regex = args.get("regex")
    if not isinstance(regex, str) or not regex:
        raise NativeTestCompileError("regex_match requires regex.")
    return f"({col} RLIKE {_sql_string(regex)})"


def _compile_is_valid_ipv4_address(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    return f"({col} RLIKE {_sql_string(DQPattern.IPV4_ADDRESS.value)})"


def _compile_is_valid_email(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    return f"({col} RLIKE {_sql_string(DQPattern.EMAIL_ADDRESS.value)})"


def _compile_is_valid_date(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    fmt = args.get("date_format")
    if fmt:
        parsed = f"TRY_TO_TIMESTAMP({col}, {_sql_string(str(fmt))})"
    else:
        parsed = f"TRY_TO_TIMESTAMP({col})"
    return f"({col} IS NULL OR {parsed} IS NOT NULL)"


def _compile_is_valid_timestamp(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    fmt = args.get("timestamp_format")
    if fmt:
        parsed = f"TRY_TO_TIMESTAMP({col}, {_sql_string(str(fmt))})"
    else:
        parsed = f"TRY_TO_TIMESTAMP({col})"
    return f"({col} IS NULL OR {parsed} IS NOT NULL)"


def _compile_is_valid_json(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    return f"({col} IS NULL OR TRY_PARSE_JSON(CAST({col} AS STRING)) IS NOT NULL)"


def _compile_is_data_fresh(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    max_age = args.get("max_age_minutes")
    if max_age is None:
        raise NativeTestCompileError("is_data_fresh requires max_age_minutes.")
    base = args.get("base_timestamp")
    if base is None:
        base_expr = "CURRENT_TIMESTAMP()"
    elif isinstance(base, str) and _SLOT_RE.match(base.strip()):
        base_expr = base.strip()
    else:
        base_expr = _sql_scalar(base)
    return f"({col} >= ({base_expr} - INTERVAL {int(max_age)} MINUTES))"


def _compile_is_not_in_future(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    offset = int(args.get("offset") or 0)
    return f"({col} <= FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) + {offset}))"


def _compile_is_not_in_near_future(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    offset = int(args.get("offset") or 0)
    return (
        f"({col} <= CURRENT_TIMESTAMP() OR "
        f"{col} >= FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) + {offset}))"
    )


def _compile_is_older_than_n_days(args: dict[str, Any]) -> str:
    col = _col_arg(args)
    days = args.get("days")
    if days is None:
        raise NativeTestCompileError("is_older_than_n_days requires days.")
    return f"(TO_DATE({col}) < DATE_SUB(CURRENT_DATE(), {int(days)}))"


def _compile_is_older_than_col2_for_n_days(args: dict[str, Any]) -> str:
    col1 = _slot(args.get("column1"), param="column1")
    col2 = _slot(args.get("column2"), param="column2")
    days = args.get("days")
    if days is None:
        raise NativeTestCompileError("is_older_than_col2_for_n_days requires days.")
    return f"(TO_DATE({col1}) < DATE_SUB(TO_DATE({col2}), {int(days)}))"


def _compile_sql_expression(args: dict[str, Any]) -> str:
    expression = args.get("expression")
    if not isinstance(expression, str) or not expression.strip():
        raise NativeTestCompileError("sql_expression requires expression.")
    return f"({expression.strip()})"


_COMPILERS: dict[str, Any] = {
    "is_not_null": _compile_is_not_null,
    "is_null": _compile_is_null,
    "is_empty": _compile_is_empty,
    "is_not_empty": _compile_is_not_empty,
    "is_null_or_empty": _compile_is_null_or_empty,
    "is_not_null_and_not_empty": _compile_is_not_null_and_not_empty,
    "is_not_null_and_not_empty_array": _compile_is_not_null_and_not_empty_array,
    "is_in_range": _compile_is_in_range,
    "is_not_in_range": _compile_is_not_in_range,
    "is_not_less_than": _compile_is_not_less_than,
    "is_not_greater_than": _compile_is_not_greater_than,
    "is_equal_to": _compile_is_equal_to,
    "is_not_equal_to": _compile_is_not_equal_to,
    "is_in_list": _compile_is_in_list,
    "is_not_in_list": _compile_is_not_in_list,
    "is_not_null_and_is_in_list": _compile_is_not_null_and_is_in_list,
    "regex_match": _compile_regex_match,
    "is_valid_ipv4_address": _compile_is_valid_ipv4_address,
    "is_valid_email": _compile_is_valid_email,
    "is_valid_date": _compile_is_valid_date,
    "is_valid_timestamp": _compile_is_valid_timestamp,
    "is_valid_json": _compile_is_valid_json,
    "is_data_fresh": _compile_is_data_fresh,
    "is_not_in_future": _compile_is_not_in_future,
    "is_not_in_near_future": _compile_is_not_in_near_future,
    "is_older_than_n_days": _compile_is_older_than_n_days,
    "is_older_than_col2_for_n_days": _compile_is_older_than_col2_for_n_days,
    "sql_expression": _compile_sql_expression,
}
