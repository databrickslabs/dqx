"""Function-to-SQL mapping for converting DQX check functions to SDP Expectations SQL expressions."""

import logging
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from databricks.labs.dqx.profiler.common import val_to_str

logger = logging.getLogger(__name__)

# Registry of function mappers
_FUNCTION_MAPPERS: dict[str, Callable[..., str]] = {}


def register_mapper(func_name: str):
    """Decorator to register a function mapper."""

    def decorator(mapper_func: Callable[..., str]):
        _FUNCTION_MAPPERS[func_name] = mapper_func
        return mapper_func

    return decorator


def get_mapper(func_name: str) -> Callable[..., str] | None:
    """Get the mapper function for a given check function name.

    Args:
        func_name: Name of the check function.

    Returns:
        The mapper function if available, None otherwise.
    """
    return _FUNCTION_MAPPERS.get(func_name)


def map_to_sql(func_name: str, column: str, **kwargs: Any) -> str | None:
    """Map a check function to SQL expression.

    Args:
        func_name: Name of the check function.
        column: Column name or expression.
        **kwargs: Additional arguments for the check function.

    Returns:
        SQL expression string if mapping is available, None otherwise.
    """
    mapper = get_mapper(func_name)
    if mapper is None:
        return None
    try:
        return mapper(column, **kwargs)
    except Exception as e:
        logger.warning(f"Failed to map {func_name} to SQL: {e}")
        return None


@register_mapper("is_not_null")
def _map_is_not_null(column: str, **kwargs: Any) -> str:
    """Map is_not_null check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments (unused for this function).

    Returns:
        SQL expression: column IS NOT NULL
    """
    return f"{column} IS NOT NULL"


@register_mapper("is_not_empty")
def _map_is_not_empty(column: str, **kwargs: Any) -> str:
    """Map is_not_empty check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments (unused for this function).

    Returns:
        SQL expression: column IS NOT NULL AND column <> ''
    """
    return f"{column} IS NOT NULL AND {column} <> ''"


@register_mapper("is_not_null_and_not_empty")
def _map_is_not_null_and_not_empty(column: str, **kwargs: Any) -> str:
    """Map is_not_null_and_not_empty check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including trim_strings.

    Returns:
        SQL expression with optional trim handling.
    """
    trim_strings = kwargs.get("trim_strings", False)
    if trim_strings:
        return f"{column} IS NOT NULL AND trim({column}) <> ''"
    return f"{column} IS NOT NULL AND {column} <> ''"


@register_mapper("is_in_list")
def _map_is_in_list(column: str, **kwargs: Any) -> str:
    """Map is_in_list check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including allowed list and case_sensitive.

    Returns:
        SQL expression: column IN (value1, value2, ...)
    """
    allowed = kwargs.get("allowed")
    if not allowed:
        raise ValueError("allowed list is required for is_in_list")
    if not isinstance(allowed, list):
        raise ValueError(f"allowed must be a list, got {type(allowed)}")

    case_sensitive = kwargs.get("case_sensitive", True)

    # Always quote values in IN lists for consistency (even numeric values)
    def format_in_value(v: Any) -> str:
        if isinstance(v, (int, float, Decimal)):
            return f"'{v}'"
        return val_to_str(v, include_sql_quotes=True)

    values_str = ", ".join([format_in_value(v) for v in allowed])

    if not case_sensitive:
        # For case-insensitive, we need to lowercase both column and values
        def format_lower_value(v: Any) -> str:
            if isinstance(v, str):
                return val_to_str(v, include_sql_quotes=True).lower()
            elif isinstance(v, (int, float, Decimal)):
                return f"'{v}'"
            return val_to_str(v, include_sql_quotes=True)

        lower_values = ", ".join([format_lower_value(v) for v in allowed])
        return f"lower({column}) IN ({lower_values})"
    return f"{column} IN ({values_str})"


@register_mapper("is_not_in_list")
def _map_is_not_in_list(column: str, **kwargs: Any) -> str:
    """Map is_not_in_list check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including forbidden list and case_sensitive.

    Returns:
        SQL expression: column NOT IN (value1, value2, ...)
    """
    forbidden = kwargs.get("forbidden")
    if not forbidden:
        raise ValueError("forbidden list is required for is_not_in_list")
    if not isinstance(forbidden, list):
        raise ValueError(f"forbidden must be a list, got {type(forbidden)}")

    case_sensitive = kwargs.get("case_sensitive", True)

    # Always quote values in IN lists for consistency (even numeric values)
    def format_in_value(v: Any) -> str:
        if isinstance(v, (int, float, Decimal)):
            return f"'{v}'"
        return val_to_str(v, include_sql_quotes=True)

    values_str = ", ".join([format_in_value(v) for v in forbidden])

    if not case_sensitive:
        # For case-insensitive, we need to lowercase both column and values
        def format_lower_value(v: Any) -> str:
            if isinstance(v, str):
                return val_to_str(v, include_sql_quotes=True).lower()
            elif isinstance(v, (int, float, Decimal)):
                return f"'{v}'"
            return val_to_str(v, include_sql_quotes=True)

        lower_values = ", ".join([format_lower_value(v) for v in forbidden])
        return f"lower({column}) NOT IN ({lower_values})"
    return f"{column} NOT IN ({values_str})"


def _format_limit(limit: Any) -> str:
    """Format a limit value for SQL, handling column references and expressions.

    Args:
        limit: The limit value (can be literal, column name, or SQL expression).

    Returns:
        Formatted string for use in SQL.
    """
    if isinstance(limit, str):
        # If it's a string, it might be a column reference or SQL expression
        if any(char in limit for char in [" ", "(", ")", "+", "-", "*", "/"]):
            # Looks like an expression, use as-is
            return limit
        # Might be a column name, use as-is
        return limit
    # For literals, use val_to_str
    return val_to_str(limit)


@register_mapper("is_in_range")
def _map_is_in_range(column: str, **kwargs: Any) -> str:
    """Map is_in_range check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including min_limit and max_limit.

    Returns:
        SQL expression: column BETWEEN min AND max or column >= min AND column <= max
    """
    min_limit = kwargs.get("min_limit")
    max_limit = kwargs.get("max_limit")

    if min_limit is not None and max_limit is not None:
        min_str = _format_limit(min_limit)
        max_str = _format_limit(max_limit)
        return f"{column} >= {min_str} AND {column} <= {max_str}"
    elif min_limit is not None:
        min_str = _format_limit(min_limit)
        return f"{column} >= {min_str}"
    elif max_limit is not None:
        max_str = _format_limit(max_limit)
        return f"{column} <= {max_str}"
    else:
        raise ValueError("At least one of min_limit or max_limit must be provided for is_in_range")


@register_mapper("is_not_in_range")
def _map_is_not_in_range(column: str, **kwargs: Any) -> str:
    """Map is_not_in_range check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including min_limit and max_limit.

    Returns:
        SQL expression: column NOT BETWEEN min AND max or column < min OR column > max
    """
    min_limit = kwargs.get("min_limit")
    max_limit = kwargs.get("max_limit")

    if min_limit is not None and max_limit is not None:
        min_str = _format_limit(min_limit)
        max_str = _format_limit(max_limit)
        return f"{column} < {min_str} OR {column} > {max_str}"
    elif min_limit is not None:
        min_str = _format_limit(min_limit)
        return f"{column} < {min_str}"
    elif max_limit is not None:
        max_str = _format_limit(max_limit)
        return f"{column} > {max_str}"
    else:
        raise ValueError("At least one of min_limit or max_limit must be provided for is_not_in_range")


@register_mapper("is_equal_to")
def _map_is_equal_to(column: str, **kwargs: Any) -> str:
    """Map is_equal_to check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including value.

    Returns:
        SQL expression: column = value
    """
    value = kwargs.get("value")
    if value is None:
        raise ValueError("value is required for is_equal_to")

    # Handle string values - might be column references or expressions
    if isinstance(value, str):
        if any(char in value for char in [" ", "(", ")", "+", "-", "*", "/"]):
            # Looks like an expression, use as-is
            value_str = value
        else:
            # Might be a column name or literal string
            # Check if it looks like a column name (no quotes)
            if value.startswith("'") and value.endswith("'"):
                # Already quoted string literal
                value_str = value
            else:
                # Treat as column reference
                value_str = value
    else:
        value_str = val_to_str(value)
    return f"{column} = {value_str}"


@register_mapper("is_not_equal_to")
def _map_is_not_equal_to(column: str, **kwargs: Any) -> str:
    """Map is_not_equal_to check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including value.

    Returns:
        SQL expression: column <> value
    """
    value = kwargs.get("value")
    if value is None:
        raise ValueError("value is required for is_not_equal_to")

    # Handle string values - might be column references or expressions
    if isinstance(value, str):
        if any(char in value for char in [" ", "(", ")", "+", "-", "*", "/"]):
            # Looks like an expression, use as-is
            value_str = value
        else:
            # Might be a column name or literal string
            if value.startswith("'") and value.endswith("'"):
                # Already quoted string literal
                value_str = value
            else:
                # Treat as column reference
                value_str = value
    else:
        value_str = val_to_str(value)
    return f"{column} <> {value_str}"


@register_mapper("is_not_less_than")
def _map_is_not_less_than(column: str, **kwargs: Any) -> str:
    """Map is_not_less_than check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including limit.

    Returns:
        SQL expression: column >= limit
    """
    limit = kwargs.get("limit")
    if limit is None:
        raise ValueError("limit is required for is_not_less_than")

    # Handle string values - might be column references or expressions
    if isinstance(limit, str):
        if any(char in limit for char in [" ", "(", ")", "+", "-", "*", "/"]):
            # Looks like an expression, use as-is
            limit_str = limit
        else:
            # Might be a column name, use as-is
            limit_str = limit
    else:
        limit_str = val_to_str(limit)
    return f"{column} >= {limit_str}"


@register_mapper("is_not_greater_than")
def _map_is_not_greater_than(column: str, **kwargs: Any) -> str:
    """Map is_not_greater_than check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including limit.

    Returns:
        SQL expression: column <= limit
    """
    limit = kwargs.get("limit")
    if limit is None:
        raise ValueError("limit is required for is_not_greater_than")

    # Handle string values - might be column references or expressions
    if isinstance(limit, str):
        if any(char in limit for char in [" ", "(", ")", "+", "-", "*", "/"]):
            # Looks like an expression, use as-is
            limit_str = limit
        else:
            # Might be a column name, use as-is
            limit_str = limit
    else:
        limit_str = val_to_str(limit)
    return f"{column} <= {limit_str}"


@register_mapper("regex_match")
def _map_regex_match(column: str, **kwargs: Any) -> str:
    """Map regex_match check to SQL.

    Args:
        column: Column name or expression.
        **kwargs: Additional arguments including regex and negate.

    Returns:
        SQL expression: column RLIKE pattern or NOT (column RLIKE pattern)
    """
    regex = kwargs.get("regex")
    if not regex or not isinstance(regex, str):
        raise ValueError("regex is required for regex_match and must be a string")

    negate = kwargs.get("negate", False)
    # Escape single quotes in regex pattern
    regex_escaped = regex.replace("'", "''")
    if negate:
        return f"NOT ({column} RLIKE '{regex_escaped}')"
    return f"{column} RLIKE '{regex_escaped}'"


@register_mapper("sql_expression")
def _map_sql_expression(column: str, **kwargs: Any) -> str:
    """Map sql_expression check to SQL.

    Args:
        column: Column name or expression (may be unused if expression is provided).
        **kwargs: Additional arguments including expression and negate.

    Returns:
        SQL expression extracted directly from the expression parameter.
    """
    expression = kwargs.get("expression")
    if not expression or not isinstance(expression, str):
        raise ValueError("expression is required for sql_expression and must be a string")

    negate = kwargs.get("negate", False)
    if negate:
        return f"NOT ({expression})"
    return expression
