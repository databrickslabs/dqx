import datetime
import re
from decimal import Decimal
from typing import Any


def val_to_str(value: Any, include_sql_quotes: bool = True):
    """
    Converts a value to a string.

    Args:
        value: The value to convert. Can be a datetime, date, int, float, or other type.
        include_sql_quotes: Whether to include quotes around the value. Default is True.

    Returns:
        The string representation of the value
    """
    quote = "'" if include_sql_quotes else ""
    if isinstance(value, datetime.datetime):
        return f"{quote}{value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}{quote}"
    if isinstance(value, datetime.date):
        return f"{quote}{value.isoformat()}{quote}"

    if isinstance(value, (int, float, Decimal)):
        return str(value)

    escaped_value = re.sub(r"(['\\])", r"\\\1", str(value))
    return f"{quote}{escaped_value}{quote}"


def val_maybe_to_str(value: Any, include_sql_quotes: bool = True):
    """
    Converts a value to a string if it is a datetime or date.

    Args:
        value: The value to convert. Can be a datetime, date, or other type.
        include_sql_quotes: Whether to include quotes around the value. Default is True.

    Returns:
        The string representation of the value.
    """
    quote = "'" if include_sql_quotes else ""
    if isinstance(value, datetime.datetime):
        return f"{quote}{value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}{quote}"
    if isinstance(value, datetime.date):
        return f"{quote}{value.isoformat()}{quote}"

    return value
