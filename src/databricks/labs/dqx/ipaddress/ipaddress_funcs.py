import logging
import ipaddress
import pandas as pd
import warnings
from collections.abc import Callable
from pyspark.sql import types
from pyspark.sql import Column
import pyspark.sql.functions as F

from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, _get_normalized_column_and_expr

logging.getLogger("ipaddress").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


@register_rule("row")
def is_valid_ipv6_address(column: str | Column) -> pd.Series:
    """
    Checks whether a column contains properly formatted IPv6 addresses.

    This rule checks for four accepted IPv6 formats:
      - Fully uncompressed (e.g. '2001:0db8:0000:0000:0000:0000:0000:0001')
      - Compressed (e.g. '2001:db8::1')
      - Loopback ('::1')
      - Unspecified ('::')

    A value fails the check if it does not match **any** of these valid IPv6 patterns.

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    warnings.warn(
        "IP Address validation uses pandas user-defined functions which may degrade performance. "
        "Sample or limit large datasets when running IPV6 address validation.",
    )
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)

    is_valid_ipv6_address_with_ipaddress_udf = _build_is_valid_ipv6_address_with_ipaddress_udf()
    ipv6_match_condition = is_valid_ipv6_address_with_ipaddress_udf(col_expr)
    final_condition = F.when(col_expr.isNotNull(), ipv6_match_condition).otherwise(F.lit(None))
    condition_str = f"' in Column '{col_expr_str}' does not match pattern 'IPV6_ADDRESS'"

    return make_condition(
        final_condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_does_not_match_pattern_ipv6_address",
    )


@register_rule("row")
def is_ipv6_address_in_cidr(column: str | Column, cidr_block: str) -> Column:
    """
    Checks if an IPv6 column value falls within the given CIDR block with ipaddress module.
    Uses _is_valid_ipv6_address_with_ipaddress

    Args:
        column: column to check; can be a string column name or a column expression
        cidr_block: CIDR block string (e.g., '2001:db8::/32')

    Raises:
        ValueError: If cidr_block is not a valid string in CIDR notation.

    Returns:
        Column object for condition
    """
    warnings.warn(
        "Checking if an IPv6 Address is in CIDR block uses pandas user-defined functions which may degrade performance. "
        "Sample or limit large datasets when running IPV6 address validation.",
    )

    if not cidr_block:
        raise ValueError("'cidr_block' must be a non-empty string.")

    if not _is_ipv6_check(cidr_block):
        raise ValueError(f"CIDR block '{cidr_block}' is not a valid IPv6 CIDR block.")

    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    cidr_col_expr = F.lit(cidr_block)
    ipv6_msg_col = is_valid_ipv6_address_with_ipaddress(column)
    is_ipv6_address_in_cidr_udf = _build_is_ipv6_address_in_cidr_udf()
    ip_in_cidr_col = is_ipv6_address_in_cidr_udf(col_expr, cidr_col_expr)
    cidr_msg = F.concat_ws(
        "",
        F.lit("Value '"),
        col_expr.cast("string"),
        F.lit(f"' in Column '{col_expr_str}' is not in the CIDR block '{cidr_block}'"),
    )
    return make_condition(
        condition=ipv6_msg_col.isNotNull() | (~ip_in_cidr_col),
        message=F.when(ipv6_msg_col.isNotNull(), ipv6_msg_col).otherwise(cidr_msg),
        alias=f"{col_str_norm}_is_not_ipv6_in_cidr",
    )


def _is_ipv6_check(ip: str) -> bool:
    """
    Check if a string is a valid IPv6 address.

    Args
    ----------
    ip : str
        The string to check.
    Returns
    -------
    bool
        True if the string is a valid IPv6 address, False otherwise.
    """
    try:
        ipaddress.IPv6Address(ip)
        return True
    except AttributeError:
        return False


def _build_is_valid_ipv6_address_with_ipaddress_udf() -> Callable:
    """
    Build a user-defined function (UDF) to check if a string is a valid IPv6 address.

    Returns:
        Callable: A UDF that checks if a string is a valid IPv6 address
    """
    @F.pandas_udf(types.BooleanType())  # type: ignore[call-overload]
    def _is_valid_ipv6_address_with_ipaddress_udf(column: pd.Series) -> pd.Series:
        return column.apply(_is_ipv6_check)

    return _is_valid_ipv6_address_with_ipaddress_udf


def _ipv6_in_cidr(ip: str, cidr: str) -> bool:
    """
    Check if an IPv6 address is in a given CIDR block.

    Args:
    ip : The IPv6 address to check.
    cidr : str
        The CIDR block to check against.

    Returns
    -------
    bool
        True if the IP address is in the CIDR block, False otherwise.
    """
    try:
        ip_obj = ipaddress.IPv6Address(ip)
    except ValueError:
        return False

    try:
        network = ipaddress.IPv6Network(cidr, strict=False)
        return ip_obj in network
    except ValueError:
        return False

def _build_is_ipv6_address_in_cidr_udf() -> Callable:
    """
    Build a user-defined function (UDF) to check if an IPv6 address is in a CIDR block.

    Returns:
        Callable: A UDF that checks if an IPv6 address is in a CIDR block
    """
    @F.pandas_udf(types.BooleanType())  # type: ignore[call-overload]
    def handler(ipv6_column: pd.Series, cidr_column: pd.Series) -> pd.Series:
        return _ipv6_in_cidr(ipv6_column, cidr_column)

    return handler
