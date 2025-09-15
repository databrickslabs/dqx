import re
import logging
import ipaddress
import warnings
from collections.abc import Callable
import pandas as pd  # type: ignore[import-untyped]
from pyspark.sql import Column
import pyspark.sql.functions as F
import pyspark.sql.connect.session

from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, _get_normalized_column_and_expr

logging.getLogger("ipaddress").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


@register_rule("row")
def is_valid_ipv6_address(column: str | Column) -> pd.Series:
    """
    Validate if the column contains properly formatted IPv6 addresses.

    Args:
        column: The column to check; can be a string column name or a Column expression.

    Returns:
        pd.Series: A Series indicating whether each value is a valid IPv6 address.
    """
    warnings.warn(
        "IPv6 Address validation uses pandas user-defined functions which may degrade performance. "
        "Sample or limit large datasets when running IPV6 address validation.",
    )

    _validate_environment("is_valid_ipv6_address")

    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)

    is_valid_ipv6_address_udf = _build_is_valid_ipv6_address_udf()
    ipv6_match_condition = is_valid_ipv6_address_udf(col_expr)
    final_condition = F.when(col_expr.isNotNull(), ~ipv6_match_condition).otherwise(F.lit(None))
    condition_str = f"' in Column '{col_expr_str}' does not match pattern 'IPV6_ADDRESS'"

    return make_condition(
        final_condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_does_not_match_pattern_ipv6_address",
    )


@register_rule("row")
def is_ipv6_address_in_cidr(column: str | Column, cidr_block: str) -> Column:
    """
    Fail if IPv6 is invalid OR (valid AND not in CIDR). Null for null inputs.

    Args:
        column: The column to check; can be a string column name or a Column expression.
        cidr_block: The CIDR block to check against.

    Returns:
        Column: A Column expression indicating whether each value is not a valid IPv6 address or not in the CIDR block.
    """
    warnings.warn(
        "Checking if an IPv6 Address is in CIDR block uses pandas user-defined functions "
        "which may degrade performance. Sample or limit large datasets when running IPv6 validation.",
        UserWarning,
    )

    if not cidr_block:
        raise ValueError("'cidr_block' must be a non-empty string.")

    if not _is_valid_cidr_block(cidr_block):
        raise ValueError(f"CIDR block '{cidr_block}' is not a valid IPv6 CIDR block.")

    _validate_environment("is_ipv6_address_in_cidr")

    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    cidr_lit = F.lit(cidr_block)
    ipv6_msg_col = is_valid_ipv6_address(column)
    is_valid_ipv6 = ipv6_msg_col.isNull()
    in_cidr = _build_is_ipv6_address_in_cidr_udf()(col_expr, cidr_lit)
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(
        F.when(~is_valid_ipv6, F.lit(True)).otherwise(~in_cidr)
    )
    cidr_msg = F.concat_ws(
        "",
        F.lit("Value '"),
        col_expr.cast("string"),
        F.lit("' in Column '"),
        F.lit(col_expr_str),
        F.lit(f"' is not in the CIDR block '{cidr_block}'"),
    )
    message = F.when(~is_valid_ipv6, ipv6_msg_col).otherwise(cidr_msg)

    return make_condition(
        condition=condition,
        message=message,
        alias=f"{col_str_norm}_is_not_ipv6_in_cidr",
    )


def _is_valid_ipv6(ip_address: str) -> bool:
    """Validate if the string is a valid IPv6 address."""
    try:
        ipaddress.IPv6Address(ip_address)
        return True
    except ipaddress.AddressValueError:
        return False


def _is_ipv6_check(ip_address: str) -> bool:
    """
    Check if a string is a valid IPv6 address.

    Args
        ip_address: The string to check.

    Returns
        True if the string is a valid IPv6 address, False otherwise.
    """
    return _is_valid_ipv6(ip_address)


def _ipv6_in_cidr(ip_address: str, cidr: str) -> bool:
    """
    Check if an IPv6 address is in a given CIDR block.

    Args
        ip_address: The IPv6 address to check.
        cidr: The CIDR block to check against.

    Returns
        True if the IP address is in the CIDR block, False otherwise.
    """

    try:
        ip_obj = ipaddress.IPv6Address(ip_address)
        network = ipaddress.IPv6Network(cidr, strict=False)
        return ip_obj in network
    except ipaddress.AddressValueError:
        return False


def _build_is_valid_ipv6_address_udf() -> Callable:
    """
    Build a user-defined function (UDF) to check if a string is a valid IPv6 address.

    Returns:
        Callable: A UDF that checks if a string is a valid IPv6 address
    """

    @F.pandas_udf("boolean")  # type: ignore[call-overload]
    def _is_valid_ipv6_address_udf(column: pd.Series) -> pd.Series:
        return column.apply(_is_ipv6_check)

    return _is_valid_ipv6_address_udf


def _build_is_ipv6_address_in_cidr_udf() -> Callable:
    """
    Build a user-defined function (UDF) to check if an IPv6 address is in a CIDR block.

    Returns:
        Callable: A UDF that checks if an IPv6 address is in a CIDR block
    """

    @F.pandas_udf("boolean")  # type: ignore[call-overload]
    def handler(ipv6_column: pd.Series, cidr_column: pd.Series) -> pd.Series:
        return ipv6_column.combine(cidr_column, _ipv6_in_cidr)

    return handler


def _is_valid_cidr_block(cidr: str) -> bool:
    """Validate if the string is a valid CIDR block.

    Args:
        cidr: The CIDR block string to validate.
    Returns:
        True if the string is a valid CIDR block, False otherwise.
    """
    try:
        ipaddress.IPv6Network(cidr, strict=False)
        return True
    except (ipaddress.AddressValueError, ipaddress.NetmaskValueError):
        return False


def _validate_environment(func_name: str) -> None:
    """
    Validates that the environment can run IPV6 validation checks.

    As of Databricks Connect 17.1, strict limits are imposed on the size of dependencies for
    user-defined functions. UDFs will fail with out-of-memory errors if these limits are exceeded.

    Because of this limitation, we limit the use of this function to local Spark or a Databricks
    workspace. Databricks Connect uses a *pyspark.sql.connect.session.SparkSession* with an external
    host (e.g. 'https://hostname.cloud.databricks.com'). To raise a clear error message, we check
    the session and intentionally fail if *{func_name}* is called using Databricks Connect.
    """
    connect_session_pattern = re.compile(r"127.0.0.1|.*grpc.sock")
    session = pyspark.sql.SparkSession.builder.getOrCreate()
    if isinstance(session, pyspark.sql.connect.session.SparkSession) and not connect_session_pattern.search(
        session.client.host
    ):
        raise ImportError(f"'{func_name}' is not supported when running checks with Databricks Connect")
