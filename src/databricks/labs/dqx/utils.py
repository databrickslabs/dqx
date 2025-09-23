import datetime
import json
import logging
import re
from typing import Any
from fnmatch import fnmatch
from pyspark.sql import Column

# Import spark connect column if spark session is created using spark connect
try:
    from pyspark.sql.connect.column import Column as ConnectColumn
except ImportError:
    ConnectColumn = None  # type: ignore

from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.limiter import rate_limited


logger = logging.getLogger(__name__)


COLUMN_NORMALIZE_EXPRESSION = re.compile("[^a-zA-Z0-9]+")
COLUMN_PATTERN = re.compile(r"Column<'(.*?)(?: AS (\w+))?'>$")
INVALID_COLUMN_NAME_PATTERN = re.compile(r"[\s,;{}\(\)\n\t=]+")


def get_column_name_or_alias(
    column: "str | Column | ConnectColumn", normalize: bool = False, allow_simple_expressions_only: bool = False
) -> str:
    """
    Extracts the column alias or name from a PySpark Column or ConnectColumn expression.

    PySpark does not provide direct access to the alias of an unbound column, so this function
    parses the alias from the column's string representation.

    - Supports columns with one or multiple aliases.
    - Ensures the extracted expression is truncated to 255 characters.
    - Provides an optional normalization step for consistent naming.
    - Supports ConnectColumn when PySpark Connect is available (falls back gracefully when not available).

    Args:
        column: Column, ConnectColumn (if PySpark Connect available), or string representing a column.
        normalize: If True, normalizes the column name (removes special characters, converts to lowercase).
        allow_simple_expressions_only: If True, raises an error if the column expression is not a simple expression.
            Complex PySpark expressions (e.g., conditionals, arithmetic, or nested transformations), cannot be fully
            reconstructed correctly when converting to string (e.g. F.col("a") + F.lit(1)).
            However, in certain situations this is acceptable, e.g. when using the output for reporting purposes.

    Returns:
        The extracted column alias or name.

    Raises:
        ValueError: If the column expression is invalid.
        TypeError: If the column type is unsupported.
    """
    if isinstance(column, str):
        col_str = column
    else:
        # Extract the last alias or column name from the PySpark Column string representation
        match = COLUMN_PATTERN.search(str(column))
        if not match:
            raise ValueError(f"Invalid column expression: {column}")
        col_expr, alias = match.groups()
        if alias:
            return alias
        col_str = col_expr

        if normalize:
            col_str = normalize_col_str(col_str)

    if allow_simple_expressions_only and not is_simple_column_expression(col_str):
        raise ValueError(
            "Unable to interpret column expression. Only simple references are allowed, e.g: F.col('name')"
        )
    return col_str


def get_columns_as_strings(columns: list[str | Column], allow_simple_expressions_only: bool = True) -> list[str]:
    """
    Extracts column names from a list of PySpark Column or ConnectColumn expressions.

    This function processes each column, ensuring that only valid column names are returned.
    Supports ConnectColumn when PySpark Connect is available (falls back gracefully when not available).

    Args:
        columns: List of columns, ConnectColumns (if PySpark Connect available), or strings representing columns.
        allow_simple_expressions_only: If True, raises an error if the column expression is not a simple expression.

    Returns:
        List of column names as strings.
    """
    columns_as_strings = []
    for col in columns:
        col_str = (
            get_column_name_or_alias(col, allow_simple_expressions_only=allow_simple_expressions_only)
            if not isinstance(col, str)
            else col
        )
        columns_as_strings.append(col_str)
    return columns_as_strings


def is_simple_column_expression(col_name: str) -> bool:
    """
    Returns True if the column name does not contain any disallowed characters:
    space, comma, semicolon, curly braces, parentheses, newline, tab, or equals sign.

    Args:
        col_name: Column name to validate.

    Returns:
        True if the column name is valid, False otherwise.
    """
    return not bool(INVALID_COLUMN_NAME_PATTERN.search(col_name))


def normalize_bound_args(val: Any) -> Any:
    """
    Normalize a value or collection of values for consistent processing.

    Handles primitives, dates, and column-like objects. Lists, tuples, and sets are
    recursively normalized with type preserved.

    Args:
        val: Value or collection of values to normalize.

    Returns:
        Normalized value or collection.

    Raises:
        ValueError: If a column resolves to an invalid name.
        TypeError: If a column type is unsupported.
    """
    if isinstance(val, (list, tuple, set)):
        normalized = [normalize_bound_args(v) for v in val]
        return normalized

    if isinstance(val, (str, int, float, bool)):
        return val

    if isinstance(val, (datetime.date, datetime.datetime)):
        return str(val)

    if ConnectColumn is not None:
        column_types: tuple[type[Any], ...] = (Column, ConnectColumn)
    else:
        column_types = (Column,)

    if isinstance(val, column_types):
        col_str = get_column_name_or_alias(val, allow_simple_expressions_only=True)
        return col_str
    raise TypeError(f"Unsupported type for normalization: {type(val).__name__}")


def normalize_col_str(col_str: str) -> str:
    """
    Normalizes string to be compatible with metastore column names by applying the following transformations:
    * remove special characters
    * convert to lowercase
    * limit the length to 255 characters to be compatible with metastore column names

    Args:
        col_str: Column or string representing a column.

    Returns:
        Normalized column name.
    """
    max_chars = 255
    return re.sub(COLUMN_NORMALIZE_EXPRESSION, "_", col_str[:max_chars].lower()).rstrip("_")


def is_sql_query_safe(query: str) -> bool:
    # Normalize the query by removing extra whitespace and converting to lowercase
    normalized_query = re.sub(r"\s+", " ", query).strip().lower()

    # Check for prohibited statements
    forbidden_statements = [
        "delete",
        "insert",
        "update",
        "drop",
        "truncate",
        "alter",
        "create",
        "replace",
        "grant",
        "revoke",
        "merge",
        "use",
        "refresh",
        "analyze",
        "optimize",
        "zorder",
    ]
    return not any(re.search(rf"\b{kw}\b", normalized_query) for kw in forbidden_statements)


def safe_json_load(value: str):
    """
    Safely load a JSON string, returning the original value if it fails to parse.
    This allows to specify string value without a need to escape the quotes.

    Args:
        value: The value to parse as JSON.
    """
    try:
        return json.loads(value)  # load as json if possible
    except json.JSONDecodeError:
        return value


@rate_limited(max_requests=100)
def list_tables(client: WorkspaceClient, patterns: list[str] | None, exclude_matched: bool = False) -> list[str]:
    """
    Gets a list of table names from Unity Catalog given a list of wildcard patterns.

    Args:
        client (WorkspaceClient): Databricks SDK WorkspaceClient.
        patterns (list[str] | None): A list of wildcard patterns to match against the table name.
        exclude_matched (bool): Specifies whether to include tables matched by the pattern.
            If True, matched tables are excluded. If False, matched tables are included.

    Returns:
        list[str]: A list of fully qualify table names.

    Raises:
        ValueError: If no tables are found matching the include or exclude criteria.
    """
    tables = []
    for catalog in client.catalogs.list():
        if not catalog.name:
            continue
        for schema in client.schemas.list(catalog_name=catalog.name):
            if not schema.name:
                continue
            table_infos = client.tables.list_summaries(catalog_name=catalog.name, schema_name_pattern=schema.name)
            tables.extend([table_info.full_name for table_info in table_infos if table_info.full_name])

    if patterns and exclude_matched:
        tables = [table for table in tables if not match_table_patterns(table, patterns)]
    if patterns and not exclude_matched:
        tables = [table for table in tables if match_table_patterns(table, patterns)]
    if len(tables) > 0:
        return tables
    raise ValueError("No tables found matching include or exclude criteria")


def match_table_patterns(table: str, patterns: list[str]) -> bool:
    """
    Checks if a table name matches any of the provided wildcard patterns.

    Args:
        table (str): The table name to check.
        patterns (list[str]): A list of wildcard patterns (e.g., 'catalog.schema.*') to match against the table name.

    Returns:
        bool: True if the table name matches any of the patterns, False otherwise.
    """
    return any(fnmatch(table, pattern) for pattern in patterns)
