import os
import json
import datetime
import logging
import re
from importlib.util import find_spec
from typing import Any
from fnmatch import fnmatch

from pyspark.sql import Column

# Import spark connect column if spark session is created using spark connect
try:
    from pyspark.sql.connect.column import Column as ConnectColumn
except ImportError:
    ConnectColumn = None  # type: ignore

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.limiter import rate_limited
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk.errors import NotFound

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
        InvalidParameterError: If the column expression is invalid or unsupported.
    """
    if isinstance(column, str):
        col_str = column
    else:
        # Extract the last alias or column name from the PySpark Column string representation
        match = COLUMN_PATTERN.search(str(column))
        if not match:
            raise InvalidParameterError(f"Invalid column expression: {column}")
        col_expr, alias = match.groups()
        if alias:
            return alias
        col_str = col_expr

        if normalize:
            col_str = normalize_col_str(col_str)

    if allow_simple_expressions_only and not is_simple_column_expression(col_str):
        raise InvalidParameterError(
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

    Raises:
        InvalidParameterError: If any column expression is invalid or unsupported.
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


def safe_strip_file_from_path(path: str) -> str:
    """
    Safely removes the file name from a given path, treating it as a directory if no file extension is present.
    - Hidden directories (e.g., .folder) are preserved.
    - Hidden files with extensions (e.g., .file.yml) are treated as files.

    Args:
        path: The input path from which to remove the file name.

    Returns:
        The path without the file name, or the original path if it is already a directory.
    """
    if not path:
        return ""

    # Remove trailing slash
    path = path.rstrip("/")

    head, tail = os.path.split(path)

    if not tail:
        return path  # it's already a directory

    # If it looks like a file:
    # - contains a dot and (doesn't start with '.' OR has another dot after the first char)
    if "." in tail and (not tail.startswith(".") or tail.count(".") > 1):
        return head

    # Otherwise, treat as directory
    return path


@rate_limited(max_requests=100)
def list_tables(
    workspace_client: WorkspaceClient,
    patterns: list[str] | None,
    exclude_matched: bool = False,
    exclude_patterns: list[str] | None = None,
) -> list[str]:
    """
    Gets a list of table names from Unity Catalog given a list of wildcard patterns.

    Args:
        workspace_client (WorkspaceClient): Databricks SDK WorkspaceClient.
        patterns (list[str] | None): A list of wildcard patterns to match against the table name.
        exclude_matched (bool): Specifies whether to include tables matched by the pattern.
            If True, matched tables are excluded. If False, matched tables are included.
        exclude_patterns (list[str] | None): A list of wildcard patterns to exclude from the table names.

    Returns:
        list[str]: A list of fully qualified table names.
        DataFrame with values read from the input data

    Raises:
        NotFound: If no tables are found matching the include or exclude criteria.
    """
    allowed_catalogs, allowed_schemas = _get_allowed_catalogs_and_schemas(patterns, exclude_matched)
    tables = _get_tables_from_catalogs(workspace_client, allowed_catalogs, allowed_schemas)

    if patterns:
        tables = _filter_tables_by_patterns(tables, patterns, exclude_matched)

    if exclude_patterns:
        tables = _filter_tables_by_patterns(tables, exclude_patterns, exclude_matched=True)

    if tables:
        return tables
    raise NotFound("No tables found matching include or exclude criteria")


def _split_pattern(pattern: str) -> tuple[str, str, str]:
    """
    Splits a wildcard pattern into its catalog, schema, and table components.

    Args:
        pattern (str): A wildcard pattern in the form 'catalog.schema.table'.

    Returns:
        tuple[str, str, str]: A tuple containing the catalog, schema, and table components.
        DataFrame with values read from the file data
    """
    parts = pattern.split(".")
    catalog = parts[0] if len(parts) > 0 else "*"
    schema = parts[1] if len(parts) > 1 else "*"
    table = ".".join(parts[2:]) if len(parts) > 2 else "*"
    return catalog, schema, table


def _build_include_scope_for_patterns(patterns: list[str]) -> tuple[set[str] | None, dict[str, set[str]] | None]:
    """
    Builds allowed catalogs and schemas from a list of wildcard patterns.

    Args:
        patterns (list[str]): A list of wildcard patterns to match against the table name.

    Returns:
        tuple[set[str] | None, dict[str, set[str]] | None]: A tuple containing:
            - A set of allowed catalogs or None if no specific catalogs are constrained.
            - A dictionary mapping allowed catalogs to their respective sets of allowed schemas or
              None if no specific schemas are constrained.
    """
    parts = [_split_pattern(p) for p in patterns]
    # If any pattern uses '*' at catalog → don’t constrain catalogs
    if any(cat == "*" for cat, _, _ in parts):
        return None, None
    allowed_catalogs: set[str] = set()
    allowed_schemas: dict[str, set[str]] = {}
    for catalog, schema, _ in parts:
        if catalog != "*":
            allowed_catalogs.add(catalog)
            if schema != "*":
                allowed_schemas.setdefault(catalog, set()).add(schema)
    return (allowed_catalogs or None), (allowed_schemas or None)


def _get_allowed_catalogs_and_schemas(
    patterns: list[str] | None, exclude_matched: bool
) -> tuple[set[str] | None, dict[str, set[str]] | None]:
    """
    Determines allowed catalogs and schemas based on provided patterns and exclusion flag.

    Args:
        patterns (list[str] | None): A list of wildcard patterns to match against the table name.
        exclude_matched (bool): Specifies whether to include tables matched by the pattern.
            If True, matched tables are excluded. If False, matched tables are included.

    Returns:
        tuple[set[str] | None, dict[str, set[str]] | None]: A tuple containing:
            - A set of allowed catalogs or None if no specific catalogs are constrained.
            - A dictionary mapping allowed catalogs to their respective sets of allowed schemas or
              None if no specific schemas are constrained.
    """
    if patterns and not exclude_matched:
        return _build_include_scope_for_patterns(patterns)
    return None, None


def _get_tables_from_catalogs(
    client: WorkspaceClient, allowed_catalogs: set[str] | None, allowed_schemas: dict[str, set[str]] | None
) -> list[str]:
    """
    Retrieves tables from Unity Catalog based on allowed catalogs and schemas.

    Args:
        client (WorkspaceClient): Databricks SDK WorkspaceClient.
        allowed_catalogs (set[str] | None): A set of allowed catalogs or None if no specific catalogs are constrained.
        allowed_schemas (dict[str, set[str]] | None): A dictionary mapping allowed catalogs to their respective sets
            of allowed schemas or None if no specific schemas are constrained.

    Returns:
        list[str]: A list of fully qualified table names.
    """
    tables: list[str] = []
    for catalog in client.catalogs.list():
        catalog_name = catalog.name
        if not catalog_name or (allowed_catalogs and catalog_name not in allowed_catalogs):
            continue

        schema_filter = allowed_schemas.get(catalog_name) if allowed_schemas else None
        tables.extend(_get_tables_from_schemas(client, catalog_name, schema_filter))
    return tables


def _get_tables_from_schemas(client: WorkspaceClient, catalog_name: str, schema_filter: set[str] | None) -> list[str]:
    """
    Retrieves tables from schemas within a specified catalog.

    Args:
        client (WorkspaceClient): Databricks SDK WorkspaceClient.
        catalog_name (str): The name of the catalog to retrieve tables from.
        schema_filter (set[str] | None): A set of allowed schemas within the catalog or None if no specific schemas are constrained.

    Returns:
        list[str]: A list of fully qualified table names within the specified catalog and schemas.
    """
    tables: list[str] = []
    for schema in client.schemas.list(catalog_name=catalog_name):
        schema_name = schema.name
        if not schema_name or (schema_filter and schema_name not in schema_filter):
            continue

        tables.extend(
            table.full_name
            for table in client.tables.list_summaries(catalog_name=catalog_name, schema_name_pattern=schema_name)
            if table.full_name
        )
    return tables


def _filter_tables_by_patterns(tables: list[str], patterns: list[str], exclude_matched: bool) -> list[str]:
    """
    Filters a list of table names based on provided wildcard patterns.

    Args:
        tables (list[str]): A list of fully qualified table names.
        patterns (list[str]): A list of wildcard patterns to match against the table name.
        exclude_matched (bool): Specifies whether to include tables matched by the pattern.
            If True, matched tables are excluded. If False, matched tables are included.

    Returns:
        list[str]: A filtered list of table names based on the matching criteria.
    """
    if exclude_matched:
        return [table for table in tables if not _match_table_patterns(table, patterns)]
    return [table for table in tables if _match_table_patterns(table, patterns)]


def _match_table_patterns(table: str, patterns: list[str]) -> bool:
    """
    Checks if a table name matches any of the provided wildcard patterns.

    Args:
        table (str): The table name to check.
        patterns (list[str]): A list of wildcard patterns (e.g., 'catalog.schema.*') to match against the table name.

    Returns:
        bool: True if the table name matches any of the patterns, False otherwise.
    """
    return any(fnmatch(table, pattern) for pattern in patterns)


def to_lowercase(col_expr: Column, is_array: bool = False) -> Column:
    """Converts a column expression to lowercase, handling both scalar and array types.

    Args:
        col_expr: Column expression to convert
        is_array: Whether the column contains array values

    Returns:
        Column expression with lowercase transformation applied
    """
    if is_array:
        return F.transform(col_expr, F.lower)
    return F.lower(col_expr)


def missing_required_packages(packages: list[str]) -> bool:
    """
    Checks if any of the required packages are missing.

    Args:
        packages: A list of package names to check.

    Returns:
        True if any package is missing, False otherwise.
    """
    return not all(find_spec(spec) for spec in packages)
