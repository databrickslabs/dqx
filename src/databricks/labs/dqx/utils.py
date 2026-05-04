import os
import json
import datetime
import logging
import re
from decimal import Decimal
from enum import Enum
from importlib.util import find_spec
from typing import Any, TypeVar, overload
from fnmatch import fnmatch
from pathlib import Path


from pyspark.sql import Column
from pyspark.sql.types import StructType

# Import spark connect column if spark session is created using spark connect
try:
    from pyspark.sql.connect.column import Column as ConnectColumn
except ImportError:
    ConnectColumn = None  # type: ignore

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.limiter import rate_limited
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.table_manager import SparkTableDataProvider
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)


T = TypeVar("T")


COLUMN_NORMALIZE_EXPRESSION = re.compile("[^a-zA-Z0-9]+")
COLUMN_PATTERN = re.compile(r"Column<'(.*?)(?: AS (\w+))?'>$", re.DOTALL)
INVALID_COLUMN_NAME_PATTERN = re.compile(r"[\s,;{}\(\)\n\t=]+")
_UNRESOLVED_PLACEHOLDER_PATTERN = re.compile(r"\{\{[^}]*\}\}")
_SCALAR_VARIABLE_TYPES = (str, int, float, bool, Decimal, datetime.date, datetime.datetime, datetime.time)

VariableValue = str | int | float | bool | Decimal | datetime.date | datetime.datetime | datetime.time
"""Supported scalar types for variable substitution values."""


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
        # Extract the last alias or column name from the PySpark Column string representation.
        # Strip the representation first to guard against trailing whitespace or CRLF line endings
        # that may appear in some PySpark versions (e.g. Databricks Serverless v5), which would prevent the
        # end-of-string anchor in COLUMN_PATTERN from matching.
        match = COLUMN_PATTERN.search(str(column).strip())
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


def _normalize_leaf_value(val: Any, allow_simple_expressions_only: bool) -> Any:
    """Normalize a leaf (non-collection) value. Called by normalize_bound_args."""
    if isinstance(val, (str, int, float, bool)):
        return val

    if isinstance(val, (datetime.date, datetime.datetime)):
        return str(val)

    if isinstance(val, Decimal):
        return {"__decimal__": str(val)}

    column_types: tuple[type[Any], ...] = (Column, ConnectColumn) if ConnectColumn is not None else (Column,)
    if isinstance(val, column_types):
        return get_column_name_or_alias(val, allow_simple_expressions_only=allow_simple_expressions_only)

    if isinstance(val, StructType):
        return val.simpleString()

    if isinstance(val, Enum):
        return normalize_bound_args(val.value, allow_simple_expressions_only)

    raise TypeError(f"Unsupported type for normalization: {type(val).__name__}")


def normalize_bound_args(val: Any, allow_simple_expressions_only: bool = True) -> Any:
    """
    Normalize a value or collection of values for consistent processing.

    Handles primitives, dates, Decimal, and column-like objects. Lists, tuples, and sets are
    recursively normalized with type preserved.

    For Decimal values, uses a special JSON-serializable format to preserve type information
    for round-trip deserialization.

    Args:
        val: Value or collection of values to normalize.
        allow_simple_expressions_only: If True (default), Column values must be simple expressions
            (e.g. F.col("name")). If False, complex expressions (e.g. F.try_element_at(...)) are
            allowed and serialized as their string representation. Use False when serializing
            for fingerprinting/metadata only, where round-trip reconstruction is not required.

    Returns:
        Normalized value or collection.

    Raises:
        TypeError: If a column type is unsupported.
    """
    if val is None:
        return None

    if isinstance(val, (list, tuple, set, frozenset)):
        return [normalize_bound_args(v, allow_simple_expressions_only) for v in val]

    if isinstance(val, dict):
        return {k: normalize_bound_args(v, allow_simple_expressions_only) for k, v in val.items()}

    return _normalize_leaf_value(val, allow_simple_expressions_only)


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
        return json.loads(value)
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


def table_exists(spark: Any, table: str) -> bool:
    """
    Check if a table exists (Unity Catalog compatible).

    Uses the catalog API only (no Spark job). Requires Spark 3.4+ for
    fully qualified table names (e.g. catalog.schema.table).

    Args:
        spark: SparkSession instance.
        table: Fully qualified table name (e.g. "catalog.schema.table").

    Returns:
        True if the table exists, False otherwise.
    """
    return spark.catalog.tableExists(table)


def get_table_primary_keys(table: str, spark: Any) -> set[str]:
    """
    Retrieve primary key columns from Unity Catalog table metadata.

    Uses SparkTableDataProvider (table_manager) to read table properties and
    parses the primary key constraint into a set of column names.

    Args:
        table: Fully qualified table name (e.g., "catalog.schema.table")
        spark: SparkSession instance

    Returns:
        Set of column names that are primary keys. Returns empty set if:
        - Table doesn't exist
        - No primary key is defined
        - Metadata is not accessible

    Examples:
        >>> pk_cols = get_table_primary_keys("main.default.users", spark)
        >>> if "user_id" in pk_cols:
        ...     print("user_id is a primary key")
    """
    try:
        provider = SparkTableDataProvider(spark)
        pk_str = provider.get_existing_primary_key(table)
        if pk_str is None:
            return set()
        return {c.strip() for c in pk_str.split(",")}
    except Exception:
        # Silently handle errors (table not found, permissions, etc.)
        return set()


def missing_required_packages(packages: list[str]) -> bool:
    """
    Checks if any of the required packages are missing.

    Args:
        packages: A list of package names to check.

    Returns:
        True if any package is missing, False otherwise.
    """
    return not all(find_spec(spec) for spec in packages)


def _replace_template(text: str, variables: dict[str, str]) -> str:
    """Replace **{{ key }}** placeholders in *text* with values from *variables*.

    Uses a single-pass regex substitution.
    Tolerates whitespace inside braces (e.g. **{{ key }}**, **{{key}}**).
    Logs a warning if any unresolved **{{ ... }}** placeholders remain after substitution.

    Args:
        text: Input string potentially containing **{{ key }}** placeholders.
        variables: Pre-stringified mapping of placeholder names to values.

    Returns:
        String with all matching placeholders replaced.
    """
    if not variables:
        if _UNRESOLVED_PLACEHOLDER_PATTERN.search(text):
            logger.warning(f"Unresolved placeholder found: '{text}'")
        return text

    def _resolve(match_obj: re.Match[str]) -> str:
        key = match_obj.group(0).strip("{} \t")
        if key in variables:
            return variables[key]
        unresolved.append(key)
        return match_obj.group(0)

    unresolved: list[str] = []
    output = _UNRESOLVED_PLACEHOLDER_PATTERN.sub(_resolve, text)
    if unresolved:
        logger.warning(
            f"Unresolved placeholders found: {unresolved}. "
            f"They may be resolved at runtime for certain checks (e.g. sql_query)."
        )
    return output


@overload
def _substitute_variables(obj: str, variables: dict[str, str]) -> str: ...


@overload
def _substitute_variables(obj: list[T], variables: dict[str, str]) -> list[T]: ...


@overload
def _substitute_variables(obj: dict[str, T], variables: dict[str, str]) -> dict[str, T]: ...


@overload
def _substitute_variables(obj: T, variables: dict[str, str]) -> T: ...


def _substitute_variables(obj: Any, variables: dict[str, str]) -> Any:
    """Recursively replace **{{ key }}** placeholders in all string values within *obj*.

    Traverses dicts, lists, and strings. Non-string/non-collection values are
    returned unchanged. Dict keys are not substituted.

    Args:
        obj: A string, dict, list, or other value to process.
        variables: Pre-stringified mapping of placeholder names to values.

    Returns:
        A new object with all string values having placeholders replaced.
    """
    if isinstance(obj, str):
        return _replace_template(obj, variables)
    if isinstance(obj, dict):
        return {k: _substitute_variables(v, variables) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute_variables(item, variables) for item in obj]
    return obj


def _validate_variable_types(variables: dict[str, VariableValue]) -> None:
    """Raise :class:`InvalidParameterError` if any variable value is not a supported scalar type."""
    for key, val in variables.items():
        if not isinstance(val, _SCALAR_VARIABLE_TYPES):
            raise InvalidParameterError(
                f"Variable '{key}' has unsupported type '{type(val).__name__}'. "
                f"Only scalar types are supported: str, int, float, bool, Decimal, "
                f"datetime.date, datetime.datetime, datetime.time."
            )


def resolve_variables(checks: list[dict], variables: dict[str, VariableValue] | None) -> list[dict]:
    """Resolve variable substitution in check definitions.

    Replaces placeholders in all string values of *checks* with the corresponding values
    from *variables*.

    Variable values must be scalar types (e.g. *str*, *int*, *float*, *bool*, *Decimal*,
    *datetime.date*, *datetime.datetime*, *datetime.time*). Non-string scalars are
    converted to strings via *str()* in the substituted string. Collection type
    variables (e.g. *list*, *dict*, *set*, etc.) are rejected with
    *databricks.labs.dqx.errors.InvalidParameterError* because their string representation
    is rarely meaningful in SQL or column expressions.

    Logs a warning for any placeholders that remain unresolved after substitution
    (e.g. misspelled variable names).

    Note:
    Variable values substituted into *sql_expression* checks are not sanitized and are
    passed directly to *F.expr()*. Callers must **ensure variable values come from trusted
    sources** to prevent SQL injection.

    Args:
        checks: List of check definition dictionaries (metadata format).
        variables: Mapping of placeholder names to scalar replacement values.
            If *None* or empty the checks are returned unchanged.

    Returns:
        A new list of check dicts with placeholders resolved, or the original list
        when no substitution is needed.

    Raises:
        InvalidParameterError: If any variable value is not a supported scalar type.
    """
    if not variables:
        return checks

    _validate_variable_types(variables)
    str_variables = {k: str(v) for k, v in variables.items()}
    return _substitute_variables(checks, str_variables)


def get_file_extension(file_path: str | os.PathLike) -> str:
    """
    Extract file extension from a file path.

    Args:
        file_path: File path as string or path-like object.

    Returns:
        File extension (e.g., ".json", ".yaml", ".yml") or empty string if no extension.
    """
    return Path(file_path).suffix
