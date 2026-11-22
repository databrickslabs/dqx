"""SQL safety helpers: destructive-statement detection and identifier quoting.

This module has no dependencies on other DQX modules apart from *errors*, so any module that builds SQL
statements (including *table_manager*, which *utils* depends on) can import it without an import cycle.
"""

import re

from databricks.labs.dqx.errors import InvalidParameterError

# Destructive SQL statement keywords rejected by `is_sql_query_safe`. SELECT is intentionally
# omitted: filters may legitimately use subqueries and are authored by trusted operators.
_FORBIDDEN_SQL_KEYWORDS = (
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
)
# Precompiled once at module scope (this helper is on the LLM / rule-validation hot path).
_FORBIDDEN_SQL_KEYWORDS_PATTERN = re.compile(rf"\b(?:{'|'.join(_FORBIDDEN_SQL_KEYWORDS)})\b")
# Matches SQL comments (line `-- ...`, block `/* ... */`), quoted string literals, and backtick-quoted
# identifiers. Scanned left-to-right so a comment marker inside a string literal (e.g. '-- not a comment')
# is correctly treated as data, and a quote inside a comment is treated as part of the comment.
# String literals recognise both doubled-quote ('') and backslash (\') escapes, matching Spark's default
# parser, so a closing quote is not detected prematurely (e.g. 'can\'t drop' is one literal, not two).
_SQL_COMMENT_OR_LITERAL_PATTERN = re.compile(
    r"(?P<comment>--[^\n]*|/\*.*?\*/)"  # line or block comment
    r"|'(?:[^'\\]|''|\\.)*'"  # single-quoted string literal ('' and \\ escapes)
    r"|\"(?:[^\"\\]|\"\"|\\.)*\""  # double-quoted string literal ('' and \\ escapes)
    r"|`(?:[^`]|``)*`",  # backtick-quoted identifier (with `` escapes)
    re.DOTALL,
)
# A single part of a table or view name: a bare name, or a backtick-quoted name without embedded backticks.
# Databricks permits leading digits in bare identifiers (e.g. 1abc is a valid table name; verified against a
# live workspace), so a bare part is [A-Za-z0-9_]+; special characters must be backtick-quoted. Also used by
# the table-location patterns in *config*, so both accept exactly the same names.
TABLE_NAME_PART = r"(?:[A-Za-z0-9_]+|`[^`]+`)"
_TABLE_NAME_PART_PATTERN = re.compile(TABLE_NAME_PART)
# One to three parts: view, schema.table or catalog.schema.table.
_TABLE_NAME_PATTERN = re.compile(rf"{TABLE_NAME_PART}(?:\.{TABLE_NAME_PART}){{0,2}}")


def _strip_literals_keep_comments(match: re.Match[str]) -> str:
    """Replace a quoted literal/identifier with a space, keeping comment text so its keywords are scanned."""
    return match.group() if match.group("comment") is not None else " "


def _strip_literals_and_comments(match: re.Match[str]) -> str:
    """Remove a comment entirely and replace a quoted literal/identifier with a space."""
    return "" if match.group("comment") is not None else " "


def quote_column_name(name: str) -> str:
    """
    Wraps a column name in backticks so it can be used as a SQL identifier.

    Column names containing spaces, non-ASCII characters, or other characters that require escaping
    (e.g. "Customer Name", "Ääkkönen") are not valid bare SQL identifiers and must be back-quoted before
    being parsed by *F.expr*.

    Args:
        name: Column name to quote.

    Returns:
        The column name wrapped in backticks with embedded backticks escaped.
    """
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def quote_table_name(name: str) -> str:
    """
    Validates a table or view name and backtick-quotes each of its parts for use as a SQL identifier.

    Accepts one to three dot-separated parts (*view*, *schema.table* or *catalog.schema.table*). Each part is
    either a bare name of letters, digits and underscores, or a name that is already backtick-quoted (with no
    backtick inside), the same names *is_table_location* accepts. Bare parts
    are quoted with *quote_column_name*; quoted parts are kept as they are. Anything else, such as a name that
    breaks out of the identifier with a semicolon, a quote or an unbalanced backtick, is rejected, so the name
    cannot inject SQL into the statement it is interpolated into.

    Args:
        name: Table or view name, e.g. "catalog.schema.table". Any part may already be backtick-quoted.

    Returns:
        The name with every part wrapped in backticks, joined by dots.

    Raises:
        InvalidParameterError: If *name* is not a valid one- to three-part table or view name.
    """
    if not _TABLE_NAME_PATTERN.fullmatch(name):
        raise InvalidParameterError(f"Invalid table or view name: {name!r}")
    return ".".join(
        part if part.startswith("`") else quote_column_name(part) for part in _TABLE_NAME_PART_PATTERN.findall(name)
    )


def is_sql_query_safe(query: str) -> bool:
    """
    Returns True if the query contains no destructive SQL statement keyword, otherwise False.

    Quoted string literals and backtick-quoted identifiers are stripped before scanning so that a
    forbidden keyword appearing as data (e.g. *status = 'drop'*) or as a backtick-quoted column name
    is not mistaken for a statement. Comments are checked two ways so both a keyword *inside* a
    comment (e.g. a block comment around *delete*) and a keyword *split* by an empty block comment
    between its letters are caught. *SELECT* is allowed: filters may legitimately use subqueries
    and are authored by trusted operators.

    Args:
        query: The SQL query or filter predicate to validate.

    Returns:
        True if the query is free of destructive statement keywords, False otherwise.
    """
    # Strip quoted literals/identifiers so keywords appearing as data are not flagged, keeping comment
    # text so a keyword hidden inside a comment (e.g. /* delete */) is still caught. Lowercase for a
    # case-insensitive match.
    with_comments = _SQL_COMMENT_OR_LITERAL_PATTERN.sub(_strip_literals_keep_comments, query).lower()
    if _FORBIDDEN_SQL_KEYWORDS_PATTERN.search(with_comments):
        return False

    # The first pass is clean. Only when a comment is present do we re-scan with comments removed, to
    # catch a keyword split by an inline comment (e.g. dr/**/op). This second pass is skipped for the
    # common comment-free query (this helper is on the LLM / rule-validation hot path).
    if "--" not in with_comments and "/*" not in with_comments:
        return True
    without_comments = _SQL_COMMENT_OR_LITERAL_PATTERN.sub(_strip_literals_and_comments, query).lower()
    return not _FORBIDDEN_SQL_KEYWORDS_PATTERN.search(without_comments)
