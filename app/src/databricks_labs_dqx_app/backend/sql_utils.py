"""Shared SQL sanitization utilities.

All SQL string escaping MUST use these functions instead of inline
.replace() calls to ensure consistent, correct Databricks SQL escaping.
"""

from __future__ import annotations

import re

# Unity Catalog does not restrict catalog/schema/table names to "simple"
# identifiers — objects created via the REST API (bypassing the SQL parser)
# or via backtick-quoted DDL can legitimately contain spaces, quotes,
# hyphens, or punctuation (e.g. a real schema literally named
# ``'ftr_mv_test'``, quote characters included). Rejecting those blocks
# discovery/registration of real tables, so this allowlist accepts any
# character *except*:
#   - a backtick, which is the delimiter ``quote_fqn`` uses to embed the
#     identifier in SQL — an unescaped backtick inside the name would let
#     the identifier "break out" of its quoting;
#   - C0/C1 control characters (incl. newline/CR), which enable log
#     injection (CWE-117) when the FQN is written to logs, and have no
#     legitimate use in an identifier.
# Every other "special" character (quotes, semicolons, comment markers,
# parentheses, …) is inert once the part is backtick-quoted by
# ``quote_fqn`` — it is never interpreted as SQL syntax, only as literal
# identifier text — so it does not need to be blocked here.
_FQN_PART_RE = re.compile(r"^[^`\x00-\x1f\x7f]+$")
_MAX_FQN_PART_LEN = 255  # Unity Catalog's documented identifier length limit.

_SQL_CHECK_RE = re.compile(r"^__sql_check__/[a-zA-Z0-9_\-]+$")

# Colon is allowed alongside the original charset so scheduler bookkeeping
# rows can use namespaced names such as ``product:<product_id>`` (Data
# Products Task 5) without widening the surface for anything unsafe — the
# character set stays a strict allowlist, just with one more safe symbol.
_SCHEDULE_NAME_RE = re.compile(r"^[a-zA-Z0-9_:\-]{1,64}$")


def escape_sql_string(value: str) -> str:
    """Escape a value for embedding in a SQL single-quoted string literal.

    Databricks SQL uses doubled single-quotes ('') for escaping,
    NOT backslash-escape (\\'). This function normalizes to the correct form.
    """
    return value.replace("'", "''")


def validate_fqn(fqn: str) -> str:
    """Validate that a string is a valid three-part Unity Catalog identifier.

    Raises ValueError if the FQN doesn't match the expected pattern.
    Returns the validated FQN unchanged.
    """
    if not fqn:
        raise ValueError("Fully qualified name must not be empty.")

    if _SQL_CHECK_RE.match(fqn):
        return fqn

    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid fully qualified name: '{fqn}'. " "Expected exactly three parts: catalog.schema.table"
        )

    for part in parts:
        # A part that arrives already backtick-quoted (e.g. a caller passing
        # through a previously-quoted name) is unwrapped before validation —
        # the backticks themselves aren't part of the identifier.
        cleaned = part[1:-1] if len(part) >= 2 and part.startswith("`") and part.endswith("`") else part
        if not cleaned or len(cleaned) > _MAX_FQN_PART_LEN or not _FQN_PART_RE.match(cleaned):
            raise ValueError(
                f"Invalid fully qualified name: '{fqn}'. "
                f"Part '{part}' contains invalid characters. "
                "Each part must be 1-255 characters and must not contain a backtick "
                "or control characters."
            )

    return fqn


def quote_fqn(fqn: str) -> str:
    """Quote a validated FQN for safe embedding in SQL.

    Wraps each part in backticks (stripping any existing ones first) to
    prevent identifier injection. Any backtick remaining inside a part
    (there shouldn't be one if ``validate_fqn()`` was called first) is
    doubled per Spark's escaping rule, as defense in depth. Call
    ``validate_fqn()`` first.
    """
    parts = fqn.split(".")
    unwrapped = (p[1:-1] if len(p) >= 2 and p.startswith("`") and p.endswith("`") else p for p in parts)
    return ".".join(f"`{p.replace('`', '``')}`" for p in unwrapped)


def validate_schedule_name(name: str) -> str:
    """Validate that a schedule name contains only safe characters.

    Raises ValueError if the name doesn't match ``^[a-zA-Z0-9_:-]{1,64}$``.
    Returns the validated name unchanged.
    """
    if not _SCHEDULE_NAME_RE.match(name):
        raise ValueError(
            f"Invalid schedule name: '{name}'. "
            "Must be 1–64 characters using only letters, digits, underscores, hyphens, or colons."
        )
    return name


def validate_entity_type(entity_type: str, valid_types: set[str]) -> str:
    """Validate that an entity type is in the allowed set.

    Raises ValueError if not valid. Returns the validated value.
    """
    if entity_type not in valid_types:
        raise ValueError(f"Invalid entity_type: '{entity_type}'. Must be one of {valid_types}")
    return entity_type
