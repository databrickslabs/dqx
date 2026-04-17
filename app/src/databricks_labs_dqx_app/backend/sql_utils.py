"""Shared SQL sanitization utilities.

All SQL string escaping MUST use these functions instead of inline
.replace() calls to ensure consistent, correct Databricks SQL escaping.
"""

from __future__ import annotations

import re

# Each part: starts with a letter or underscore, followed by alphanumerics,
# underscores, or hyphens.  No backticks, spaces, or other special characters.
_FQN_PART_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\-]*$")

_SQL_CHECK_RE = re.compile(r"^__sql_check__/[a-zA-Z0-9_\-]+$")


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
            f"Invalid fully qualified name: '{fqn}'. "
            "Expected exactly three parts: catalog.schema.table"
        )

    for part in parts:
        cleaned = part.strip("`")
        if not cleaned or not _FQN_PART_RE.match(cleaned):
            raise ValueError(
                f"Invalid fully qualified name: '{fqn}'. "
                f"Part '{part}' contains invalid characters. "
                "Each part must start with a letter or underscore and contain only "
                "alphanumeric characters, underscores, or hyphens."
            )

    return fqn


def quote_fqn(fqn: str) -> str:
    """Quote a validated FQN for safe embedding in SQL.

    Wraps each part in backticks (stripping any existing ones first)
    to prevent identifier injection.  Call validate_fqn() first.
    """
    parts = fqn.split(".")
    return ".".join(f"`{p.strip('`')}`" for p in parts)


def validate_entity_type(entity_type: str, valid_types: set[str]) -> str:
    """Validate that an entity type is in the allowed set.

    Raises ValueError if not valid. Returns the validated value.
    """
    if entity_type not in valid_types:
        raise ValueError(f"Invalid entity_type: '{entity_type}'. Must be one of {valid_types}")
    return entity_type
