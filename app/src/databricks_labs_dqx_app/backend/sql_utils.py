"""Shared SQL sanitization utilities.

All SQL string escaping MUST use these functions instead of inline
.replace() calls to ensure consistent, correct Databricks SQL escaping.
"""

from __future__ import annotations

import re

_FQN_RE = re.compile(r"^[a-zA-Z0-9_`\-]+\.[a-zA-Z0-9_`\-]+\.[a-zA-Z0-9_`\-]+$")

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
    if not fqn or not (_FQN_RE.match(fqn) or _SQL_CHECK_RE.match(fqn)):
        raise ValueError(
            f"Invalid fully qualified name: '{fqn}'. "
            "Expected format: catalog.schema.table (alphanumeric, underscores, hyphens, backticks only)"
        )
    return fqn


def validate_entity_type(entity_type: str, valid_types: set[str]) -> str:
    """Validate that an entity type is in the allowed set.

    Raises ValueError if not valid. Returns the validated value.
    """
    if entity_type not in valid_types:
        raise ValueError(f"Invalid entity_type: '{entity_type}'. Must be one of {valid_types}")
    return entity_type
