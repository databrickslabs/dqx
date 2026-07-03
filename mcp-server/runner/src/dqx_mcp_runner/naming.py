"""Pure naming + identifier-validation helpers for the DQX MCP runner.

Intentionally dependency-free (stdlib only): no pyspark, no databricks-sdk, no dqx. Keeping these
pure functions in their own module lets them be unit-tested without a Spark session or the DQX
library, and separates naming/validation (a pure concern) from the Spark-coupled operations in
``runner.py``.
"""

from __future__ import annotations

import hashlib
import re

# A bare SQL identifier (catalog/schema/table name part) and a UC principal (email / SP id / group).
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
PRINCIPAL_RE = re.compile(r"^[A-Za-z0-9._%+\-@]+$")

# Per-user output schemas are named dqx_mcp_<sanitized-local-part>_<sha8>. Cap the local part so a
# long email can't produce an over-length schema name; the sha8 keeps it unique regardless.
_MAX_SCHEMA_USER_PART = 40


def output_schema_for_user(email: str) -> str:
    """Derive a collision-safe, per-user output schema name from a caller's email.

    Each caller's MCP outputs (saved rule sets, applied-check result tables) live in their own
    schema ``dqx_mcp_<sanitized-local-part>_<sha8>`` — always a valid SQL identifier, deterministic
    for a given email, and distinct per user (the sha8 suffix disambiguates callers whose local
    parts sanitize to the same string). The runner SP owns the schema and grants only that caller
    access, so users can neither see nor collide with each other's outputs.

    Raises:
        ValueError: If *email* is empty (a caller identity is required to namespace outputs).
    """
    normalized = (email or "").strip().lower()
    if not normalized:
        raise ValueError("A caller identity (email) is required to derive an output schema.")
    local = normalized.split("@", 1)[0]
    sanitized = re.sub(r"[^a-z0-9]+", "_", local).strip("_")[:_MAX_SCHEMA_USER_PART]
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:8]
    suffix = f"{sanitized}_{digest}" if sanitized else digest
    return f"dqx_mcp_{suffix}"


def validate_identifier(value: str, label: str) -> str:
    """Return *value* if it is a bare SQL identifier, else raise ``ValueError`` naming *label*."""
    if not IDENTIFIER_RE.match(str(value)):
        raise ValueError(f"Invalid {label}: {value!r} (letters, digits, and underscores only).")
    return value


def qualify_output(catalog: str, user_schema: str, name: str) -> str:
    """Validate each part and return the unquoted 3-part FQN for a per-user output object.

    The name is caller-supplied, so it is re-validated here (defense-in-depth on top of the tool's
    check) before it is interpolated into DQX config / ``spark.table`` calls.
    """
    validate_identifier(catalog, "catalog")
    validate_identifier(user_schema, "schema")
    validate_identifier(name, "output name")
    return f"{catalog}.{user_schema}.{name}"
