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
#   - a backslash. A validated FQN also flows into single-quoted SQL string
#     literals (via ``escape_sql_string`` — e.g. the INSERT in
#     ``MonitoredTableService.register`` and the ``escape_sql_string`` call
#     sites in ``materializer``/``metrics``/``rules_catalog_service``).
#     ``escape_sql_string`` doubles single quotes but does NOT escape
#     backslashes, and on the Delta / Databricks SQL string-literal path a
#     backslash is an escape character: a part ending in ``\`` (e.g.
#     ``tab\``) would turn the doubled closing ``''`` into an escaped quote
#     and let the literal "break out", corrupting the statement. Backslash
#     is not a legitimate UC identifier character, so we reject it here
#     rather than widening ``escape_sql_string``'s escaping regime.
#   - C0/C1 control characters (incl. newline/CR), which enable log
#     injection (CWE-117) when the FQN is written to logs, and have no
#     legitimate use in an identifier.
# Every other "special" character (quotes, semicolons, comment markers,
# parentheses, …) is inert once the part is backtick-quoted by
# ``quote_fqn`` — it is never interpreted as SQL syntax, only as literal
# identifier text — so it does not need to be blocked here.
_FQN_PART_RE = re.compile(r"^[^`\\\x00-\x1f\x7f]+$")
_MAX_FQN_PART_LEN = 255  # Unity Catalog's documented identifier length limit.

# A "simple" identifier that Spark/Databricks SQL parses without any
# backtick-quoting: a leading letter/underscore followed by
# letters/digits/underscores. Used by ``fqn_needs_quoting`` to decide
# whether a raw FQN can be handed to ``spark.table`` unquoted.
_SIMPLE_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

_SQL_CHECK_RE = re.compile(r"^__sql_check__/[a-zA-Z0-9_\-]+$")

# Colon is allowed alongside the original charset so scheduler bookkeeping
# rows can use namespaced names such as ``product:<product_id>`` (Data
# Products Task 5) without widening the surface for anything unsafe — the
# character set stays a strict allowlist, just with one more safe symbol.
_SCHEDULE_NAME_RE = re.compile(r"^[a-zA-Z0-9_:\-]{1,64}$")


_SQL_QUOTES = ("'", '"', "`")


def strip_sql_line_comments(sql: str) -> str:
    """Remove SQL comments from a predicate/query for a safety-keyword scan.

    The SQL "Explain" affordance prepends the AI explanation to a rule
    predicate as ``-- <line>`` comment lines. Those lines are inert at runtime
    (Spark's SQL lexer skips ``--`` line and ``/* */`` block comments), but the
    explanation *prose* can contain words that look like forbidden DDL/DML
    keywords ("this deletes duplicates"). ``is_sql_query_safe`` scans the raw
    text and would falsely reject such a predicate, so every app-side gate runs
    the scan on the de-commented copy produced here.

    Security: the stripper is quote-aware. A ``--`` / ``/* */`` inside a string
    literal or a backtick-quoted identifier is NOT a comment and is preserved,
    so a crafted ``'... --'`` can never hide a live forbidden keyword after a
    fake comment marker from the scan while Spark still executes it. Block
    comments are treated as NON-nesting (stop at the first ``*/``), which only
    ever removes LESS than Spark would — never enough to hide live SQL. This
    de-commented text is used ONLY for the safety scan; the stored predicate
    keeps its comments so it round-trips and Spark strips them at runtime.

    Args:
        sql: The raw predicate or query text, possibly containing comments.

    Returns:
        The text with ``--`` line comments and ``/* */`` block comments removed
        (comments outside string/identifier literals only); newlines preserved.
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        # Quoted region (string literal ' " or backtick identifier). Spark
        # escapes an embedded quote by doubling it, so a doubled quote stays
        # in-region.
        if ch in _SQL_QUOTES:
            quote = ch
            out.append(ch)
            i += 1
            while i < n:
                out.append(sql[i])
                if sql[i] == quote:
                    if i + 1 < n and sql[i + 1] == quote:
                        out.append(sql[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        # Line comment `-- ...` -> drop to end of line, keep the newline.
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            i += 2
            while i < n and sql[i] != "\n":
                i += 1
            continue
        # Block comment `/* ... */` (non-nesting) -> drop the whole span.
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            i += 2
            while i < n and not (sql[i] == "*" and i + 1 < n and sql[i + 1] == "/"):
                i += 1
            i += 2  # skip the closing */ (harmless if unterminated)
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def escape_sql_string(value: str) -> str:
    """Escape a value for embedding in a SQL single-quoted string literal.

    Databricks SQL uses doubled single-quotes ('') for escaping,
    NOT backslash-escape (\\'). This function normalizes to the correct form.

    Note: this deliberately does NOT escape backslashes. On the Delta /
    Databricks SQL string-literal path a backslash is itself an escape
    character, so a value ending in ``\\`` would consume the following
    quote and let the literal break out. ``validate_fqn`` is relied upon to
    reject backslashes in any FQN before it reaches this function, so we do
    not widen the escaping here (which could silently double-escape values
    that were already correct in other call sites).
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
        raise ValueError(f"Invalid fully qualified name: '{fqn}'. Expected exactly three parts: catalog.schema.table")

    for part in parts:
        # A part that arrives already backtick-quoted (e.g. a caller passing
        # through a previously-quoted name) is unwrapped before validation —
        # the backticks themselves aren't part of the identifier.
        cleaned = part[1:-1] if len(part) >= 2 and part.startswith("`") and part.endswith("`") else part
        if not cleaned or len(cleaned) > _MAX_FQN_PART_LEN or not _FQN_PART_RE.match(cleaned):
            raise ValueError(
                f"Invalid fully qualified name: '{fqn}'. "
                f"Part '{part}' contains invalid characters. "
                "Each part must be 1-255 characters and must not contain a backtick, "
                "a backslash, or control characters."
            )

    return fqn


def validate_identifier(name: str) -> str:
    """Validate a single SQL identifier part (e.g. a column or slot name).

    Applies the same per-part character allowlist as :func:`validate_fqn` —
    rejecting backticks (the quoting delimiter), backslashes, and C0/C1 control
    characters, and capping length at 255. That guarantees the name can be
    backtick-quoted (via ``quote_fqn``-style doubling) without any break-out or
    log-injection risk, and is the identifier-side counterpart to
    :func:`escape_sql_string` for string literals.

    Raises ValueError if the name is empty or contains a disallowed character.
    Returns the name unchanged.
    """
    if not name or len(name) > _MAX_FQN_PART_LEN or not _FQN_PART_RE.match(name):
        raise ValueError(
            f"Invalid identifier: '{name}'. "
            "Must be 1-255 characters and must not contain a backtick, "
            "a backslash, or control characters."
        )
    return name


def fqn_needs_quoting(fqn: str) -> bool:
    """Return whether a three-part FQN requires backtick-quoting.

    A FQN is "simple" (and safe to hand to ``spark.table`` unquoted) only if
    it splits into exactly three parts and every part is a plain identifier
    (``^[a-zA-Z_][a-zA-Z0-9_]*$``). Anything else — quotes, spaces, leading
    digits, punctuation — must be routed through ``quote_fqn`` before use.

    Kept separate from ``validate_fqn`` so callers that pass a raw FQN
    straight to a Spark/SQL consumer can quote *only* the exotic names,
    leaving the byte representation of normal names unchanged.
    """
    parts = fqn.split(".")
    if len(parts) != 3:
        return True
    return not all(_SIMPLE_IDENT_RE.match(p) for p in parts)


def quote_ident(part: str) -> str:
    """Backtick-quote a single identifier part for Delta / Databricks SQL.

    Strips one existing layer of backtick wrapping first (the backticks are
    the quoting, not part of the identifier), then doubles any backtick left
    inside per Spark's escaping rule as defense in depth. This is the
    single-part building block behind :func:`quote_fqn`; use it directly when
    assembling an FQN from parts that may themselves contain dots (a dotted
    part must not be re-split by ``quote_fqn``) — e.g. a hyphenated or
    otherwise exotic catalog/schema name read from app config.
    """
    unwrapped = part[1:-1] if len(part) >= 2 and part.startswith("`") and part.endswith("`") else part
    return f"`{unwrapped.replace('`', '``')}`"


def quote_fqn(fqn: str) -> str:
    """Quote a validated FQN for safe embedding in SQL.

    Wraps each part in backticks (stripping any existing ones first) to
    prevent identifier injection. Any backtick remaining inside a part
    (there shouldn't be one if ``validate_fqn()`` was called first) is
    doubled per Spark's escaping rule, as defense in depth. Call
    ``validate_fqn()`` first.
    """
    return ".".join(quote_ident(p) for p in fqn.split("."))


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


# \A/\Z (not ^/$) so a trailing newline can't sneak past the end anchor.
_OBJECT_ID_RE = re.compile(r"\A[a-zA-Z0-9_-]{1,128}\Z")


def validate_object_id(object_id: str) -> str:
    """Validate a securable object id (registry rule / binding / data product id).

    Object ids are app-minted (``uuid4().hex`` or a truncation of it — see
    ``RulesCatalogService``, ``MonitoredTableService``, ``DataProductService``),
    so a strict allowlist is safe: letters, digits, underscore, hyphen, bounded
    length. This is the identifier-side counterpart to ``escape_sql_string``:
    object ids reach :mod:`permissions_service` as raw path parameters from any
    authenticated user and are interpolated into single-quoted SQL string
    literals via ``escape_sql_string``, which deliberately does not escape
    backslashes (see its docstring). Rejecting anything outside the allowlist
    here — before the value ever reaches SQL — closes that string-literal
    break-out class regardless of backend (Postgres/Lakebase or the Delta
    fallback).

    Raises ValueError if the id is empty, too long, or contains a disallowed
    character. Returns the id unchanged.
    """
    if not object_id or len(object_id) > 128 or not _OBJECT_ID_RE.match(object_id):
        raise ValueError(
            f"Invalid object id: '{object_id}'. "
            "Must be 1-128 characters using only letters, digits, underscores, or hyphens."
        )
    return object_id


def validate_entity_type(entity_type: str, valid_types: set[str]) -> str:
    """Validate that an entity type is in the allowed set.

    Raises ValueError if not valid. Returns the validated value.
    """
    if entity_type not in valid_types:
        raise ValueError(f"Invalid entity_type: '{entity_type}'. Must be one of {valid_types}")
    return entity_type
