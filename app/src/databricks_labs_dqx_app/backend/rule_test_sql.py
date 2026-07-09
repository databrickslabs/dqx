"""Pure SQL builders for the Rules Registry "Test rule" feature (P22-E).

Ported from dqlake's ``test_rule/sql_builder.py`` and adapted to DQX. The app
has no Spark in the request path, so a rule is tested by translating its SQL
predicate to a query and running the result on a SQL warehouse (OBO). The final
query always exposes a boolean ``__passed`` column carrying the per-row verdict.

DQX adaptation vs dqlake
------------------------
DQX registry ``sql`` / ``lowcode`` rules materialize as a **row-level**
``sql_expression`` check (``negate = polarity == "fail"``; see
``services/materializer.py``). ``sql_expression`` passes a row when the
expression is TRUE and ``negate`` is False, and passes when the expression is
FALSE when ``negate`` is True (see ``check_funcs.sql_expression``). So the
per-row "passed" expression is:

* ``polarity == "pass"``  -> ``(predicate)``
* ``polarity == "fail"``  -> ``(NOT (predicate))``

which is exactly dqlake's ``_passed_expr``. There is no aggregate / group-by /
join predicate classifier in DQX's registry model, so — unlike dqlake — only
the ROW evaluation shape is reproduced here. ``dqx_native`` rules are not
testable at all (the caller rejects them before reaching this module).

All functions here are pure: they take dicts/dataclasses and return SQL text.
No SDK, no DB, no I/O — so they are exhaustively unit-tested.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal

from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn, validate_identifier

SampleKind = Literal["records", "percent", "full"]
Polarity = Literal["pass", "fail"]

# DQX slot families (lowercase) -> the SQL type each ad-hoc column is TRY_CAST to
# so a typed grid cell round-trips as the right type. Anything else (text /
# array / any / unknown) stays STRING so arbitrary values are still allowed.
_FAMILY_SQL_TYPE: dict[str, str] = {
    "numeric": "DOUBLE",
    "temporal": "TIMESTAMP",
    "boolean": "BOOLEAN",
    "text": "STRING",
}

# Hidden verdict/ordinal columns excluded from the display grid.
PASSED_COL = "__passed"
ROW_IDX_COL = "__row_idx"

# C0/C1 control characters other than the common whitespace (\n, \r, \t), which
# are re-emitted as Spark escape sequences by ``_lit``. These have no legitimate
# use in a scalar test cell and are dropped for log-injection hygiene (CWE-117)
# once the break-out vectors (quote / backslash) are already neutralised.
_STRIP_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


def _sql_type_for_family(family: str | None) -> str:
    return _FAMILY_SQL_TYPE.get((family or "").lower(), "STRING")


def _q(identifier: str) -> str:
    """Validate then backtick-quote a Databricks identifier.

    Every column/slot name that reaches the built SQL is first validated with
    ``validate_identifier`` (rejecting backticks, backslashes, and control
    characters) and then backtick-quoted — doubling any residual backtick as
    belt-and-braces, exactly as ``quote_fqn`` does for FQN parts. This closes
    the identifier break-out vector for the ad-hoc VALUES/CTE header and the
    real column names substituted in table mode, on top of the predicate's
    ``is_sql_query_safe`` gate applied by the service. Raises ValueError on a
    disallowed identifier (surfaced by the route as a 400).
    """
    validate_identifier(identifier)
    return "`" + identifier.replace("`", "``") + "`"


def substitute_slots(text: str, mapping: dict[str, str]) -> str:
    """Replace every ``{{slot}}`` placeholder with its mapped, quoted column.

    Mirrors ``services/materializer._substitute_text`` (exact ``{{name}}``
    match) but emits a backtick-quoted identifier so the reference resolves
    against a real UC column (table mode) or the ad-hoc VALUES column named
    after the slot (manual mode), regardless of the column name's characters.
    """
    result = text
    for slot_name, column in mapping.items():
        result = result.replace("{{" + slot_name + "}}", _q(column))
    return result


def passed_expr(predicate: str, polarity: str) -> str:
    """Boolean SQL that is TRUE when a row satisfies the rule.

    Reproduces ``check_funcs.sql_expression`` (``negate = polarity == 'fail'``):
    a ``fail``-polarity predicate describes the *failure* shape, so a row passes
    when the predicate is NOT true.
    """
    if polarity == "pass":
        return f"({predicate})"
    return f"(NOT ({predicate}))"


# ---------------------------------------------------------------------------
# Table mode — sample a real UC table
# ---------------------------------------------------------------------------


@dataclass
class TableSource:
    table: str
    column_mapping: dict[str, str]  # slot name -> real column name
    sample_kind: SampleKind = "records"
    sample_value: int = 10000
    display_cap: int = 5000


def _sample_clause(kind: SampleKind, value: int) -> str:
    # TABLESAMPLE (n ROWS) is NOT random in Spark/Databricks (first n rows), so
    # a genuine random sample of n records orders by rand() + LIMIT. TABLESAMPLE
    # (p PERCENT) IS a real Bernoulli sample.
    if kind == "records":
        return f"ORDER BY rand() LIMIT {int(value)}"
    if kind == "percent":
        return f"TABLESAMPLE ({int(value)} PERCENT)"
    return ""  # full


def build_table_sql(predicate: str, polarity: str, src: TableSource) -> str:
    """Build the ROW test query for a real UC table sample.

    Returns every sampled row with its ``__passed`` verdict so the grid can
    tint each row. Raises ``ValueError`` (via ``validate_fqn``) on a malformed
    table name.
    """
    validate_fqn(src.table)
    table = quote_fqn(src.table)
    pred = substitute_slots(predicate, src.column_mapping)
    passed = passed_expr(pred, polarity)
    sample = _sample_clause(src.sample_kind, src.sample_value)
    return (
        f"WITH src AS (SELECT * FROM {table} {sample})\n"
        f"SELECT src.*, {passed} AS {PASSED_COL} FROM src\n"
        f"LIMIT {int(src.display_cap)}"
    )


# ---------------------------------------------------------------------------
# Manual (ad-hoc inline VALUES) mode
# ---------------------------------------------------------------------------


@dataclass
class AdhocSource:
    columns: list[str]  # grid column names == slot names
    rows: list[list[Any]]  # one list of cell values per input row
    families: dict[str, str] = field(default_factory=dict)  # column name -> family
    column_mapping: dict[str, str] = field(default_factory=dict)  # slot -> column (identity)
    display_cap: int = 5000


def _lit(value: Any) -> str:
    """Emit a single VALUES cell as a Databricks SQL literal.

    NULL / boolean cells are emitted as typed tokens (``NULL`` / ``'true'`` /
    ``'false'``) rather than by interpolating arbitrary user text. Every other
    non-null value is quoted as a STRING literal so each VALUES column is
    uniformly STRING (mixing ``5`` and ``'hi'`` in one column would be a type
    error); the per-family ``TRY_CAST`` in ``_cast_col`` does the real typing.

    Cell values are arbitrary user DATA (unlike FQNs, which ``validate_fqn``
    already strips of backslashes/control chars upstream), so the string path
    must close BOTH literal break-out vectors:

    * single quotes are doubled (``''``) per Databricks' literal escaping;
    * backslashes are doubled (``\\\\``). On the Databricks/Delta string-literal
      path a backslash is itself an escape character, so a value ending in
      ``\\`` would otherwise consume the closing quote and let the literal break
      out — the P22-E trailing-backslash injection, where the NEXT cell would
      splice as raw SQL. ``escape_sql_string`` (sql_utils) can skip this only
      because ``validate_fqn`` rejects backslashes before it; here there is no
      such upstream guard, so both quote AND backslash must be escaped.

    Order matters: backslashes are doubled FIRST, then quotes, then the common
    whitespace control chars are re-emitted as Spark escape sequences (their
    single backslash is intentional and not re-doubled); any remaining C0/C1
    control characters are dropped for log-injection hygiene. This is defence in
    depth beneath the fully-assembled-query ``is_sql_query_safe`` gate the
    service applies before execution.
    """
    if value is None or value == "":
        return "NULL"
    if isinstance(value, bool):
        return "'true'" if value else "'false'"
    text = str(value)
    text = text.replace("\\", "\\\\").replace("'", "''")
    text = text.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    text = _STRIP_CONTROL_RE.sub("", text)
    return "'" + text + "'"


def _cast_col(families: dict[str, str], col: str) -> str:
    if col == ROW_IDX_COL:
        return f"CAST({ROW_IDX_COL} AS BIGINT) AS {ROW_IDX_COL}"
    return f"TRY_CAST({_q(col)} AS {_sql_type_for_family(families.get(col))}) AS {_q(col)}"


def _values_cell(col: str, value: Any) -> str:
    if col == ROW_IDX_COL:
        return str(int(value))
    return _lit(value)


def build_adhoc_sql(predicate: str, polarity: str, src: AdhocSource) -> str:
    """Build the ROW test query over inline VALUES (manual test grid).

    A leading synthetic ``__row_idx`` ordinal is injected so the frontend can
    map each verdict back to its input row. Ragged rows are normalised to the
    column count (short rows padded with NULL, overflow dropped).
    """
    columns = [ROW_IDX_COL, *src.columns]
    indexed_rows = [[i, *row] for i, row in enumerate(src.rows)]

    cast_cols = ", ".join(_cast_col(src.families, c) for c in columns)
    collist = ", ".join(ROW_IDX_COL if c == ROW_IDX_COL else _q(c) for c in columns)

    if not indexed_rows:
        raw = ", ".join(f"NULL AS {ROW_IDX_COL if c == ROW_IDX_COL else _q(c)}" for c in columns)
        values_block = f"SELECT {raw} WHERE 1=0"
    else:
        rows_sql = ", ".join(
            "("
            + ", ".join(
                _values_cell(c, row[i] if i < len(row) else None) for i, c in enumerate(columns)
            )
            + ")"
            for row in indexed_rows
        )
        values_block = f"SELECT * FROM (VALUES {rows_sql}) AS raw ({collist})"

    pred = substitute_slots(predicate, src.column_mapping)
    passed = passed_expr(pred, polarity)
    return (
        f"WITH src AS (SELECT {cast_cols} FROM ({values_block}) AS raw2)\n"
        f"SELECT src.*, {passed} AS {PASSED_COL} FROM src\n"
        f"LIMIT {int(src.display_cap)}"
    )


# ---------------------------------------------------------------------------
# Result parsing
# ---------------------------------------------------------------------------


@dataclass
class TestRow:
    cells: dict[str, str | None]
    passed: bool
    row_idx: int | None = None


@dataclass
class TestRunResult:
    columns: list[str]
    rows: list[TestRow]
    truncated: bool


def _coerce_passed(raw: Any) -> bool:
    # statement_execution returns booleans as the strings "true"/"false".
    return raw is True or (isinstance(raw, str) and raw.lower() == "true")


def parse_result(rows: list[dict[str, str | None]], *, display_cap: int) -> TestRunResult:
    """Turn warehouse dict-rows into a :class:`TestRunResult`.

    ``__passed`` carries the verdict; ``__row_idx`` (when present) is the input
    row ordinal (manual mode). Both are stripped from the display ``cells``.
    Display columns are derived from the first row's key order, minus the hidden
    columns, so column order matches the warehouse manifest.
    """
    hidden = {PASSED_COL, ROW_IDX_COL}
    display_cols = [c for c in (rows[0].keys() if rows else []) if c not in hidden]
    parsed: list[TestRow] = []
    for row in rows:
        row_idx_raw = row.get(ROW_IDX_COL)
        parsed.append(
            TestRow(
                cells={c: row.get(c) for c in display_cols},
                passed=_coerce_passed(row.get(PASSED_COL)),
                row_idx=int(row_idx_raw) if row_idx_raw is not None else None,
            )
        )
    return TestRunResult(columns=display_cols, rows=parsed, truncated=len(parsed) >= display_cap)
