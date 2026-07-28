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

which is exactly dqlake's ``_passed_expr``. ``dqx_native`` rules are compiled
to a row-level SQL predicate by :mod:`native_test_predicate` before reaching
this module; dataset / geo / UDF checks are rejected upstream.

Cross-table rules materialize as ``sql_query`` instead — a whole SELECT that
reads from ``{{input_view}}`` and joins reference tables — so their verdict
comes from the query's own condition column rather than from wrapping a
predicate. :func:`build_query_test_sql` handles that shape (see
:func:`condition_passed_expr` for why its polarity handling is the inverse of
``passed_expr``'s), and :func:`is_query_shaped` decides which builder applies.

All functions here are pure: they take dicts/dataclasses and return SQL text.
No SDK, no DB, no I/O — so they are exhaustively unit-tested.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from collections.abc import Iterable
from typing import Any, Literal

from databricks_labs_dqx_app.backend.sql_utils import (
    quote_fqn,
    strip_sql_line_comments,
    validate_fqn,
    validate_identifier,
)

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

# Cross-table (``sql_query``) rules. The rule body is a whole SELECT rather than
# a boolean predicate, so it is tested by running the query itself against a
# sampled CTE standing in for the monitored table, and reading the verdict off
# the query's own condition column.
SAMPLE_CTE = "src"
CONDITION_COL = "condition"
INPUT_VIEW_SLOT = "input_view"
# Prefix for a manual reference grid's CTE, so it cannot collide with the input
# grid's ``src`` even for a slot named ``src``.
REF_CTE_PREFIX = "__ref_"
# Matches DQX's own placeholder scan (``check_funcs.sql_query._replace_template``),
# whitespace inside the braces included.
_INPUT_VIEW_RE = re.compile(r"\{\{\s*" + INPUT_VIEW_SLOT + r"\s*\}\}")
_QUERY_SHAPE_RE = re.compile(r"^\s*select\b", re.IGNORECASE)
_CONDITION_REF_RE = re.compile(r"\b" + CONDITION_COL + r"\b", re.IGNORECASE)

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


def is_query_shaped(text: str) -> bool:
    """Whether *text* is a whole ``SELECT`` (a cross-table ``sql_query`` rule).

    Mirrors the authoring-side classifier (``lib/lowcodeCompile.buildSqlBody``,
    which persists a leading-``SELECT`` body as ``sql_query`` rather than
    ``sql_expression``) so what the editor stores and what the test runs agree on
    the rule's shape. Scanned with line comments stripped, since an author's
    leading ``-- note`` block would otherwise hide the ``SELECT``.
    """
    return bool(_QUERY_SHAPE_RE.match(strip_sql_line_comments(text).strip()))


def substitute_table_slots(text: str, mapping: dict[str, str]) -> str:
    """Replace every ``family="table"`` ``{{slot}}`` with a quoted table FQN.

    The column counterpart (:func:`substitute_slots`) emits a single quoted
    identifier, which would render ``main.sales.customers`` as one absurd column
    name; a table slot must become a three-part quoted FQN instead. Each value is
    validated by ``validate_fqn`` before quoting, so a malformed or injected name
    is rejected rather than assembled into the query.
    """
    result = text
    for slot_name, fqn in mapping.items():
        validate_fqn(fqn)
        result = result.replace("{{" + slot_name + "}}", quote_fqn(fqn))
    return result


def substitute_input_view(text: str, alias: str = SAMPLE_CTE) -> str:
    """Point the rule's ``{{input_view}}`` marker at the sampled CTE.

    At runtime DQX registers the monitored DataFrame as a temp view and swaps it
    in here; the test stands the sample in for it, which is what makes the query
    runnable against a real table.
    """
    return _INPUT_VIEW_RE.sub(alias, text)


def require_input_view(query: str, column_slots: Iterable[str]) -> None:
    """Reject a query that uses column slots but never reads ``{{input_view}}``.

    A column slot resolves to a bare identifier, so a query that names one
    without reading the monitored data has no relation to resolve it against and
    dies deep inside Spark with ``UNRESOLVED_COLUMN`` — at run time just as in
    the test. Caught here so the author is told what is actually wrong with the
    rule instead of being handed the warehouse's error.

    Reading only reference tables is legitimate (a dataset-level verdict about
    another table), hence the guard triggers on the combination, not on a
    missing ``{{input_view}}`` alone.

    Raises:
        ValueError: the query uses column slots and never reads the input view.
    """
    scan = strip_sql_line_comments(query)
    if _INPUT_VIEW_RE.search(scan):
        return
    used = [s for s in column_slots if re.search(r"\{\{\s*" + re.escape(s) + r"\s*\}\}", scan)]
    if not used:
        return
    listed = ", ".join(f"{{{{{s}}}}}" for s in used)
    raise ValueError(
        f"The query uses columns of the table being checked ({listed}) but never reads it. "
        f"Add FROM {{{{{INPUT_VIEW_SLOT}}}}} — that placeholder becomes the monitored table when the rule runs."
    )


def condition_passed_expr(condition_ref: str, polarity: str) -> str:
    """Boolean SQL that is TRUE when a row of the query's output PASSES.

    Reproduces ``check_funcs.sql_query`` + ``make_condition`` exactly. The
    condition column flags a VIOLATION, and ``make_condition`` only fails a row
    when its condition evaluates to TRUE (NULL and FALSE both pass), so with
    ``negate`` (i.e. ``polarity == "fail"``) the failing case is the condition
    being *FALSE* and a NULL still passes. Hence the asymmetric COALESCE
    defaults: FALSE when un-negated, TRUE when negated.

    Note this is the INVERSE of :func:`passed_expr`, which handles
    ``sql_expression`` predicates — those describe when a row is *good*, whereas
    a query's condition column describes when it is *bad*.
    """
    if polarity == "fail":
        return f"COALESCE({condition_ref}, TRUE)"
    return f"(NOT COALESCE({condition_ref}, FALSE))"


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
    # Cross-table rules only: ``family="table"`` slot name -> reference table FQN.
    table_mapping: dict[str, str] = field(default_factory=dict)


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


def build_query_test_sql(query: str, polarity: str, src: TableSource) -> str:
    """Build the test query for a cross-table (``sql_query``) rule.

    Unlike :func:`build_table_sql`, which embeds a boolean predicate per sampled
    row, the rule here IS a query: it selects from ``{{input_view}}`` and joins
    reference tables. So the sample becomes a CTE the query reads from, the
    query runs as a subquery, and the verdict is read off its condition column:

    .. code-block:: sql

        WITH src AS (SELECT * FROM `c`.`s`.`monitored` ORDER BY rand() LIMIT 1000)
        SELECT q.*, (NOT COALESCE(q.`condition`, FALSE)) AS __passed
        FROM (<the rule's query, placeholders resolved>) q
        LIMIT 5000

    The result grid therefore shows the rows the rule's QUERY returns (its merge
    keys and condition), not every sampled input row — a query with a ``WHERE``
    that keeps only violations legitimately returns just those.

    Substitution order matters: table slots and column slots are resolved first,
    then ``{{input_view}}``. A slot sharing the reserved ``input_view`` name is
    dropped from both mappings beforehand, so an author who mistakenly declared
    it can't redirect the query away from the sample being tested.

    Raises:
        ValueError: the table FQN or an identifier is malformed, the query has no
            condition column to read a verdict from, or it uses column slots
            without reading ``{{input_view}}``.
    """
    validate_fqn(src.table)
    if not _CONDITION_REF_RE.search(strip_sql_line_comments(query)):
        raise ValueError(
            f"A cross-table rule's query must return a '{CONDITION_COL}' column "
            f"(e.g. `(c.id IS NULL) AS {CONDITION_COL}`) for the test to read a verdict from."
        )
    table_mapping = {k: v for k, v in src.table_mapping.items() if k != INPUT_VIEW_SLOT}
    column_mapping = {k: v for k, v in src.column_mapping.items() if k != INPUT_VIEW_SLOT}
    require_input_view(query, column_mapping)
    resolved = substitute_table_slots(query, table_mapping)
    resolved = substitute_slots(resolved, column_mapping)
    resolved = substitute_input_view(resolved)
    passed = condition_passed_expr(f"q.{_q(CONDITION_COL)}", polarity)
    sample = _sample_clause(src.sample_kind, src.sample_value)
    return (
        f"WITH {SAMPLE_CTE} AS (SELECT * FROM {quote_fqn(src.table)} {sample})\n"
        f"SELECT q.*, {passed} AS {PASSED_COL}\n"
        f"FROM (\n{resolved}\n) q\n"
        f"LIMIT {int(src.display_cap)}"
    )


# ---------------------------------------------------------------------------
# Manual (ad-hoc inline VALUES) mode
# ---------------------------------------------------------------------------


@dataclass
class AdhocGrid:
    """One inline ``VALUES`` grid standing in for a reference table.

    Unlike the input grid — whose columns ARE the rule's column slots — a
    reference grid's columns are the real column names of the table it stands in
    for (``id``, ``tier``, …), because the rule's SQL refers to them through its
    own join alias (``c.id``).
    """

    columns: list[str]
    rows: list[list[Any]]
    families: dict[str, str] = field(default_factory=dict)  # column name -> family


@dataclass
class AdhocSource:
    columns: list[str]  # grid column names == slot names
    rows: list[list[Any]]  # one list of cell values per input row
    families: dict[str, str] = field(default_factory=dict)  # column name -> family
    column_mapping: dict[str, str] = field(default_factory=dict)  # slot -> column (identity)
    display_cap: int = 5000
    # Cross-table rules: ``family="table"`` slot name -> the grid standing in for
    # that reference table. Each becomes its own CTE, so an orphan check can be
    # tested against fabricated data without creating any table.
    ref_grids: dict[str, AdhocGrid] = field(default_factory=dict)


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


def _grid_select(
    columns: list[str],
    rows: list[list[Any]],
    families: dict[str, str],
    *,
    row_idx: bool,
) -> str:
    """Emit one grid as a typed ``SELECT`` over inline ``VALUES``.

    Ragged rows are normalised to the column count (short rows padded with NULL,
    overflow dropped). With *row_idx* a leading synthetic ``__row_idx`` ordinal is
    injected so the frontend can map each verdict back to its input row; a
    reference grid needs no ordinal, and adding one would leak a column into the
    rule's own ``SELECT *``.
    """
    cols = [ROW_IDX_COL, *columns] if row_idx else list(columns)
    grid_rows = [[i, *row] for i, row in enumerate(rows)] if row_idx else [list(r) for r in rows]

    cast_cols = ", ".join(_cast_col(families, c) for c in cols)
    collist = ", ".join(ROW_IDX_COL if c == ROW_IDX_COL else _q(c) for c in cols)

    if not grid_rows:
        raw = ", ".join(f"NULL AS {ROW_IDX_COL if c == ROW_IDX_COL else _q(c)}" for c in cols)
        values_block = f"SELECT {raw} WHERE 1=0"
    else:
        rows_sql = ", ".join(
            "(" + ", ".join(_values_cell(c, row[i] if i < len(row) else None) for i, c in enumerate(cols)) + ")"
            for row in grid_rows
        )
        values_block = f"SELECT * FROM (VALUES {rows_sql}) AS raw ({collist})"

    return f"SELECT {cast_cols} FROM ({values_block}) AS raw2"


def ref_cte_name(slot_name: str) -> str:
    """Quoted CTE identifier standing in for the reference table in *slot_name*.

    Prefixed so it can never collide with the input grid's ``src`` CTE, even if a
    slot is itself named ``src``. Validated before quoting, like every other
    identifier that reaches the assembled SQL.
    """
    validate_identifier(slot_name)
    return _q(f"{REF_CTE_PREFIX}{slot_name}")


def build_adhoc_sql(predicate: str, polarity: str, src: AdhocSource) -> str:
    """Build the ROW test query over inline VALUES (manual test grid).

    A leading synthetic ``__row_idx`` ordinal is injected so the frontend can
    map each verdict back to its input row.
    """
    pred = substitute_slots(predicate, src.column_mapping)
    passed = passed_expr(pred, polarity)
    return (
        f"WITH src AS ({_grid_select(src.columns, src.rows, src.families, row_idx=True)})\n"
        f"SELECT src.*, {passed} AS {PASSED_COL} FROM src\n"
        f"LIMIT {int(src.display_cap)}"
    )


def build_adhoc_query_sql(query: str, polarity: str, src: AdhocSource) -> str:
    """Build the test query for a cross-table / dataset-level rule over manual grids.

    The manual counterpart of :func:`build_query_test_sql`: instead of sampling
    real tables, every data source the rule reads is an inline ``VALUES`` grid —
    the input grid becomes ``src``, and each ``family="table"`` slot becomes its
    own CTE — so an orphan check can be exercised on fabricated rows without
    creating a single table:

    .. code-block:: sql

        WITH src AS (SELECT …VALUES…), `__ref_ref_table` AS (SELECT …VALUES…)
        SELECT q.*, (NOT COALESCE(q.`condition`, FALSE)) AS __passed
        FROM (<the rule's query, placeholders resolved>) q

    No ``__row_idx`` is projected: the rule's query decides which rows come back
    (it may aggregate to one, or filter to violations only), so verdicts can't be
    mapped onto input-grid rows and the result is shown as its own grid.

    Raises:
        ValueError: an identifier is malformed, the query has no condition column,
            a table slot has no grid to stand in for it, or the query uses column
            slots without reading ``{{input_view}}``.
    """
    if not _CONDITION_REF_RE.search(strip_sql_line_comments(query)):
        raise ValueError(
            f"A cross-table rule's query must return a '{CONDITION_COL}' column "
            f"(e.g. `(c.id IS NULL) AS {CONDITION_COL}`) for the test to read a verdict from."
        )
    ref_grids = {k: v for k, v in src.ref_grids.items() if k != INPUT_VIEW_SLOT}
    column_mapping = {k: v for k, v in src.column_mapping.items() if k != INPUT_VIEW_SLOT}
    require_input_view(query, column_mapping)

    resolved = query
    for slot_name in ref_grids:
        resolved = resolved.replace("{{" + slot_name + "}}", ref_cte_name(slot_name))
    resolved = substitute_slots(resolved, column_mapping)
    resolved = substitute_input_view(resolved)

    ctes = [f"{SAMPLE_CTE} AS ({_grid_select(src.columns, src.rows, src.families, row_idx=False)})"]
    for slot_name, grid in ref_grids.items():
        body = _grid_select(grid.columns, grid.rows, grid.families, row_idx=False)
        ctes.append(f"{ref_cte_name(slot_name)} AS ({body})")

    passed = condition_passed_expr(f"q.{_q(CONDITION_COL)}", polarity)
    return (
        "WITH " + ",\n     ".join(ctes) + "\n"
        f"SELECT q.*, {passed} AS {PASSED_COL}\n"
        f"FROM (\n{resolved}\n) q\n"
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
