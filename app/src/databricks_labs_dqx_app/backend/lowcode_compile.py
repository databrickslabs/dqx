"""Pure Python compiler for the Rules-Registry low-code AST (B2-132).

A faithful port of the client-side compiler in ``ui/lib/lowcodeCompile.ts``
(itself ported from dqlake). The app has no Spark in the request path and no
backend low-code compiler existed until now — the visual builder compiled the
AST to SQL entirely in TypeScript on save. This module reproduces that exact
folding so the **AI "build with AI"** flow can propose a low-code rule
server-side, compile it to the same ``body`` payload the UI would have stored,
and safety-validate the result before it ever reaches the editor.

The stored ``body`` shape produced here is byte-for-byte what
``RegistryRuleFormDialog.buildDefinition`` writes for a low-code rule:

* simple row stack (no joins, no group-by)  ->  ``{"predicate": P}``
* joins and/or group-by                      ->  ``{"sql_query": Q, "merge_columns": [...]}``

Polarity is NOT baked in here — the compiled predicate is the *pass* condition
and the materializer's ``render_check`` applies ``negate`` from the rule's
polarity, exactly as for a hand-written sql-mode rule (see
``services/materializer.py`` and the ``lowcodeCompile.ts`` header).

All functions are pure (dicts in, SQL text out) — no SDK, no I/O — so they are
exhaustively unit-tested and never widen the ``Any`` surface beyond the
JSON-shaped AST the model returns.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# --- Operator / aggregate vocabulary (mirrors ui/lib/lowcodeOperators.ts) ----

# The types a text column can be validated against; ``value`` is what the AST
# stores, the mapped SQL type is the ``TRY_CAST`` target.
VALIDITY_SQL_TYPE: dict[str, str] = {
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "int": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP_NTZ",
    "boolean": "BOOLEAN",
    "binary": "BINARY",
}

# Operators legal per column family (families here are lowercase to match the
# Rules-Registry ``SlotFamily`` vocabulary — the builder uses uppercase, but the
# AI proposal declares slots with the lowercase family names).
# Kept in lock-step with the frontend catalog in ``ui/lib/lowcodeOperators.ts``
# (OPERATORS_BY_FAMILY there, uppercase families). If you add/remove an operator
# here, mirror it there and add its ``_row_sql`` arm below — the AI generator
# proposes only operators listed here and its output is compiled + safety-checked
# through this module, so an operator missing from either side is silently
# unusable.
OPERATORS_BY_FAMILY: dict[str, list[str]] = {
    "numeric": [
        "between",
        "=",
        "!=",
        ">=",
        ">",
        "<=",
        "<",
        "in",
        "not in",
        "is positive",
        "is negative",
        "is non-negative",
        "is a whole number",
        "is a multiple of",
        "passes luhn check",
    ],
    "text": [
        "equals",
        "not equals",
        "contains",
        "does not contain",
        "starts with",
        "ends with",
        "in",
        "not in",
        "matches regex",
        "does not match regex",
        "has length",
        "is longer than",
        "is shorter than",
        "length between",
        "is not empty",
        "is empty",
        "contains only digits",
        "is uppercase",
        "is lowercase",
        "is a valid uuid",
        "is a valid ipv4",
        "passes luhn check",
        "has leading or trailing whitespace",
        "has no leading or trailing whitespace",
        "is a valid",
        "is not a valid",
        "has positive sentiment",
        "has negative sentiment",
    ],
    "temporal": [
        "on or after",
        "on or before",
        "after",
        "before",
        "between",
        "is in last",
        "is in the future",
        "is in the past",
        "is today",
        "=",
        "!=",
    ],
    "boolean": ["is true", "is false"],
    "any": ["is null", "is not null", "=", "!=", "in", "not in", "is not empty", "is empty"],
}

# Aggregate name -> the column families it accepts ("any" = every family).
AGGREGATE_INPUT_FAMILIES: dict[str, list[str] | str] = {
    "count": "any",
    "count_distinct": "any",
    "null_rate": "any",
    "approx_count_distinct": "any",
    "any_value": "any",
    "mode": "any",
    "sum": ["numeric"],
    "avg": ["numeric"],
    "stddev": ["numeric"],
    "stddev_samp": ["numeric"],
    "variance": ["numeric"],
    "var_samp": ["numeric"],
    "median": ["numeric"],
    "percentile": ["numeric"],
    "percentile_approx": ["numeric"],
    "min": ["numeric", "temporal", "text"],
    "max": ["numeric", "temporal", "text"],
    "bool_and": ["boolean"],
    "bool_or": ["boolean"],
}

AGGREGATES: list[str] = list(AGGREGATE_INPUT_FAMILIES)

_AGG_SQL: dict[str, Callable[[str, float | int | None], str]] = {
    "count": lambda c, _p=None: f"COUNT({c})",
    "count_distinct": lambda c, _p=None: f"COUNT(DISTINCT {c})",
    "approx_count_distinct": lambda c, _p=None: f"APPROX_COUNT_DISTINCT({c})",
    "null_rate": lambda c, _p=None: f"(SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0))",
    "sum": lambda c, _p=None: f"SUM({c})",
    "avg": lambda c, _p=None: f"AVG({c})",
    "min": lambda c, _p=None: f"MIN({c})",
    "max": lambda c, _p=None: f"MAX({c})",
    "stddev": lambda c, _p=None: f"STDDEV_POP({c})",
    "stddev_samp": lambda c, _p=None: f"STDDEV_SAMP({c})",
    "variance": lambda c, _p=None: f"VAR_POP({c})",
    "var_samp": lambda c, _p=None: f"VAR_SAMP({c})",
    "median": lambda c, _p=None: f"MEDIAN({c})",
    "percentile": lambda c, p=None: f"PERCENTILE({c}, {p if p is not None else 0.5})",
    "percentile_approx": lambda c, p=None: f"PERCENTILE_APPROX({c}, {p if p is not None else 0.5})",
    "bool_and": lambda c, _p=None: f"BOOL_AND({c})",
    "bool_or": lambda c, _p=None: f"BOOL_OR({c})",
    "any_value": lambda c, _p=None: f"ANY_VALUE({c})",
    "mode": lambda c, _p=None: f"MODE({c})",
}

_COMPARISON_OPS = frozenset({"=", "!=", "<", "<=", ">", ">="})

# Global ``{{token}}`` scanner (the anchored single-slot form lives in
# ai_rules_service as ``_SLOT_TOKEN_RE``).
_SLOT_TOKEN_SCAN_RE = re.compile(r"\{\{\s*(.+?)\s*\}\}")


# --- Value / reference helpers (mirror lowcodeCompile.ts) --------------------


def _ref(column: str) -> str:
    """Qualified (dotted) refs name a joined-table column and stay raw; plain
    refs name a declared slot and are wrapped as ``{{name}}`` placeholders the
    materializer substitutes with the real column."""
    return column if "." in column else f"{{{{{column}}}}}"


def _quote(value: object) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _column_ref_name(value: object) -> str | None:
    """The referenced column name when *value* is a column reference, else None.

    A column reference is ``{"$col": "<column>"}`` with a non-empty string name
    (item 42); mirrors ``isColumnRef`` in ``lowcodeAst.ts``. Returned so callers
    can both test for and read the name in one narrowing step.
    """
    if isinstance(value, dict):
        name = value.get("$col")
        if isinstance(name, str) and name:
            return name
    return None


def _value_sql(value: object) -> str:
    """Render a comparison RHS operand — a column reference OR a literal.

    Mirrors ``valueSql`` in ``lowcodeCompile.ts`` (item 42): a column reference
    ``{"$col": "b"}`` emits ``_ref("b")`` (a plain name -> ``{{b}}`` placeholder,
    a joined-table column -> raw), so one column can be compared against another;
    anything else is quoted as a literal.
    """
    name = _column_ref_name(value)
    return _ref(name) if name is not None else _quote(value)


def _quote_list(values: list[object]) -> str:
    return ", ".join(_value_sql(v) for v in values)


def _like_literal(value: object) -> str:
    return str(value).replace("'", "''")


def _split_top_level_commas(value: str) -> list[str]:
    """Split at TOP-LEVEL commas only — commas inside parens or single-quoted
    literals are not split points (mirrors ``splitTopLevelCommas``)."""
    out: list[str] = []
    depth = 0
    in_quote = False
    start = 0
    i = 0
    length = len(value)
    while i < length:
        ch = value[i]
        if in_quote:
            if ch == "'" and i + 1 < length and value[i + 1] == "'":
                i += 2
                continue
            if ch == "'":
                in_quote = False
            i += 1
            continue
        if ch == "'":
            in_quote = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif ch == "," and depth == 0:
            out.append(value[start:i])
            start = i + 1
        i += 1
    out.append(value[start:])
    return [s.strip() for s in out if s.strip()]


def _join_key_refs(joins: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for join in joins or []:
        if join.get("join_type") == "CROSS" or not join.get("target_table"):
            continue
        for key in join.get("keys") or []:
            column_ref = key.get("column_ref")
            if not column_ref:
                continue
            token = _ref(column_ref)
            if token in seen:
                continue
            seen.add(token)
            out.append(token)
    return out


def _agg_expr(spec: dict[str, Any]) -> str:
    agg = spec.get("aggregate")
    col = spec.get("column_ref")
    if not agg or agg not in _AGG_SQL:
        return ""
    if not col:
        return ""
    return _AGG_SQL[agg](_ref(col), spec.get("aggregate_param"))


def _row_sql(left: str, operator: str, value: object) -> str:
    op = operator
    if op in _COMPARISON_OPS:
        return f"{left} {op} {_value_sql(value)}"
    if op == "equals":
        return f"{left} = {_value_sql(value)}"
    if op == "not equals":
        return f"{left} != {_value_sql(value)}"
    if op == "contains":
        return f"{left} LIKE '%{_like_literal(value)}%'"
    if op == "does not contain":
        return f"{left} NOT LIKE '%{_like_literal(value)}%'"
    if op == "starts with":
        return f"{left} LIKE '{_like_literal(value)}%'"
    if op == "ends with":
        return f"{left} LIKE '%{_like_literal(value)}'"
    if op == "matches regex":
        return f"{left} RLIKE {_quote(value)}"
    if op == "between":
        lo, hi = (value[0], value[1]) if isinstance(value, list) and len(value) >= 2 else (None, None)
        return f"{left} BETWEEN {_value_sql(lo)} AND {_value_sql(hi)}"
    if op == "in":
        return f"{left} IN ({_quote_list(value if isinstance(value, list) else [])})"
    if op == "not in":
        return f"{left} NOT IN ({_quote_list(value if isinstance(value, list) else [])})"
    if op == "is null":
        return f"{left} IS NULL"
    if op == "is not null":
        return f"{left} IS NOT NULL"
    if op == "is true":
        return f"{left} = TRUE"
    if op == "is false":
        return f"{left} = FALSE"
    if op == "before":
        return f"{left} < {_value_sql(value)}"
    if op == "after":
        return f"{left} > {_value_sql(value)}"
    if op == "on or before":
        return f"{left} <= {_value_sql(value)}"
    if op == "on or after":
        return f"{left} >= {_value_sql(value)}"
    if op == "is in last":
        obj = value if isinstance(value, dict) else {}
        number = obj.get("number", 0)
        unit = obj.get("unit", "days")
        return f"{left} >= current_timestamp() - INTERVAL '{number} {unit}'"
    if op in ("is a valid", "is not a valid"):
        as_type = VALIDITY_SQL_TYPE.get(str(value))
        if not as_type:
            return ""
        null_check = "IS NOT NULL" if op == "is a valid" else "IS NULL"
        return f"TRY_CAST({left} AS {as_type}) {null_check}"
    if op == "has leading or trailing whitespace":
        return f"{left} != TRIM({left})"
    if op == "has no leading or trailing whitespace":
        return f"{left} = TRIM({left})"
    # --- Length (mirror lowcodeCompile.ts) ---
    if op == "has length":
        return f"length({left}) = {_quote(value)}"
    if op == "is longer than":
        return f"length({left}) > {_quote(value)}"
    if op == "is shorter than":
        return f"length({left}) < {_quote(value)}"
    if op == "length between":
        lo, hi = (value[0], value[1]) if isinstance(value, list) and len(value) >= 2 else (None, None)
        return f"length({left}) BETWEEN {_value_sql(lo)} AND {_value_sql(hi)}"
    if op == "is not empty":
        return f"length(trim({left})) > 0"
    if op == "is empty":
        return f"length(trim({left})) = 0"
    # --- Text pattern / format ---
    if op == "does not match regex":
        return f"NOT ({left} RLIKE {_quote(value)})"
    if op == "contains only digits":
        return f"{left} RLIKE '^[0-9]+$'"
    if op == "is uppercase":
        return f"{left} = upper({left})"
    if op == "is lowercase":
        return f"{left} = lower({left})"
    if op == "is a valid uuid":
        return f"{left} RLIKE '^[0-9a-fA-F]{{8}}-[0-9a-fA-F]{{4}}-[0-9a-fA-F]{{4}}-[0-9a-fA-F]{{4}}-[0-9a-fA-F]{{12}}$'"
    if op == "is a valid ipv4":
        return f"{left} RLIKE '^((25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.){{3}}(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])$'"
    # --- Numeric predicates ---
    if op == "is positive":
        return f"{left} > 0"
    if op == "is negative":
        return f"{left} < 0"
    if op == "is non-negative":
        return f"{left} >= 0"
    if op == "is a whole number":
        return f"{left} = round({left})"
    if op == "is a multiple of":
        return f"mod({left}, {_quote(value)}) = 0"
    # --- Temporal predicates ---
    if op == "is in the future":
        return f"{left} > current_timestamp()"
    if op == "is in the past":
        return f"{left} < current_timestamp()"
    if op == "is today":
        return f"to_date({left}) = current_date()"
    # --- AI (Foundation Model) checks ---
    if op == "has positive sentiment":
        return f"ai_analyze_sentiment({left}) = 'positive'"
    if op == "has negative sentiment":
        return f"ai_analyze_sentiment({left}) = 'negative'"
    # --- Luhn checksum via the Databricks built-in luhn_check() ---
    if op == "passes luhn check":
        digits = f"regexp_replace({left}, '[^0-9]', '')"
        return f"length({digits}) > 0 AND luhn_check({digits})"
    return ""


def _compile_row(row: dict[str, Any]) -> str:
    if row.get("kind") == "row":
        column_ref = row.get("column_ref")
        if not column_ref:
            return ""
        return _row_sql(_ref(column_ref), str(row.get("operator", "")), row.get("value"))
    left = _agg_expr(row)
    if not left:
        return ""
    op = str(row.get("operator", ""))
    value = row.get("value")
    if op in _COMPARISON_OPS:
        if isinstance(value, dict) and "aggregate" in value:
            right = _agg_expr(value)
        else:
            right = _quote(value)
        return f"{left} {op} {right}"
    if op == "is null":
        return f"{left} IS NULL"
    if op == "is not null":
        return f"{left} IS NOT NULL"
    if op == "between":
        lo, hi = (value[0], value[1]) if isinstance(value, list) and len(value) >= 2 else (None, None)
        return f"{left} BETWEEN {_quote(lo)} AND {_quote(hi)}"
    return ""


def compile_ast_to_sql(ast: dict[str, Any]) -> str:
    """Compile the row stack into a single boolean *pass* condition."""
    rows = ast.get("rows") or []
    if not rows:
        return ""
    parts: list[str] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        frag = _compile_row(row)
        if not frag:
            continue
        if i == 0:
            parts.append(frag)
        else:
            parts.append(f"{row.get('combinator') or 'AND'} {frag}")
    return " ".join(parts)


def compile_joins_to_sql(joins: list[dict[str, Any]]) -> str:
    """Compile joins into a ``LEFT JOIN … ON …`` FROM-clause fragment."""
    if not joins:
        return ""
    type_sql = {
        "INNER": "INNER JOIN",
        "LEFT": "LEFT JOIN",
        "RIGHT": "RIGHT JOIN",
        "FULL": "FULL OUTER JOIN",
        "LEFT SEMI": "LEFT SEMI JOIN",
        "LEFT ANTI": "LEFT ANTI JOIN",
        "CROSS": "CROSS JOIN",
    }
    out: list[str] = []
    for join in joins:
        target = join.get("target_table")
        join_type = join.get("join_type")
        keys = join.get("keys") or []
        if not target or (join_type != "CROSS" and not keys):
            continue
        head = f"{type_sql.get(str(join_type), 'INNER JOIN')} {target}"
        if join_type == "CROSS":
            out.append(head)
            continue
        conds = [
            f"{target}.{k['joined_column']} = {_ref(k['column_ref'])}"
            for k in keys
            if k.get("joined_column") and k.get("column_ref")
        ]
        out.append(f"{head} ON {' AND '.join(conds)}")
    return " ".join(out)


@dataclass
class CompiledLowcodeBody:
    """Result of folding an AST + group-by into the stored ``body`` payload."""

    predicate: str | None = None
    sql_query: str | None = None
    merge_columns: list[str] | None = None


def compile_lowcode_body(ast: dict[str, Any], group_by: str) -> CompiledLowcodeBody:
    """Fold the row predicate, joins and group-by into the single body payload
    the materializer's sql-mode path consumes (mirrors ``compileLowcodeBody``)."""
    predicate = compile_ast_to_sql(ast)
    joins_sql = compile_joins_to_sql(ast.get("joins") or [])
    gb_columns = _split_top_level_commas(group_by or "")

    if not joins_sql and not gb_columns:
        return CompiledLowcodeBody(predicate=predicate)

    fail_cond = f"NOT ({predicate})"
    from_clause = "{{input_view}}" + (f" {joins_sql}" if joins_sql else "")

    if gb_columns:
        gb_list = ", ".join(gb_columns)
        return CompiledLowcodeBody(
            sql_query=f"SELECT {gb_list}, ({fail_cond}) AS condition FROM {from_clause} GROUP BY {gb_list}",
            merge_columns=gb_columns,
        )

    key_refs = _join_key_refs(ast.get("joins") or [])
    if key_refs:
        return CompiledLowcodeBody(
            sql_query=f"SELECT {', '.join(key_refs)}, ({fail_cond}) AS condition FROM {from_clause}",
            merge_columns=key_refs,
        )

    return CompiledLowcodeBody(sql_query=f"SELECT ({fail_cond}) AS condition FROM {from_clause}")


def lowcode_is_usable(ast: dict[str, Any]) -> bool:
    """True when the AST carries at least one row the compiler can represent.

    Mirrors dqlake's ``_lowcode_rows_usable`` gate: an AST that compiles to an
    empty predicate (no rows, or only rows with unknown operators the builder
    can't represent) is not a valid low-code rule, so the AI caller falls
    through to the ``dqx_native`` attempt rather than emitting a broken body.
    """
    return bool(compile_ast_to_sql(ast))


def extract_slot_tokens(*sql_fragments: str | None) -> list[str]:
    """Return the distinct ``{{slot}}`` placeholder names across SQL fragments,
    in first-appearance order.

    Used to derive a rule's declared column slots from the compiled body so
    every placeholder the materializer must substitute has a matching slot —
    the safe analogue of dqlake's column reconciliation, applied to the already
    compiled SQL rather than the raw AST.
    """
    seen: set[str] = set()
    out: list[str] = []
    for fragment in sql_fragments:
        if not fragment:
            continue
        for match in _SLOT_TOKEN_SCAN_RE.finditer(fragment):
            name = match.group(1).strip()
            # Qualified joined-table columns (e.g. ``orders.total``) are emitted
            # raw, never as placeholders, so any token here is a real slot name;
            # skip the reserved input-view marker.
            if not name or name == "input_view" or name in seen:
                continue
            seen.add(name)
            out.append(name)
    return out


def lowcode_prompt_vocab() -> str:
    """Operator / aggregate vocabulary block for the low-code AI system prompt.

    Built from the same constants the compiler uses so prompt and compiler can
    never drift. Trimmed port of dqlake's ``_lowcode_vocab_section`` +
    ``_LOWCODE_NO_WINDOW_HINT`` uniqueness/group-by guidance.
    """
    ops = "\n".join(
        f"  {family}: {', '.join(repr(o) for o in OPERATORS_BY_FAMILY[family])}"
        for family in ("numeric", "text", "temporal", "boolean", "any")
    )
    return (
        "Pick each row's `operator` from the legal vocabulary for the chosen column's family. "
        "The 'any'-family operators work on every column.\n"
        f"Legal operators by family:\n{ops}\n"
        "Operator value shapes:\n"
        "  - between / length between -> [lo, hi] (two-element list)\n"
        "  - in / not in -> list of literals, e.g. ['a', 'b']\n"
        "  - has length / is longer than / is shorter than / is a multiple of -> a single number\n"
        "  - is null / is not null / is true / is false / has (no) leading or trailing whitespace / "
        "is empty / is not empty / contains only digits / is uppercase / is lowercase / "
        "is a valid uuid / is a valid ipv4 / passes luhn check / is positive / is negative / "
        "is non-negative / is a whole number / is in the future / is in the past / is today / "
        "has positive sentiment / has negative sentiment -> null (value ignored)\n"
        "  - is a valid / is not a valid -> value is the type name (one of: "
        f"{', '.join(VALIDITY_SQL_TYPE)})\n"
        '  - is in last -> {"number": int, "unit": "days|weeks|months|years|hours|minutes"}\n'
        "  - matches regex / does not match regex -> a single regex string literal\n"
        "  - everything else -> a single literal\n"
        "Common phrasings:\n"
        '  - "must be X or Y" -> operator "in", value ["X", "Y"]\n'
        '  - "must not be empty" -> operator "is not null"\n'
        '  - "must be between A and B" -> operator "between", value [A, B]\n'
        '  - "must look like a number" -> operator "is a valid", value "double"\n'
        f"Aggregates (use kind=\"aggregated\" for group-level metrics): {', '.join(AGGREGATES)}. "
        "count/count_distinct/null_rate/approx_count_distinct/any_value/mode accept any family; "
        "sum/avg/stddev/variance/median/percentile require numeric; min/max accept numeric, temporal or "
        "text; bool_and/bool_or require boolean. percentile/percentile_approx also take a numeric "
        "aggregate_param (the quantile, 0..1).\n"
        "==== UNIQUENESS ====\n"
        "'X must be unique' / 'no duplicate X' / 'X is a key' ALWAYS translate to ONE aggregated row "
        'plus group_by_columns naming X: row {"kind": "aggregated", "combinator": null, '
        '"aggregate": "count", "column_ref": "<X>", "operator": "=", "value": 1} with '
        'group_by_columns "{{<X>}}". For \'X unique per Y\', group_by_columns lists both keys, '
        'e.g. "{{X}}, {{Y}}". NEVER express uniqueness with IN, LIKE, any_value(), or two rows.\n'
        "==== GROUP-LEVEL METRICS ====\n"
        "Window functions, OVER(), PARTITION BY and subqueries are NOT supported. For anything "
        "'per something' ('per customer', 'by region'), use kind=\"aggregated\" rows AND set "
        "group_by_columns to a comma-separated list of {{column_ref}} placeholders naming the group "
        "key(s). An aggregated row with group_by_columns null means 'across the whole table' — only "
        "use that when the rule is genuinely table-wide.\n"
        "Every column_ref (and every name in `columns` / group_by_columns) MUST be snake_case: "
        "lowercase letters, digits and underscores only. Joined-table columns use their qualified "
        "form raw (e.g. prod.crm.customers.region), never wrapped in {{...}}."
    )


# Re-exported so callers can build slots without hand-rolling the field set.
__all__ = [
    "CompiledLowcodeBody",
    "compile_ast_to_sql",
    "compile_joins_to_sql",
    "compile_lowcode_body",
    "extract_slot_tokens",
    "lowcode_is_usable",
    "lowcode_prompt_vocab",
    "OPERATORS_BY_FAMILY",
    "AGGREGATES",
    "VALIDITY_SQL_TYPE",
]
