"""RuleTestService — run a registry rule's SQL predicate against sample data (P22-E).

Powers the Rules Registry "Test" tab. Two run modes, both executed on the
configured SQL warehouse with the caller's OBO token (Unity Catalog perms
enforced), mirroring the View Data feature's executor seam (P22-B):

- :meth:`run_adhoc` evaluates the predicate over an inline VALUES grid (the
  manual test), returning a per-row pass/fail verdict.
- :meth:`run_table` samples a real UC table and evaluates the rule over that
  sample — a boolean predicate per row, or, for a cross-table rule, its whole
  ``sql_query`` run against the sample joined to the real reference tables.

The AI helper :meth:`generate_test_data` asks the app's AI gateway (OBO) to
invent a deliberate mix of passing/failing rows for the manual grid.

Security rails (AGENTS.md): the rule's SQL predicate must pass DQX's
:func:`is_sql_query_safe` after slot substitution — the same gate the
materializer applies before a rule ever runs — else :class:`UnsafeSqlQueryError`
is raised. ``dqx_native`` rules are compiled to a row-level SQL predicate by
:mod:`native_test_predicate` before evaluation; dataset / geo / UDF checks are
rejected by the route.
literals (never executed as SQL) and the raw model response is never relayed.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from databricks.labs.dqx.utils import is_sql_query_safe

from databricks_labs_dqx_app.backend.rule_test_sql import (
    AdhocSource,
    TableSource,
    TestRunResult,
    build_adhoc_query_sql,
    build_adhoc_sql,
    build_query_test_sql,
    build_table_sql,
    is_query_shaped,
    parse_result,
    substitute_slots,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import AIGateway, AIResponseParseError
from databricks_labs_dqx_app.backend.sql_utils import strip_sql_line_comments, validate_fqn

logger = logging.getLogger(__name__)

# Bounds on the AI-generated grid, matching dqlake's ge=5/le=20 contract.
_GEN_MIN_ROWS = 5
_GEN_MAX_ROWS = 20

_GEN_TEST_DATA_SYSTEM = (
    "You generate test data for a data-quality rule. Given a SQL predicate, its "
    "polarity, and a single table with typed columns, return JSON only: "
    '{"columns": [..], "rows": [[..], ..]}. '
    "Produce a DELIBERATE MIX of rows that PASS and rows that FAIL the rule "
    "(roughly half and half). Respect each column's family for typing: numeric -> "
    "numbers, text -> strings, temporal -> 'YYYY-MM-DD' strings, boolean -> "
    "true/false. The 'columns' array MUST equal the requested column names in the "
    "same order, and every row MUST have exactly that many cells. Return exactly "
    "the requested number of rows. No prose, JSON only."
)

# Cross-table variant: the rule reads its own table AND one or more reference
# tables, so the generated data is only useful if it is consistent ACROSS them —
# some input rows finding a match and some deliberately not.
_GEN_CROSS_TABLE_SYSTEM = (
    "You generate test data for a cross-table data-quality rule. You are given the "
    "rule's SQL query, the columns of the table being checked, and the fully-qualified "
    "names of the reference tables it joins (each appears in the SQL as a "
    "catalog.schema.table name). Return JSON only: "
    '{"columns": [..], "rows": [[..], ..], '
    '"refs": {"<table_name>": {"columns": [{"name": .., "family": ..}], "rows": [[..], ..]}}}. '
    "'columns'/'rows' are the table being checked; 'refs' holds one entry per "
    "reference table, keyed by the table name EXACTLY as given. "
    "Infer each reference table's columns from how the SQL joins and filters it "
    "(e.g. `LEFT JOIN main.ref.customers c ON c.id = {{customer_id}}` means that "
    "table needs an 'id' column whose values are comparable to 'customer_id'). "
    "CRITICAL: make the data CONSISTENT across tables — some input rows MUST match "
    "a reference row and some MUST NOT, so the rule produces both passing and "
    "failing results. Respect each column's family for typing: numeric -> numbers, "
    "text -> strings, temporal -> 'YYYY-MM-DD' strings, boolean -> true/false. The "
    "'columns' array MUST equal the requested column names in the same order, and "
    "every row MUST have exactly that many cells. Return exactly the requested "
    "number of input rows; reference tables may have any small number of rows. "
    "No prose, JSON only."
)


@dataclass
class GeneratedGrid:
    """One generated reference-table grid (columns invented by the model)."""

    columns: list[tuple[str, str]]  # (name, family), in grid order
    rows: list[list[str | None]]


@dataclass
class GeneratedTestData:
    columns: list[str]
    rows: list[list[str | None]]
    # Cross-table only: reference table FQN -> its generated grid.
    refs: dict[str, GeneratedGrid] = field(default_factory=dict)


class RuleTestService:
    """Execute rule-test queries and generate AI test data (OBO-scoped)."""

    def __init__(self, sql: Any, ai_gateway: AIGateway) -> None:
        self._sql = sql
        self._ai = ai_gateway

    def ai_available(self) -> bool:
        """Whether AI test-data generation can be offered (kill-switch + endpoint)."""
        return self._ai.is_enabled() and bool(self._ai.endpoint_name())

    async def run_adhoc(self, *, predicate: str, polarity: str, source: AdhocSource) -> TestRunResult:
        """Evaluate a rule over the manual VALUES grid(s).

        A boolean predicate is evaluated per input row. A cross-table or
        dataset-level rule — a whole ``SELECT`` — runs against the grids instead,
        with each reference table standing in as its own CTE, and its verdict read
        off the query's condition column.

        Both the AI-generated grids and hand-typed rows flow through here, so the
        same safety gates cover every ad-hoc cell.
        """
        self._guard_predicate(predicate, source.column_mapping)
        sql = (
            build_adhoc_query_sql(predicate, polarity, source)
            if is_query_shaped(predicate)
            else build_adhoc_sql(predicate, polarity, source)
        )
        self._guard_assembled(sql)
        rows = await asyncio.to_thread(self._sql.query_dicts, sql)
        return parse_result(rows, display_cap=source.display_cap)

    async def run_table(self, *, predicate: str, polarity: str, source: TableSource) -> TestRunResult:
        """Evaluate a rule over a sample of a real UC table, per-row verdicts.

        A boolean predicate is embedded per sampled row; a cross-table rule —
        whose body is a whole ``SELECT`` reading from ``{{input_view}}`` — runs as
        a query against the sample instead, with the verdict taken from its
        condition column (see :func:`build_query_test_sql`).
        """
        validate_fqn(source.table)
        self._guard_predicate(predicate, source.column_mapping)
        sql = (
            build_query_test_sql(predicate, polarity, source)
            if is_query_shaped(predicate)
            else build_table_sql(predicate, polarity, source)
        )
        self._guard_assembled(sql)
        rows = await asyncio.to_thread(self._sql.query_dicts, sql)
        return parse_result(rows, display_cap=source.display_cap)

    async def generate_test_data(
        self,
        *,
        predicate: str,
        polarity: str,
        columns: list[tuple[str, str]],
        row_count: int,
        user_email: str,
        ref_tables: list[str] | None = None,
    ) -> GeneratedTestData:
        """Ask the AI gateway for a passing/failing mix of rows for *columns*.

        Args:
            predicate: The rule's effective SQL predicate or query (slot
                placeholders kept as ``{{slot}}`` — the model reasons over the
                column names).
            polarity: ``"pass"`` or ``"fail"``.
            columns: ``(name, family)`` pairs, in grid order.
            row_count: Requested number of rows (clamped to [5, 20]).
            user_email: Caller identity (rate limiting + hashed audit).
            ref_tables: fully-qualified names of the tables the rule joins. When given,
                the model is asked to invent each reference table's columns and to
                keep the data consistent across tables (some input rows matching,
                some deliberately not) — otherwise a cross-table rule's generated
                data could never produce a meaningful verdict.

        Raises:
            AIUnavailableError / AIRateLimitExceededError: from the gateway.
            AIResponseParseError: model output isn't the expected JSON shape.
        """
        rows = max(_GEN_MIN_ROWS, min(_GEN_MAX_ROWS, row_count))
        refs = list(ref_tables or [])
        payload: dict[str, Any] = {
            "predicate": predicate,
            "polarity": polarity,
            "row_count": rows,
            "columns": [{"name": name, "family": family} for name, family in columns],
        }
        if refs:
            payload["reference_tables"] = refs
        content = await self._ai.query(
            user_email=user_email,
            purpose="generate_test_data",
            messages=[
                {"role": "system", "content": _GEN_CROSS_TABLE_SYSTEM if refs else _GEN_TEST_DATA_SYSTEM},
                {"role": "user", "content": json.dumps(payload)},
            ],
            max_tokens=4096,
        )
        return self._parse_generated(
            content,
            expected_columns=[name for name, _ in columns],
            expected_refs=refs,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _guard_predicate(predicate: str, column_mapping: dict[str, str]) -> None:
        """Reject a predicate that fails DQX's SQL-safety gate after substitution."""
        substituted = substitute_slots(predicate, column_mapping)
        # Scan with comments removed (item 6): a leading `-- explanation` block is
        # inert at runtime and its prose must not trip the keyword scan. Quote-
        # aware, so a `--` inside a string literal still counts as live SQL.
        if not is_sql_query_safe(strip_sql_line_comments(substituted)):
            raise UnsafeSqlQueryError("The rule's SQL predicate contains prohibited statements and cannot be tested.")

    @staticmethod
    def _guard_assembled(sql: str) -> None:
        """Re-run DQX's SQL-safety gate on the FULLY assembled query.

        Defence in depth beyond ``_guard_predicate``: the pre-substitution
        predicate check cannot see what the VALUES literals / slot substitution
        expand to, so the final query — post-substitution, post-VALUES — is
        re-validated here before it ever reaches the warehouse. Combined with
        ``_lit``'s quote+backslash escaping this makes an injected statement in
        an ad-hoc cell either a harmless quoted literal or an outright rejection.
        """
        # The assembled query embeds the predicate, which may carry a leading
        # `-- explanation` comment block (item 6). The newline terminating each
        # comment line is preserved through assembly (str.replace substitution),
        # so the live SQL after it still runs; strip comments here only so their
        # prose can't trip this defence-in-depth keyword scan.
        if not is_sql_query_safe(strip_sql_line_comments(sql)):
            raise UnsafeSqlQueryError("The assembled test query contains prohibited statements and cannot be run.")

    @staticmethod
    def _parse_generated(
        content: str,
        *,
        expected_columns: list[str],
        expected_refs: list[str] | None = None,
    ) -> GeneratedTestData:
        obj = AIGateway.parse_json_object(content)
        raw_rows = obj.get("rows")
        if not isinstance(raw_rows, list):
            raise AIResponseParseError("AI response did not contain a 'rows' array.")
        # Always project onto the columns we asked for (in order) so a model that
        # renames/reorders columns can't desync the grid.
        normalized: list[list[str | None]] = []
        for raw_row in raw_rows:
            if not isinstance(raw_row, list):
                continue
            normalized.append(
                [_cell_to_text(raw_row[i] if i < len(raw_row) else None) for i in range(len(expected_columns))]
            )
        return GeneratedTestData(
            columns=list(expected_columns),
            rows=normalized,
            refs=RuleTestService._parse_generated_refs(obj.get("refs"), expected_refs or []),
        )

    @staticmethod
    def _parse_generated_refs(raw: Any, expected_refs: list[str]) -> dict[str, GeneratedGrid]:
        """Normalize the model's reference-table grids, keyed by slot name.

        Unlike the input grid, these columns are the MODEL's invention (it reads
        them off the rule's join conditions), so they're taken as given — but only
        for reference tables we actually asked about, and only when a grid is
        structurally sound. A malformed or unexpected entry is dropped rather than
        failing the whole generation: the author still gets usable input rows and
        can fill the rest by hand.
        """
        if not expected_refs or not isinstance(raw, dict):
            return {}
        out: dict[str, GeneratedGrid] = {}
        for name in expected_refs:
            entry = raw.get(name)
            if not isinstance(entry, dict):
                continue
            raw_cols = entry.get("columns")
            raw_grid_rows = entry.get("rows")
            if not isinstance(raw_cols, list) or not isinstance(raw_grid_rows, list):
                continue
            cols: list[tuple[str, str]] = []
            for col in raw_cols:
                if isinstance(col, dict) and isinstance(col.get("name"), str) and col["name"].strip():
                    family = col.get("family")
                    cols.append((col["name"].strip(), family if isinstance(family, str) and family else "any"))
            if not cols:
                continue
            grid_rows: list[list[str | None]] = []
            for raw_row in raw_grid_rows:
                if not isinstance(raw_row, list):
                    continue
                grid_rows.append([_cell_to_text(raw_row[i] if i < len(raw_row) else None) for i in range(len(cols))])
            out[name] = GeneratedGrid(columns=cols, rows=grid_rows)
        return out


def _cell_to_text(value: object) -> str | None:
    """Coerce an AI-produced cell to the grid's string|null convention."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    # Objects/arrays aren't valid scalar cells — drop to null rather than dump JSON.
    return None
