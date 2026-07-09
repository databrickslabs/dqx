"""RuleTestService — run a registry rule's SQL predicate against sample data (P22-E).

Powers the Rules Registry "Test" tab. Two run modes, both executed on the
configured SQL warehouse with the caller's OBO token (Unity Catalog perms
enforced), mirroring the View Data feature's executor seam (P22-B):

- :meth:`run_adhoc` evaluates the predicate over an inline VALUES grid (the
  manual test), returning a per-row pass/fail verdict.
- :meth:`run_table` samples a real UC table and evaluates the predicate over
  the sample.

The AI helper :meth:`generate_test_data` asks the app's AI gateway (OBO) to
invent a deliberate mix of passing/failing rows for the manual grid.

Security rails (AGENTS.md): the rule's SQL predicate must pass DQX's
:func:`is_sql_query_safe` after slot substitution — the same gate the
materializer applies before a rule ever runs — else :class:`UnsafeSqlQueryError`
is raised. ``dqx_native`` rules are not testable and are rejected by the route
before reaching this service. AI-generated data is inserted as escaped string
literals (never executed as SQL) and the raw model response is never relayed.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from databricks.labs.dqx.utils import is_sql_query_safe

from databricks_labs_dqx_app.backend.rule_test_sql import (
    AdhocSource,
    TableSource,
    TestRunResult,
    build_adhoc_sql,
    build_table_sql,
    parse_result,
    substitute_slots,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import AIGateway, AIResponseParseError
from databricks_labs_dqx_app.backend.sql_utils import validate_fqn

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


@dataclass
class GeneratedTestData:
    columns: list[str]
    rows: list[list[str | None]]


class RuleTestService:
    """Execute rule-test queries and generate AI test data (OBO-scoped)."""

    def __init__(self, sql: Any, ai_gateway: AIGateway) -> None:
        self._sql = sql
        self._ai = ai_gateway

    def ai_available(self) -> bool:
        """Whether AI test-data generation can be offered (kill-switch + endpoint)."""
        return self._ai.is_enabled() and bool(self._ai.endpoint_name())

    async def run_adhoc(self, *, predicate: str, polarity: str, source: AdhocSource) -> TestRunResult:
        """Evaluate *predicate* over the manual VALUES grid, per-row verdicts."""
        self._guard_predicate(predicate, source.column_mapping)
        sql = build_adhoc_sql(predicate, polarity, source)
        rows = await asyncio.to_thread(self._sql.query_dicts, sql)
        return parse_result(rows, display_cap=source.display_cap)

    async def run_table(self, *, predicate: str, polarity: str, source: TableSource) -> TestRunResult:
        """Evaluate *predicate* over a sample of a real UC table, per-row verdicts."""
        validate_fqn(source.table)
        self._guard_predicate(predicate, source.column_mapping)
        sql = build_table_sql(predicate, polarity, source)
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
    ) -> GeneratedTestData:
        """Ask the AI gateway for a passing/failing mix of rows for *columns*.

        Args:
            predicate: The rule's effective SQL predicate (slot placeholders kept
                as ``{{slot}}`` — the model reasons over the column names).
            polarity: ``"pass"`` or ``"fail"``.
            columns: ``(name, family)`` pairs, in grid order.
            row_count: Requested number of rows (clamped to [5, 20]).
            user_email: Caller identity (rate limiting + hashed audit).

        Raises:
            AIUnavailableError / AIRateLimitExceededError: from the gateway.
            AIResponseParseError: model output isn't the expected JSON shape.
        """
        rows = max(_GEN_MIN_ROWS, min(_GEN_MAX_ROWS, row_count))
        user = json.dumps(
            {
                "predicate": predicate,
                "polarity": polarity,
                "row_count": rows,
                "columns": [{"name": name, "family": family} for name, family in columns],
            }
        )
        content = await self._ai.query(
            user_email=user_email,
            purpose="generate_test_data",
            messages=[
                {"role": "system", "content": _GEN_TEST_DATA_SYSTEM},
                {"role": "user", "content": user},
            ],
            max_tokens=1024,
        )
        return self._parse_generated(content, expected_columns=[name for name, _ in columns])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _guard_predicate(predicate: str, column_mapping: dict[str, str]) -> None:
        """Reject a predicate that fails DQX's SQL-safety gate after substitution."""
        substituted = substitute_slots(predicate, column_mapping)
        if not is_sql_query_safe(substituted):
            raise UnsafeSqlQueryError("The rule's SQL predicate contains prohibited statements and cannot be tested.")

    @staticmethod
    def _parse_generated(content: str, *, expected_columns: list[str]) -> GeneratedTestData:
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
            normalized.append([_cell_to_text(raw_row[i] if i < len(raw_row) else None) for i in range(len(expected_columns))])
        return GeneratedTestData(columns=list(expected_columns), rows=normalized)


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
