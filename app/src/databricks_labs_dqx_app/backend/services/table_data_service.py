"""TableDataService — View Data preview + pragmatic AI question→SQL (P22-B, item 7).

Powers the monitored-table "View Data" tab:

- :meth:`preview` returns the first N rows of a table via a SQL warehouse using
  the caller's OBO token, so Unity Catalog permissions are enforced.
- :meth:`query` implements a *pragmatic* version of Databricks' native
  "ask a question about this data" pattern. Rather than integrating the
  space-scoped Genie Conversations API (which needs a pre-provisioned Genie
  space + warehouse binding per data scope — see the task's Genie-decision
  note), it asks the app's existing AI gateway to translate a natural-language
  question into a single read-only SELECT over the table, validates it, wraps
  it in an outer ``LIMIT`` for containment, and runs it via OBO. When AI is
  off the tab degrades to the plain preview.

Security rails (AGENTS.md): the generated SQL must be a single SELECT/WITH
statement and must pass DQX's :func:`is_sql_query_safe`; anything else raises
:class:`UnsafeSqlQueryError`. The query runs under the caller's own UC grants.
"""

from __future__ import annotations

import asyncio
import logging
import re

from databricks.labs.dqx.errors import UnsafeSqlQueryError
from databricks.labs.dqx.utils import is_sql_query_safe

from databricks_labs_dqx_app.backend.services.ai_gateway import AIGateway
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

logger = logging.getLogger(__name__)

# A generated query must start with SELECT or WITH (a read-only projection).
_READ_ONLY_PREFIX_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)


class PreviewResult:
    """Columns + rows for a table preview / query result."""

    def __init__(
        self,
        *,
        columns: list[str],
        rows: list[dict[str, str | None]],
        generated_sql: str | None,
        truncated: bool,
    ) -> None:
        self.columns = columns
        self.rows = rows
        self.generated_sql = generated_sql
        self.truncated = truncated


class TableDataService:
    """Read-only table preview and AI-assisted preview queries (OBO-scoped)."""

    PREVIEW_LIMIT = 500

    def __init__(self, sql: SqlExecutor, ai_gateway: AIGateway) -> None:
        self._sql = sql
        self._ai = ai_gateway

    def ai_available(self) -> bool:
        """Whether the ask-a-question feature can be offered (kill-switch + endpoint)."""
        return self._ai.is_enabled() and bool(self._ai.endpoint_name())

    async def preview(self, table_fqn: str) -> PreviewResult:
        """Return the first :attr:`PREVIEW_LIMIT` rows of *table_fqn*."""
        validate_fqn(table_fqn)
        sql = f"SELECT * FROM {quote_fqn(table_fqn)} LIMIT {self.PREVIEW_LIMIT}"
        rows = await asyncio.to_thread(self._sql.query_dicts, sql)
        return self._to_result(rows, generated_sql=None)

    async def query(self, table_fqn: str, question: str, user_email: str) -> PreviewResult:
        """Translate *question* to a safe SELECT over *table_fqn*, run it, return rows."""
        validate_fqn(table_fqn)
        cleaned_question = (question or "").strip()
        if not cleaned_question:
            raise ValueError("A question is required.")

        columns = await self._table_columns(table_fqn)
        candidate = await self._generate_sql(table_fqn, cleaned_question, columns, user_email)
        safe_sql = self._sanitize_generated_sql(candidate)

        # Outer LIMIT wrapper guarantees horizontal + row containment regardless
        # of what the model produced (8E rule: the preview never runs away).
        wrapped = f"SELECT * FROM ({safe_sql}) AS dqx_view_data LIMIT {self.PREVIEW_LIMIT}"
        rows = await asyncio.to_thread(self._sql.query_dicts, wrapped)
        return self._to_result(rows, generated_sql=safe_sql)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _table_columns(self, table_fqn: str) -> list[str]:
        """Best-effort column names for the prompt context (empty on failure)."""
        try:
            rows = await asyncio.to_thread(
                self._sql.query_dicts, f"SELECT * FROM {quote_fqn(table_fqn)} LIMIT 1"
            )
        except Exception:
            logger.warning("Could not read columns for AI query context", exc_info=True)
            return []
        return list(rows[0].keys()) if rows else []

    async def _generate_sql(
        self, table_fqn: str, question: str, columns: list[str], user_email: str
    ) -> str:
        column_hint = ", ".join(columns) if columns else "(unknown — inspect the table)"
        system = (
            "You translate a natural-language question into ONE Databricks SQL SELECT "
            "statement over a single Unity Catalog table. Rules: return only the SQL, no "
            "prose, no markdown fences; use a single read-only SELECT (or WITH ... SELECT); "
            "never write DDL/DML; reference the table by its fully-qualified name; keep result "
            f"sets small. Table: {table_fqn}. Columns: {column_hint}."
        )
        content = await self._ai.query(
            user_email=user_email,
            purpose="view_data_query",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": question},
            ],
            max_tokens=400,
        )
        return content

    def _sanitize_generated_sql(self, raw: str) -> str:
        """Strip fences, enforce single read-only SELECT, and run DQX's safety check."""
        candidate = (raw or "").strip()
        fence = re.search(r"```(?:sql)?\s*\n?(.*?)```", candidate, re.DOTALL)
        if fence:
            candidate = fence.group(1).strip()
        candidate = candidate.rstrip(";").strip()

        if not candidate:
            raise UnsafeSqlQueryError("The AI did not return a SQL query.")
        if ";" in candidate:
            raise UnsafeSqlQueryError("Only a single SQL statement is allowed.")
        if not _READ_ONLY_PREFIX_RE.match(candidate):
            raise UnsafeSqlQueryError("Only read-only SELECT queries are allowed here.")
        if not is_sql_query_safe(candidate):
            raise UnsafeSqlQueryError("The generated query contains prohibited statements.")
        return candidate

    def _to_result(self, rows: list[dict[str, str | None]], *, generated_sql: str | None) -> PreviewResult:
        columns = list(rows[0].keys()) if rows else []
        return PreviewResult(
            columns=columns,
            rows=rows,
            generated_sql=generated_sql,
            truncated=len(rows) >= self.PREVIEW_LIMIT,
        )
