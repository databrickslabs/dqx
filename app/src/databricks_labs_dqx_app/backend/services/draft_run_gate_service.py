"""Draft-run gate (issue B2-12 / B2-118) — the "a FRESH run must exist before
submit" check.

When the admin setting ``require_draft_run_before_submit`` is ON, an author
cannot submit a monitored table / table space (or a per-table applied rule)
for review — and cannot take the approvals-mode auto-approve shortcut —
until a *draft run* has been recorded for the target table(s). This forces a
dry-run test of the checks before they enter review.

B2-118 tightens this from "any qualifying run has ever happened" to "a
qualifying run happened AFTER the most recent change to what's being
submitted", so an edit made after the last test forces a re-test. The caller
supplies that *last change* instant (``last_change_time``) — see the route
call sites for the exact signal chosen per surface:

* monitored table  → the binding's ``updated_at`` (bumped on every applied-
  rules save, see ``ApplyRulesService.save_applied_rules``, and on status /
  schedule changes);
* table space      → the product's ``updated_at`` (bumped on every
  membership / config edit, which flips the space back to ``draft``);
* per-table rule   → the materialized rule's ``updated_at``.

A run instant is the ``dq_validation_runs`` row's ``created_at``. When
``last_change_time`` is ``None`` the gate degrades to the original
existence-only predicate (any qualifying run satisfies it).

"A qualifying run exists" is deliberately defined as the SAME predicate the
"last run" denormalization uses (see
``MonitoredTableService._latest_validation_run_at_map``): a
``dq_validation_runs`` row for the target ``source_table_fqn`` whose status is
terminal (not ``RUNNING``) and whose ``run_type`` is not ``preview``. Rationale:

* It is surface-agnostic — every run trigger (MT-direct, table-space fan-out,
  per-table dry-run) writes ``source_table_fqn`` = the member table, so the
  gate is satisfied regardless of *which* surface produced the run, and it can
  never wrongly block because one surface omits the ``run_mode`` provenance
  tag.
* It keeps the frontend's cache-friendly ``last_run_at`` hint consistent with
  this authoritative check — the UI compares that same denormalized instant
  against the object's ``updated_at`` to pre-disable the Submit button.

The service is pure (no FastAPI coupling): it returns booleans and raises the
domain-level :class:`DraftRunRequiredError`, which the route layer maps to a
409. ``dq_validation_runs`` is always a Delta table, so this reads off the SP
Delta executor regardless of whether the OLTP tables live in Lakebase.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

#: Cross-table SQL checks use this synthetic ``table_fqn`` prefix and have no
#: single home table to validate, so the gate skips them (mirrors the registry
#: rule carve-out). Kept in sync with the ``__sql_check__/`` convention
#: documented in ``backend/CLAUDE.md``.
SYNTHETIC_FQN_PREFIX = "__sql_check__/"

#: User-facing 409 message when the gate blocks a submit and NO qualifying run
#: has ever been recorded. Deliberately generic (no table names) so it is safe
#: to surface directly in a toast.
DRAFT_RUN_REQUIRED_MESSAGE = (
    "A draft run is required before this can be submitted for review. "
    "Run the checks in draft mode first, then submit."
)

#: User-facing 409 message when a qualifying run DOES exist but predates the
#: most recent change — the edit needs to be re-tested (B2-118).
DRAFT_RUN_STALE_MESSAGE = "You cannot submit for approval without first running your draft"


class DraftRunRequiredError(RuntimeError):
    """Raised when the require-draft-run gate is ON and no qualifying run exists.

    The route layer maps this to HTTP 409 (Conflict) with
    :data:`DRAFT_RUN_REQUIRED_MESSAGE`.
    """


class DraftRunGateService:
    """Answers "has a draft run been recorded for these table(s)?" against Delta."""

    def __init__(self, validation_sql: SqlExecutor) -> None:
        self._sql = validation_sql
        self._validation_runs_table = validation_sql.fqn("dq_validation_runs")

    def has_any_run(self, table_fqns: list[str], *, since: datetime | None = None) -> bool:
        """Return ``True`` if any of *table_fqns* has a qualifying (non-preview) run.

        A single grouped ``EXISTS``-style query over every supplied FQN; empty
        or all-synthetic input returns ``False`` here (callers decide whether an
        empty concrete set means "allow" — see :meth:`enforce`).

        Args:
            table_fqns: Target FQNs (synthetic / empty entries are dropped).
            since: When provided, only runs whose ``created_at`` is at or after
                this instant count — the "fresh run since the last edit" filter
                (B2-118). ``None`` counts any qualifying run (existence-only).
        """
        concrete = self._concrete_fqns(table_fqns)
        if not concrete:
            return False
        in_list = ", ".join(f"'{escape_sql_string(f)}'" for f in concrete)
        since_clause = ""
        if since is not None:
            since_clause = f"AND created_at >= {self._ts_literal(since)} "
        sql = (
            f"SELECT 1 FROM {self._validation_runs_table} "  # noqa: S608
            f"WHERE source_table_fqn IN ({in_list}) "
            f"AND UPPER(status) <> 'RUNNING' AND COALESCE(run_type, 'dryrun') <> 'preview' "
            f"{since_clause}"
            f"LIMIT 1"
        )
        return bool(self._sql.query(sql))

    def enforce(
        self, *, enabled: bool, table_fqns: list[str], last_change_time: datetime | None = None
    ) -> None:
        """Raise :class:`DraftRunRequiredError` when the gate should block the submit.

        No-op when *enabled* is ``False`` (setting off) or when there are no
        concrete tables to validate (registry rules / cross-table SQL checks —
        table-agnostic submits the gate deliberately does not cover). Otherwise
        raises unless at least one concrete table has a qualifying run recorded
        AT OR AFTER *last_change_time* (B2-118).

        Args:
            enabled: The ``require_draft_run_before_submit`` setting value.
            table_fqns: The target's table FQN(s) — one for a monitored table or
                per-table rule, the member tables for a table space. Synthetic
                ``__sql_check__/`` FQNs and empty entries are ignored.
            last_change_time: The instant the submitted content last changed. A
                qualifying run must be at or after this to satisfy the gate.
                ``None`` (e.g. a never-edited object, or a surface with no such
                timestamp) falls back to existence-only — any qualifying run
                satisfies it.
        """
        if not enabled:
            return
        concrete = self._concrete_fqns(table_fqns)
        if not concrete:
            # Nothing concrete to validate (registry rule / cross-table SQL
            # check / empty space) — the gate does not apply. Allow the submit.
            return
        if self.has_any_run(concrete, since=last_change_time):
            return
        # A run since the last change is what's missing. Distinguish "never
        # tested" from "tested, but before the last edit" for a clearer 409.
        if last_change_time is not None and self.has_any_run(concrete):
            raise DraftRunRequiredError(DRAFT_RUN_STALE_MESSAGE)
        raise DraftRunRequiredError(DRAFT_RUN_REQUIRED_MESSAGE)

    @staticmethod
    def _ts_literal(value: datetime) -> str:
        """SQL literal for comparing against ``dq_validation_runs.created_at``.

        Normalised to a naive-UTC ``'YYYY-MM-DD HH:MM:SS.ffffff'`` string wrapped
        in ``CAST(... AS TIMESTAMP)`` — parses identically on Delta and Postgres
        and matches how ``created_at`` (written server-side in UTC) is stored,
        so the comparison never skews by the caller's timezone offset.
        """
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return f"CAST('{escape_sql_string(value.isoformat(sep=' '))}' AS TIMESTAMP)"

    @staticmethod
    def _concrete_fqns(table_fqns: list[str]) -> list[str]:
        """Drop empty and synthetic (cross-table SQL) FQNs, de-duplicating."""
        out: list[str] = []
        for fqn in dict.fromkeys(table_fqns):
            if fqn and not fqn.startswith(SYNTHETIC_FQN_PREFIX):
                out.append(fqn)
        return out
