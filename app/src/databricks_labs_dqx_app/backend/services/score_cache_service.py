"""Lakebase-backed DQ score cache (``dq_score_cache``) — P3.4.

The monitored-tables and table-spaces list pages (and the homepage) need
dqlake-style DQ score columns that load instantly. Recomputing scores from
the ``mv_dq_scores`` metric view on every page load would put a SQL
warehouse round-trip on the hot path, so scores are persisted into the
OLTP store (Lakebase Postgres, or the Delta OLTP fallback) and the list
endpoints LEFT JOIN them in the same round-trip they already make.

Refresh model (no polling, no cron):

- ``refresh_for_tables(fqns)`` — ONE batched warehouse query over the
  metric view (latest PUBLISHED run per table — the same
  ``GROUP BY run_id, run_time`` + latest-run window the dq-score routes
  use, batched over the fqns) followed by one upsert per table.
- ``refresh_product(product_id)`` / ``refresh_global()`` — derived from
  the cached 'table' rows (unweighted mean over non-NULL scores, summed
  failed/total counters) entirely in the OLTP store; no warehouse hit.
- ``refresh_all_for_tables(fqns)`` — the run-completion orchestration
  the ``POST /api/v1/dq-results/refresh-scores`` route calls: refresh
  the tables, then every product containing any of them, then global.

The cache is SHARED and viewer-independent (it is written SP-side); the
VIEW of it is filtered at read time by the existing catalog filtering on
the list endpoints. Rows carry ``computed_at`` so the UI can surface
staleness; a table that has never produced a published run still gets a
row (NULL score) so "computed, nothing found" is distinguishable from
"never computed".

Scores are PUBLISHED-only by construction (``run_mode = 'published'``
filter on the metric view — the run-level tag stamped at run assembly,
with the legacy run_type heuristic resolved inside the shaping view).

P3.5 addition — ``dq_score_history``: every SCORED upsert (any scope)
also appends one append-only trend row and count-trims the scope to
:data:`HISTORY_KEEP_ROWS`. ``get_history`` reads the last N points for
one scope (the homepage's global score trend + delta) — Postgres-only,
no warehouse.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from databricks_labs_dqx_app.backend.metrics_utils import safe_float, safe_int
from databricks_labs_dqx_app.backend.services.score_view_service import (
    RUN_MODE_PUBLISHED,
    metric_view_fqn,
)
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql, SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)

SCOPE_TABLE = "table"
SCOPE_PRODUCT = "product"
SCOPE_GLOBAL = "global"
# The single row key used for the global scope.
GLOBAL_SCOPE_KEY = "global"

# How many ``dq_score_history`` rows are kept per scope. Every scored
# upsert appends one trend point and count-trims to this cap, so the
# table's growth is bounded by (scopes x cap) with no retention sweep.
# 200 comfortably covers the homepage trend read (last ~30 points).
HISTORY_KEEP_ROWS = 200


@dataclass(frozen=True)
class CachedScore:
    """One ``dq_score_cache`` row as read back by the app.

    Timestamps are ISO-ish strings (whatever the executor's ``ts_text``
    projection yields) — the Out models pass them through verbatim.
    """

    score: float | None = None
    failed_tests: int | None = None
    total_tests: int | None = None
    latest_run_id: str | None = None
    run_time: str | None = None
    computed_at: str | None = None


def parse_cached_score(
    score: str | None,
    failed_tests: str | None,
    total_tests: str | None,
    computed_at: str | None,
    latest_run_id: str | None = None,
    run_time: str | None = None,
) -> CachedScore:
    """Build a :class:`CachedScore` from stringified SQL cells.

    Shared by this service's reads and the list services' LEFT-JOIN row
    parsing so the string→number coercion lives in exactly one place.
    """
    return CachedScore(
        score=safe_float(score),
        failed_tests=safe_int(failed_tests),
        total_tests=safe_int(total_tests),
        latest_run_id=latest_run_id or None,
        run_time=run_time or None,
        computed_at=computed_at or None,
    )


class ScoreCacheService:
    """Recomputes and reads the ``dq_score_cache`` rows.

    ``oltp`` owns the cache table (plus the product-membership lookups);
    ``warehouse_sql`` is the SP warehouse executor the batched metric-view
    recompute runs on. Only :meth:`refresh_for_tables` ever touches the
    warehouse.
    """

    def __init__(self, oltp: OltpExecutorProtocol, warehouse_sql: SqlExecutor) -> None:
        self._oltp = oltp
        self._warehouse_sql = warehouse_sql
        self._cache_table = oltp.fqn("dq_score_cache")
        self._history_table = oltp.fqn("dq_score_history")
        self._members_table = oltp.fqn("dq_data_product_members")
        self._monitored_table = oltp.fqn("dq_monitored_tables")

    # ------------------------------------------------------------------
    # Refresh — tables (the only warehouse hit)
    # ------------------------------------------------------------------

    def refresh_for_tables(self, table_fqns: list[str]) -> int:
        """Recompute + upsert the 'table' rows for *table_fqns*.

        ONE batched warehouse query over ``mv_dq_scores``: per-run
        MEASURE() aggregates grouped by (input_location, run_id,
        run_time), restricted to published runs, then a latest-run-per-
        table window (QUALIFY over the derived table — the view's own
        ``is_latest_run`` flag is computed over ALL runs regardless of
        mode, so it cannot be used here; see the same reasoning on
        ``dq_score._compute_score_for_table``).

        Syntactically invalid FQNs are dropped (never interpolated —
        *escape_sql_string* relies on *validate_fqn* having rejected
        backslashes). Tables with no published run still get a row with
        a NULL score so ``computed_at`` records the attempt.

        Returns the number of table rows upserted.
        """
        valid: list[str] = []
        for fqn in dict.fromkeys(table_fqns):
            try:
                validate_fqn(fqn)
            except ValueError:
                logger.warning("Dropping invalid table FQN from score-cache refresh")
                continue
            valid.append(fqn)
        if not valid:
            return 0

        by_fqn = {row.get("input_location"): row for row in self._query_latest_published_scores(valid)}
        for fqn in valid:
            row = by_fqn.get(fqn)
            score = safe_float(row.get("score")) if row else None
            self._upsert(
                SCOPE_TABLE,
                fqn,
                score=round(score, 4) if score is not None else None,
                failed_tests=safe_int(row.get("failed_tests")) if row else None,
                total_tests=safe_int(row.get("total_tests")) if row else None,
                latest_run_id=(row.get("run_id") or None) if row else None,
                run_time=(row.get("run_time") or None) if row else None,
            )
        return len(valid)

    def _query_latest_published_scores(self, table_fqns: list[str]) -> list[dict[str, str | None]]:
        """The batched metric-view query: latest published run per table."""
        mv = metric_view_fqn(self._warehouse_sql.catalog, self._warehouse_sql.schema)
        in_list = ", ".join(f"'{escape_sql_string(fqn)}'" for fqn in table_fqns)
        stmt = (
            f"SELECT input_location, run_id, run_time, score, failed_tests, total_tests FROM ("
            f"SELECT input_location, run_id, CAST(run_time AS STRING) AS run_time, "
            f"MEASURE(score) AS score, MEASURE(failed_tests) AS failed_tests, "
            f"MEASURE(total_tests) AS total_tests "
            f"FROM {mv} "  # noqa: S608
            f"WHERE input_location IN ({in_list}) AND run_mode = '{RUN_MODE_PUBLISHED}' "
            f"GROUP BY input_location, run_id, run_time"
            f") QUALIFY ROW_NUMBER() OVER (PARTITION BY input_location ORDER BY run_time DESC) = 1"
        )
        return self._warehouse_sql.query_dicts(stmt)

    # ------------------------------------------------------------------
    # Refresh — derived scopes (OLTP-only, no warehouse hit)
    # ------------------------------------------------------------------

    def refresh_product(self, product_id: str) -> None:
        """Recompute + upsert one 'product' row from its members' cached table rows.

        Unweighted mean over the member tables' non-NULL cached scores
        (dqlake's ``compute_product_score`` semantics), with the failed/
        total counters summed for the "X failed of Y tests" subtitle. A
        product whose members carry no cached scores still gets a row
        (NULL score) so ``computed_at`` records the recompute.
        """
        e = escape_sql_string(product_id)
        stmt = (
            f"SELECT AVG(sc.score) AS score, SUM(sc.failed_tests) AS failed_tests, "
            f"SUM(sc.total_tests) AS total_tests "
            f"FROM {self._members_table} m "  # noqa: S608
            f"JOIN {self._monitored_table} mt ON mt.binding_id = m.binding_id "
            f"JOIN {self._cache_table} sc "
            f"ON sc.scope_type = '{SCOPE_TABLE}' AND sc.scope_key = mt.table_fqn "
            f"WHERE m.product_id = '{e}' AND sc.score IS NOT NULL"
        )
        self._upsert_aggregate(SCOPE_PRODUCT, product_id, self._oltp.query_dicts(stmt))

    def refresh_global(self) -> None:
        """Recompute + upsert the single 'global' row from all cached table rows."""
        stmt = (
            f"SELECT AVG(score) AS score, SUM(failed_tests) AS failed_tests, "
            f"SUM(total_tests) AS total_tests "
            f"FROM {self._cache_table} "  # noqa: S608
            f"WHERE scope_type = '{SCOPE_TABLE}' AND score IS NOT NULL"
        )
        self._upsert_aggregate(SCOPE_GLOBAL, GLOBAL_SCOPE_KEY, self._oltp.query_dicts(stmt))

    def _upsert_aggregate(self, scope_type: str, scope_key: str, rows: list[dict[str, str | None]]) -> None:
        row = rows[0] if rows else {}
        score = safe_float(row.get("score"))
        self._upsert(
            scope_type,
            scope_key,
            score=round(score, 4) if score is not None else None,
            failed_tests=safe_int(row.get("failed_tests")),
            total_tests=safe_int(row.get("total_tests")),
            latest_run_id=None,
            run_time=None,
        )

    # ------------------------------------------------------------------
    # Orchestration (run-completion refresh trigger)
    # ------------------------------------------------------------------

    def refresh_all_for_tables(self, table_fqns: list[str]) -> tuple[int, int]:
        """Refresh *table_fqns*, then every product containing any of them, then global.

        The exact recompute the ``refresh-scores`` route performs after a
        run completes. Returns ``(refreshed_tables, refreshed_products)``.
        """
        refreshed_tables = self.refresh_for_tables(table_fqns)
        product_ids = self.product_ids_containing_tables(table_fqns) if refreshed_tables else []
        for product_id in product_ids:
            self.refresh_product(product_id)
        self.refresh_global()
        return refreshed_tables, len(product_ids)

    def product_ids_containing_tables(self, table_fqns: list[str]) -> list[str]:
        """Product ids with at least one member bound to any of *table_fqns*."""
        candidates: list[str] = []
        for fqn in dict.fromkeys(table_fqns):
            try:
                validate_fqn(fqn)
            except ValueError:
                continue
            candidates.append(fqn)
        if not candidates:
            return []
        in_list = ", ".join(f"'{escape_sql_string(fqn)}'" for fqn in candidates)
        stmt = (
            f"SELECT DISTINCT m.product_id "
            f"FROM {self._members_table} m "  # noqa: S608
            f"JOIN {self._monitored_table} mt ON mt.binding_id = m.binding_id "
            f"WHERE mt.table_fqn IN ({in_list})"
        )
        rows = self._oltp.query(stmt)
        return [row[0] for row in rows if row and row[0]]

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_many(self, scope_type: str, scope_keys: list[str]) -> dict[str, CachedScore]:
        """Fast batched cache read: ``scope_key -> CachedScore`` for one scope type.

        Keys with no cached row are simply absent from the result.
        """
        if not scope_keys:
            return {}
        e_scope = escape_sql_string(scope_type)
        in_list = ", ".join(f"'{escape_sql_string(k)}'" for k in dict.fromkeys(scope_keys))
        run_time = self._oltp.ts_text("run_time")
        computed_at = self._oltp.ts_text("computed_at")
        stmt = (
            f"SELECT scope_key, score, failed_tests, total_tests, latest_run_id, "
            f"{run_time} AS run_time, {computed_at} AS computed_at "
            f"FROM {self._cache_table} "  # noqa: S608
            f"WHERE scope_type = '{e_scope}' AND scope_key IN ({in_list})"
        )
        out: dict[str, CachedScore] = {}
        for row in self._oltp.query_dicts(stmt):
            key = row.get("scope_key")
            if not key:
                continue
            out[key] = parse_cached_score(
                row.get("score"),
                row.get("failed_tests"),
                row.get("total_tests"),
                row.get("computed_at"),
                latest_run_id=row.get("latest_run_id"),
                run_time=row.get("run_time"),
            )
        return out

    def get_history(self, scope_type: str, scope_key: str, limit: int = 30) -> list[CachedScore]:
        """Last *limit* scored trend points for one scope, oldest first.

        Reads the ``dq_score_history`` append rows (newest first, capped
        by *limit*) and returns them ascending for charting. Every row
        carries a non-NULL score by construction (NULL-score recomputes
        never append — see :meth:`_append_history`). ``latest_run_id``
        is not recorded in history, so it is always None here.
        """
        e_type = escape_sql_string(scope_type)
        e_key = escape_sql_string(scope_key)
        run_time = self._oltp.ts_text("run_time")
        computed_at = self._oltp.ts_text("computed_at")
        stmt = (
            f"SELECT score, failed_tests, total_tests, "
            f"{run_time} AS run_time, {computed_at} AS computed_at "
            f"FROM {self._history_table} "  # noqa: S608
            f"WHERE scope_type = '{e_type}' AND scope_key = '{e_key}' "
            f"ORDER BY computed_at DESC LIMIT {int(limit)}"
        )
        points = [
            parse_cached_score(
                row.get("score"),
                row.get("failed_tests"),
                row.get("total_tests"),
                row.get("computed_at"),
                run_time=row.get("run_time"),
            )
            for row in self._oltp.query_dicts(stmt)
        ]
        points.reverse()
        return points

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _upsert(
        self,
        scope_type: str,
        scope_key: str,
        *,
        score: float | None,
        failed_tests: int | None,
        total_tests: int | None,
        latest_run_id: str | None,
        run_time: str | None,
    ) -> None:
        self._oltp.upsert(
            self._cache_table,
            {"scope_type": scope_type, "scope_key": scope_key},
            {
                "score": score,
                "failed_tests": failed_tests,
                "total_tests": total_tests,
                "latest_run_id": latest_run_id,
                # CAST('...' AS TIMESTAMP) parses on both backends; the
                # value is the warehouse's own stringified run_time.
                "run_time": (
                    RawSql(f"CAST('{escape_sql_string(run_time)}' AS TIMESTAMP)") if run_time else None
                ),
                "computed_at": RawSql("current_timestamp()"),
            },
        )
        if score is not None:
            self._append_history(
                scope_type,
                scope_key,
                score=score,
                failed_tests=failed_tests,
                total_tests=total_tests,
                run_time=run_time,
            )

    def _append_history(
        self,
        scope_type: str,
        scope_key: str,
        *,
        score: float,
        failed_tests: int | None,
        total_tests: int | None,
        run_time: str | None,
    ) -> None:
        """Append one ``dq_score_history`` trend point and count-trim the scope.

        Called from :meth:`_upsert` for every SCORED recompute (uniform
        across table/product/global scopes). NULL-score recomputes
        ("computed, nothing found") update the cache but never append —
        they would only punch holes in the trend. The trim keeps the
        newest :data:`HISTORY_KEEP_ROWS` rows per scope: rows strictly
        older than the oldest kept ``computed_at`` are deleted, so ties
        on the boundary timestamp are kept rather than over-trimmed.
        """
        e_type = escape_sql_string(scope_type)
        e_key = escape_sql_string(scope_key)
        failed_expr = str(int(failed_tests)) if failed_tests is not None else "NULL"
        total_expr = str(int(total_tests)) if total_tests is not None else "NULL"
        run_time_expr = f"CAST('{escape_sql_string(run_time)}' AS TIMESTAMP)" if run_time else "NULL"
        self._oltp.execute(
            f"INSERT INTO {self._history_table} "  # noqa: S608
            f"(scope_type, scope_key, score, failed_tests, total_tests, run_time, computed_at) "
            f"VALUES ('{e_type}', '{e_key}', {float(score)}, {failed_expr}, {total_expr}, "
            f"{run_time_expr}, now())"
        )
        self._oltp.execute(
            f"DELETE FROM {self._history_table} "  # noqa: S608
            f"WHERE scope_type = '{e_type}' AND scope_key = '{e_key}' AND computed_at < ("
            f"SELECT MIN(computed_at) FROM ("
            f"SELECT computed_at FROM {self._history_table} "
            f"WHERE scope_type = '{e_type}' AND scope_key = '{e_key}' "
            f"ORDER BY computed_at DESC LIMIT {HISTORY_KEEP_ROWS}"
            f") newest_rows)"
        )
