"""View management service for creating/dropping temporary views.

Uses the **OBO-authenticated** SqlExecutor so that view creation
inherits the user's table permissions.
"""

import logging
import math
import secrets
from uuid import uuid4

from databricks_labs_dqx_app.backend.services.app_settings_service import (
    PROFILER_SAMPLE_KIND_PERCENT,
    PROFILER_SAMPLE_KIND_RECORDS,
    ProfilerSample,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)


_tmp_schema_ready = False


def mark_tmp_schema_ready() -> None:
    """Called at startup after the SP has ensured the tmp schema exists."""
    global _tmp_schema_ready
    _tmp_schema_ready = True


def reset_tmp_schema_ready() -> None:
    """Reset the flag — only used in tests."""
    global _tmp_schema_ready
    _tmp_schema_ready = False


# Safety margin applied when converting a target row count into a sampling
# percentage. Bernoulli sampling returns a variable number of rows, so asking
# for exactly n/total would frequently come back short; over-sampling and then
# capping with LIMIT keeps the result at n rows whenever the table is big enough.
ROW_SAMPLE_MARGIN = 1.5

# Upper bound for a generated TABLESAMPLE seed; well inside Spark's INT range.
_MAX_SAMPLE_SEED = 2_000_000_000


def needs_row_count(sample: ProfilerSample | None) -> bool:
    """Whether resolving *sample* into SQL requires the table's row count.

    Only a ``records`` sample does: it converts a target row count into a
    percentage. ``full`` and ``percent`` must not pay for a count.
    """
    return sample is not None and sample.kind == PROFILER_SAMPLE_KIND_RECORDS


def build_sample_select(quoted_source: str, sample: ProfilerSample | None, total_rows: int | None, seed: int) -> str:
    """Build the SELECT body that applies *sample* to *quoted_source*.

    Pure: given the same arguments it always returns the same SQL and performs
    no I/O. The caller supplies *total_rows* (see :func:`needs_row_count`) and a
    *seed*.

    ``percent`` maps straight onto ``TABLESAMPLE (p PERCENT)``, a genuine
    Bernoulli sample. ``records`` deliberately does NOT use
    ``TABLESAMPLE (n ROWS)`` — Spark implements that as a plain ``LIMIT``, so it
    would return the *first* n rows rather than a random n. Instead it
    over-samples by percentage and caps with ``LIMIT``, which needs
    *total_rows*. When that is unavailable it falls back to a bare ``LIMIT``,
    because a non-random sample beats a failed profile run.

    Every ``TABLESAMPLE`` carries ``REPEATABLE (seed)``. The sample lives in a
    plain (non-materialized) view, so the query re-executes on every scan — and
    the profiler scans it more than once (a ``count()`` for the row total, then
    the profiling passes themselves). Without a fixed seed each scan draws a
    *different* sample, which would make the reported row count disagree with
    the rows actually profiled and leave per-column statistics computed over
    different row sets.

    Args:
        quoted_source: Already-quoted source table FQN.
        sample: Sampling policy, or ``None`` for the whole table.
        total_rows: Row count of the source, or ``None`` if unknown.
        seed: Sampling seed, fixed per view so repeated scans agree. Vary it
            per run so successive profiles still see different rows.

    Returns:
        A ``SELECT ...`` string. Never interpolates untrusted text: the source
        is pre-quoted and every numeric is coerced with ``int()``.
    """
    if sample is None or sample.is_full_table:
        return f"SELECT * FROM {quoted_source}"

    if sample.kind == PROFILER_SAMPLE_KIND_PERCENT:
        return f"SELECT * FROM {quoted_source} TABLESAMPLE ({int(sample.value)} PERCENT) REPEATABLE ({int(seed)})"

    if sample.kind != PROFILER_SAMPLE_KIND_RECORDS:
        # Unknown kind — profile the whole table rather than guessing.
        logger.warning("Unknown profiler sample kind %r; profiling the whole table", sample.kind)
        return f"SELECT * FROM {quoted_source}"

    rows = int(sample.value)
    if total_rows is None or total_rows <= rows:
        # Either the count was unavailable, or the table already fits inside
        # the cap so no sampling is needed and LIMIT alone is exact.
        if total_rows is None:
            logger.warning(
                "No row count for %s; falling back to a non-random LIMIT %d for the profiler sample",
                quoted_source,
                rows,
            )
        return f"SELECT * FROM {quoted_source} LIMIT {rows}"

    percent = min(100, max(1, math.ceil(rows / total_rows * 100 * ROW_SAMPLE_MARGIN)))
    return f"SELECT * FROM {quoted_source} TABLESAMPLE ({percent} PERCENT) REPEATABLE ({int(seed)}) LIMIT {rows}"


class ViewService:
    """Create and drop temporary views via the SQL Statement Execution API."""

    def __init__(self, sql: SqlExecutor, sp_sql: SqlExecutor | None = None) -> None:
        self._sql = sql
        self._sp_sql = sp_sql

    def _ensure_schema(self) -> None:
        """Ensure the tmp schema exists. Uses SP credentials for DDL if available."""
        global _tmp_schema_ready
        if _tmp_schema_ready:
            return
        cat = self._sql.catalog.replace("`", "")
        schema = self._sql.schema.replace("`", "")
        if self._sp_sql is None:
            raise RuntimeError(
                f"Tmp schema `{cat}`.`{schema}` has not been created yet and no "
                f"service principal is available. Check app startup logs."
            )
        try:
            self._sp_sql.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{schema}`")
            _tmp_schema_ready = True
        except Exception as e:
            raise RuntimeError(
                f"Cannot create tmp schema `{cat}`.`{schema}` via service principal. " f"Original error: {e}"
            ) from e

    def _sample_select(self, quoted_source: str, sample: ProfilerSample | None, seed: int) -> str:
        """Resolve *sample* into a SELECT body, fetching a row count if needed.

        Keeps the I/O (the row count) at this boundary so the SQL-shaping
        decision itself stays a pure function — see :func:`build_sample_select`.
        """
        total = self._row_count(quoted_source) if needs_row_count(sample) else None
        return build_sample_select(quoted_source, sample, total, seed)

    def _row_count(self, quoted_source: str) -> int | None:
        """Return the source row count, or ``None`` when it cannot be determined.

        Cheap on Delta, which answers an unfiltered ``COUNT(*)`` from table
        statistics without scanning data files.
        """
        try:
            rows = self._sql.query(f"SELECT COUNT(*) FROM {quoted_source}")  # noqa: S608
        except Exception as e:
            logger.warning("Row count failed for %s: %s", quoted_source, e)
            return None
        if not rows or not rows[0] or rows[0][0] is None:
            return None
        try:
            return int(rows[0][0])
        except (TypeError, ValueError):
            logger.warning("Row count for %s was not an integer: %r", quoted_source, rows[0][0])
            return None

    def create_view(self, source_table_fqn: str, sample: ProfilerSample | None = None) -> str:
        """Create a temporary view over *source_table_fqn*.

        Returns the fully qualified view name.  The view is created using
        the caller's OBO token so that the user's table permissions are
        enforced.

        Args:
            source_table_fqn: Fully qualified source table.
            sample: Optional sampling policy. ``None`` (the default) and the
                ``full`` kind both select the whole table; ``percent`` and
                ``records`` narrow it to a *random* subset. See
                :func:`build_sample_select` for the SQL shape.
        """
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

        validate_fqn(source_table_fqn)
        self._ensure_schema()

        view_id = uuid4().hex[:12]
        view_name = f"{self._sql.catalog}.{self._sql.schema}.tmp_view_{view_id}"
        quoted_source = quote_fqn(source_table_fqn)
        quoted_view = quote_fqn(view_name)
        # One seed per view: baked into the view SQL so every scan of it draws
        # the same rows, while a later run gets a new view and a new seed.
        seed = secrets.randbelow(_MAX_SAMPLE_SEED)
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS {self._sample_select(quoted_source, sample, seed)}"

        logger.info("Creating view %s from %s", view_name, source_table_fqn)
        self._sql.execute(sql)

        grant_sql = f"GRANT SELECT ON VIEW {quoted_view} TO `account users`"
        try:
            self._sql.execute(grant_sql)
            logger.info("Granted SELECT on %s to account users", view_name)
        except Exception as e:
            logger.error(
                "GRANT SELECT failed on %s: %s — the background job (running as SP) "
                "will not be able to read this view. Check that the user has GRANT "
                "privileges on schema %s.%s",
                view_name,
                e,
                self._sql.catalog,
                self._sql.schema,
            )
            raise RuntimeError(
                f"Cannot grant SELECT on temporary view to account users. "
                f"Ensure the user has ownership or GRANT privilege on "
                f"schema {self._sql.catalog}.{self._sql.schema}: {e}"
            ) from e

        if not self._view_exists(view_name):
            raise RuntimeError(f"View creation succeeded but view not found: {view_name}")

        logger.info("View created and verified: %s", view_name)
        return view_name

    def _view_exists(self, view_fqn: str) -> bool:
        """Check if a view exists in Unity Catalog."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        sql = f"DESCRIBE TABLE {quote_fqn(view_fqn)}"
        try:
            self._sql.execute(sql)
            return True
        except Exception as e:
            logger.warning("View existence check failed for %s: %s", view_fqn, e)
            return False

    def create_view_from_sql(self, sql_query: str) -> str:
        """Create a temporary view whose body is an arbitrary SQL query.

        Used for cross-table SQL checks where the query itself returns
        the violation rows.  Returns the fully qualified view name.
        """
        from databricks.labs.dqx.utils import is_sql_query_safe
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        if not is_sql_query_safe(sql_query):
            raise UnsafeSqlQueryError(
                "The SQL query contains prohibited statements and cannot be used to create a view."
            )

        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        self._ensure_schema()

        view_id = uuid4().hex[:12]
        view_name = f"{self._sql.catalog}.{self._sql.schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS {sql_query}"

        logger.info("Creating SQL-check view %s", view_name)
        self._sql.execute(sql)

        grant_sql = f"GRANT SELECT ON VIEW {quoted_view} TO `account users`"
        try:
            self._sql.execute(grant_sql)
            logger.info("Granted SELECT on %s to account users", view_name)
        except Exception as e:
            logger.error(
                "GRANT SELECT failed on SQL-check view %s: %s — the background job "
                "will not be able to read this view.",
                view_name,
                e,
            )
            raise RuntimeError(
                f"Cannot grant SELECT on temporary view to account users. "
                f"Ensure the user has ownership or GRANT privilege on "
                f"schema {self._sql.catalog}.{self._sql.schema}: {e}"
            ) from e

        if not self._view_exists(view_name):
            raise RuntimeError(f"View creation succeeded but view not found: {view_name}")

        logger.info("SQL-check view created and verified: %s", view_name)
        return view_name

    def drop_view(self, view_fqn: str) -> None:
        """Drop a temporary view.  Best-effort -- logs warnings on failure.

        Tries the caller's OBO credentials first (views are created OBO so
        the creating user is the owner). Falls back to the service principal
        when wired — the SP holds ALL_PRIVILEGES on the tmp schema and can
        reap orphans the hourly sweep discovers after a client never polled
        run status to terminal.
        """
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        sql = f"DROP VIEW IF EXISTS {quote_fqn(view_fqn)}"
        try:
            self._sql.execute(sql)
            logger.info("Dropped view %s", view_fqn)
            return
        except Exception:
            logger.warning("OBO DROP failed for %s; trying service principal", view_fqn, exc_info=True)
        if self._sp_sql is not None:
            try:
                self._sp_sql.execute(sql)
                logger.info("Dropped view %s via service principal", view_fqn)
                return
            except Exception:
                logger.warning("Failed to drop view %s via service principal", view_fqn, exc_info=True)
        else:
            logger.warning("Failed to drop view %s and no service principal is available", view_fqn)
