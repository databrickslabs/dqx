"""DDL management for the UC objects backing the dq-score endpoints.

Two SP-owned objects in the app's main schema (dqlake-parity
architecture):

- *v_dq_check_results* — a plain UC shaping view over the long-format
  *dq_metrics* table. It pivots each run's *input_row_count* /
  *check_metrics* metric rows and explodes the per-rule JSON array into
  one row per (run_id, input_location, check_name), carrying
  *error_count*, *warning_count*, the table-wide *input_row_count*,
  *run_time*, and an *is_latest_run* flag (window function per
  input_location). A run whose *check_metrics* is absent, malformed, or
  empty still yields a single placeholder row (all three numeric
  columns NULL) so the endpoints can report its run id with a null
  score.
- *mv_dq_scores* — a UC metric view (CREATE VIEW ... WITH METRICS
  LANGUAGE YAML) over the shaping view with dimensions
  (input_location, run_id, run_time, is_latest_run, check_name) and
  measures *failed_tests* / *total_tests* / *score*. The score measure
  uses TRY_DIVIDE so a zero or NULL denominator yields SQL NULL — the
  exact analogue of ScoreService.compute_table_score returning None.

The score formula is numerically identical to
*ScoreService.compute_table_score* (including the approved filter
approximation: every check is treated as evaluated against all input
rows). ScoreService remains the formula's unit-tested specification;
see tests/test_score_view_service.py for the parity test.

Permission model: both views are created by the app's service
principal and execute with definer's rights. They are NOT a permission
boundary — the app-layer OBO catalog filtering
(*get_user_catalog_names*) in the dq_score routes remains the
enforcement point.

DDL is idempotent (CREATE OR REPLACE, one statement per call to the
Statement Execution API) and is re-applied on every app startup so
definition changes ship with the app — see *app._ensure_score_views*.
"""

from __future__ import annotations

import logging

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)

SHAPING_VIEW_NAME = "v_dq_check_results"
METRIC_VIEW_NAME = "mv_dq_scores"

# from_json schema for the observer's check_metrics JSON-array payload:
# [{"check_name": ..., "error_count": ..., "warning_count": ...}, ...].
# Mirrors metrics_utils.parse_check_metrics / CheckMetricBreakdown.
_CHECK_METRICS_JSON_SCHEMA = "ARRAY<STRUCT<check_name: STRING, error_count: BIGINT, warning_count: BIGINT>>"


def metric_view_fqn(catalog: str, schema: str) -> str:
    """Return the unquoted three-part name of the score metric view."""
    return f"{catalog}.{schema}.{METRIC_VIEW_NAME}"


class ScoreViewService:
    """Creates/refreshes the score shaping view + metric view (SP credentials)."""

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        # Quoted forms so hyphenated catalog names (prod-east) stay
        # parseable in object-name positions — same convention as
        # MigrationRunner.
        self._catalog_q = sql.q(sql.catalog)
        self._schema_q = sql.q(sql.schema)

    @property
    def shaping_view_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{SHAPING_VIEW_NAME}"

    @property
    def metric_view_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{METRIC_VIEW_NAME}"

    def shaping_view_ddl(self) -> str:
        """CREATE OR REPLACE VIEW statement for *v_dq_check_results*.

        Notes on fidelity to the Python path (metrics_utils +
        ScoreService):

        - TRY_CAST via DOUBLE mirrors *safe_int*'s tolerance of decimal
          strings ('123.0'); an unparseable/absent input_row_count
          becomes NULL, which the measures treat like the Python path's
          0 (score NULL).
        - LATERAL VIEW OUTER keeps no-check runs visible as a
          placeholder row whose numeric columns are all NULL so they
          never contribute to any SUM.
        - error_count/warning_count are COALESCE'd to 0 on real check
          rows, mirroring parse_check_metrics.
        """
        metrics_table = f"{self._catalog_q}.{self._schema_q}.dq_metrics"
        return (
            f"CREATE OR REPLACE VIEW {self.shaping_view_fqn_quoted} AS\n"
            "WITH per_run AS (\n"
            "  SELECT\n"
            "    run_id,\n"
            "    input_location,\n"
            "    MAX(run_time) AS run_time,\n"
            "    MAX(CASE WHEN metric_name = 'input_row_count' THEN metric_value END) AS input_row_count_str,\n"
            "    MAX(CASE WHEN metric_name = 'check_metrics' THEN metric_value END) AS check_metrics_json\n"
            f"  FROM {metrics_table}\n"
            "  GROUP BY run_id, input_location\n"
            "),\n"
            "ranked AS (\n"
            "  SELECT\n"
            "    per_run.*,\n"
            "    ROW_NUMBER() OVER (PARTITION BY input_location ORDER BY run_time DESC) AS rn\n"
            "  FROM per_run\n"
            "),\n"
            "exploded AS (\n"
            "  SELECT\n"
            "    r.run_id,\n"
            "    r.input_location,\n"
            "    r.run_time,\n"
            "    (r.rn = 1) AS is_latest_run,\n"
            "    r.input_row_count_str,\n"
            "    c.check_name,\n"
            "    c.error_count,\n"
            "    c.warning_count,\n"
            "    (c.check_name IS NULL AND c.error_count IS NULL AND c.warning_count IS NULL)\n"
            "      AS is_placeholder\n"
            "  FROM ranked r\n"
            "  LATERAL VIEW OUTER inline(\n"
            f"    from_json(r.check_metrics_json, '{_CHECK_METRICS_JSON_SCHEMA}')\n"
            "  ) c AS check_name, error_count, warning_count\n"
            ")\n"
            "SELECT\n"
            "  run_id,\n"
            "  input_location,\n"
            "  run_time,\n"
            "  is_latest_run,\n"
            "  check_name,\n"
            "  CASE WHEN is_placeholder THEN CAST(NULL AS BIGINT)\n"
            "       ELSE COALESCE(error_count, 0) END AS error_count,\n"
            "  CASE WHEN is_placeholder THEN CAST(NULL AS BIGINT)\n"
            "       ELSE COALESCE(warning_count, 0) END AS warning_count,\n"
            "  CASE WHEN is_placeholder THEN CAST(NULL AS BIGINT)\n"
            "       ELSE TRY_CAST(TRY_CAST(input_row_count_str AS DOUBLE) AS BIGINT) END AS input_row_count\n"
            "FROM exploded"
        )

    def metric_view_ddl(self) -> str:
        """CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML for *mv_dq_scores*."""
        yaml_body = (
            "version: 1.1\n"
            # Double-quoted: a backtick may not start a YAML plain scalar
            # (it is a reserved indicator character).
            f'source: "{self.shaping_view_fqn_quoted}"\n'
            "comment: Row-weighted DQ score measures over dq_metrics check results\n"
            "dimensions:\n"
            "  - name: input_location\n"
            "    expr: input_location\n"
            "  - name: run_id\n"
            "    expr: run_id\n"
            "  - name: run_time\n"
            "    expr: run_time\n"
            "  - name: is_latest_run\n"
            "    expr: is_latest_run\n"
            "  - name: check_name\n"
            "    expr: check_name\n"
            "measures:\n"
            "  - name: failed_tests\n"
            "    expr: SUM(error_count + warning_count)\n"
            "    comment: Total failed tests (errors + warnings) across the grouped check rows\n"
            "  - name: total_tests\n"
            "    expr: SUM(input_row_count)\n"
            "    comment: Total evaluated tests (input rows x checks) across the grouped check rows\n"
            "  - name: score\n"
            "    expr: 1 - TRY_DIVIDE(SUM(error_count + warning_count), SUM(input_row_count))\n"
            "    comment: Row-weighted DQ score between 0 and 1 (NULL when no rows or no checks)\n"
        )
        return (
            f"CREATE OR REPLACE VIEW {self.metric_view_fqn_quoted}\n"
            "WITH METRICS\n"
            "LANGUAGE YAML\n"
            "AS $$\n"
            f"{yaml_body}"
            "$$"
        )

    def ensure_views(self) -> None:
        """Create or replace both views, shaping view first.

        Raises on failure — the caller decides whether that is fatal
        (it is best-effort at app startup; see *app._ensure_score_views*).
        """
        shaping_ddl = self.shaping_view_ddl()
        logger.info(f"Creating/refreshing shaping view {SHAPING_VIEW_NAME}")
        self._sql.execute(shaping_ddl)
        metric_ddl = self.metric_view_ddl()
        logger.info(f"Creating/refreshing metric view {METRIC_VIEW_NAME}")
        self._sql.execute(metric_ddl)
