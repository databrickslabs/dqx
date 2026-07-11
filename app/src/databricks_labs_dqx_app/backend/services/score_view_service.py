"""DDL management for the UC objects backing the dq-score endpoints.

Four SP-owned objects in the app's main schema (dqlake-parity
architecture):

- *v_dq_check_attribution* — AS-OF-THE-RUN rule attribution parsed out
  of *dq_validation_runs.checks_json*, the complete rendered rule set
  the frozen runner persisted for each run (one row per run_id,
  source_table_fqn, check_name). It carries the check's *criticality*,
  the reserved *severity* / *dimension* tags frozen into
  *user_metadata* at materialization time, the *registry_rule_id*
  provenance tag, and the mapped *columns* (merging the single
  *check.arguments.column* and the plural *check.arguments.columns*
  into one ARRAY<STRING>). Because the source is the run's own frozen
  payload, editing or renaming a rule's tags today never rewrites
  historical results — attribution is version-accurate by
  construction. Runs without *checks_json* (legacy rows, the app's
  RUNNING lifecycle row) simply contribute no attribution rows.
- *v_dq_check_results* — a plain UC shaping view over the long-format
  *dq_metrics* table. It pivots each run's *input_row_count* /
  *check_metrics* metric rows and explodes the per-rule JSON array into
  one row per (run_id, input_location, check_name), carrying
  *error_count*, *warning_count*, the table-wide *input_row_count*,
  *run_time*, and an *is_latest_run* flag (window function per
  input_location), LEFT JOINed to *v_dq_check_attribution* so every
  check row also carries the metadata it RAN with (NULL — the untagged
  bucket — when the run has no frozen rule set or the check carries no
  tags, e.g. hand-authored or synthesized SQL checks). A run whose
  *check_metrics* is absent, malformed, or empty still yields a single
  placeholder row (all three numeric columns NULL) so the endpoints
  can report its run id with a null score.
- *v_dq_check_results_asof* — table-agnostic AS-OF expansion of the
  shaping view for carry-forward trends: one partition of rows per
  (include_drafts scope, run instant), where each table with a run
  at-or-before the instant repeats the check rows of its latest such
  run. The UC-side replacement for computing carry-forward averages
  server-side; see *asof_view_ddl* for the draft-handling and cost
  notes.
- *mv_dq_scores* — a UC metric view (CREATE VIEW ... WITH METRICS
  LANGUAGE YAML) over the shaping view with dimensions
  (input_location, run_id, run_time, is_latest_run, check_name,
  severity, dimension, criticality) and
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
from databricks_labs_dqx_app.backend.sql_utils import quote_object_fqn

logger = logging.getLogger(__name__)

ATTRIBUTION_VIEW_NAME = "v_dq_check_attribution"
SHAPING_VIEW_NAME = "v_dq_check_results"
ASOF_VIEW_NAME = "v_dq_check_results_asof"
METRIC_VIEW_NAME = "mv_dq_scores"

# Run-provenance tags stamped uniformly onto EVERY check's user_metadata at
# run-assembly time (services.binding_run_service). The frozen runner's
# _aggregate_rule_labels keeps only keys carrying the SAME value on every
# check, so a uniform stamp survives the intersection into the run-level
# dq_metrics.user_metadata map — which is where the shaping view reads it
# back out. RUN_MODE_TAG is "draft" | "published"; BINDING_VERSION_TAG is the
# approved snapshot version as a string (absent on draft runs).
RUN_MODE_TAG = "run_mode"
BINDING_VERSION_TAG = "binding_version"
RUN_MODE_DRAFT = "draft"
RUN_MODE_PUBLISHED = "published"

# from_json schema for the observer's check_metrics JSON-array payload:
# [{"check_name": ..., "error_count": ..., "warning_count": ...}, ...].
# Mirrors metrics_utils.parse_check_metrics / CheckMetricBreakdown.
_CHECK_METRICS_JSON_SCHEMA = "ARRAY<STRUCT<check_name: STRING, error_count: BIGINT, warning_count: BIGINT>>"

# from_json schema for dq_validation_runs.checks_json — the rendered rule
# set the frozen runner json-dumps per run. Addresses exactly the paths
# services.materializer.render_check produces (name / criticality /
# check.arguments.column|columns / user_metadata string map with the
# reserved severity+dimension tags and registry provenance); extra JSON
# fields (message_expr, filter, other arguments) are ignored by from_json.
# The contract test lives in test_score_view_service.py
# (TestChecksJsonAttributionContract).
_CHECKS_JSON_SCHEMA = (
    "ARRAY<STRUCT<"
    "name: STRING, "
    "criticality: STRING, "
    "check: STRUCT<arguments: STRUCT<column: STRING, columns: ARRAY<STRING>>>, "
    "user_metadata: MAP<STRING, STRING>"
    ">>"
)


def metric_view_fqn(catalog: str, schema: str) -> str:
    """Return the backtick-quoted three-part name of the score metric view.

    Catalog and schema are quoted per part (the same convention as the
    DDL side's *sql.q*) so a hyphenated catalog (``prod-east``) stays
    parseable on the READ paths too. The view-name constant is a known
    simple identifier and stays bare, matching the DDL.
    """
    return quote_object_fqn(catalog, schema, METRIC_VIEW_NAME)


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
    def attribution_view_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{ATTRIBUTION_VIEW_NAME}"

    @property
    def shaping_view_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{SHAPING_VIEW_NAME}"

    @property
    def asof_view_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{ASOF_VIEW_NAME}"

    @property
    def metric_view_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{METRIC_VIEW_NAME}"

    def attribution_view_ddl(self) -> str:
        """CREATE OR REPLACE VIEW statement for *v_dq_check_attribution*.

        Reads *dq_validation_runs* (READ-ONLY — the frozen runner owns the
        writes) and explodes each run's frozen *checks_json* rendered rule
        set into one attribution row per (run_id, source_table_fqn,
        check_name). Guards baked into the DDL:

        - *checks_json IS NOT NULL* — skips the app-inserted RUNNING
          lifecycle row and legacy pre-checks_json runs;
        - latest-row dedupe per (run_id, source_table_fqn) — the runner
          APPENDS its result row next to the app's lifecycle row(s), so
          only the newest row with a payload counts;
        - QUALIFY dedupe per check_name — duplicate names within one
          rendered set should not happen (names are unique per rule set)
          but a malformed payload must not fan out the join;
        - *check_name IS NOT NULL* — an unnamed check gets a
          DQX-generated name at run time and can never join back to its
          metrics row;
        - a malformed *checks_json* makes from_json yield NULL, so the
          run simply contributes no attribution rows (untagged bucket).
        """
        validation_runs = f"{self._catalog_q}.{self._schema_q}.dq_validation_runs"
        return (
            f"CREATE OR REPLACE VIEW {self.attribution_view_fqn_quoted} AS\n"
            "WITH run_checks AS (\n"
            "  SELECT\n"
            "    run_id,\n"
            "    source_table_fqn,\n"
            "    checks_json,\n"
            "    ROW_NUMBER() OVER (PARTITION BY run_id, source_table_fqn ORDER BY created_at DESC) AS rn\n"
            f"  FROM {validation_runs}\n"
            "  WHERE checks_json IS NOT NULL\n"
            "),\n"
            "exploded AS (\n"
            "  SELECT\n"
            "    r.run_id,\n"
            "    r.source_table_fqn,\n"
            "    c.pos,\n"
            "    c.col.name AS check_name,\n"
            "    c.col.criticality AS criticality,\n"
            "    c.col.user_metadata AS user_metadata,\n"
            "    c.col.check.arguments.column AS arg_column,\n"
            "    c.col.check.arguments.columns AS arg_columns\n"
            "  FROM run_checks r\n"
            "  LATERAL VIEW posexplode(\n"
            f"    from_json(r.checks_json, '{_CHECKS_JSON_SCHEMA}')\n"
            "  ) c AS pos, col\n"
            "  WHERE r.rn = 1\n"
            ")\n"
            "SELECT\n"
            "  run_id,\n"
            "  source_table_fqn,\n"
            "  check_name,\n"
            "  criticality,\n"
            "  user_metadata['severity'] AS severity,\n"
            "  user_metadata['dimension'] AS dimension,\n"
            "  user_metadata['registry_rule_id'] AS registry_rule_id,\n"
            "  COALESCE(arg_columns, CASE WHEN arg_column IS NOT NULL THEN array(arg_column) END) AS columns\n"
            "FROM exploded\n"
            "WHERE check_name IS NOT NULL\n"
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id, source_table_fqn, check_name ORDER BY pos) = 1"
        )

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
        - the LEFT JOIN to *v_dq_check_attribution* stamps every check
          row with the AS-OF-RUN severity/dimension/criticality/columns
          it executed with; rows without a frozen rule set keep NULLs
          (untagged bucket). LEFT — never INNER — so legacy runs stay
          visible.
        - *run_mode* ('draft' | 'published') is read from the run-level
          ``dq_metrics.user_metadata`` map (the run-provenance tag the
          app stamps uniformly onto every check at run-assembly time —
          the map repeats per metric row of a run, so MAX over the
          grouped rows picks it from any row). Untagged (legacy) runs
          classify as 'published', full stop: the draft concept did not
          exist when they ran (every pre-tag app run was a real,
          user-visible result), and preview runs never persist metrics
          so they can never appear here. A run_type-based heuristic
          (scheduled -> published, dryrun -> draft) was tried and
          reverted: before the tag existed EVERY app run was submitted
          with task_type='dryrun' (only promoted to 'scheduled' when
          sample_size == 0), so that heuristic reclassified the entire
          pre-upgrade run history as drafts and hid it under the
          endpoints' published-only default.
        - *binding_version* is the approved snapshot version the run
          executed (tag-only — NULL for draft runs and every legacy run).
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
            "    MAX(CASE WHEN metric_name = 'check_metrics' THEN metric_value END) AS check_metrics_json,\n"
            f"    MAX(user_metadata['{RUN_MODE_TAG}']) AS run_mode_tag,\n"
            f"    MAX(user_metadata['{BINDING_VERSION_TAG}']) AS binding_version_tag\n"
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
            "    r.run_mode_tag,\n"
            "    r.binding_version_tag,\n"
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
            "  e.run_id,\n"
            "  e.input_location,\n"
            "  e.run_time,\n"
            "  e.is_latest_run,\n"
            "  e.check_name,\n"
            "  CASE WHEN e.is_placeholder THEN CAST(NULL AS BIGINT)\n"
            "       ELSE COALESCE(e.error_count, 0) END AS error_count,\n"
            "  CASE WHEN e.is_placeholder THEN CAST(NULL AS BIGINT)\n"
            "       ELSE COALESCE(e.warning_count, 0) END AS warning_count,\n"
            "  CASE WHEN e.is_placeholder THEN CAST(NULL AS BIGINT)\n"
            "       ELSE TRY_CAST(TRY_CAST(e.input_row_count_str AS DOUBLE) AS BIGINT) END AS input_row_count,\n"
            # Run-mode resolution: the run-level tag wins; untagged
            # (legacy) runs classify as published — see the docstring
            # for why no run_type heuristic is applied here.
            f"  COALESCE(e.run_mode_tag, '{RUN_MODE_PUBLISHED}') AS run_mode,\n"
            "  TRY_CAST(e.binding_version_tag AS INT) AS binding_version,\n"
            "  a.criticality,\n"
            "  a.severity,\n"
            "  a.dimension,\n"
            "  a.registry_rule_id,\n"
            "  a.columns\n"
            "FROM exploded e\n"
            f"LEFT JOIN {self.attribution_view_fqn_quoted} a\n"
            "  ON a.run_id = e.run_id\n"
            "  AND a.source_table_fqn = e.input_location\n"
            "  AND a.check_name = e.check_name"
        )

    def asof_view_ddl(self) -> str:
        """CREATE OR REPLACE VIEW statement for *v_dq_check_results_asof*.

        Table-agnostic AS-OF expansion of the shaping view — the UC-side
        source for every carry-forward trend (the product/global Average
        and the dimension/severity popovers; dqlake's
        ``v_product_check_consolidated`` analogue, generalised to all
        tables). For every distinct run instant across ALL tables
        (*as_of_time*), each table with a run at-or-before that instant
        contributes the check rows of its LATEST such run, stamped with
        the instant. Readers scope with a plain
        ``input_location IN (...)`` filter at query time — no window
        functions or as-of joins needed on the read side.

        DRAFT HANDLING — the *include_drafts* discriminator column. The
        expansion is built TWICE, once per read scope, because both the
        instant set and the carry choice must be computed WITHIN the run
        universe the reader wants: filtering an all-runs expansion by
        run_mode after the fact would leave draft-run instants in the
        published series and would DROP a table at instants where its
        latest run was a draft instead of carrying its latest published
        run. The ``include_drafts = false`` partition is built over
        published runs only; the ``true`` partition over all runs
        (published runs therefore appear in both). Every reader filters
        to exactly ONE partition (``NOT include_drafts`` by default) and
        must never mix the two.

        COST — a plain view, computed on read; nothing is stored. The
        instants x runs join is quadratic in runs-per-scope in the worst
        case, but the source is the app-managed ``dq_metrics`` (90-day
        retention, page-scale run counts), the two partitions prune via
        the reader's ``include_drafts`` predicate, and the check-row
        fan-out happens only after the rn = 1 filter picks one run per
        (instant, table). Rows whose *run_time* is NULL cannot be
        ordered and never enter the expansion (the join drops them).
        """
        v = self.shaping_view_fqn_quoted
        return (
            f"CREATE OR REPLACE VIEW {self.asof_view_fqn_quoted} AS\n"
            "WITH runs AS (\n"
            "  SELECT DISTINCT input_location, run_id, run_time, run_mode\n"
            f"  FROM {v}\n"
            "  WHERE run_time IS NOT NULL\n"
            "),\n"
            # The two read scopes. A published run belongs to both; a
            # draft run only to the drafts-inclusive one.
            "scopes AS (SELECT explode(array(false, true)) AS include_drafts),\n"
            "scoped AS (\n"
            "  SELECT r.input_location, r.run_id, r.run_time, s.include_drafts\n"
            "  FROM runs r\n"
            f"  JOIN scopes s ON s.include_drafts OR r.run_mode = '{RUN_MODE_PUBLISHED}'\n"
            "),\n"
            "instants AS (\n"
            "  SELECT DISTINCT include_drafts, run_time AS as_of_time FROM scoped\n"
            "),\n"
            "asof AS (\n"
            "  SELECT i.include_drafts, i.as_of_time, s.input_location, s.run_id,\n"
            "         ROW_NUMBER() OVER (\n"
            "           PARTITION BY i.include_drafts, i.as_of_time, s.input_location\n"
            "           ORDER BY s.run_time DESC) AS rn\n"
            "  FROM instants i\n"
            "  JOIN scoped s\n"
            "    ON s.include_drafts = i.include_drafts AND s.run_time <= i.as_of_time\n"
            ")\n"
            "SELECT\n"
            "  a.include_drafts,\n"
            "  a.as_of_time,\n"
            "  c.run_id,\n"
            "  c.input_location,\n"
            "  c.run_time,\n"
            "  c.is_latest_run,\n"
            "  c.check_name,\n"
            "  c.error_count,\n"
            "  c.warning_count,\n"
            "  c.input_row_count,\n"
            "  c.run_mode,\n"
            "  c.binding_version,\n"
            "  c.criticality,\n"
            "  c.severity,\n"
            "  c.dimension,\n"
            "  c.registry_rule_id,\n"
            "  c.columns\n"
            "FROM asof a\n"
            f"JOIN {v} c\n"
            "  ON c.run_id = a.run_id AND c.input_location = a.input_location\n"
            "WHERE a.rn = 1"
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
            # Run provenance ('draft' | 'published') — the stamped tag,
            # with untagged legacy runs resolved to 'published' in the
            # shaping view.
            "  - name: run_mode\n"
            "    expr: run_mode\n"
            "  - name: check_name\n"
            "    expr: check_name\n"
            # As-of-run attribution (frozen into checks_json at
            # materialization time — later tag edits never rewrite these).
            "  - name: severity\n"
            "    expr: severity\n"
            "  - name: dimension\n"
            "    expr: dimension\n"
            "  - name: criticality\n"
            "    expr: criticality\n"
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
        """Create or replace all four views, dependencies first.

        Order: attribution view (no dependencies), then the shaping view
        (joins the attribution view), then the as-of expansion and the
        metric view (both source the shaping view). Raises on failure —
        the caller decides whether that is fatal (it is best-effort at
        app startup; see *app._ensure_score_views*).
        """
        attribution_ddl = self.attribution_view_ddl()
        logger.info(f"Creating/refreshing attribution view {ATTRIBUTION_VIEW_NAME}")
        self._sql.execute(attribution_ddl)
        shaping_ddl = self.shaping_view_ddl()
        logger.info(f"Creating/refreshing shaping view {SHAPING_VIEW_NAME}")
        self._sql.execute(shaping_ddl)
        asof_ddl = self.asof_view_ddl()
        logger.info(f"Creating/refreshing as-of expansion view {ASOF_VIEW_NAME}")
        self._sql.execute(asof_ddl)
        metric_ddl = self.metric_view_ddl()
        logger.info(f"Creating/refreshing metric view {METRIC_VIEW_NAME}")
        self._sql.execute(metric_ddl)
