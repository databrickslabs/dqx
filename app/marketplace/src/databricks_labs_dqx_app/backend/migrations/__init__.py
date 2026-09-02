"""Database migration runner for DQX Studio (Delta).

Migrations are versioned DDL statements applied in order against the
configured Unity Catalog catalog/schema. The runner tracks every
applied version in a ``dq_migrations`` meta-table, so re-starting the
app never re-applies a migration that already succeeded.

Storage split
-------------
The Delta schema contains only the high-volume analytical tables:

- **v1 — Delta analytical baseline.** Holds the
  Spark-written tables: ``dq_validation_runs``,
  ``dq_profiling_results``, ``dq_quarantine_records``,
  ``dq_metrics``.

Transactional application state is always created by
:mod:`backend.migrations.postgres` in Lakebase. There is no Delta OLTP
storage path.

Atomicity model — Delta vs Postgres asymmetry
---------------------------------------------
The two backends have **different** failure-recovery semantics. Read
this before adding a new Delta migration:

- **Postgres path** (:mod:`backend.migrations.postgres`) — every DDL
  statement in a migration *and* the ``dq_migrations`` version-row
  ``INSERT`` execute inside a **single transaction** (``BEGIN`` …
  ``COMMIT``). If any statement fails the whole migration rolls back;
  the next run retries cleanly from the beginning.
- **Delta path** (this module) — the Databricks Statement Execution
  API auto-commits **per call**; there is no ``BEGIN``/``COMMIT``
  primitive for DDL on a SQL warehouse, so true atomicity is not
  available. A mid-migration failure therefore leaves earlier
  statements committed but the version row **uninserted**. Recovery
  relies on every DDL statement being individually re-runnable.

That recovery contract has three concrete invariants every Delta
migration author MUST satisfy:

1. **Idempotent DDL.** Use ``CREATE TABLE IF NOT EXISTS`` and
   ``CREATE SCHEMA IF NOT EXISTS``; never plain ``CREATE``. Plain
   ``ALTER TABLE ADD COLUMN`` and ``ALTER TABLE ADD CONSTRAINT`` are
   OK because their "already-exists" error fragments are swallowed
   (see ``_IDEMPOTENT_ERROR_FRAGMENTS``).
2. **Every "already exists" error a statement can raise must appear in
   ``_IDEMPOTENT_ERROR_FRAGMENTS``.** If you introduce a new DDL kind
   (``CREATE INDEX``, ``CREATE VIEW``, ``CREATE FUNCTION`` …) and
   half-applied recovery surfaces an unfamiliar error message, the
   migration becomes un-replayable in production until someone
   appends the fragment and ships a fix. Validate by checking the
   error catalog for the DDL kind you're adding.
3. **Template scanner invariants** — checked at module import by
   ``_validate_template_safe`` against every entry in :data:`MIGRATIONS`,
   so any violation crashes the app at boot rather than mid-deploy:

   - **No ``;`` inside a single-quoted string literal.** The runner
     splits compound migration bodies on ``;`` because the Statement
     Execution API only accepts one statement per call. A ``;``
     inside a CHECK constraint string, default value, or comment
     would split a statement mid-literal and silently corrupt it.
     If you ever genuinely need ``;`` in a literal, replace
     ``str.split(";")`` inside ``_apply`` with a real SQL statement
     splitter (e.g. ``sqlparse.split``).
   - **No ``{catalog}`` / ``{schema}`` placeholder inside a single-
     quoted string literal.** ``_apply`` substitutes the
     :meth:`SqlExecutor.q`-quoted form into these placeholders so
     hyphenated Databricks catalog names like ``prod-east`` parse
     correctly when used as object identifiers. The quoted form
     contains backticks, which would be nonsense inside a string
     literal. If you need the raw identifier inside a literal,
     introduce a separate placeholder rather than reusing
     ``{catalog}`` / ``{schema}``.

The Postgres runner does not need invariants 1 and 2 (the transaction
guarantees it) but does still split on ``;`` and so applies invariant 3
to its own templates.

Status casing convention
------------------------
Two status families intentionally use different casing:

- **Run-lifecycle** (``dq_validation_runs.status``,
  ``dq_profiling_results.status``) — UPPERCASE
  (``RUNNING``/``SUCCESS``/``FAILED``/``CANCELED``). These mirror the
  Databricks Jobs SDK ``life_cycle_state`` / ``result_state`` values
  that are passed straight through ``RunStatusOut`` to the frontend.
- **App-domain workflow** (``dq_quality_rules.status``,
  ``dq_schedule_runs.status``) — lowercase. These are pure DQX
  vocabulary (``draft``/``approved``, ``pending``/``partial_failure``)
  with no SDK counterpart.

CHECK constraints enforce the agreed values per domain — see each
table's ``chk_*_status`` constraint below.

Changing the schema
-------------------
:data:`MIGRATIONS` holds only the analytical baseline above — the schema is
expressed as ``CREATE TABLE`` at its final shape, not as a replayable
chain of ``ALTER TABLE`` steps. The app has no external installs to
upgrade yet, so a shape change is edited into the baseline in place
(adding the column to the ``CREATE TABLE`` body). OLTP changes belong only
in :mod:`backend.migrations.postgres`.

The only ``ALTER TABLE`` statements here are ``ADD CONSTRAINT``: Delta
accepts just PRIMARY KEY / FOREIGN KEY inline in ``CREATE TABLE``, so
every CHECK constraint has to follow its table as a separate statement.

**An existing deployment does not pick up an edited baseline.** Its
``dq_migrations`` table already records v1 as applied, so the runner
skips them and the old columns stay. Re-provision such a workspace::

    DROP SCHEMA <catalog>.<schema> CASCADE;

then redeploy — the baselines run from scratch.

Once the app ships externally this has to change: append a new
:class:`Migration` with the next version number instead of editing a
baseline, and never reorder existing entries. For column additions use
``ALTER TABLE ... ADD COLUMN`` (do *not* use ``ADD COLUMN IF NOT
EXISTS`` — it is not supported on all Databricks SQL warehouse
versions; ``_apply`` instead catches and tolerates
``COLUMN_ALREADY_EXISTS`` so re-running is safe).
"""

import logging
import re
from dataclasses import dataclass

from databricks_labs_dqx_app.backend.migrations.postgres import OLTP_TABLE_NAMES
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


# Migration templates have two invariants that ``_apply`` relies on but
# the language doesn't enforce. ``_validate_template_safe`` checks both
# at module import time against every entry in :data:`MIGRATIONS` so a
# future author hits an :class:`AssertionError` on app boot instead of
# a half-applied or mis-quoted migration in production:
#
# 1. **No ``;`` inside a single-quoted string literal.**
#    ``_apply`` splits the template on ``;`` to feed one statement per
#    Statement Execution API call. A semicolon inside a string literal
#    would split the literal in half and corrupt both halves.
#
# 2. **No ``{catalog}`` or ``{schema}`` placeholder inside a single-
#    quoted string literal.** ``_apply`` substitutes the executor-
#    *quoted* form into those placeholders (so hyphenated catalogs like
#    ``prod-east`` parse correctly when used as identifiers). The
#    quoted form contains backticks, which would be nonsense inside a
#    string literal context. Today no migration uses placeholders in a
#    literal — this guard prevents a future author from introducing one
#    without adding a separate raw placeholder first.
#
# Doubled single-quotes (``''``) are the standard SQL escape for a
# literal apostrophe inside a single-quoted string; the scanner handles
# them correctly by keeping the in-literal flag set across the pair.
_SINGLE_QUOTE = "'"
_PLACEHOLDER_TOKENS = ("{catalog}", "{schema}")


def _validate_template_safe(template: str) -> None:
    """Assert *template* satisfies the runner's substitution invariants.

    See the comment block above this function for the full list. In
    short: no ``;`` and no ``{catalog}``/``{schema}`` placeholder may
    appear inside a single-quoted string literal.

    The scanner is a tiny state machine: it walks the template character
    by character, flips an ``in_literal`` flag on each unescaped ``'``,
    and asserts neither forbidden pattern is seen while the flag is set.

    Raises:
        AssertionError: if any forbidden pattern appears inside a
            single-quoted string literal in *template*.
    """
    in_literal = False
    i = 0
    n = len(template)
    while i < n:
        ch = template[i]
        if ch == _SINGLE_QUOTE:
            # Doubled single-quote ('') inside a literal is the SQL
            # escape for a literal apostrophe; keep the flag as-is and
            # skip both characters.
            if in_literal and i + 1 < n and template[i + 1] == _SINGLE_QUOTE:
                i += 2
                continue
            in_literal = not in_literal
            i += 1
            continue
        if in_literal:
            if ch == ";":
                _raise_template_violation(template, i, "';' inside a single-quoted string literal", split_hint=True)
            for tok in _PLACEHOLDER_TOKENS:
                if template.startswith(tok, i):
                    _raise_template_violation(
                        template,
                        i,
                        f"placeholder {tok!r} inside a single-quoted string literal",
                        split_hint=False,
                    )
        i += 1


def _raise_template_violation(template: str, offset: int, what: str, *, split_hint: bool) -> None:
    """Raise an actionable :class:`AssertionError` for a template scanner failure."""
    start = max(0, offset - 40)
    end = min(len(template), offset + 40)
    excerpt = template[start:end].replace("\n", " ")
    msg = f"Migration template contains {what} at offset {offset} (... {excerpt!r} ...). "
    if split_hint:
        msg += (
            "MigrationRunner._apply uses str.split(';') which would silently corrupt this "
            "statement. Either move the ';' out of the literal, or replace the splitter in "
            "_apply with a real SQL statement splitter (e.g. sqlparse.split)."
        )
    else:
        msg += (
            "MigrationRunner._apply substitutes the executor-quoted form (containing "
            "backticks) into these placeholders so identifiers like 'prod-east' stay "
            "parseable in object-name positions — but that quoted form would be nonsense "
            "inside a string literal. Use a separate raw placeholder for literal contexts."
        )
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Migration definitions
# ---------------------------------------------------------------------------

_PLACEHOLDER = "{catalog}.{schema}"


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    sql_template: str


# Order is significant: v1 before v2.
#
# Both entries are baselines — every table is defined at its final shape
# with liquid clustering, primary keys, and CHECK constraints. Editing a
# baseline in place is the intended way to change the schema until the
# app ships externally; existing dev workspaces pick the change up by
# ``DROP SCHEMA … CASCADE`` and re-running migrations from scratch.
#
# Notes on column choices:
# - PRIMARY KEY constraints are informational (``NOT ENFORCED RELY``);
#   Delta uses them for query optimization, lineage, AI/BI tooling.
# - CHECK constraints ARE enforced — picking the right value-set on
#   day one is cheap; loosening later is just an ALTER TABLE.
# - ``VARIANT`` (DBR 15.3+ / serverless) replaces ad-hoc JSON-in-string
#   for the largest blob columns (``dq_quality_rules.check``,
#   ``dq_quarantine_records.row_data``/``errors``).
# - Run-lifecycle ``status`` columns use UPPERCASE values to mirror the
#   Databricks Jobs SDK; app-domain ``status`` columns use lowercase.
# v1 always runs against Delta and only contains the high-volume
# analytical tables that the Spark task runner writes.  Keeping these
# in Delta lets AI/BI dashboards consume them directly via SQL
# warehouse without round-tripping through Postgres.
_V1_ANALYTICAL_BASELINE = (
    # Profiler runs — one row per profile job. Mutable lifecycle
    # (RUNNING → SUCCESS/FAILED/CANCELED). Status values mirror
    # Databricks Jobs SDK convention.
    #
    # NOTE: Delta only allows PRIMARY KEY and FOREIGN KEY constraints
    # inline in CREATE TABLE — every CHECK constraint must be added
    # via a separate ALTER TABLE … ADD CONSTRAINT statement after the
    # table exists. The migration runner swallows
    # ``DELTA_CONSTRAINT_ALREADY_EXISTS`` so re-runs are idempotent.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_profiling_results ("
    "  run_id STRING NOT NULL,"
    "  requesting_user STRING,"
    "  source_table_fqn STRING NOT NULL,"
    "  view_fqn STRING,"
    "  sample_limit INT,"
    "  rows_profiled INT,"
    "  columns_profiled INT,"
    "  duration_seconds DOUBLE,"
    "  summary_json STRING,"
    "  generated_rules_json STRING,"
    "  status STRING NOT NULL,"
    "  error_message STRING,"
    "  canceled_by STRING,"
    "  updated_at TIMESTAMP,"
    "  job_run_id BIGINT,"
    "  rule_set_fingerprint STRING,"
    "  created_at TIMESTAMP"
    ") CLUSTER BY (source_table_fqn, run_id, created_at);"
    f"ALTER TABLE {_PLACEHOLDER}.dq_profiling_results "
    f"  ADD CONSTRAINT chk_dq_profiling_results_status "
    f"  CHECK (status IN ('RUNNING','SUCCESS','FAILED','CANCELED'));"
    #
    # Validation (dryrun + scheduled) runs — one row per run, mutable
    # lifecycle.  Joins to ``dq_metrics`` on
    # ``(run_id, rule_set_fingerprint)``.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_validation_runs ("
    "  run_id STRING NOT NULL,"
    "  requesting_user STRING,"
    "  source_table_fqn STRING NOT NULL,"
    "  view_fqn STRING,"
    "  checks_json STRING,"
    "  sample_size INT,"
    "  total_rows INT,"
    "  valid_rows INT,"
    "  invalid_rows INT,"
    "  error_rows INT,"
    "  warning_rows INT,"
    "  error_summary_json STRING,"
    "  sample_invalid_json STRING,"
    "  status STRING NOT NULL,"
    "  error_message STRING,"
    "  canceled_by STRING,"
    "  updated_at TIMESTAMP,"
    "  run_type STRING,"
    "  job_run_id BIGINT,"
    "  rule_set_fingerprint STRING,"
    "  created_at TIMESTAMP"
    ") CLUSTER BY (source_table_fqn, run_id, created_at);"
    f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs "
    f"  ADD CONSTRAINT chk_dq_validation_runs_status "
    f"  CHECK (status IN ('RUNNING','SUCCESS','FAILED','CANCELED'));"
    f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs "
    f"  ADD CONSTRAINT chk_dq_validation_runs_run_type "
    f"  CHECK (run_type IS NULL OR run_type IN ('dryrun','scheduled','preview'));"
    #
    # Quarantined invalid rows captured during validation.  ``row_data``
    # and ``errors`` are VARIANT for native JSON predicate pushdown and
    # ~10x compression vs. STRING.
    # Liquid-clustered by (run_id, source_table_fqn): run-scoped writes
    # co-locate, and the by-table quarantine views filter on source_table_fqn.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quarantine_records ("
    "  quarantine_id STRING NOT NULL,"
    "  run_id STRING NOT NULL,"
    "  source_table_fqn STRING NOT NULL,"
    "  requesting_user STRING,"
    "  row_data VARIANT,"
    "  errors VARIANT,"
    "  warnings VARIANT,"
    "  created_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_quarantine_records PRIMARY KEY (quarantine_id) RELY"
    ") CLUSTER BY (run_id, source_table_fqn);"
    #
    # Long-format observability events written by DQMetricsObserver.
    # Schema mirrors the public DQX OBSERVATION_TABLE_SCHEMA so AI/BI
    # dashboard templates targeting the spec drop straight in.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_metrics ("
    "  run_id STRING NOT NULL,"
    "  run_name STRING,"
    "  input_location STRING,"
    "  output_location STRING,"
    "  quarantine_location STRING,"
    "  checks_location STRING,"
    "  rule_set_fingerprint STRING,"
    "  metric_name STRING NOT NULL,"
    "  metric_value STRING,"
    "  run_time TIMESTAMP NOT NULL,"
    "  error_column_name STRING,"
    "  warning_column_name STRING,"
    "  user_metadata MAP<STRING, STRING>"
    ") CLUSTER BY (input_location, run_id, run_time)"
)


MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        description="Delta analytical baseline (validation, profiling, quarantine, metrics)",
        sql_template=_V1_ANALYTICAL_BASELINE,
    ),
]


# Fail loudly at app import if any migration template violates the
# scanner invariants (no ``;`` and no ``{catalog}``/``{schema}``
# placeholder inside a string literal). Converts latent correctness
# risks into deploy-time AssertionErrors; see the "Atomicity model"
# section of the module docstring for the full recovery contract.
for _m in MIGRATIONS:
    _validate_template_safe(_m.sql_template)

# ---------------------------------------------------------------------------
# App-owned table registry (derived, single source of truth)
# ---------------------------------------------------------------------------
#
# The authoritative list of tables the DQX Studio owns is derived directly
# from the ``CREATE TABLE IF NOT EXISTS`` statements in the migration
# templates above, so it can never drift from what the migrations actually
# create. Consumers that need to operate over *all* app-owned tables — most
# notably the admin "Reset database" feature
# (``services/database_reset_service.py``) — import these tuples rather than
# hand-maintaining a parallel list.
#
# The split mirrors the physical backend routing (see the module docstring):
# analytical tables are derived here and always live in Delta; OLTP tables are
# derived from the Postgres migration catalogue and always live in Lakebase.
#
# The ``dq_migrations`` meta-table is deliberately NOT included: it tracks
# applied schema versions and must survive a data reset so migrations are not
# re-run against an already-migrated schema.
_CREATE_TABLE_RE = re.compile(r"CREATE TABLE IF NOT EXISTS \{catalog\}\.\{schema\}\.([a-z_][a-z0-9_]*)")


def _created_table_names() -> tuple[str, ...]:
    """Extract analytical table names from the Delta migrations."""
    names: list[str] = []
    seen: set[str] = set()
    for migration in MIGRATIONS:
        for name in _CREATE_TABLE_RE.findall(migration.sql_template):
            if name not in seen:
                seen.add(name)
                names.append(name)
    return tuple(names)


# Tables that always live in Delta (analytical / Spark-written).
ANALYTICAL_TABLE_NAMES: tuple[str, ...] = _created_table_names()

# Every table the app owns, across both backends.
ALL_APP_TABLE_NAMES: tuple[str, ...] = ANALYTICAL_TABLE_NAMES + OLTP_TABLE_NAMES

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_META_TABLE = f"{_PLACEHOLDER}.dq_migrations"


class MigrationRunner:
    """Applies pending database migrations against the DQX Studio schema.

    All DDL is executed via the Databricks Statement Execution API using
    the app's service-principal credentials, *not* the OBO user token.

    Usage::

        runner = MigrationRunner(sql=sql_executor)
        applied = runner.run_all()
        # applied == number of migrations just executed (0 if already up to date)
    """

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        self._catalog = sql.catalog
        self._schema = sql.schema
        # Pre-compute the executor-quoted forms once. Every place the
        # catalog/schema appears in a SQL statement uses the quoted
        # form so a hyphenated Databricks catalog name (``prod-east``,
        # ``team-data-platform``) — which is parse-invalid raw — stays
        # safe end-to-end. Templates use ``{catalog}``/``{schema}``
        # placeholders in object-name positions only (never inside a
        # string literal), so substituting the quoted form is always
        # correct. See :meth:`SqlExecutor.q` for the dialect contract.
        self._catalog_q = sql.q(sql.catalog)
        self._schema_q = sql.q(sql.schema)
        self._meta_table = _META_TABLE.format(catalog=self._catalog_q, schema=self._schema_q)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_all(self) -> int:
        """Ensure the schema exists and apply all pending analytical Delta migrations.

        Returns:
            The number of migrations applied in this invocation.
        """
        self._ensure_schema()
        self._ensure_meta_table()
        applied_versions = self._applied_versions()

        count = 0
        for migration in MIGRATIONS:
            if migration.version in applied_versions:
                logger.debug(
                    "Migration v%d (%s) already applied, skipping",
                    migration.version,
                    migration.description,
                )
                continue

            logger.info(
                "Applying migration v%d: %s",
                migration.version,
                migration.description,
            )
            self._apply(migration)
            count += 1

        return count

    def status(self) -> list[dict[str, object]]:
        """Return the list of all migrations with their applied status.

        Returns:
            A list of dicts with keys ``version``, ``description``,
            ``applied``, and ``applied_at`` (ISO string or ``None``).
        """
        self._ensure_schema()
        self._ensure_meta_table()
        applied_at = self._applied_at_map()
        return [
            {
                "version": m.version,
                "description": m.description,
                "applied": m.version in applied_at,
                "applied_at": applied_at.get(m.version),
            }
            for m in MIGRATIONS
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        """Create the catalog schema if it doesn't exist.

        Must use the bootstrap executor (catalog-only context) because the
        schema does not exist yet and passing ``schema=<name>`` to the
        statement-execution API would cause it to fail before the DDL runs.

        Catalog and schema are interpolated through :attr:`_catalog_q` /
        :attr:`_schema_q` so a hyphenated catalog (``prod-east``) — which
        is parse-invalid raw — gets correctly backtick-quoted by
        :meth:`SqlExecutor.q`.
        """
        sql = f"CREATE SCHEMA IF NOT EXISTS {self._catalog_q}.{self._schema_q}"
        self._sql.execute_no_schema(sql)
        logger.debug("Ensured schema exists: %s.%s", self._catalog, self._schema)

    def _ensure_meta_table(self) -> None:
        """Create the migration tracking table if it doesn't exist."""
        sql = (
            f"CREATE TABLE IF NOT EXISTS {self._meta_table} ("
            "  version INT NOT NULL,"
            "  description STRING NOT NULL,"
            "  applied_at TIMESTAMP NOT NULL"
            ")"
        )
        self._sql.execute(sql)
        logger.debug("Ensured migrations meta-table exists: %s", self._meta_table)

    def _applied_versions(self) -> set[int]:
        return set(self._applied_at_map().keys())

    def _applied_at_map(self) -> dict[int, str]:
        sql = f"SELECT version, CAST(applied_at AS STRING) FROM {self._meta_table} ORDER BY version"
        rows = self._sql.query(sql)
        return {int(row[0]): row[1] for row in rows}

    # Errors that mean "the desired state is already in place" — safe
    # to swallow so that re-running a migration is a no-op. The
    # baseline statement uses ``CREATE TABLE IF NOT EXISTS``, but if a
    # future migration appends an ``ALTER TABLE … ADD COLUMN`` or
    # ``CLUSTER BY`` and the workspace already has it, we don't want
    # the whole migration to abort.
    _IDEMPOTENT_ERROR_FRAGMENTS = (
        "COLUMN_ALREADY_EXISTS",
        # Databricks Delta surfaces an ``ADD COLUMN`` that targets an
        # already-present column as ``FIELD_ALREADY_EXISTS`` (singular).
        # Older versions used the plural ``FIELDS_ALREADY_EXISTS``;
        # keep both to defend against runtime wording drift across
        # workspaces / DBR versions.
        "FIELD_ALREADY_EXISTS",
        "FIELDS_ALREADY_EXISTS",
        "TABLE_OR_VIEW_ALREADY_EXISTS",
        "TABLE_ALREADY_EXISTS",
        "already has liquid clustering defined",
        # CHECK constraints are added via ``ALTER TABLE … ADD CONSTRAINT``
        # in a separate statement after CREATE TABLE (Delta only allows
        # PK/FK inline). When a previous migration attempt got past
        # CREATE TABLE but failed before recording the version, the
        # next run sees the constraint and emits this error — safe to
        # swallow because the desired state is already in place. The
        # second fragment guards against future error-message tweaks
        # since Databricks has used both the SQLSTATE-prefixed code and
        # plain English wording at different times.
        "DELTA_CONSTRAINT_ALREADY_EXISTS",
        "constraint already exists",
    )

    def _apply(self, migration: Migration) -> None:
        """Apply *migration* statement-by-statement against the warehouse.

        Unlike the Postgres runner, this method **cannot** wrap a
        multi-statement migration + the ``dq_migrations`` version-row
        ``INSERT`` in a single transaction: the Databricks Statement
        Execution API auto-commits per call and there is no ``BEGIN``/
        ``COMMIT`` primitive for DDL on a SQL warehouse. A mid-migration
        failure therefore leaves earlier statements committed but the
        version row uninserted, and the next ``run_all`` re-runs the
        whole migration from the beginning.

        Recovery is safe **only** because every DDL kind we currently
        emit raises an "already exists"-style error that
        ``_IDEMPOTENT_ERROR_FRAGMENTS`` swallows, letting the rerun
        skip past committed statements and reach the
        previously-failed one. See the module docstring's
        "Atomicity model" section for the full contract that any new
        migration author has to satisfy.

        Templates are split on ``;`` so each statement reaches the API
        as a single call (the API rejects compound statements). The
        ``_validate_template_split_safe`` check at module import time
        guarantees no template embeds a ``;`` inside a string literal,
        which would otherwise silently corrupt the split.

        ``{catalog}`` / ``{schema}`` placeholders receive the
        :meth:`SqlExecutor.q`-quoted form so identifiers containing
        characters outside ``[A-Za-z0-9_]`` (most commonly a hyphen
        in a Databricks catalog name) stay parseable. Placeholders
        are only used in object-name positions in the shipped
        templates — substituting the quoted form would be incorrect
        inside a string literal, but :func:`_validate_template_split_safe`
        rules out the dangerous shape ahead of time.
        """
        formatted = migration.sql_template.format(catalog=self._catalog_q, schema=self._schema_q)
        for stmt in formatted.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    self._sql.execute(stmt)
                except Exception as exc:
                    if any(frag in str(exc) for frag in self._IDEMPOTENT_ERROR_FRAGMENTS):
                        logger.warning(
                            "Migration v%d DDL already applied (safe to skip): %s",
                            migration.version,
                            exc,
                        )
                    else:
                        raise

        escaped_desc = escape_sql_string(migration.description)
        record_sql = (
            f"INSERT INTO {self._meta_table} (version, description, applied_at) "
            f"VALUES ({migration.version}, '{escaped_desc}', current_timestamp())"
        )
        self._sql.execute(record_sql)
        logger.info(
            "Migration v%d applied successfully: %s",
            migration.version,
            migration.description,
        )
