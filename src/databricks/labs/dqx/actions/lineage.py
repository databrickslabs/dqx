"""Lineage collection action for the DQX actions & alerting subsystem.

*CollectLineageAction* persists upstream and downstream table lineage and per-failed-column
lineage for the source table of a DQX run. Each lineage edge is emitted as a row in a single
Delta table (schema declared in *LINEAGE_TABLE_SCHEMA*) via the same *io.save_dataframe_as_table*
helper DQX uses for failed-check output — so the write mode / format / options semantics are
handled identically.

The action never fails the pipeline: collection errors are logged (via the *_sanitize*
helper, per CWE-117) and swallowed so a stale system-table read cannot mask check results.

The persisted table's *location* is emitted verbatim as the *extras* payload
(``{"lineage_location": "<full name>"}``); downstream actions read the table when they
need the full structure.

Per-failed-column lineage is derived from the run's *output_location*: DQX writes each row's
check failures into two array-of-struct columns (*_errors* and *_warnings*), and each struct's
*columns* field lists the column(s) the check flagged. The lineage action reads that table,
explodes those two arrays plus their inner *columns* field, and takes the distinct set to seed
the column-lineage walks. See *docs/dqx/docs/reference/table_schemas.mdx* for the full shape.

Table-graph walks (upstream / downstream) use a single recursive CTE against
*system.access.table_lineage* with an in-CTE full-path cycle guard, and require DBR 17+
for the recursive-CTE SQL feature.
"""

import logging
import uuid
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from databricks.labs.dqx.actions.base import Action, ActionContext, ActionResult, ActionServices, ActionStatus
from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log as _sanitize
from databricks.labs.dqx.actions.registry import register_action
from databricks.labs.dqx.config import TABLE_PATTERN, OutputConfig
from databricks.labs.dqx.errors import InvalidActionError
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.utils import quote_column_name

logger = logging.getLogger(__name__)

LINEAGE_TABLE_LINEAGE = "system.access.table_lineage"
LINEAGE_COLUMN_LINEAGE = "system.access.column_lineage"

# Default names of the DQX-appended result columns on the output / quarantine tables. These are
# the column names DQX writes when no engine-level rename is configured; both *_errors* and
# *_warnings* are ARRAY<STRUCT<..., columns ARRAY<STRING>, ...>>. Callers who renamed these at
# the engine level should override *errors_column* / *warnings_column* on *LineageActionConfig*.
_DEFAULT_ERRORS_COLUMN = "_errors"
_DEFAULT_WARNINGS_COLUMN = "_warnings"

# Schema of the failed-columns DataFrame threaded between *extract_failed_columns* and
# *_collect_column_lineage_df*. A single STRING column keeps the join predicate trivial.
_FAILED_COLUMN_NAME = "col_name"
_FAILED_COLUMNS_SCHEMA = StructType([StructField(_FAILED_COLUMN_NAME, StringType(), nullable=True)])

# Persisted lineage-table schema. Declared as an explicit *StructType* (not schema-on-write
# inference) so downstream consumers see the same contract on empty runs. Each field carries a
# ``comment`` metadata entry — Delta / UC persists these as column comments on write, so consumers
# see the same documentation via ``DESCRIBE TABLE`` that the code declares here.
LINEAGE_TABLE_SCHEMA = StructType(
    [
        StructField(
            "run_id",
            StringType(),
            nullable=False,
            metadata={"comment": "DQX run identifier that produced this lineage row."},
        ),
        StructField(
            "run_time",
            TimestampType(),
            nullable=False,
            metadata={"comment": "Timestamp at which the DQX run producing this lineage row started."},
        ),
        StructField(
            "source_table",
            StringType(),
            nullable=False,
            metadata={"comment": "3-level UC name of the anchor table for which lineage was collected."},
        ),
        StructField(
            "edge_type",
            StringType(),
            nullable=False,
            metadata={
                "comment": (
                    "Kind of lineage edge: 'upstream' / 'downstream' for table-graph walks, "
                    "'column_upstream' / 'column_downstream' for per-failed-column lineage."
                )
            },
        ),
        StructField(
            "depth",
            LongType(),
            nullable=True,
            metadata={"comment": "Distance from *source_table* in hops (1 = direct neighbour)."},
        ),
        StructField(
            "target_table",
            StringType(),
            nullable=True,
            metadata={"comment": "3-level UC name of the neighbour table reached by this edge."},
        ),
        StructField(
            "target_delta_version",
            LongType(),
            nullable=True,
            metadata={
                "comment": (
                    "Latest Delta version of *target_table* at action run time, resolved via "
                    "DESCRIBE HISTORY. NULL if the table is not a Delta table or lookup failed."
                )
            },
        ),
        StructField(
            "source_column",
            StringType(),
            nullable=True,
            metadata={"comment": "Source column of a column-lineage edge; NULL for table-lineage rows."},
        ),
        StructField(
            "target_column",
            StringType(),
            nullable=True,
            metadata={"comment": "Target column of a column-lineage edge; NULL for table-lineage rows."},
        ),
    ]
)

LINEAGE_TABLE_COMMENT = (
    "DQX lineage records emitted by CollectLineageAction: one row per upstream/downstream table edge "
    "or per-failed-column edge derived from system.access lineage tables at action run time."
)

_EDGE_UPSTREAM = "upstream"
_EDGE_DOWNSTREAM = "downstream"
_EDGE_COLUMN_UPSTREAM = "column_upstream"
_EDGE_COLUMN_DOWNSTREAM = "column_downstream"


class LineageSearchConfig(BaseModel):
    """Search parameters for one lineage traversal direction (upstream, downstream, or columns).

    Attributes:
        depth: Maximum walk depth from the source table in hops. Must be ``>= 1``. Bounds the
            recursion depth for both the table-lineage and column-lineage walks.
        lookback_days: How far back to consider lineage entries in *system.access.table_lineage*
            / *system.access.column_lineage*. Must be ``>= 1``.
        max_nodes: Guardrail row cap applied both **inside** each member of the recursive
            lineage CTE (bounding the frontier per hop, so intermediate expansion is capped
            per iteration) and once at the tail (bounding total output). Must be ``>= 1``
            (default 100). Together with *depth* this bounds the traversal's materialised
            row count to ``O(depth * max_nodes)``. This is a safety limit — it does **not**
            guarantee which rows survive when the graph produces more edges than the cap
            (no ordering contract).
    """

    model_config = ConfigDict(extra="forbid")

    depth: int = 1
    lookback_days: int = 30
    max_nodes: int = 100

    @field_validator("depth")
    @classmethod
    def _validate_depth(cls, value: int) -> int:
        if value < 1:
            raise InvalidActionError(f"LineageSearchConfig.depth must be >= 1, got {value}.")
        return value

    @field_validator("lookback_days")
    @classmethod
    def _validate_lookback(cls, value: int) -> int:
        if value < 1:
            raise InvalidActionError(f"LineageSearchConfig.lookback_days must be >= 1, got {value}.")
        return value

    @field_validator("max_nodes")
    @classmethod
    def _validate_max_nodes(cls, value: int) -> int:
        if value < 1:
            raise InvalidActionError(f"LineageSearchConfig.max_nodes must be >= 1, got {value}.")
        return value


FailuresSource = Literal["output", "quarantine", "both"]


class LineageActionConfig(BaseModel):
    """Top-level configuration for *CollectLineageAction*.

    Composes four independent sub-configs — *upstream*, *downstream*, *column_upstream*,
    *column_downstream* — plus a *failures_source* selector that picks which sink to read
    per-failed-column lineage seeds from.

    Each sub-config defaults to a fresh *LineageSearchConfig* (direction on by default); set
    any field to *None* to disable that direction. Table and column directions are symmetric:
    the four fields let a caller collect, e.g., only table-upstream + column-downstream if
    that is all they need.

    *failures_source* controls where *extract_failed_columns* reads issue structs from at
    execute time:

      * ``"output"`` — only *context.output_location* (skips column lineage when the location
        is missing or has no failures).
      * ``"quarantine"`` — only *context.quarantine_location*.
      * ``"both"`` (default) — read from whichever of the two locations are present and union
        their distinct failed columns. DQX writes failures to *quarantine_location* in
        split-run mode and to *output_location* in non-split mode, so *"both"* makes the
        action work in either configuration without extra wiring.

    *errors_column* and *warnings_column* name the two ARRAY<STRUCT> columns DQX appends to
    the output / quarantine tables. Defaults match DQX's built-in names (``_errors`` /
    ``_warnings``); override them when the engine's error / warning column renames have been
    applied so this action can still locate the issue structs.
    """

    model_config = ConfigDict(extra="forbid")

    upstream: LineageSearchConfig | None = Field(default_factory=LineageSearchConfig)
    downstream: LineageSearchConfig | None = Field(default_factory=LineageSearchConfig)
    column_upstream: LineageSearchConfig | None = Field(default_factory=LineageSearchConfig)
    column_downstream: LineageSearchConfig | None = Field(default_factory=LineageSearchConfig)
    failures_source: FailuresSource = "both"
    errors_column: str = _DEFAULT_ERRORS_COLUMN
    warnings_column: str = _DEFAULT_WARNINGS_COLUMN


@register_action
class CollectLineageAction(Action):
    """Collect table / column lineage into a Delta table.

    On every run, gathers upstream/downstream tables (recursive-CTE walk bounded by *depth*)
    and recursive per-failed-column lineage (upstream and downstream, seeded from failure
    records in *output_location* and/or *quarantine_location* per *config.failures_source*),
    and writes one row per edge to the Delta table specified by *output_config*. The action
    never fails the pipeline — collection errors are logged and turned into *CONFIG_ERROR*
    results. To disable a specific direction, set the corresponding sub-config on
    *LineageActionConfig* (*upstream*, *downstream*, *column_upstream*, *column_downstream*)
    to *None*.

    Attributes:
        type: Discriminator literal, always *"collect_lineage"*.
        name: Logical identifier for this action; derived to *"collect_lineage"* when left empty.
        output_config: Required output sink for the lineage table. *mode* should be set equal to
            *RunConfig.output_config.mode* so lineage writes stay consistent with how DQX writes the
            failed-checks output.
        config: Optional collection knobs — see *LineageActionConfig*.
    """

    type: Literal["collect_lineage"] = "collect_lineage"
    name: str = "collect_lineage"
    output_config: OutputConfig
    # Bare BaseModel default (not Field(default_factory=...)) so pylint's type inference on
    # `self.config` produces LineageActionConfig instead of FieldInfo. Pydantic v2 deep-copies
    # mutable defaults per instance, so this is safe against cross-instance mutation.
    config: LineageActionConfig = LineageActionConfig()

    @field_validator("output_config", mode="before")
    @classmethod
    def _coerce_output_config(cls, value: Any) -> Any:
        """Accept a dict (metadata form) or an *OutputConfig* instance.

        *OutputConfig* is a plain *@dataclass* and Pydantic cannot construct it from a dict without
        this hook; the metadata / YAML form must round-trip so this action is deserializable like
        any other registered action.
        """
        if isinstance(value, dict):
            try:
                return OutputConfig(**value)
            except TypeError as exc:
                raise InvalidActionError(f"Invalid OutputConfig for CollectLineageAction: {exc}") from exc
        return value

    @field_serializer("output_config")
    def _serialize_output_config(self, value: OutputConfig) -> dict[str, Any]:
        """Serialize *OutputConfig* to a plain dict for JSON / YAML persistence."""
        return {
            "location": value.location,
            "format": value.format,
            "mode": value.mode,
            "options": dict(value.options),
            "trigger": dict(value.trigger),
            "partition_by": list(value.partition_by),
            "cluster_by": list(value.cluster_by),
        }

    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        """Collect lineage rows and persist them; emit the target-table full name as extras."""
        safe_name = _sanitize(self.name)
        source_table = context.input_location
        if source_table is None:
            logger.warning(
                f"CollectLineageAction '{safe_name}' has no input_location on the context; skipping collection."
            )
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.HEALTHY, extras=None)

        try:
            spark = services.spark
            if spark is None:
                raise InvalidActionError("CollectLineageAction requires SparkSession in ActionServices.spark.")
            return self._execute(context, safe_name, source_table, spark)
        except Exception as exc:  # broad catch: lineage failures must not break the run
            logger.warning(f"CollectLineageAction '{safe_name}' failed and was skipped: {_sanitize(str(exc))}")
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.CONFIG_ERROR, extras=None)

    def _execute(self, context: ActionContext, safe_name: str, source_table: str, spark: SparkSession) -> ActionResult:
        failed_columns_df = _resolve_failed_columns(
            spark=spark,
            output_location=context.output_location,
            quarantine_location=context.quarantine_location,
            failures_source=self.config.failures_source,
            errors_column=self.config.errors_column,
            warnings_column=self.config.warnings_column,
            safe_name=safe_name,
            run_id=context.run_id,
        )
        # The failed-columns set is exposed to the recursive column-lineage CTE as a session
        # temp view (see *_column_direction_df*). The view must outlive *save_dataframe_as_table*
        # because the returned DataFrame is lazy — the write is the point at which the CTE
        # actually executes. Scoping the create/drop pair around the write keeps the DataFrame
        # lazy end-to-end without leaking the view.
        failed_columns_view = _register_failed_columns_view(failed_columns_df)
        try:
            df = _collect_lineage_rows(
                spark=spark,
                source_table=source_table,
                run_id=context.run_id,
                run_time_iso=context.run_time.isoformat(),
                failed_columns_view=failed_columns_view,
                config=self.config,
            )
            save_dataframe_as_table(df, self.output_config)
            _document_table(spark=spark, location=self.output_config.location, safe_name=safe_name)
            return ActionResult(
                action_name=self.name,
                fired=True,
                status=ActionStatus.HEALTHY,
                extras={"lineage_location": self.output_config.location},
            )
        finally:
            if failed_columns_view is not None:
                spark.catalog.dropTempView(failed_columns_view)


def extract_failed_columns(
    spark: SparkSession,
    output_location: str,
    run_id: str,
    errors_column: str = _DEFAULT_ERRORS_COLUMN,
    warnings_column: str = _DEFAULT_WARNINGS_COLUMN,
) -> DataFrame:
    """Read *output_location* and return a DataFrame of distinct column names that failed a check
    **for the current DQX run**.

    DQX appends two ARRAY<STRUCT> columns to the output table (default names ``_errors`` /
    ``_warnings`` — override via *errors_column* / *warnings_column* when the DQX engine has
    renamed them) and every struct exposes a *run_id* STRING plus a *columns* ARRAY<STRING>
    listing the column(s) the check flagged. In append-mode writes the same table accumulates
    rows from many runs, so this helper explodes both arrays, keeps only issues whose
    ``issue.run_id`` matches the supplied *run_id*, then explodes the inner *columns* field,
    unions the two, filters nulls, and returns the distinct set as a one-column DataFrame
    (schema *_FAILED_COLUMNS_SCHEMA* with a single ``col_name STRING`` column). Without the
    run-id filter a prior run's failure could seed column-lineage rows stamped with the
    current *run_id*.

    The DataFrame shape is deliberate: the downstream column-lineage CTE joins it via a
    session temp view rather than iterating one query per name, so the full pipeline stays
    lazy and Spark can broadcast the (small) failed-columns set.

    A missing table, an issue-column name that is not present on the output, or a read
    failure returns an empty DataFrame with the same schema and logs a sanitised warning —
    the join then produces zero rows and column lineage becomes a natural no-op.

    Args:
        spark: Active *SparkSession* used to read *output_location*.
        output_location: 3-level UC name of the DQX output (or quarantine) table.
        run_id: Current DQX run identifier — only issue structs whose ``issue.run_id`` equals
            this value contribute to the returned column set.
        errors_column: Name of the ARRAY<STRUCT> column that carries error-level issues
            (default ``_errors``).
        warnings_column: Name of the ARRAY<STRUCT> column that carries warning-level issues
            (default ``_warnings``).

    Returns:
        DataFrame with a single ``col_name STRING`` column, distinct-only, non-null-only.
    """
    empty = spark.createDataFrame([], _FAILED_COLUMNS_SCHEMA)
    try:
        df = spark.table(output_location)
    except Exception as exc:  # broad catch: read errors are non-fatal for lineage
        logger.warning(f"Failed-column extraction: cannot read '{_sanitize(output_location)}': {_sanitize(str(exc))}")
        return empty

    present_columns = set(df.columns)
    issue_frames: list[DataFrame] = []
    for issues_col in (errors_column, warnings_column):
        if issues_col not in present_columns:
            continue
        exploded = df.select(F.explode(F.col(issues_col)).alias("issue")).where(F.col("issue.run_id") == F.lit(run_id))
        issue_frames.append(exploded.select(F.explode(F.col("issue.columns")).alias(_FAILED_COLUMN_NAME)))

    if not issue_frames:
        return empty

    unioned = issue_frames[0]
    for frame in issue_frames[1:]:
        unioned = unioned.unionByName(frame)

    return unioned.where(F.col(_FAILED_COLUMN_NAME).isNotNull()).select(_FAILED_COLUMN_NAME).distinct()


def _resolve_failed_columns(
    *,
    spark: SparkSession,
    output_location: str | None,
    quarantine_location: str | None,
    failures_source: FailuresSource,
    errors_column: str,
    warnings_column: str,
    safe_name: str,
    run_id: str,
) -> DataFrame:
    """Read failed-column names from the sinks selected by *failures_source* and union them.

    *failures_source* controls which sinks are consulted:

      * ``"output"``   — only *output_location*.
      * ``"quarantine"`` — only *quarantine_location*.
      * ``"both"``     — read from whichever of the two is non-*None* and union the distinct
        column names. DQX writes failures to *quarantine_location* in split-run mode and to
        *output_location* in non-split mode; ``"both"`` therefore ensures column-lineage seeds
        are populated regardless of routing.

    *errors_column* / *warnings_column* are the names of the ARRAY<STRUCT> issue columns on
    the sinks — forwarded to *extract_failed_columns* so custom engine renames are honoured.

    When no configured sink is available a sanitised warning is logged and an empty
    *_FAILED_COLUMNS_SCHEMA* DataFrame is returned so column lineage becomes a natural no-op.
    *run_id* is forwarded to *extract_failed_columns* so only issues emitted by the current
    run seed column-lineage — see that function's docstring for why the filter is required in
    append mode.
    """
    locations: list[str] = []
    if failures_source in {"output", "both"} and output_location is not None:
        locations.append(output_location)
    if failures_source in {"quarantine", "both"} and quarantine_location is not None:
        locations.append(quarantine_location)
    if not locations:
        logger.warning(
            f"CollectLineageAction '{safe_name}' has no readable failures source "
            f"(failures_source='{failures_source}', output_location={output_location!r}, "
            f"quarantine_location={quarantine_location!r}); column lineage will be skipped."
        )
        return spark.createDataFrame([], _FAILED_COLUMNS_SCHEMA)
    result = extract_failed_columns(spark, locations[0], run_id, errors_column, warnings_column)
    for location in locations[1:]:
        result = result.unionByName(extract_failed_columns(spark, location, run_id, errors_column, warnings_column))
    return result.distinct()


def _register_failed_columns_view(failed_columns_df: DataFrame) -> str | None:
    """Register *failed_columns_df* as a session temp view and return its name; *None* if empty.

    The view carries a single ``col_name STRING`` column and is referenced by the recursive
    column-lineage CTE via ``IN (SELECT col_name FROM <view>)``. The name is UUID-suffixed to
    avoid collisions when multiple actions share a Spark session; lifecycle (drop) is owned
    by *_execute*.
    """
    non_null = failed_columns_df.where(F.col(_FAILED_COLUMN_NAME).isNotNull())
    if non_null.limit(1).count() == 0:
        return None
    name = f"_dqx_failed_columns_{uuid.uuid4().hex[:12]}"
    non_null.createOrReplaceTempView(name)
    return name


def _empty_lineage_df(spark: SparkSession) -> DataFrame:
    """Empty DataFrame with *LINEAGE_TABLE_SCHEMA* — identity for *unionByName* composition."""
    return spark.createDataFrame([], LINEAGE_TABLE_SCHEMA)


def _sql_str_literal(value: str) -> str:
    """Escape a Python string for safe inclusion as a single-quoted SQL string literal.

    Doubles any embedded single quotes (the SQL escape for a literal quote inside a string
    literal). Used by the lineage queries where parameter markers cannot be applied — e.g.
    inside ``INTERVAL n DAYS`` — and the table name has to be inlined as a literal.
    """
    return "'" + value.replace("'", "''") + "'"


def _fully_qualified_identifier(name: str) -> str:
    """Backtick-quote each dot-separated segment of a UC name for use as a SQL identifier.

    Used where the anchor table has to appear as an identifier (e.g. ``DESCRIBE HISTORY <ident>``)
    rather than a string literal — each segment is escaped via *quote_column_name* so embedded
    backticks or reserved words are handled the same way as elsewhere in the codebase.
    """
    return ".".join(quote_column_name(segment) for segment in name.split("."))


_VERSION_LOOKUP_SCHEMA = StructType(
    [
        StructField("target_table", StringType(), nullable=False),
        StructField("resolved_delta_version", LongType(), nullable=True),
    ]
)


def _document_table(*, spark: SparkSession, location: str, safe_name: str) -> None:
    """Attach *LINEAGE_TABLE_COMMENT* and each column's declared comment to the persisted lineage
    table via ``COMMENT ON TABLE`` / ``COMMENT ON COLUMN``
    (https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment).

    Column comments are declared in *LINEAGE_TABLE_SCHEMA* as ``StructField`` metadata and are
    re-asserted explicitly here so the persisted table always exposes them via ``DESCRIBE TABLE``,
    independent of whether the underlying writer propagated the schema metadata.

    Only applies when *location* is a UC table name (matches *TABLE_PATTERN*) — path-based sinks
    (``/Volumes/…``, ``s3://…`` etc.) don't accept these DDL statements. Table- and per-column
    failures are logged and swallowed independently so the action never breaks on a missing
    catalog privilege or on a single-column error.
    """
    if not TABLE_PATTERN.match(location):
        return
    table_ident = _fully_qualified_identifier(location)
    try:
        spark.sql(f"COMMENT ON TABLE {table_ident} IS {_sql_str_literal(LINEAGE_TABLE_COMMENT)}")
    except Exception as exc:  # broad catch: table-comment application is best-effort
        logger.warning(
            f"CollectLineageAction '{safe_name}' failed to set table comment on "
            f"'{_sanitize(location)}': {_sanitize(str(exc))}"
        )
    for field in LINEAGE_TABLE_SCHEMA.fields:
        comment = field.metadata.get("comment")
        if not comment:
            continue
        column_ident = f"{table_ident}.{quote_column_name(field.name)}"
        try:
            spark.sql(f"COMMENT ON COLUMN {column_ident} IS {_sql_str_literal(comment)}")
        except Exception as exc:  # broad catch: per-column comment application is best-effort
            logger.warning(
                f"CollectLineageAction '{safe_name}' failed to set column comment on "
                f"'{_sanitize(location)}.{_sanitize(field.name)}': {_sanitize(str(exc))}"
            )


def _collect_lineage_rows(
    *,
    spark: SparkSession,
    source_table: str,
    run_id: str,
    run_time_iso: str,
    failed_columns_view: str | None,
    config: LineageActionConfig,
) -> DataFrame:
    """Collect all lineage rows for *source_table* into a single DataFrame with *LINEAGE_TABLE_SCHEMA*.

    Each enabled branch (*upstream*, *downstream*, *column_upstream*, *column_downstream*)
    returns its own *LINEAGE_TABLE_SCHEMA*-shaped DataFrame; the composition is a plain
    *unionByName* over an empty-schema seed, so downstream writes are well-formed Delta writes
    even when no branch contributed rows.

    *failed_columns_view* is the name of a session temp view (registered by
    *_register_failed_columns_view*) with a single ``col_name STRING`` column, or *None* when
    the failed-columns set is empty / column lineage is skipped. It is referenced by the
    recursive column-lineage CTE via ``IN (SELECT col_name FROM <view>)``. When *None*, both
    column-lineage branches are skipped regardless of their sub-configs.
    """
    common: dict[str, Any] = {
        "run_id": run_id,
        # run_time is an ISO-8601 string that Spark parses to TIMESTAMP on write.
        "run_time": run_time_iso,
        "source_table": source_table,
    }
    result = _empty_lineage_df(spark)
    if config.upstream is not None:
        result = result.unionByName(
            _collect_upstream_df(spark=spark, source_table=source_table, search=config.upstream, common=common)
        )
    if config.downstream is not None:
        result = result.unionByName(
            _collect_downstream_df(spark=spark, source_table=source_table, search=config.downstream, common=common)
        )
    if failed_columns_view is not None:
        if config.column_upstream is not None:
            result = result.unionByName(
                _column_direction_df(
                    spark=spark,
                    source_table=source_table,
                    failed_columns_view=failed_columns_view,
                    edge_type=_EDGE_COLUMN_UPSTREAM,
                    search=config.column_upstream,
                    common=common,
                )
            )
        if config.column_downstream is not None:
            result = result.unionByName(
                _column_direction_df(
                    spark=spark,
                    source_table=source_table,
                    failed_columns_view=failed_columns_view,
                    edge_type=_EDGE_COLUMN_DOWNSTREAM,
                    search=config.column_downstream,
                    common=common,
                )
            )
    return _resolve_target_delta_versions(spark, result)


def _resolve_target_delta_versions(spark: SparkSession, lineage_df: DataFrame) -> DataFrame:
    """Populate *target_delta_version* with the latest Delta version of each *target_table*.

    Collects the distinct non-null *target_table* values from *lineage_df*, runs
    ``DESCRIBE HISTORY <ident> LIMIT 1`` per name, and left-joins the resulting version onto the
    input. Failures (missing table, non-Delta table, access denied, malformed name) leave the
    field NULL and log a sanitised warning — the pipeline never fails on a single-table lookup.

    The output preserves *LINEAGE_TABLE_SCHEMA* column order so downstream *save_dataframe_as_table*
    writes remain schema-stable.
    """
    distinct_targets = lineage_df.select("target_table").where(F.col("target_table").isNotNull()).distinct().collect()
    if not distinct_targets:
        return lineage_df

    resolved: list[tuple[str, int | None]] = []
    for row in distinct_targets:
        table = row["target_table"]
        try:
            history_row = (
                spark.sql(f"DESCRIBE HISTORY {_fully_qualified_identifier(table)} LIMIT 1").select("version").first()
            )
        except Exception as exc:  # broad catch: per-table lookup errors are non-fatal for lineage
            logger.warning(f"Delta-version lookup failed for '{_sanitize(table)}': {_sanitize(str(exc))}")
            resolved.append((table, None))
            continue
        version = int(history_row["version"]) if history_row is not None else None
        resolved.append((table, version))

    lookup = spark.createDataFrame(resolved, _VERSION_LOOKUP_SCHEMA)
    joined = lineage_df.drop("target_delta_version").join(lookup, on="target_table", how="left")
    return joined.withColumnRenamed("resolved_delta_version", "target_delta_version").select(
        *[F.col(field.name) for field in LINEAGE_TABLE_SCHEMA.fields]
    )


def _collect_upstream_df(
    *,
    spark: SparkSession,
    source_table: str,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """Thin wrapper — upstream walk from *source_table* as a *LINEAGE_TABLE_SCHEMA* DataFrame.

    Args:
        spark: Active *SparkSession* used to issue the recursive-CTE query.
        source_table: 3-level UC name of the anchor table.
        search: Bound for depth, lookback window, and node cap.
        common: Constants (run_id, run_time_iso, source_table) added to each emitted row.
    """
    return _walk_lineage(
        spark=spark, table_full_name=source_table, direction=_EDGE_UPSTREAM, search=search, common=common
    )


def _collect_downstream_df(
    *,
    spark: SparkSession,
    source_table: str,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """Thin wrapper — downstream walk from *source_table* as a *LINEAGE_TABLE_SCHEMA* DataFrame.

    Args:
        spark: Active *SparkSession* used to issue the recursive-CTE query.
        source_table: 3-level UC name of the anchor table.
        search: Bound for depth, lookback window, and node cap.
        common: Constants (run_id, run_time_iso, source_table) added to each emitted row.
    """
    return _walk_lineage(
        spark=spark, table_full_name=source_table, direction=_EDGE_DOWNSTREAM, search=search, common=common
    )


def _walk_lineage(
    *,
    spark: SparkSession,
    table_full_name: str,
    direction: str,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """Walk *table_full_name*'s upstream or downstream neighbours with a recursive CTE.

    Uses a single ``WITH RECURSIVE`` query against *LINEAGE_TABLE_LINEAGE*. Cycle detection is
    performed inside the CTE by carrying the visited path as an array column and rejecting any
    neighbour already in the path — so multi-hop cycles (a → b → a → c) are blocked, not only
    the nearest-anchor case. Requires DBR 17+ for the recursive-CTE feature; on parse / read
    failure the caller-visible result is an empty *LINEAGE_TABLE_SCHEMA* DataFrame and a
    sanitised warning is logged.

    *table_full_name* is inlined as a single-quoted SQL string literal via *_sql_str_literal*
    (escapes embedded quotes). *search.depth*, *search.lookback_days*, and *search.max_nodes*
    are validated Pydantic ints (>= 1) and interpolated as bare constants. Spark SQL does not
    accept parameter markers inside ``INTERVAL n DAYS`` literals or after ``LIMIT``, so the
    whole query is built via Python string interpolation rather than ``spark.sql`` args.

    *search.max_nodes* is applied as a ``LIMIT`` inside **both** the anchor and the recursive
    members of the CTE (bounding the frontier per hop, so intermediate expansion cannot exceed
    ``max_nodes`` rows per iteration) and once more at the tail (bounding the total output).
    Combined with the depth cap this yields an ``O(max_depth * max_nodes)`` upper bound on the
    row count Spark has to materialise. This is a *guardrail* — it does **not** guarantee which
    rows survive when the graph produces more edges than the cap (no ordering contract).

    Returns:
        DataFrame conforming to *LINEAGE_TABLE_SCHEMA* — one row per discovered edge, with the
        constant columns from *common* prefixed and unrelated columns set to typed NULLs.
    """
    key_source = "target_table_full_name" if direction == _EDGE_UPSTREAM else "source_table_full_name"
    key_other = "source_table_full_name" if direction == _EDGE_UPSTREAM else "target_table_full_name"
    max_depth = int(search.depth)
    lookback_days = int(search.lookback_days)
    max_nodes = int(search.max_nodes)
    table_literal = _sql_str_literal(table_full_name)
    # nosec B608: identifiers + validated ints only; the anchor table name is escaped and wrapped
    # by *_sql_str_literal* before interpolation. *max_depth*, *lookback_days*, and *max_nodes*
    # are validated Pydantic ints (>= 1) interpolated as bare constants.
    query = (
        "WITH RECURSIVE edges(anchor, neighbour, depth, path, event_time) AS ("
        " ("
        f"  SELECT {table_literal} AS anchor, {key_other} AS neighbour, 1 AS depth, "
        f"         array({table_literal}, {key_other}) AS path, event_time "
        f"  FROM {LINEAGE_TABLE_LINEAGE} "
        f"  WHERE {key_source} = {table_literal} "
        f"    AND {key_other} IS NOT NULL "
        f"    AND {key_other} <> {table_literal} "
        f"    AND event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f"  LIMIT {max_nodes} "
        " )"
        "  UNION ALL "
        f" ("
        f"  SELECT e.neighbour AS anchor, t.{key_other} AS neighbour, e.depth + 1, "
        f"         array_append(e.path, t.{key_other}), t.event_time "
        f"  FROM edges e JOIN {LINEAGE_TABLE_LINEAGE} t "
        f"    ON t.{key_source} = e.neighbour "
        f"  WHERE e.depth < {max_depth} "
        f"    AND t.{key_other} IS NOT NULL "
        f"    AND NOT array_contains(e.path, t.{key_other}) "
        f"    AND t.event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f"  LIMIT {max_nodes} "
        " )"
        ") "
        "SELECT DISTINCT anchor, neighbour, depth "
        f"FROM edges LIMIT {max_nodes}"
    )
    try:
        walk_df = spark.sql(query)
    except Exception as exc:  # broad catch: system-table read errors are non-fatal for lineage
        logger.warning(f"Lineage read failed for '{_sanitize(table_full_name)}' ({direction}): {_sanitize(str(exc))}")
        return _empty_lineage_df(spark)
    return _project_edge_rows(
        walk_df,
        edge_type=direction,
        target_table_col=F.col("neighbour"),
        depth_col=F.col("depth").cast("long"),
        source_column_col=F.lit(None).cast("string"),
        target_column_col=F.lit(None).cast("string"),
        common=common,
    )


def _column_direction_df(
    *,
    spark: SparkSession,
    source_table: str,
    failed_columns_view: str,
    edge_type: str,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """Recursive column-lineage walk for one direction, seeded from the failed-columns temp view.

    Mirrors *_walk_lineage*: a single ``WITH RECURSIVE`` query against
    *LINEAGE_COLUMN_LINEAGE* with in-CTE full-path cycle detection carried as a ``path
    ARRAY<STRING>`` of ``<table>.<column>`` identifiers. *search.depth*, *search.lookback_days*,
    and *search.max_nodes* are validated Pydantic ints (>= 1) and interpolated as bare
    constants. *failed_columns_view* is the name of a session-scoped temp view (registered by
    *_collect_column_lineage_df*) with a single ``col_name STRING`` column; the CTE references
    it via ``IN (SELECT col_name FROM <view>)`` rather than inlining a driver-collected literal.

    *search.max_nodes* is applied as a ``LIMIT`` inside **both** the anchor and the recursive
    members of the CTE (bounding the frontier per hop) and once more at the tail (bounding
    total output) — same ``O(max_depth * max_nodes)`` bound and same "no ordering contract"
    semantics as *_walk_lineage*.

    The recursive member also drops any hop that would return to an originally-failed
    ``(anchor_table, failed_col)`` pair — ``NOT (t.frontier_table = <anchor> AND
    t.frontier_col IN (SELECT col_name FROM <view>))``. The check is scoped to the anchor
    table so unrelated tables that happen to have a column with the same name still surface;
    only cycles back to the seed pair itself are rejected, since those are already covered by
    the depth-1 seed and re-visiting them would inflate the walk with redundant paths.
    """
    if edge_type == _EDGE_COLUMN_UPSTREAM:
        anchor_key, anchor_col_key = "target_table_full_name", "target_column"
        frontier_key, frontier_col_key = "source_table_full_name", "source_column"
    else:
        anchor_key, anchor_col_key = "source_table_full_name", "source_column"
        frontier_key, frontier_col_key = "target_table_full_name", "target_column"

    max_depth = int(search.depth)
    lookback_days = int(search.lookback_days)
    max_nodes = int(search.max_nodes)
    table_literal = _sql_str_literal(source_table)
    failed_in_subquery = f"SELECT {_FAILED_COLUMN_NAME} FROM {failed_columns_view}"
    # nosec B608: identifiers + validated ints only; the anchor table name is escaped and
    # wrapped by *_sql_str_literal* before interpolation, the failed-columns view name is a
    # UUID-suffixed identifier generated internally by *_collect_column_lineage_df* (never
    # user input), and *max_depth*, *lookback_days*, and *max_nodes* are validated Pydantic
    # ints (>= 1).
    query = (
        "WITH RECURSIVE edges("
        "frontier_table, frontier_column, neighbour, source_column, target_column, "
        "depth, path, event_time) AS ("
        " ("
        f"  SELECT {frontier_key} AS frontier_table, {frontier_col_key} AS frontier_column, "
        f"         {frontier_key} AS neighbour, source_column, target_column, "
        f"         1 AS depth, "
        f"         array(concat_ws('.', {frontier_key}, {frontier_col_key})) AS path, "
        f"         event_time "
        f"  FROM {LINEAGE_COLUMN_LINEAGE} "
        f"  WHERE {anchor_key} = {table_literal} "
        f"    AND {anchor_col_key} IN ({failed_in_subquery}) "
        f"    AND {frontier_key} IS NOT NULL "
        f"    AND {frontier_col_key} IS NOT NULL "
        f"    AND event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f"  LIMIT {max_nodes} "
        " )"
        "  UNION ALL "
        " ("
        f"  SELECT t.{frontier_key} AS frontier_table, t.{frontier_col_key} AS frontier_column, "
        f"         t.{frontier_key} AS neighbour, t.source_column, t.target_column, "
        f"         e.depth + 1 AS depth, "
        f"         array_append(e.path, concat_ws('.', t.{frontier_key}, t.{frontier_col_key})) AS path, "
        f"         t.event_time "
        f"  FROM edges e JOIN {LINEAGE_COLUMN_LINEAGE} t "
        f"    ON t.{anchor_key} = e.frontier_table "
        f"   AND t.{anchor_col_key} = e.frontier_column "
        f"  WHERE e.depth < {max_depth} "
        f"    AND t.{frontier_key} IS NOT NULL "
        f"    AND t.{frontier_col_key} IS NOT NULL "
        f"    AND NOT array_contains(e.path, concat_ws('.', t.{frontier_key}, t.{frontier_col_key})) "
        f"    AND NOT (t.{frontier_key} = {table_literal} "
        f"             AND t.{frontier_col_key} IN ({failed_in_subquery})) "
        f"    AND t.event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f"  LIMIT {max_nodes} "
        " )"
        ") "
        "SELECT DISTINCT neighbour, source_column, target_column, depth "
        "FROM edges "
        f"LIMIT {max_nodes}"
    )
    try:
        walk_df = spark.sql(query)
    except Exception as exc:  # broad catch: system-table read errors are non-fatal for lineage
        logger.warning(
            f"Column lineage read failed for '{_sanitize(source_table)}' ({edge_type}): {_sanitize(str(exc))}"
        )
        return _empty_lineage_df(spark)
    return _project_edge_rows(
        walk_df,
        edge_type=edge_type,
        target_table_col=F.col("neighbour"),
        depth_col=F.col("depth").cast("long"),
        source_column_col=F.col("source_column"),
        target_column_col=F.col("target_column"),
        common=common,
    )


def _project_edge_rows(
    df: DataFrame,
    *,
    edge_type: str,
    target_table_col: Any,
    depth_col: Any,
    source_column_col: Any,
    target_column_col: Any,
    common: dict[str, Any],
) -> DataFrame:
    """Project a walk DataFrame to *LINEAGE_TABLE_SCHEMA* with typed NULLs for absent columns.

    Args:
        df: Source DataFrame with the raw walk / column-lineage columns.
        edge_type: Constant edge-type discriminator written to every emitted row.
        target_table_col: Column expression producing the *target_table* value.
        depth_col: Column expression producing the *depth* value (long).
        source_column_col: Column expression producing *source_column* (string or NULL).
        target_column_col: Column expression producing *target_column* (string or NULL).
        common: Constants (*run_id*, *run_time*, *source_table*) added to each row.
    """
    return df.select(
        F.lit(common["run_id"]).cast("string").alias("run_id"),
        F.to_timestamp(F.lit(common["run_time"])).alias("run_time"),
        F.lit(common["source_table"]).cast("string").alias("source_table"),
        F.lit(edge_type).cast("string").alias("edge_type"),
        depth_col.alias("depth"),
        target_table_col.cast("string").alias("target_table"),
        F.lit(None).cast("long").alias("target_delta_version"),
        source_column_col.alias("source_column"),
        target_column_col.alias("target_column"),
    )


__all__ = [
    "CollectLineageAction",
    "LINEAGE_TABLE_COMMENT",
    "LINEAGE_TABLE_SCHEMA",
    "LineageActionConfig",
    "LineageSearchConfig",
    "extract_failed_columns",
]
