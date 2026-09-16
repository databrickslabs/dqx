"""Lineage collection action for the DQX actions & alerting subsystem.

*CollectLineageAction* persists upstream and downstream table lineage, per-failed-column
lineage, and last-modifier metadata for the source table of a DQX run. Each lineage
edge / last-modifier record is emitted as a row in a single Delta table (schema declared
in *LINEAGE_TABLE_SCHEMA*) via the same *io.save_dataframe_as_table* helper DQX uses for
failed-check output — so the write mode / format / options semantics are handled identically.

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
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from databricks.labs.dqx.actions.base import Action, ActionContext, ActionResult, ActionServices, ActionStatus
from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log as _sanitize
from databricks.labs.dqx.actions.registry import register_action
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.errors import InvalidActionError
from databricks.labs.dqx.io import save_dataframe_as_table

logger = logging.getLogger(__name__)

LINEAGE_TABLE_LINEAGE = "system.access.table_lineage"
LINEAGE_COLUMN_LINEAGE = "system.access.column_lineage"
LINEAGE_JOBS_RUNS = "system.workflow.job_run_timeline"
LINEAGE_WORKSPACES = "system.access.workspaces"

# Default names of the DQX-appended result columns on the output / quarantine tables. These are
# the column names DQX writes when no engine-level rename is configured; both *_errors* and
# *_warnings* are ARRAY<STRUCT<..., columns ARRAY<STRING>, ...>>.
_ERRORS_COLUMN = "_errors"
_WARNINGS_COLUMN = "_warnings"

# Schema of the failed-columns DataFrame threaded between *extract_failed_columns* and
# *_collect_column_lineage_df*. A single STRING column keeps the join predicate trivial.
_FAILED_COLUMN_NAME = "col_name"
_FAILED_COLUMNS_SCHEMA = StructType([StructField(_FAILED_COLUMN_NAME, StringType(), nullable=True)])

# Persisted lineage-table schema. Declared as an explicit *StructType* (not schema-on-write
# inference) so downstream consumers see the same contract on empty runs.
LINEAGE_TABLE_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("run_time", TimestampType(), nullable=False),
        StructField("source_table", StringType(), nullable=False),
        StructField("edge_type", StringType(), nullable=False),
        StructField("depth", LongType(), nullable=True),
        StructField("target_table", StringType(), nullable=True),
        StructField("target_delta_version", LongType(), nullable=True),
        StructField("source_column", StringType(), nullable=True),
        StructField("target_column", StringType(), nullable=True),
        StructField("modifier_job_id", LongType(), nullable=True),
        StructField("modifier_run_id", LongType(), nullable=True),
        StructField("modifier_job_name", StringType(), nullable=True),
        StructField("modifier_status", StringType(), nullable=True),
        StructField("modifier_start_ms", LongType(), nullable=True),
        StructField("modifier_end_ms", LongType(), nullable=True),
        StructField("modifier_url", StringType(), nullable=True),
    ]
)

_EDGE_UPSTREAM = "upstream"
_EDGE_DOWNSTREAM = "downstream"
_EDGE_COLUMN_UPSTREAM = "column_upstream"
_EDGE_COLUMN_DOWNSTREAM = "column_downstream"
_EDGE_LAST_MODIFIER = "last_modifier"


# ---------------------------------------------------------------------------
# Configuration classes
# ---------------------------------------------------------------------------


class LineageSearchConfig(BaseModel):
    """Search parameters for one lineage traversal direction (upstream, downstream, or columns).

    Attributes:
        enabled: When *False*, the corresponding walk is skipped entirely.
        depth: Maximum walk depth from the source table. Must be ``>= 0``. A value of *0* means
            "no traversal" (equivalent to *enabled=False* but kept explicit for callers who prefer
            numeric knobs).
        lookback_days: How far back to consider lineage entries in *system.access.table_lineage*
            / *system.access.column_lineage*. Must be ``>= 1``.
        max_nodes: Hard cap on the number of distinct nodes visited per direction; guards against
            unbounded fan-out on dense graphs. Must be ``>= 1``.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    depth: int = 1
    lookback_days: int = 30
    max_nodes: int = 500

    @field_validator("depth")
    @classmethod
    def _validate_depth(cls, value: int) -> int:
        if value < 0:
            raise InvalidActionError(f"LineageSearchConfig.depth must be >= 0, got {value}.")
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


class LineageEntitySearchConfig(BaseModel):
    """Search parameters for last-modifier (entity) resolution.

    Attributes:
        enabled: When *False*, last-modifier resolution is skipped.
        lookback_days: How far back to consider modifier events. Must be ``>= 1``.
        max_last_runs: Maximum number of most-recent job runs to emit as last-modifier rows.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    lookback_days: int = 1
    max_last_runs: int = 1

    @field_validator("lookback_days")
    @classmethod
    def _validate_lookback(cls, value: int) -> int:
        if value < 1:
            raise InvalidActionError(f"LineageEntitySearchConfig.lookback_days must be >= 1, got {value}.")
        return value

    @field_validator("max_last_runs")
    @classmethod
    def _validate_max_last_runs(cls, value: int) -> int:
        if value < 1:
            raise InvalidActionError(f"LineageEntitySearchConfig.max_last_runs must be >= 1, got {value}.")
        return value


class LineageActionConfig(BaseModel):
    """Top-level configuration for *CollectLineageAction*.

    Composes four independent knobs — upstream, downstream, columns, entities — each defaulting to
    its type's default. Any direction can be disabled individually.
    """

    model_config = ConfigDict(extra="forbid")

    upstream: LineageSearchConfig = Field(default_factory=LineageSearchConfig)
    downstream: LineageSearchConfig = Field(default_factory=LineageSearchConfig)
    columns: LineageSearchConfig = Field(default_factory=LineageSearchConfig)
    entities: LineageEntitySearchConfig = Field(default_factory=LineageEntitySearchConfig)


@register_action
class CollectLineageAction(Action):
    """Collect table / column lineage and last-modifier metadata into a Delta table.

    On every run, gathers upstream/downstream tables (recursive-CTE walk bounded by *depth* +
    *max_nodes*), per-failed-column lineage, and the most recent modifier job runs, and writes
    one row per edge / modifier to the Delta table specified by *output_config*. The action never
    fails the pipeline — collection errors are logged and turned into *CONFIG_ERROR* results.

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
    config: LineageActionConfig = Field(default_factory=LineageActionConfig)

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

            failed_columns_df = _resolve_failed_columns(
                spark=spark, output_location=context.output_location, safe_name=safe_name
            )
            df = _collect_lineage_rows(
                spark=spark,
                source_table=source_table,
                run_id=context.run_id,
                run_time_iso=context.run_time.isoformat(),
                failed_columns_df=failed_columns_df,
                config=self.config,
            )
            save_dataframe_as_table(df, self.output_config)
            return ActionResult(
                action_name=self.name,
                fired=True,
                status=ActionStatus.HEALTHY,
                extras={"lineage_location": self.output_config.location},
            )
        except Exception as exc:  # broad catch: lineage failures must not break the run
            logger.warning(f"CollectLineageAction '{safe_name}' failed and was skipped: {_sanitize(str(exc))}")
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.CONFIG_ERROR, extras=None)


# ---------------------------------------------------------------------------
# Helpers — failed-columns extraction, empty-schema seed
# ---------------------------------------------------------------------------


def extract_failed_columns(spark: SparkSession, output_location: str) -> DataFrame:
    """Read *output_location* and return a DataFrame of distinct column names that failed a check.

    DQX appends two ARRAY<STRUCT> columns to the output table — *_errors* and *_warnings* — and
    every struct exposes a *columns* ARRAY<STRING> listing the column(s) the check flagged. This
    helper explodes both arrays and then their inner *columns* field, unions the two, filters
    nulls, and returns the distinct set as a one-column DataFrame (schema *_FAILED_COLUMNS_SCHEMA*
    with a single ``col_name STRING`` column).

    The DataFrame shape is deliberate: the downstream *_collect_column_lineage_df* joins it into
    *system.access.column_lineage* rather than iterating one query per name, so the full pipeline
    stays lazy and Spark can broadcast the (small) failed-columns set.

    A missing table, a customised error/warning column name, or a read failure returns an empty
    DataFrame with the same schema and logs a sanitised warning — the join then produces zero
    rows and column lineage becomes a natural no-op.

    Args:
        spark: Active *SparkSession* used to read *output_location*.
        output_location: 3-level UC name of the DQX output (or quarantine) table.

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
    for issues_col in (_ERRORS_COLUMN, _WARNINGS_COLUMN):
        if issues_col not in present_columns:
            continue
        exploded = df.select(F.explode(F.col(issues_col)).alias("issue"))
        issue_frames.append(exploded.select(F.explode(F.col("issue.columns")).alias(_FAILED_COLUMN_NAME)))

    if not issue_frames:
        return empty

    unioned = issue_frames[0]
    for frame in issue_frames[1:]:
        unioned = unioned.unionByName(frame)

    return unioned.where(F.col(_FAILED_COLUMN_NAME).isNotNull()).select(_FAILED_COLUMN_NAME).distinct()


def _resolve_failed_columns(*, spark: SparkSession, output_location: str | None, safe_name: str) -> DataFrame:
    """Wrap *extract_failed_columns* with the *output_location=None* guard.

    Column lineage requires a table to read the failed-check items from. When *output_location*
    is absent on the context (e.g. an in-memory run), a warning is logged and an empty
    failed-columns DataFrame is returned so the join downstream produces no rows.
    """
    if output_location is None:
        logger.warning(
            f"CollectLineageAction '{safe_name}' has no output_location on the context; "
            "column lineage will be skipped."
        )
        return spark.createDataFrame([], _FAILED_COLUMNS_SCHEMA)
    return extract_failed_columns(spark, output_location)


def _empty_lineage_df(spark: SparkSession) -> DataFrame:
    """Empty DataFrame with *LINEAGE_TABLE_SCHEMA* — identity for *unionByName* composition."""
    return spark.createDataFrame([], LINEAGE_TABLE_SCHEMA)


def _collect_lineage_rows(
    *,
    spark: SparkSession,
    source_table: str,
    run_id: str,
    run_time_iso: str,
    failed_columns_df: DataFrame,
    config: LineageActionConfig,
) -> DataFrame:
    """Collect all lineage rows for *source_table* into a single DataFrame with *LINEAGE_TABLE_SCHEMA*.

    Each enabled branch (*upstream*, *downstream*, *columns*, *entities*) returns its own
    *LINEAGE_TABLE_SCHEMA*-shaped DataFrame; the composition is a plain *unionByName* over an
    empty-schema seed, so downstream writes are well-formed Delta writes even when no branch
    contributed rows.

    *failed_columns_df* is a DataFrame with a single ``col_name STRING`` column produced by
    *extract_failed_columns* — it is inner-joined into the column-lineage read rather than
    iterated one column at a time, so the whole pipeline stays lazy.
    """
    common: dict[str, Any] = {
        "run_id": run_id,
        # run_time is an ISO-8601 string that Spark parses to TIMESTAMP on write.
        "run_time": run_time_iso,
        "source_table": source_table,
    }
    result = _empty_lineage_df(spark)
    if config.upstream.enabled:
        result = result.unionByName(
            _collect_upstream_df(spark=spark, source_table=source_table, search=config.upstream, common=common)
        )
    if config.downstream.enabled:
        result = result.unionByName(
            _collect_downstream_df(spark=spark, source_table=source_table, search=config.downstream, common=common)
        )
    if config.columns.enabled:
        result = result.unionByName(
            _collect_column_lineage_df(
                spark=spark,
                source_table=source_table,
                failed_columns_df=failed_columns_df,
                search=config.columns,
                common=common,
            )
        )
    if config.entities.enabled:
        result = result.unionByName(
            _collect_modifier_df(spark=spark, source_table=source_table, entities_cfg=config.entities, common=common)
        )
    return result


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

    *table_full_name* and *search.lookback_days* are passed via ``args={...}`` (parameterised).
    *search.depth* and *search.max_nodes* are validated Pydantic ints and interpolated as bare
    constants; Spark SQL does not accept parameter markers for ``LIMIT`` / plain integer
    comparisons on all supported DBR versions, and the values come from a validated schema so
    interpolation is safe.

    Returns:
        DataFrame conforming to *LINEAGE_TABLE_SCHEMA* — one row per discovered edge, with the
        constant columns from *common* prefixed and unrelated columns set to typed NULLs.
    """
    key_source = "source_table_full_name" if direction == _EDGE_UPSTREAM else "target_table_full_name"
    key_other = "target_table_full_name" if direction == _EDGE_UPSTREAM else "source_table_full_name"
    max_depth = int(search.depth)
    max_nodes = int(search.max_nodes)
    # nosec B608: identifiers only; user input flows through :table_name / :lookback below.
    query = (
        "WITH RECURSIVE edges(anchor, neighbour, depth, path, event_time) AS ("
        f"  SELECT :table_name AS anchor, {key_other} AS neighbour, 1 AS depth, "
        f"         array(:table_name, {key_other}) AS path, event_time "
        f"  FROM {LINEAGE_TABLE_LINEAGE} "
        f"  WHERE {key_source} = :table_name "
        f"    AND {key_other} IS NOT NULL "
        f"    AND {key_other} <> :table_name "
        "    AND event_time >= current_timestamp() - INTERVAL :lookback DAYS "
        "  UNION ALL "
        f"  SELECT e.neighbour AS anchor, t.{key_other} AS neighbour, e.depth + 1, "
        f"         array_append(e.path, t.{key_other}), t.event_time "
        f"  FROM edges e JOIN {LINEAGE_TABLE_LINEAGE} t "
        f"    ON t.{key_source} = e.neighbour "
        f"  WHERE e.depth < {max_depth} "
        f"    AND t.{key_other} IS NOT NULL "
        f"    AND NOT array_contains(e.path, t.{key_other}) "
        "    AND t.event_time >= current_timestamp() - INTERVAL :lookback DAYS "
        ") "
        "SELECT DISTINCT anchor, neighbour, depth "
        "FROM edges "
        f"LIMIT {max_nodes}"
    )
    try:
        walk_df = spark.sql(query, args={"table_name": table_full_name, "lookback": search.lookback_days})
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


def _collect_column_lineage_df(
    *,
    spark: SparkSession,
    source_table: str,
    failed_columns_df: DataFrame,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """Column-lineage rows for *source_table* — join-based, DataFrame-only.

    Both directions read *LINEAGE_COLUMN_LINEAGE* filtered by *source_table* + lookback window,
    then broadcast-join the (small) *failed_columns_df* on ``target_column`` (upstream) or
    ``source_column`` (downstream) so the per-column filter is a single join, not a loop.

    Args:
        spark: Active *SparkSession*.
        source_table: 3-level UC anchor.
        failed_columns_df: DataFrame with a single ``col_name STRING`` column (from
            *extract_failed_columns*). May be empty — the join then yields no rows.
        search: Per-direction cap on emitted rows.
        common: Constants added to each emitted row.
    """
    result = _empty_lineage_df(spark)
    for edge_type, filter_column in (
        (_EDGE_COLUMN_UPSTREAM, "target_column"),
        (_EDGE_COLUMN_DOWNSTREAM, "source_column"),
    ):
        result = result.unionByName(
            _column_direction_df(
                spark=spark,
                source_table=source_table,
                failed_columns_df=failed_columns_df,
                edge_type=edge_type,
                filter_column=filter_column,
                search=search,
                common=common,
            )
        )
    return result


def _column_direction_df(
    *,
    spark: SparkSession,
    source_table: str,
    failed_columns_df: DataFrame,
    edge_type: str,
    filter_column: str,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """Single-direction column-lineage read joined against the failed-columns DataFrame."""
    if edge_type == _EDGE_COLUMN_UPSTREAM:
        query = (
            "SELECT source_table_full_name AS neighbour, source_column, target_column "  # nosec B608
            f"FROM {LINEAGE_COLUMN_LINEAGE} "
            "WHERE target_table_full_name = :table_name "
            "AND event_time >= current_timestamp() - INTERVAL :lookback DAYS"
        )
    else:
        query = (
            "SELECT target_table_full_name AS neighbour, source_column, target_column "  # nosec B608
            f"FROM {LINEAGE_COLUMN_LINEAGE} "
            "WHERE source_table_full_name = :table_name "
            "AND event_time >= current_timestamp() - INTERVAL :lookback DAYS"
        )
    try:
        neighbours = spark.sql(
            query,
            args={"table_name": source_table, "lookback": search.lookback_days},
        )
    except Exception as exc:  # broad catch: system-table read errors are non-fatal for lineage
        logger.warning(
            f"Column lineage read failed for '{_sanitize(source_table)}' ({edge_type}): {_sanitize(str(exc))}"
        )
        return _empty_lineage_df(spark)
    # Broadcast the (small) failed-columns DataFrame into the join so filtering happens in one
    # shot on the workers. If *failed_columns_df* is empty the inner-join yields zero rows.
    filtered = neighbours.join(
        F.broadcast(failed_columns_df),
        neighbours[filter_column] == failed_columns_df[_FAILED_COLUMN_NAME],
        "inner",
    ).drop(_FAILED_COLUMN_NAME)
    deduped = filtered.dropDuplicates(["neighbour", "source_column", "target_column"]).limit(int(search.max_nodes))
    return _project_edge_rows(
        deduped,
        edge_type=edge_type,
        target_table_col=F.col("neighbour"),
        depth_col=F.lit(1).cast("long"),
        source_column_col=F.col("source_column"),
        target_column_col=F.col("target_column"),
        common=common,
    )


def _collect_modifier_df(
    *,
    spark: SparkSession,
    source_table: str,
    entities_cfg: LineageEntitySearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """Resolve the most recent JOB modifier runs for *source_table* as a DataFrame.

    The pipeline filters *LINEAGE_TABLE_LINEAGE* to *JOB* modifier events targeting *source_table* within the
    configured lookback window, keeps the *max_last_runs* most recent by *event_time*, joins the
    latest run per *job_id* from *LINEAGE_JOBS_RUNS*, and left-joins *LINEAGE_WORKSPACES* on
    *workspace_id* so the modifier URL is composed against the workspace where the job actually
    ran (not the current *WorkspaceClient*'s workspace, which may differ).

    Args:
        spark: Active *SparkSession*.
        source_table: 3-level UC name whose modifier history is being resolved.
        entities_cfg: Lookback / row-cap knobs.
        common: Constants added to each emitted row.

    Returns:
        DataFrame conforming to *LINEAGE_TABLE_SCHEMA*, empty on failure.
    """
    try:
        return _build_modifier_df(spark=spark, source_table=source_table, entities_cfg=entities_cfg, common=common)
    except Exception as exc:  # broad catch: modifier lookup is best-effort
        logger.warning(f"Last-modifier read failed for '{_sanitize(source_table)}': {_sanitize(str(exc))}")
        return _empty_lineage_df(spark)


def _build_modifier_df(
    *,
    spark: SparkSession,
    source_table: str,
    entities_cfg: LineageEntitySearchConfig,
    common: dict[str, Any],
) -> DataFrame:
    """DataFrame-only body of *_collect_modifier_df*, extracted to keep the outer try small."""
    lookback_days = int(entities_cfg.lookback_days)
    max_last_runs = int(entities_cfg.max_last_runs)
    lookback_expr = F.expr(f"INTERVAL {lookback_days} DAYS")

    lineage = spark.table(LINEAGE_TABLE_LINEAGE)
    runs = spark.table(LINEAGE_JOBS_RUNS)
    workspaces = spark.table(LINEAGE_WORKSPACES)

    # `Window.orderBy` without a partition triggers a Spark warning about performance — unavoidable
    # here since we want a global "most-recent-N" across events, but the window is tiny (bounded by
    # DAG size) so this is fine.
    event_window = Window.orderBy(F.col("event_time").desc())
    modifier_events = (
        lineage.filter(F.col("target_table_full_name") == F.lit(source_table))
        .filter(F.col("event_time") >= F.current_timestamp() - lookback_expr)
        .filter(F.col("entity_type") == F.lit("JOB"))
        .filter(F.col("entity_id").isNotNull())
        .withColumn("_rn", F.row_number().over(event_window))
        .filter(F.col("_rn") <= max_last_runs)
        .drop("_rn")
        .select(F.col("entity_id").alias("mod_job_id"))
    )

    run_window = Window.partitionBy("job_id").orderBy(F.col("period_start_time").desc())
    latest_runs = (
        runs.filter(F.col("period_start_time") >= F.current_timestamp() - lookback_expr)
        .withColumn("_rn", F.row_number().over(run_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            F.col("job_id").alias("run_job_id"),
            F.col("run_id").alias("run_run_id"),
            F.col("job_name").alias("run_job_name"),
            F.col("period_start_time").alias("run_period_start_time"),
            F.col("period_end_time").alias("run_period_end_time"),
            F.col("result_state").alias("run_result_state"),
            F.col("workspace_id").alias("run_workspace_id"),
        )
    )

    workspaces_slim = workspaces.select(
        F.col("workspace_id").alias("ws_workspace_id"),
        F.col("workspace_url").alias("ws_workspace_url"),
    )

    joined = modifier_events.join(latest_runs, modifier_events["mod_job_id"] == latest_runs["run_job_id"], "left").join(
        workspaces_slim,
        latest_runs["run_workspace_id"] == workspaces_slim["ws_workspace_id"],
        "left",
    )

    url_expr = F.when(
        F.col("ws_workspace_url").isNotNull() & F.col("mod_job_id").isNotNull() & F.col("run_run_id").isNotNull(),
        F.concat(
            F.lit("https://"),
            F.col("ws_workspace_url"),
            F.lit("/jobs/"),
            F.col("mod_job_id").cast("string"),
            F.lit("/runs/"),
            F.col("run_run_id").cast("string"),
        ),
    ).otherwise(F.lit(None).cast("string"))

    return joined.select(
        F.lit(common["run_id"]).cast("string").alias("run_id"),
        F.to_timestamp(F.lit(common["run_time"])).alias("run_time"),
        F.lit(common["source_table"]).cast("string").alias("source_table"),
        F.lit(_EDGE_LAST_MODIFIER).cast("string").alias("edge_type"),
        F.lit(None).cast("long").alias("depth"),
        F.lit(None).cast("string").alias("target_table"),
        F.lit(None).cast("long").alias("target_delta_version"),
        F.lit(None).cast("string").alias("source_column"),
        F.lit(None).cast("string").alias("target_column"),
        F.col("mod_job_id").cast("long").alias("modifier_job_id"),
        F.col("run_run_id").cast("long").alias("modifier_run_id"),
        F.col("run_job_name").cast("string").alias("modifier_job_name"),
        F.col("run_result_state").cast("string").alias("modifier_status"),
        (F.unix_timestamp(F.col("run_period_start_time")) * F.lit(1000)).cast("long").alias("modifier_start_ms"),
        (F.unix_timestamp(F.col("run_period_end_time")) * F.lit(1000)).cast("long").alias("modifier_end_ms"),
        url_expr.alias("modifier_url"),
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
        F.lit(None).cast("long").alias("modifier_job_id"),
        F.lit(None).cast("long").alias("modifier_run_id"),
        F.lit(None).cast("string").alias("modifier_job_name"),
        F.lit(None).cast("string").alias("modifier_status"),
        F.lit(None).cast("long").alias("modifier_start_ms"),
        F.lit(None).cast("long").alias("modifier_end_ms"),
        F.lit(None).cast("string").alias("modifier_url"),
    )


__all__ = [
    "CollectLineageAction",
    "LINEAGE_TABLE_SCHEMA",
    "LineageActionConfig",
    "LineageEntitySearchConfig",
    "LineageSearchConfig",
    "extract_failed_columns",
]
