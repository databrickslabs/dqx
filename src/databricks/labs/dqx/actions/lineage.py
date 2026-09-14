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

The *failed_columns* signal is read from the well-known metric key ``"failed_columns"``.
Upstream metric producers must populate this list to enable per-column lineage collection.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, Literal, Mapping

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from databricks.sdk import WorkspaceClient

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

# Matches a UC 3-level identifier `catalog.schema.table`. Segments are conservative on purpose:
# alphanumerics and underscores only. Backtick-quoted identifiers are pre-stripped before the check
# (see _is_safe_table_ref) so caller-provided quoting does not fail validation.
_TABLE_REF_PATTERN = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")

# Well-known metric key that carries the list of column names that failed row-level checks.
FAILED_COLUMNS_METRIC_KEY = "failed_columns"

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
# Internal value types — Python-side collection code only; nothing persisted.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _TableRef:
    """Internal 3-level table reference used by the collector code (not persisted)."""

    catalog: str
    schema: str
    name: str
    delta_version: int | None = None

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"


@dataclass(frozen=True)
class _JobRunRef:
    """Internal last-modifier reference used by the collector code (not persisted)."""

    job_id: int | None
    run_id: int | None
    job_name: str | None
    start_time_ms: int | None
    end_time_ms: int | None
    status: str | None
    url: str | None


# ---------------------------------------------------------------------------
# Configuration classes
# ---------------------------------------------------------------------------


class LineageSearchConfig(BaseModel):
    """Search parameters for one lineage traversal direction (upstream, downstream, or columns).

    Attributes:
        enabled: When *False*, the corresponding walk is skipped entirely.
        depth: Maximum BFS depth from the source table. Must be ``>= 0``. A value of *0* means
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


# ---------------------------------------------------------------------------
# Action
# ---------------------------------------------------------------------------


@register_action
class CollectLineageAction(Action):
    """Collect table / column lineage and last-modifier metadata into a Delta table.

    On every run, gathers upstream/downstream tables (BFS bounded by *depth* + *max_nodes*),
    per-failed-column lineage, and the most recent modifier job runs, and writes one row per edge /
    modifier to the Delta table specified by *output_config*. The action never fails the pipeline —
    collection errors are logged and turned into *CONFIG_ERROR* results.

    Attributes:
        type: Discriminator literal, always *"collect_lineage"*.
        name: Logical identifier for this action; derived to *"collect_lineage"* when left empty.
        output_config: Required output sink for the lineage table. *mode* should be set equal to
            *RunConfig.output_config.mode* so lineage writes stay consistent with how DQX writes the
            failed-checks output.
        config: Optional collection knobs — see *LineageActionConfig*.
    """

    type: Literal["collect_lineage"] = "collect_lineage"
    name: str = ""
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

    @model_validator(mode="after")
    def _derive_name(self) -> "CollectLineageAction":
        if not self.name:
            self.name = "collect_lineage"
        return self

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
            # TODO (IK): needless validation - ActionContext passed by framework is always valid (trusted)
            if not _is_safe_table_ref(source_table):
                raise InvalidActionError(
                    f"Unsafe input_location for CollectLineageAction: '{_sanitize(source_table)}'."
                )

            spark = services.spark
            ws = services.ws
            if spark is None:
                raise InvalidActionError("CollectLineageAction requires SparkSession in ActionServices.spark.")

            failed_columns = _extract_failed_columns(context.metrics)
            df = _collect_lineage_rows(
                spark=spark,
                ws=ws,
                source_table=source_table,
                run_id=context.run_id,
                run_time_iso=context.run_time.isoformat(),
                failed_columns=failed_columns,
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
            logger.warning(
                f"CollectLineageAction '{safe_name}' failed and was skipped: {_sanitize(str(exc))}"
            )
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.CONFIG_ERROR, extras=None)


# ---------------------------------------------------------------------------
# Helpers — table-name validation, failed-columns extraction
# ---------------------------------------------------------------------------


def _is_safe_table_ref(name: str) -> bool:
    """Return *True* when *name* matches the safe 3-level UC identifier pattern.

    Backtick-quoted segments are unquoted before the check. Empty segments, whitespace, and
    characters outside ``[A-Za-z0-9_]`` are rejected so downstream SQL construction never sees
    caller-controlled specials (semicolons, quotes, newlines, …).
    """
    stripped = name.replace("`", "")
    return bool(_TABLE_REF_PATTERN.match(stripped))


def _extract_failed_columns(metrics: Mapping[str, object]) -> tuple[str, ...]:
    """Return the failed-column names from the well-known *failed_columns* metric key.

    Contract: upstream metric producers populate ``metrics["failed_columns"]`` with a list/tuple of
    column-name strings. Missing key or empty container returns an empty tuple, which disables
    the column-lineage phase in *_collect_lineage_rows*.
    """
    value = metrics.get(FAILED_COLUMNS_METRIC_KEY)
    if not value:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(v) for v in value if isinstance(v, str))
    return ()


# ---------------------------------------------------------------------------
# Collection — BFS walks and last-modifier resolution
# ---------------------------------------------------------------------------


def _collect_lineage_rows(
    *,
    spark: SparkSession,
    ws: WorkspaceClient | None,
    source_table: str,
    run_id: str,
    run_time_iso: str,
    failed_columns: tuple[str, ...],
    config: LineageActionConfig,
) -> DataFrame:
    """Collect all lineage rows for *source_table* into a single DataFrame with *LINEAGE_TABLE_SCHEMA*.

    Emits an empty DataFrame with the fixed schema when no rows are collected so downstream writes
    are still well-formed Delta writes with a stable schema.
    """
    rows: list[dict[str, Any]] = []
    common: dict[str, Any] = {
        "run_id": run_id,
        # run_time is passed as an ISO-8601 string that Spark parses to TIMESTAMP on write.
        "run_time": run_time_iso,
        "source_table": source_table,
    }
    # TODO (IK): split each `if enbled - walk lineage` into individual methods
    if config.upstream.enabled:
        rows.extend(
            _walk_lineage(
                spark=spark,
                table_full_name=source_table,
                direction=_EDGE_UPSTREAM,
                search=config.upstream,
                common=common,
            )
        )
    if config.downstream.enabled:
        rows.extend(
            _walk_lineage(
                spark=spark,
                table_full_name=source_table,
                direction=_EDGE_DOWNSTREAM,
                search=config.downstream,
                common=common,
            )
        )
    if config.columns.enabled and failed_columns:
        rows.extend(
            _collect_column_lineage(
                spark=spark,
                table_full_name=source_table,
                failed_columns=failed_columns,
                search=config.columns,
                common=common,
            )
        )
    if config.entities.enabled:
        modifiers = _resolve_last_modifier(
            spark=spark,
            ws=ws,
            table_full_name=source_table,
            entities_cfg=config.entities,
        )
        for modifier in modifiers:
            rows.append(_last_modifier_row(common, modifier))

    if not rows:
        return spark.createDataFrame([], LINEAGE_TABLE_SCHEMA)
    return spark.createDataFrame(rows, LINEAGE_TABLE_SCHEMA)


def _walk_lineage(
    *,
    spark: SparkSession,
    table_full_name: str,
    direction: str,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> list[dict[str, Any]]:
    """BFS *table_full_name*'s upstream or downstream neighbours bounded by *search*.

    Uses parameterised SQL — *table_full_name* is passed via ``args={...}`` and never
    string-interpolated into the query text. Emits one row per edge; excludes cycles via a
    *visited* set. On *max_nodes* reached, logs a sanitised warning and returns what was collected.
    """
    # TODO (IK): Prefer recursive CTE over looping with nesting: https://www.databricks.com/blog/introducing-recursive-common-table-expressions-databricks
    # Add to the docs that this would require DBR 17+ for feature usage.
    key_source = "source_table_full_name" if direction == _EDGE_UPSTREAM else "target_table_full_name"
    key_other = "target_table_full_name" if direction == _EDGE_UPSTREAM else "source_table_full_name"
    # SELECT source_table + target_table + event_time; parameterised for safety (CWE-89).
    query = (
        f"SELECT {key_source} AS anchor, {key_other} AS neighbour, event_time "  # nosec B608: identifiers, not user input
        f"FROM {LINEAGE_TABLE_LINEAGE} "
        f"WHERE {key_source} = :table_name "
        f"AND event_time >= current_timestamp() - INTERVAL :lookback DAYS"
    )
    visited: set[str] = {table_full_name}
    frontier: list[tuple[str, int]] = [(table_full_name, 0)]
    rows: list[dict[str, Any]] = []

    while frontier:
        current, depth = frontier.pop(0)
        if depth >= search.depth:
            continue
        try:
            neighbours_df = spark.sql(
                query,
                args={"table_name": current, "lookback": search.lookback_days},
            )
        except Exception as exc:  # broad catch: system-table read errors are non-fatal for lineage
            logger.warning(
                f"Lineage read failed for '{_sanitize(current)}' ({direction}): {_sanitize(str(exc))}"
            )
            continue
        for row in neighbours_df.collect():
            neighbour = row["neighbour"]
            if not neighbour:
                continue
            if neighbour in visited:
                # cycle exclusion — do not re-walk
                continue
            if len(visited) >= search.max_nodes:
                logger.warning(
                    f"Lineage {direction} walk reached max_nodes={search.max_nodes} at "
                    f"'{_sanitize(current)}'; truncating."
                )
                return rows
            visited.add(neighbour)
            rows.append(
                {
                    **common,
                    "edge_type": direction,
                    "depth": depth + 1,
                    "target_table": neighbour,
                    "target_delta_version": None,
                    "source_column": None,
                    "target_column": None,
                    "modifier_job_id": None,
                    "modifier_run_id": None,
                    "modifier_job_name": None,
                    "modifier_status": None,
                    "modifier_start_ms": None,
                    "modifier_end_ms": None,
                    "modifier_url": None,
                }
            )
            frontier.append((neighbour, depth + 1))
    return rows


def _collect_column_lineage(
    *,
    spark: SparkSession,
    table_full_name: str,
    failed_columns: tuple[str, ...],
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> list[dict[str, Any]]:
    """Per-failed-column BFS against *LINEAGE_COLUMN_LINEAGE* — parameterised, cycle-safe."""
    query_up = (
        "SELECT source_table_full_name AS neighbour, source_column, target_column, event_time "  # nosec B608
        f"FROM {LINEAGE_COLUMN_LINEAGE} "
        "WHERE target_table_full_name = :table_name "
        "AND target_column = :column_name "
        "AND event_time >= current_timestamp() - INTERVAL :lookback DAYS"
    )
    query_down = (
        "SELECT target_table_full_name AS neighbour, source_column, target_column, event_time "  # nosec B608
        f"FROM {LINEAGE_COLUMN_LINEAGE} "
        "WHERE source_table_full_name = :table_name "
        "AND source_column = :column_name "
        "AND event_time >= current_timestamp() - INTERVAL :lookback DAYS"
    )
    rows: list[dict[str, Any]] = []
    for column_name in failed_columns:
        rows.extend(
            _one_column_direction(
                spark=spark,
                table_full_name=table_full_name,
                column_name=column_name,
                query=query_up,
                edge_type=_EDGE_COLUMN_UPSTREAM,
                search=search,
                common=common,
            )
        )
        rows.extend(
            _one_column_direction(
                spark=spark,
                table_full_name=table_full_name,
                column_name=column_name,
                query=query_down,
                edge_type=_EDGE_COLUMN_DOWNSTREAM,
                search=search,
                common=common,
            )
        )
    return rows


def _one_column_direction(
    *,
    spark: SparkSession,
    table_full_name: str,
    column_name: str,
    query: str,
    edge_type: str,
    search: LineageSearchConfig,
    common: dict[str, Any],
) -> list[dict[str, Any]]:
    """Single-direction column-lineage read for one *(table, column)* pair — parameterised."""
    try:
        neighbours = spark.sql(
            query,
            args={"table_name": table_full_name, "column_name": column_name, "lookback": search.lookback_days},
        ).collect()
    except Exception as exc:
        logger.warning(
            f"Column lineage read failed for '{_sanitize(table_full_name)}.{_sanitize(column_name)}' "
            f"({edge_type}): {_sanitize(str(exc))}"
        )
        return []
    seen: set[tuple[str | None, str | None, str | None]] = set()
    rows: list[dict[str, Any]] = []
    for row in neighbours:
        neighbour = row["neighbour"]
        source_column = row["source_column"]
        target_column = row["target_column"]
        key = (neighbour, source_column, target_column)
        if key in seen:
            continue
        seen.add(key)
        if len(rows) >= search.max_nodes:
            logger.warning(
                f"Column lineage {edge_type} walk reached max_nodes={search.max_nodes} for "
                f"'{_sanitize(table_full_name)}.{_sanitize(column_name)}'; truncating."
            )
            break
        rows.append(
            {
                **common,
                "edge_type": edge_type,
                "depth": 1,
                "target_table": neighbour,
                "target_delta_version": None,
                "source_column": source_column,
                "target_column": target_column,
                "modifier_job_id": None,
                "modifier_run_id": None,
                "modifier_job_name": None,
                "modifier_status": None,
                "modifier_start_ms": None,
                "modifier_end_ms": None,
                "modifier_url": None,
            }
        )
    return rows


def _resolve_last_modifier(
    *,
    spark: SparkSession,
    ws: WorkspaceClient | None,
    table_full_name: str,
    entities_cfg: LineageEntitySearchConfig,
) -> list[_JobRunRef]:
    """Look up the most recent write events for *table_full_name* — parameterised, bounded."""
    modifier_query = (
        "SELECT entity_type, entity_id, event_time "  # nosec B608
        f"FROM {LINEAGE_TABLE_LINEAGE} "
        "WHERE target_table_full_name = :table_name "
        "AND event_time >= current_timestamp() - INTERVAL :lookback DAYS "
        "ORDER BY event_time DESC "
        "LIMIT :max_last_runs"
    )
    # TODO(IK): no collect - preserve working with dataframe.
    try:
        latest = spark.sql(
            modifier_query,
            args={
                "table_name": table_full_name,
                "lookback": entities_cfg.lookback_days,
                "max_last_runs": entities_cfg.max_last_runs,
            },
        ).collect()
    except Exception as exc:
        logger.warning(
            f"Last-modifier read failed for '{_sanitize(table_full_name)}': {_sanitize(str(exc))}"
        )
        return []

    # TODO (IK): this is actually a bug - job can be run on different workspace then current one
    # Hence we need to get workspace url of the workspace where job has been executed
    # This can be taken from - https://docs.databricks.com/aws/en/admin/system-tables/workspaces
    host = ws.config.host if ws is not None and getattr(ws, "config", None) is not None else None
    modifiers: list[_JobRunRef] = []
    for row in latest:
        entity_type = row["entity_type"]
        entity_id = row["entity_id"]
        if entity_type != "JOB" or entity_id is None:
            modifiers.append(_JobRunRef(None, None, None, None, None, None, None))
            continue
        job_run = _lookup_job_run(spark, entity_id, entities_cfg.lookback_days)
        modifiers.append(
            _JobRunRef(
                job_id=job_run.get("job_id"),
                run_id=job_run.get("run_id"),
                job_name=job_run.get("job_name"),
                start_time_ms=job_run.get("start_time_ms"),
                end_time_ms=job_run.get("end_time_ms"),
                status=job_run.get("status"),
                url=_job_run_url(host, job_run.get("job_id"), job_run.get("run_id")),
            )
        )
    return modifiers


def _lookup_job_run(spark: SparkSession, entity_id: Any, lookback_days: int) -> dict[str, Any]:
    """Read the most recent run row from *LINEAGE_JOBS_RUNS* for *entity_id* — parameterised."""
    # TODO (IK): Prefer PySpark API over manual query construction.
    query = (
        "SELECT job_id, run_id, job_name, period_start_time, period_end_time, result_state "  # nosec B608
        f"FROM {LINEAGE_JOBS_RUNS} "
        "WHERE job_id = :job_id "
        "AND period_start_time >= current_timestamp() - INTERVAL :lookback DAYS "
        "ORDER BY period_start_time DESC "
        "LIMIT 1"
    )
    # TODO (IK): no collect - keep it data frame and job with other data frames
    try:
        result = spark.sql(query, args={"job_id": entity_id, "lookback": lookback_days}).collect()
    except Exception as exc:
        logger.warning(f"Job run lookup failed for job_id='{_sanitize(str(entity_id))}': {_sanitize(str(exc))}")
        return {}
    if not result:
        return {}
    row = result[0]
    start = row["period_start_time"]
    end = row["period_end_time"]
    return {
        "job_id": row["job_id"],
        "run_id": row["run_id"],
        "job_name": row["job_name"],
        "start_time_ms": int(start.timestamp() * 1000) if start is not None else None,
        "end_time_ms": int(end.timestamp() * 1000) if end is not None else None,
        "status": row["result_state"],
    }

def _job_run_url(host: str | None, job_id: int | None, run_id: int | None) -> str | None:
    """Build a Databricks job-run URL when *host* and *job_id*/*run_id* are known."""
    if not host or job_id is None or run_id is None:
        return None
    return f"{host.rstrip('/')}/jobs/{job_id}/runs/{run_id}"


def _last_modifier_row(common: dict[str, Any], modifier: _JobRunRef) -> dict[str, Any]:
    """Build one row for the flat schema from a *_JobRunRef*."""
    return {
        **common,
        "edge_type": _EDGE_LAST_MODIFIER,
        "depth": None,
        "target_table": None,
        "target_delta_version": None,
        "source_column": None,
        "target_column": None,
        "modifier_job_id": modifier.job_id,
        "modifier_run_id": modifier.run_id,
        "modifier_job_name": modifier.job_name,
        "modifier_status": modifier.status,
        "modifier_start_ms": modifier.start_time_ms,
        "modifier_end_ms": modifier.end_time_ms,
        "modifier_url": modifier.url,
    }


__all__ = [
    "CollectLineageAction",
    "FAILED_COLUMNS_METRIC_KEY",
    "LINEAGE_TABLE_SCHEMA",
    "LineageActionConfig",
    "LineageEntitySearchConfig",
    "LineageSearchConfig",
]
