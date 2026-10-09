"""Unity Catalog metadata enrichment for AI-assisted rule generation.

Collects table + column comments, tags, bounded column-upstream lineage, and bounded external
upstream lineage for a source UC table and renders them as JSON that the LLM schema prompt
can consume.

The recursive-CTE walkers for *collect_column_upstream_lineage* and
*collect_upstream_table_lineage* are standalone upstream-only collectors invoked from the
profiler/LLM path. They apply bounded *depth*, *lookback_days* window, per-member + tail
``LIMIT``, in-CTE full-path cycle guard, and ``_sql_str_literal``-escaped identifiers. None
of the collectors in this module ever raise outward: lineage / tag / comment /
external-lineage failures degrade to empty structures and log via *sanitize_for_logging*
(CWE-117).

External lineage is read via the SDK's *ExternalLineageAPI.list_external_lineage_relationships*
because UC external lineage (non-Databricks sources — SAP, Salesforce, Tableau, custom
systems) is not recorded in *system.access.*_lineage*.
"""

import json
import logging
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ExternalLineageInfo,
    ExternalLineageObject,
    ExternalLineageTable,
    LineageDirection,
)

from databricks.labs.dqx.config import (
    ColumnUpstreamLineageConfig,
    ExternalLineageConfig,
    UnityCatalogMetadataConfig,
)
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.utils import sanitize_for_logging

logger = logging.getLogger(__name__)

__all__ = [
    "collect_table_comments",
    "collect_table_tags",
    "collect_column_upstream_lineage",
    "collect_upstream_table_lineage",
    "collect_external_upstream_lineage",
    "build_schema_json",
]

_TABLE_LINEAGE_TABLE = "system.access.table_lineage"
_COLUMN_LINEAGE_TABLE = "system.access.column_lineage"
_TABLE_TAGS_TABLE = "system.information_schema.table_tags"
_COLUMN_TAGS_TABLE = "system.information_schema.column_tags"


def _sanitize_text(value: str | None) -> str | None:
    """Return ``value`` with surrounding whitespace trimmed, or ``None`` for empty/missing."""
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def _sql_str_literal(value: str) -> str:
    """Escape a Python string for safe inclusion as a single-quoted SQL string literal.

    Backslash is doubled **first** so the quote-doubling introduced for a single quote is not
    itself re-escaped. Required because ``INTERVAL n DAYS`` and the anchor-table literal
    cannot use parameter markers.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _split_three_part_name(name: str) -> tuple[str, str, str]:
    """Return ``(catalog, schema, table)`` for a 3-part UC name.

    Backticks are stripped so quoted identifiers round-trip; raises
    *InvalidParameterError* when the input is not exactly three segments so callers that
    forward the result to the SDK fail loudly on malformed input.
    """
    parts = [segment.strip("`") for segment in name.split(".")]
    if len(parts) != 3 or not all(parts):
        raise InvalidParameterError(
            f"Expected 3-part Unity Catalog name 'catalog.schema.table', got {sanitize_for_logging(name)!r}"
        )
    return parts[0], parts[1], parts[2]


def collect_table_comments(ws: WorkspaceClient, table: str) -> tuple[str | None, dict[str, str]]:
    """Return ``(table_comment, {column_name: column_comment})`` for ``table`` in one SDK call.

    Reads via ``ws.tables.get(table)`` (backticks stripped first). The table comment is
    ``None`` when missing / whitespace-only; the column map omits columns without a comment.
    Returns ``(None, {})`` on any read error so the caller can still render a schema prompt.
    """
    try:
        table_info = ws.tables.get(table.replace("`", ""))
    except Exception as exc:  # broad catch: UC read failures are non-fatal for enrichment
        logger.warning(
            f"collect_table_comments: failed to read '{sanitize_for_logging(table)}': "
            f"{sanitize_for_logging(str(exc))}"
        )
        return None, {}
    table_comment = _sanitize_text(table_info.comment)
    column_comments: dict[str, str] = {}
    for column in table_info.columns or []:
        comment = _sanitize_text(column.comment)
        if column.name and comment is not None:
            column_comments[column.name] = comment
    return table_comment, column_comments


def collect_table_tags(spark: SparkSession, table: str) -> dict[str, Any]:
    """Return UC tags for a 3-part table name via ``system.information_schema.*_tags``.

    Uses the PySpark DataFrame API — ``spark.table(...).filter(...).select(...)`` — so the
    catalog, schema, and table name are passed as literal column comparisons rather than
    inlined into a SQL string (SQL-injection safe by construction).

    Returns:
        ``{"table_tags": [{"key","value"}], "column_tags": {"<col>": [{"key","value"}]}}``.
        An empty structure is returned on any read error.
    """
    empty: dict[str, Any] = {"table_tags": [], "column_tags": {}}
    try:
        catalog, schema, table_name = _split_three_part_name(table)
    except InvalidParameterError as exc:
        logger.warning(f"collect_table_tags: {sanitize_for_logging(str(exc))}")
        return empty
    name_filter = (
        (F.col("catalog_name") == catalog) & (F.col("schema_name") == schema) & (F.col("table_name") == table_name)
    )

    table_tags: list[dict[str, str]] = []
    column_tags: dict[str, list[dict[str, str]]] = {}

    try:
        table_rows = spark.table(_TABLE_TAGS_TABLE).filter(name_filter).select("tag_name", "tag_value").collect()
        for row in table_rows:
            table_tags.append({"key": row["tag_name"], "value": row["tag_value"] or ""})
    except Exception as exc:  # broad catch: tag reads are non-fatal
        logger.warning(
            f"collect_table_tags: table tags read failed for '{sanitize_for_logging(table)}': "
            f"{sanitize_for_logging(str(exc))}"
        )

    try:
        column_rows = (
            spark.table(_COLUMN_TAGS_TABLE).filter(name_filter).select("column_name", "tag_name", "tag_value").collect()
        )
        for row in column_rows:
            column = row["column_name"]
            column_tags.setdefault(column, []).append({"key": row["tag_name"], "value": row["tag_value"] or ""})
    except Exception as exc:  # broad catch: tag reads are non-fatal
        logger.warning(
            f"collect_table_tags: column tags read failed for '{sanitize_for_logging(table)}': "
            f"{sanitize_for_logging(str(exc))}"
        )

    return {"table_tags": table_tags, "column_tags": column_tags}


def collect_column_upstream_lineage(
    spark: SparkSession,
    source_table: str,
    seed_columns: list[str],
    *,
    config: ColumnUpstreamLineageConfig,
) -> list[dict[str, Any]]:
    """Walk per-column upstream lineage from ``source_table`` with a recursive CTE.

    Issues a single ``WITH RECURSIVE`` against *system.access.column_lineage* with a
    path-based cycle guard, per-member and tail ``LIMIT``s derived from *config.max_nodes*,
    an ``INTERVAL n DAYS`` lookback from *config.lookback_days*, and an optional
    ``e.depth < N`` predicate from *config.depth* (omitted when *None* → unbounded).

    *seed_columns* is inlined as an ``IN (...)`` filter against *target_column_name*; each
    entry is escaped via ``_sql_str_literal``, so user-supplied column names cannot inject
    SQL. Empty or whitespace-only entries are dropped; the walk short-circuits to an empty
    list when no usable seeds remain.

    Returns:
        ``[{"source_table","source_column","target_column","depth","predecessor"}, ...]`` or
        an empty list on any read failure.
    """
    distinct_seeds = sorted({c for c in seed_columns if c})
    if not distinct_seeds:
        return []

    max_depth = config.depth
    lookback_days = config.lookback_days
    max_nodes = config.max_nodes
    table_literal = _sql_str_literal(source_table)
    seed_list = ", ".join(_sql_str_literal(c) for c in distinct_seeds)
    depth_predicate = f"e.depth < {max_depth} AND " if max_depth is not None else ""

    # nosec B608: identifiers + validated Pydantic ints only; the anchor table name and each
    # seed column are wrapped by _sql_str_literal, and max_depth / lookback_days / max_nodes
    # are Pydantic-validated ints (>= 1).
    query = (
        "WITH RECURSIVE edges("
        "frontier_table, frontier_column, predecessor, source_column, target_column, "
        "depth, path, event_time) AS ( "
        "("
        f" SELECT source_table_full_name AS frontier_table, source_column_name AS frontier_column, "
        f"        {table_literal} AS predecessor, source_column_name, target_column_name, "
        f"        1 AS depth, "
        f"        array(concat_ws('.', source_table_full_name, source_column_name)) AS path, "
        f"        event_time "
        f" FROM {_COLUMN_LINEAGE_TABLE} "
        f" WHERE target_table_full_name = {table_literal} "
        f"   AND target_column_name IN ({seed_list}) "
        f"   AND source_table_full_name IS NOT NULL "
        f"   AND source_column_name IS NOT NULL "
        f"   AND event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f" LIMIT {max_nodes} "
        ") UNION ALL ("
        f" SELECT t.source_table_full_name AS frontier_table, t.source_column_name AS frontier_column, "
        f"        e.frontier_table AS predecessor, t.source_column_name, t.target_column_name, "
        f"        e.depth + 1 AS depth, "
        f"        array_append(e.path, concat_ws('.', t.source_table_full_name, t.source_column_name)) AS path, "
        f"        t.event_time "
        f" FROM edges e JOIN {_COLUMN_LINEAGE_TABLE} t "
        f"   ON t.target_table_full_name = e.frontier_table "
        f"  AND t.target_column_name = e.frontier_column "
        f" WHERE {depth_predicate}t.source_table_full_name IS NOT NULL "
        f"   AND t.source_column_name IS NOT NULL "
        f"   AND NOT array_contains(e.path, concat_ws('.', t.source_table_full_name, t.source_column_name)) "
        f"   AND t.event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f" LIMIT {max_nodes} "
        ") ) "
        "SELECT DISTINCT predecessor, frontier_table AS source_table, "
        "source_column, target_column, depth "
        f"FROM edges LIMIT {max_nodes}"
    )
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:  # broad catch: lineage reads are non-fatal
        logger.warning(
            f"collect_column_upstream_lineage: read failed for "
            f"'{sanitize_for_logging(source_table)}': {sanitize_for_logging(str(exc))}"
        )
        return []

    return [
        {
            "predecessor": row["predecessor"],
            "source_table": row["source_table"],
            "source_column": row["source_column"],
            "target_column": row["target_column"],
            "depth": int(row["depth"]),
        }
        for row in rows
    ]


def collect_upstream_table_lineage(
    spark: SparkSession,
    source_table: str,
    *,
    config: ColumnUpstreamLineageConfig,
) -> list[dict[str, Any]]:
    """Walk upstream table lineage from ``source_table`` with a recursive CTE.

    Issues a single ``WITH RECURSIVE`` against *system.access.table_lineage* (upstream
    direction only) with the same guardrails as *collect_column_upstream_lineage*. Used to
    resolve the set of upstream tables for metadata enrichment.

    Returns:
        ``[{"predecessor","target_table","depth"}, ...]`` where *target_table* is the upstream
        neighbour and *predecessor* is the previous hop (equal to *source_table* at depth 1).
    """
    max_depth = config.depth
    lookback_days = int(config.lookback_days)
    max_nodes = int(config.max_nodes)
    table_literal = _sql_str_literal(source_table)
    depth_predicate = f"e.depth < {max_depth} AND " if max_depth is not None else ""

    # nosec B608: identifiers + validated Pydantic ints only (see
    # collect_column_upstream_lineage for the full rationale).
    query = (
        "WITH RECURSIVE edges(anchor, neighbour, depth, path, event_time) AS ( "
        "("
        f" SELECT {table_literal} AS anchor, source_table_full_name AS neighbour, "
        f"        1 AS depth, "
        f"        array({table_literal}, source_table_full_name) AS path, event_time "
        f" FROM {_TABLE_LINEAGE_TABLE} "
        f" WHERE target_table_full_name = {table_literal} "
        f"   AND source_table_full_name IS NOT NULL "
        f"   AND source_table_full_name <> {table_literal} "
        f"   AND event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f" LIMIT {max_nodes} "
        ") UNION ALL ("
        f" SELECT e.neighbour AS anchor, t.source_table_full_name AS neighbour, "
        f"        e.depth + 1 AS depth, "
        f"        array_append(e.path, t.source_table_full_name) AS path, t.event_time "
        f" FROM edges e JOIN {_TABLE_LINEAGE_TABLE} t "
        f"   ON t.target_table_full_name = e.neighbour "
        f" WHERE {depth_predicate}t.source_table_full_name IS NOT NULL "
        f"   AND NOT array_contains(e.path, t.source_table_full_name) "
        f"   AND t.event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS "
        f" LIMIT {max_nodes} "
        ") ) "
        "SELECT DISTINCT anchor AS predecessor, neighbour AS target_table, depth "
        f"FROM edges LIMIT {max_nodes}"
    )
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:  # broad catch: lineage reads are non-fatal
        logger.warning(
            f"collect_upstream_table_lineage: read failed for "
            f"'{sanitize_for_logging(source_table)}': {sanitize_for_logging(str(exc))}"
        )
        return []
    return [
        {
            "predecessor": row["predecessor"],
            "target_table": row["target_table"],
            "depth": int(row["depth"]),
        }
        for row in rows
    ]


def _external_lineage_record(info: ExternalLineageInfo) -> dict[str, Any] | None:
    """Turn an SDK *ExternalLineageInfo* into a flat record for the LLM prompt.

    Variants are inspected in order (external_metadata_info → table_info → file_info →
    model_info); the always-present *external_lineage_info* carries the relationship's
    column mappings and properties. Returns *None* when no variant payload is populated so
    the caller can skip the entry.
    """
    columns, properties = _relationship_columns_and_properties(info.external_lineage_info)

    if info.external_metadata_info is not None:
        metadata = info.external_metadata_info
        return {
            "source_kind": "external_metadata",
            "source_name": metadata.name or "",
            "system_type": metadata.system_type.value if metadata.system_type is not None else None,
            "entity_type": _sanitize_text(metadata.entity_type),
            "columns": columns,
            "properties": properties,
        }

    if info.table_info is not None:
        table = info.table_info
        full_name = ".".join(part for part in (table.catalog_name, table.schema_name, table.name) if part)
        return {
            "source_kind": "table",
            "source_name": full_name,
            "system_type": None,
            "entity_type": "TABLE",
            "columns": columns,
            "properties": properties,
        }

    if info.file_info is not None:
        file_info = info.file_info
        return {
            "source_kind": "file",
            "source_name": file_info.path or file_info.securable_name or "",
            "system_type": None,
            "entity_type": _sanitize_text(file_info.securable_type),
            "columns": columns,
            "properties": properties,
        }

    if info.model_info is not None:
        model_info = info.model_info
        source_name = model_info.model_name or ""
        if model_info.version is not None:
            source_name = f"{source_name}@{model_info.version}" if source_name else f"@{model_info.version}"
        return {
            "source_kind": "model",
            "source_name": source_name,
            "system_type": None,
            "entity_type": "MODEL",
            "columns": columns,
            "properties": properties,
        }

    return None


def _relationship_columns_and_properties(
    rel: Any,
) -> tuple[list[dict[str, str]], dict[str, str] | None]:
    """Extract column mappings and properties from an *ExternalLineageRelationshipInfo*."""
    columns: list[dict[str, str]] = []
    properties: dict[str, str] | None = None
    if rel is None:
        return columns, properties
    for pair in rel.columns or []:
        src = _sanitize_text(pair.source)
        tgt = _sanitize_text(pair.target)
        if src and tgt:
            columns.append({"source": src, "target": tgt})
    if rel.properties:
        properties = {str(k): str(v) for k, v in rel.properties.items()}
    return columns, properties


def collect_external_upstream_lineage(
    ws: WorkspaceClient,
    source_table: str,
    *,
    config: ExternalLineageConfig,
) -> list[dict[str, Any]]:
    """Enumerate upstream external relationships for ``source_table``.

    Drives ``ws.external_lineage.list_external_lineage_relationships(..., LineageDirection
    .UPSTREAM, page_size=min(1000, max_relationships))`` and stops at *config.max_relationships*
    items so unbounded listings cannot blow up the LLM prompt. Any SDK error (API not
    enabled, missing permission, pagination failure) returns an empty list and logs a
    sanitised warning.
    """
    max_relationships = int(config.max_relationships)
    page_size = min(1000, max_relationships)
    try:
        catalog, schema, table_name = _split_three_part_name(source_table)
    except InvalidParameterError as exc:
        logger.warning(f"collect_external_upstream_lineage: {sanitize_for_logging(str(exc))}")
        return []

    object_info = ExternalLineageObject(table=ExternalLineageTable(name=f"{catalog}.{schema}.{table_name}"))

    results: list[dict[str, Any]] = []
    try:
        for info in ws.external_lineage.list_external_lineage_relationships(
            object_info=object_info,
            lineage_direction=LineageDirection.UPSTREAM,
            page_size=page_size,
        ):
            if len(results) >= max_relationships:
                break
            record = _external_lineage_record(info)
            if record is not None:
                results.append(record)
    except Exception as exc:  # broad catch: SDK listing failures are non-fatal
        logger.warning(
            f"collect_external_upstream_lineage: SDK listing failed for "
            f"'{sanitize_for_logging(source_table)}': {sanitize_for_logging(str(exc))}"
        )
        return []
    return results


def _merge_tags(column_entries: list[dict[str, Any]], column_tags: dict[str, list[dict[str, str]]]) -> None:
    """Attach each column's tag list in-place; omit the key when the column has no tags."""
    for entry in column_entries:
        tags = column_tags.get(entry["name"])
        if tags:
            entry["tags"] = tags


def build_schema_json(
    *,
    table_full_name: str,
    column_dicts: list[dict[str, Any]],
    ws: WorkspaceClient,
    spark: SparkSession | None,
    config: UnityCatalogMetadataConfig,
) -> str:
    """Render the enriched schema JSON for the LLM prompt.

    Called by both *get_table_column_metadata* and *get_column_metadata*: the two share this
    single rendering path so the LLM prompt is identical regardless of whether the schema
    was discovered via the UC SDK or via Spark. Each sub-feature is skipped (not emitted as
    an empty key) when it is disabled on *config* or when its dependency (ws / spark) is
    missing.

    *column_dicts* is the baseline list of ``{"name","type"}`` dicts the caller already
    computed from either SDK or Spark; this helper enriches it in place with ``comment`` and
    ``tags`` per column.
    """
    base_columns = [dict(c) for c in column_dicts]
    result: dict[str, Any] = {"table": table_full_name, "columns": base_columns}

    _enrich_with_comments(result, base_columns, ws=ws, table_full_name=table_full_name, config=config)
    _enrich_with_tags(result, base_columns, spark=spark, table_full_name=table_full_name, config=config)
    _enrich_with_lineage(result, base_columns, ws=ws, spark=spark, table_full_name=table_full_name, config=config)
    _enrich_with_external_lineage(result, ws=ws, table_full_name=table_full_name, config=config)

    return json.dumps(result)


def _enrich_with_comments(
    result: dict[str, Any],
    base_columns: list[dict[str, Any]],
    *,
    ws: WorkspaceClient,
    table_full_name: str,
    config: UnityCatalogMetadataConfig,
) -> None:
    """Attach table + column comments to ``result`` / ``base_columns`` in place."""
    if not (config.include_table_comment or config.include_column_comments):
        return
    table_comment, column_comments = collect_table_comments(ws, table_full_name)
    if config.include_table_comment and table_comment is not None:
        result["table_comment"] = table_comment
    if config.include_column_comments:
        for entry in base_columns:
            comment = column_comments.get(entry["name"])
            if comment:
                entry["comment"] = comment


def _enrich_with_tags(
    result: dict[str, Any],
    base_columns: list[dict[str, Any]],
    *,
    spark: SparkSession | None,
    table_full_name: str,
    config: UnityCatalogMetadataConfig,
) -> None:
    """Attach table + column tags to ``result`` / ``base_columns`` in place."""
    if not config.include_tags:
        return
    if spark is None:
        logger.warning("build_schema_json: SparkSession not provided; skipping tags enrichment")
        return
    tags = collect_table_tags(spark, table_full_name)
    if tags["table_tags"]:
        result["table_tags"] = tags["table_tags"]
    _merge_tags(base_columns, tags["column_tags"])


def _enrich_with_lineage(
    result: dict[str, Any],
    base_columns: list[dict[str, Any]],
    *,
    ws: WorkspaceClient | None,
    spark: SparkSession | None,
    table_full_name: str,
    config: UnityCatalogMetadataConfig,
) -> None:
    """Attach column + upstream-table lineage edges to ``result`` in place."""
    if config.column_upstream_lineage is None:
        return
    if spark is None:
        logger.warning("build_schema_json: SparkSession not provided; skipping column upstream lineage enrichment")
        return
    seed = [c["name"] for c in base_columns]
    column_edges = collect_column_upstream_lineage(spark, table_full_name, seed, config=config.column_upstream_lineage)
    if column_edges:
        result["column_upstream_lineage"] = column_edges
    table_edges = collect_upstream_table_lineage(spark, table_full_name, config=config.column_upstream_lineage)
    if not table_edges:
        return
    upstream_tables = [
        _collect_upstream_table_entry(
            ws=ws,
            spark=spark,
            table_full_name=edge["target_table"],
            depth=edge["depth"],
            config=config,
        )
        for edge in table_edges
    ]
    if upstream_tables:
        result["upstream_tables"] = upstream_tables


def _enrich_with_external_lineage(
    result: dict[str, Any],
    *,
    ws: WorkspaceClient | None,
    table_full_name: str,
    config: UnityCatalogMetadataConfig,
) -> None:
    """Attach upstream external-lineage edges to ``result`` in place."""
    if config.external_lineage is None:
        return
    if ws is None:
        logger.warning("build_schema_json: WorkspaceClient not provided; skipping external lineage enrichment")
        return
    external = collect_external_upstream_lineage(ws, table_full_name, config=config.external_lineage)
    if external:
        result["external_lineage"] = external


def _collect_upstream_table_entry(
    *,
    ws: WorkspaceClient | None,
    spark: SparkSession | None,
    table_full_name: str,
    depth: int,
    config: UnityCatalogMetadataConfig,
) -> dict[str, Any]:
    """Build an upstream-table enrichment record (comments + tags + columns).

    Mirrors the per-source-table enrichment but keyed to an upstream neighbour discovered by
    ``collect_upstream_table_lineage``. Missing-dependency branches degrade silently so the
    overall walk still contributes its structural shape to the prompt.
    """
    entry: dict[str, Any] = {"name": table_full_name, "depth": depth}
    _attach_upstream_comments(entry, ws=ws, table_full_name=table_full_name, config=config)
    _attach_upstream_tags(entry, spark=spark, table_full_name=table_full_name, config=config)
    return entry


def _attach_upstream_comments(
    entry: dict[str, Any],
    *,
    ws: WorkspaceClient | None,
    table_full_name: str,
    config: UnityCatalogMetadataConfig,
) -> None:
    if ws is None or not (config.include_table_comment or config.include_column_comments):
        return
    table_comment, column_comments = collect_table_comments(ws, table_full_name)
    if config.include_table_comment and table_comment is not None:
        entry["comment"] = table_comment
    if config.include_column_comments and column_comments:
        entry["columns"] = [{"name": name, "comment": value} for name, value in column_comments.items()]


def _attach_upstream_tags(
    entry: dict[str, Any],
    *,
    spark: SparkSession | None,
    table_full_name: str,
    config: UnityCatalogMetadataConfig,
) -> None:
    if spark is None or not config.include_tags:
        return
    tags = collect_table_tags(spark, table_full_name)
    if tags["table_tags"]:
        entry["tags"] = tags["table_tags"]
    if not tags["column_tags"]:
        return
    cols = entry.setdefault("columns", [])
    existing = {c["name"]: c for c in cols}
    for column_name, column_tag_list in tags["column_tags"].items():
        if column_name in existing:
            existing[column_name]["tags"] = column_tag_list
        else:
            cols.append({"name": column_name, "tags": column_tag_list})
