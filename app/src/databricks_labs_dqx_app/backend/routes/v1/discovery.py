import asyncio
import re
from typing import Annotated

from databricks.labs.dqx.utils import is_sql_query_safe
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.dependencies import get_discovery_service, get_obo_sql_executor, get_obo_ws
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    CatalogOut,
    ColumnOut,
    FilterTablesByColumnsIn,
    FilterTablesByColumnsOut,
    GovernedTagOut,
    GovernedTagsOut,
    SchemaOut,
    TableOut,
    TablePreviewOut,
    TableSchemaDdlOut,
    TableTagsOut,
)
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

# A generated filter query must be a single read-only SELECT/WITH statement — mirrors the
# rail table_data_service.py applies to its AI-generated queries (AGENTS.md LLM-output rule).
_READ_ONLY_PREFIX_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_NL_FILTER_MODEL = "databricks-claude-sonnet-4-5"

# No router-level role guard: OBO auth via get_discovery_service rejects
# unauthenticated callers, and Unity Catalog OBO permissions enforce what each
# user can see at the data layer — that's the real authorization boundary.
router = APIRouter()


@router.get("/catalogs", response_model=list[CatalogOut], operation_id="listCatalogs")
async def list_catalogs(
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> list[CatalogOut]:
    try:
        catalogs = await discovery.list_catalogs_async()
        return [CatalogOut(name=c.name or "", comment=c.comment) for c in catalogs]
    except Exception as e:
        logger.error(f"Failed to list catalogs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list catalogs: {e}")


@router.get(
    "/catalogs/{catalog}/schemas",
    response_model=list[SchemaOut],
    operation_id="listSchemas",
)
async def list_schemas(
    catalog: str,
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> list[SchemaOut]:
    try:
        schemas = await discovery.list_schemas_async(catalog)
        return [
            SchemaOut(name=s.name or "", catalog_name=s.catalog_name or catalog, comment=s.comment) for s in schemas
        ]
    except Exception as e:
        logger.error(f"Failed to list schemas in {catalog}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {e}")


@router.get(
    "/catalogs/{catalog}/schemas/{schema}/tables",
    response_model=list[TableOut],
    operation_id="listTables",
)
async def list_tables(
    catalog: str,
    schema: str,
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> list[TableOut]:
    try:
        tables = await discovery.list_tables_async(catalog, schema)
        return [
            TableOut(
                name=t.name or "",
                catalog_name=t.catalog_name or catalog,
                schema_name=t.schema_name or schema,
                table_type=t.table_type.value if t.table_type else None,
                comment=t.comment,
            )
            for t in tables
        ]
    except Exception as e:
        logger.error(f"Failed to list tables in {catalog}.{schema}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {e}")


@router.get(
    "/catalogs/{catalog}/schemas/{schema}/all-table-fqns",
    response_model=list[str],
    operation_id="listAllTableFqns",
)
async def list_all_table_fqns(
    catalog: str,
    schema: str,
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> list[str]:
    """Return fully qualified names for all tables in a schema (for batch profiling)."""
    try:
        tables = await discovery.list_tables_async(catalog, schema)
        return [f"{catalog}.{schema}.{t.name}" for t in tables if t.name]
    except Exception as e:
        logger.error(f"Failed to list table FQNs in {catalog}.{schema}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list table FQNs: {e}")


@router.get(
    "/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns",
    response_model=list[ColumnOut],
    operation_id="getTableColumns",
)
async def get_table_columns(
    catalog: str,
    schema: str,
    table: str,
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> list[ColumnOut]:
    try:
        columns = await discovery.get_table_columns_async(catalog, schema, table)
        return [
            ColumnOut(
                name=col.name,
                type_name=col.type_name,
                comment=col.comment,
                nullable=col.nullable,
                position=col.position,
            )
            for col in columns
        ]
    except Exception as e:
        logger.error(f"Failed to get columns for {catalog}.{schema}.{table}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get table columns: {e}")


@router.get(
    "/catalogs/{catalog}/schemas/{schema}/tables/{table}/tags",
    response_model=TableTagsOut,
    operation_id="getTableTags",
)
async def get_table_tags(
    catalog: str,
    schema: str,
    table: str,
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> TableTagsOut:
    """Get Unity Catalog tags for a table and its columns."""
    try:
        tags = await discovery.get_table_tags_async(catalog, schema, table)
        return TableTagsOut(
            table_fqn=tags.table_fqn,
            table_tags=tags.table_tags,
            column_tags=tags.column_tags,
        )
    except Exception as e:
        logger.error(f"Failed to get tags for {catalog}.{schema}.{table}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get table tags: {e}")


def _generate_filter_sql(ws: WorkspaceClient, nl_query: str, quoted_fqn: str, limit: int, columns: list[str]) -> str:
    """Call a Databricks Foundation Model to convert a natural-language filter into SQL.

    Returns a complete SELECT statement. Falls back to a plain SELECT on any error, and the
    generated statement is re-validated by the caller before it is ever executed — this
    function's output is untrusted LLM output, not a query to run as-is (AGENTS.md).
    """
    col_list = ", ".join(columns) if columns else "(columns unknown)"
    prompt = (
        f"You are a SQL expert. Generate a valid Spark SQL SELECT statement for the table {quoted_fqn}.\n\n"
        f"The table has EXACTLY these columns (use no others): {col_list}\n\n"
        f"User request: {nl_query}\n\n"
        "Rules:\n"
        "1. Return ONLY the raw SQL — no markdown, no code fences.\n"
        f"2. Always include LIMIT {limit}.\n"
        "3. ONLY use column names from the list above — never infer or guess column names.\n"
        "4. Return a single read-only SELECT (or WITH ... SELECT) statement — never DDL/DML.\n"
        f"5. If the request is unclear default to: SELECT * FROM {quoted_fqn} LIMIT {limit}"
    )
    payload = {
        "messages": [
            {"role": "system", "content": "You are a SQL generator for Databricks Spark SQL."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 512,
        "temperature": 0,
    }
    fallback = f"SELECT * FROM {quoted_fqn} LIMIT {limit}"
    try:
        resp = ws.api_client.do("POST", f"/serving-endpoints/{_NL_FILTER_MODEL}/invocations", body=payload)
        sql = resp["choices"][0]["message"]["content"].strip()  # type: ignore[index]
        sql = re.sub(r"```[a-z]*|```", "", sql).strip().rstrip(";").strip()
        return sql or fallback
    except Exception as exc:
        logger.warning("NL filter SQL generation failed, falling back to plain SELECT: %s", exc)
        return fallback


def _validate_generated_filter_sql(sql: str, fallback: str) -> str:
    """Validate LLM-generated filter SQL before execution, or fall back to a plain SELECT.

    Security rail (AGENTS.md): LLM-generated SQL must be a single read-only SELECT/WITH
    statement and must pass ``is_sql_query_safe`` before it is ever run against a warehouse.
    Rather than surfacing an error to the user for a merely-decorative filter box, an unsafe
    or malformed statement silently degrades to the unfiltered preview.
    """
    if ";" in sql or not _READ_ONLY_PREFIX_RE.match(sql) or not is_sql_query_safe(sql):
        logger.warning("Generated filter SQL failed validation, falling back to plain SELECT")
        return fallback
    return sql


@router.get(
    "/catalogs/{catalog}/schemas/{schema}/tables/{table}/preview",
    response_model=TablePreviewOut,
    operation_id="get_table_preview",
)
async def get_table_preview(
    catalog: str,
    schema: str,
    table: str,
    obo_sql: Annotated[SqlExecutor, Depends(get_obo_sql_executor)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    limit: int = 10,
    filter_query: str | None = Query(default=None, description="Natural-language filter applied via AI-generated SQL"),
) -> TablePreviewOut:
    """Return up to *limit* sample rows from the table for UI preview.

    When *filter_query* is provided the AI converts it to a SQL WHERE clause first; the
    generated statement is validated (single read-only SELECT, ``is_sql_query_safe``) before
    it is run, falling back to a plain unfiltered preview if it fails that check.
    Runs as the calling user (OBO) so Unity Catalog row filters and column masks apply.
    """
    limit = max(1, min(limit, 10_000))
    fqn = f"{catalog}.{schema}.{table}"
    try:
        validate_fqn(fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    quoted = quote_fqn(fqn)
    fallback_sql = f"SELECT * FROM {quoted} LIMIT {limit}"
    try:
        if filter_query and filter_query.strip():
            try:
                desc_rows = await asyncio.to_thread(obo_sql.query_dicts, f"DESCRIBE TABLE {quoted}")
                # DESCRIBE TABLE returns col_name / data_type / comment; skip partition headers (start with #)
                columns = [
                    r["col_name"] for r in desc_rows
                    if r.get("col_name") and not str(r["col_name"]).startswith("#")
                ]
            except Exception:
                columns = []
            generated = await asyncio.to_thread(
                _generate_filter_sql, obo_ws, filter_query.strip(), quoted, limit, columns
            )
            sql = _validate_generated_filter_sql(generated, fallback_sql)
        else:
            sql = fallback_sql
        rows = await asyncio.to_thread(obo_sql.query_dicts, sql)
        cols = list(rows[0].keys()) if rows else []
        return TablePreviewOut(columns=cols, rows=rows, row_count=len(rows))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Preview failed for %s: %s", fqn, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load preview: {e}")


@router.get(
    "/schema-ddl",
    response_model=TableSchemaDdlOut,
    operation_id="getTableSchemaDdl",
)
async def get_table_schema_ddl(
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
    table_fqn: Annotated[str, Query(description="Fully qualified table name (catalog.schema.table)")],
) -> TableSchemaDdlOut:
    """Snapshot the current schema of a UC table as a Spark DDL string.

    Powers the "snapshot from table" affordance in the schema-validation
    rule builder.  Returned shape is the canonical
    ``has_valid_schema`` ``expected_schema`` argument.
    """
    fqn = (table_fqn or "").strip()
    if fqn.count(".") != 2 or not all(p.strip() for p in fqn.split(".")):
        raise HTTPException(
            status_code=400,
            detail="table_fqn must be a three-part name (catalog.schema.table)",
        )
    try:
        ddl = await discovery.get_table_schema_ddl_async(fqn)
    except Exception as e:
        logger.error("Failed to snapshot schema DDL for %s: %s", fqn, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to snapshot schema: {e}")
    column_count = sum(1 for piece in ddl.split(",") if piece.strip())
    return TableSchemaDdlOut(table_fqn=fqn, ddl=ddl, column_count=column_count)


@router.post(
    "/filter-tables-by-columns",
    response_model=FilterTablesByColumnsOut,
    operation_id="filterTablesByColumns",
)
async def filter_tables_by_columns(
    body: FilterTablesByColumnsIn,
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> FilterTablesByColumnsOut:
    """Return only the tables that contain ALL of the required columns."""
    if not body.required_columns:
        return FilterTablesByColumnsOut(matching=body.table_fqns, not_matching=[], errors=[])

    required = {c.lower() for c in body.required_columns}
    matching: list[str] = []
    not_matching: list[str] = []
    errors: list[dict[str, str]] = []

    async def check_one(fqn: str):
        parts = fqn.split(".")
        if len(parts) != 3:
            errors.append({"table_fqn": fqn, "error": "Invalid FQN format"})
            return
        catalog, schema, table = parts
        try:
            columns = await discovery.get_table_columns_async(catalog, schema, table)
            col_names = {col.name.lower() for col in columns}
            if required.issubset(col_names):
                matching.append(fqn)
            else:
                not_matching.append(fqn)
        except Exception as e:
            logger.error(f"Failed to check columns for {fqn}: {e}", exc_info=True)
            errors.append({"table_fqn": fqn, "error": str(e)})

    await asyncio.gather(*(check_one(fqn) for fqn in body.table_fqns))
    return FilterTablesByColumnsOut(matching=matching, not_matching=not_matching, errors=errors)


@router.get("/governed-tags", response_model=GovernedTagsOut, operation_id="listGovernedTags")
async def list_governed_tags(
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> GovernedTagsOut:
    """List distinct governed Unity Catalog tag keys/values visible to the caller."""
    try:
        result = await discovery.list_governed_tags_async()
        return GovernedTagsOut(tags=[GovernedTagOut(tag=g.tag, description=g.description) for g in result])
    except Exception as e:
        logger.error(f"Failed to list governed tags: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list governed tags: {e}")
