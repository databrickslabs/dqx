import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.dependencies import get_discovery_service
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
    TableSchemaDdlOut,
    TableTagsOut,
)
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService

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
