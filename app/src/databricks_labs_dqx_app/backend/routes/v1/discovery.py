import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.dependencies import get_discovery_service
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    CatalogOut,
    ColumnOut,
    FilterTablesByColumnsIn,
    FilterTablesByColumnsOut,
    SchemaOut,
    TableOut,
    TableTagsOut,
)
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService

router = APIRouter()


@router.get("/catalogs", response_model=list[CatalogOut], operation_id="list_catalogs")
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
    operation_id="list_schemas",
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
    operation_id="list_tables",
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
    operation_id="list_all_table_fqns",
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
    operation_id="get_table_columns",
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
    operation_id="get_table_tags",
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


@router.post(
    "/filter-tables-by-columns",
    response_model=FilterTablesByColumnsOut,
    operation_id="filter_tables_by_columns",
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
