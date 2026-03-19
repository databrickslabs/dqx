from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.dependencies import get_discovery_service
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import CatalogOut, ColumnOut, SchemaOut, TableOut
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
