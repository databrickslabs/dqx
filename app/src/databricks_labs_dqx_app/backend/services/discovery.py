import asyncio
import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

logger = logging.getLogger(__name__)


@dataclass
class TableColumn:
    name: str
    type_name: str
    comment: str | None
    nullable: bool
    position: int


class DiscoveryService:
    """OBO-scoped Unity Catalog browsing."""

    def __init__(self, ws: WorkspaceClient) -> None:
        self._ws = ws

    # ── synchronous ────────────────────────────────────────────

    def list_catalogs(self) -> list[CatalogInfo]:
        return list(self._ws.catalogs.list())

    def list_schemas(self, catalog: str) -> list[SchemaInfo]:
        return list(self._ws.schemas.list(catalog_name=catalog))

    def list_tables(self, catalog: str, schema: str) -> list[TableInfo]:
        return list(self._ws.tables.list(catalog_name=catalog, schema_name=schema))

    def get_table_columns(self, catalog: str, schema: str, table: str) -> list[TableColumn]:
        full_name = f"{catalog}.{schema}.{table}"
        table_info = self._ws.tables.get(full_name=full_name)
        if not table_info.columns:
            return []
        return [
            TableColumn(
                name=col.name or "",
                type_name=col.type_name.value if col.type_name else "",
                comment=col.comment,
                nullable=bool(col.nullable),
                position=col.position or 0,
            )
            for col in table_info.columns
        ]

    # ── async wrappers ─────────────────────────────────────────

    async def list_catalogs_async(self) -> list[CatalogInfo]:
        return await asyncio.to_thread(self.list_catalogs)

    async def list_schemas_async(self, catalog: str) -> list[SchemaInfo]:
        return await asyncio.to_thread(self.list_schemas, catalog)

    async def list_tables_async(self, catalog: str, schema: str) -> list[TableInfo]:
        return await asyncio.to_thread(self.list_tables, catalog, schema)

    async def get_table_columns_async(self, catalog: str, schema: str, table: str) -> list[TableColumn]:
        return await asyncio.to_thread(self.get_table_columns, catalog, schema, table)
