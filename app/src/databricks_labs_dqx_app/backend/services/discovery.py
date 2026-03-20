import asyncio
import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from ..cache import app_cache

logger = logging.getLogger(__name__)

_CATALOG_TTL = 300  # 5 min  — catalog list changes rarely
_SCHEMA_TTL = 300  # 5 min
_TABLE_TTL = 180  # 3 min  — tables added more frequently
_COLUMN_TTL = 600  # 10 min — schema changes rarely


@dataclass
class TableColumn:
    name: str
    type_name: str
    comment: str | None
    nullable: bool
    position: int


class DiscoveryService:
    """OBO-scoped Unity Catalog browsing with per-user response caching."""

    def __init__(self, ws: WorkspaceClient, user_id: str) -> None:
        self._ws = ws
        self.user_id = user_id  # exposed for the {_user} cache key expansion

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

    # ── async wrappers (cached per user) ───────────────────────

    @app_cache.cached("discovery:{_user}:catalogs", ttl=_CATALOG_TTL)
    async def list_catalogs_async(self) -> list[CatalogInfo]:
        return await asyncio.to_thread(self.list_catalogs)

    @app_cache.cached("discovery:{_user}:schemas:{catalog}", ttl=_SCHEMA_TTL)
    async def list_schemas_async(self, catalog: str) -> list[SchemaInfo]:
        return await asyncio.to_thread(self.list_schemas, catalog)

    @app_cache.cached("discovery:{_user}:tables:{catalog}:{schema}", ttl=_TABLE_TTL)
    async def list_tables_async(self, catalog: str, schema: str) -> list[TableInfo]:
        return await asyncio.to_thread(self.list_tables, catalog, schema)

    @app_cache.cached("discovery:{_user}:columns:{catalog}:{schema}:{table}", ttl=_COLUMN_TTL)
    async def get_table_columns_async(self, catalog: str, schema: str, table: str) -> list[TableColumn]:
        return await asyncio.to_thread(self.get_table_columns, catalog, schema, table)
