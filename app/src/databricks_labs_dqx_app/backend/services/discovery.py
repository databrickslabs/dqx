import asyncio
import logging
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from ..cache import app_cache

logger = logging.getLogger(__name__)

_CATALOG_TTL = 300  # 5 min  — catalog list changes rarely
_SCHEMA_TTL = 300  # 5 min
_TABLE_TTL = 180  # 3 min  — tables added more frequently
_COLUMN_TTL = 600  # 10 min — schema changes rarely
_TAGS_TTL = 300  # 5 min — tags change infrequently


@dataclass
class TableColumn:
    name: str
    type_name: str
    comment: str | None
    nullable: bool
    position: int


@dataclass
class TableTags:
    table_fqn: str
    table_tags: list[str]
    column_tags: dict[str, list[str]]


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

    def get_table_owner(self, table_fqn: str) -> str | None:
        """Return the Unity Catalog owner of *table_fqn*, or ``None``.

        Runs on-behalf-of the calling user, so it only succeeds when the user
        can read the table's metadata. The owner is UC's raw principal display
        value — it may be a user, a group, or a service principal; callers
        store it verbatim and must not assume it is a person. Any failure
        (missing table, permission denied, transient error) is swallowed and
        reported as ``None`` so callers can fall back gracefully.
        """
        try:
            table_info = self._ws.tables.get(full_name=table_fqn)
        except Exception as e:
            logger.warning("Failed to resolve UC owner for %s: %s", table_fqn, e)
            return None
        owner = (table_info.owner or "").strip()
        return owner or None

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

    def get_table_schema_ddl(self, table_fqn: str) -> str:
        """Return the Spark DDL string for a UC table, ordered by column position.

        Used by the schema-validation rule builder to snapshot the current
        shape of a table so the user has something concrete to start from.
        We prefer ``type_text`` because it preserves parameterised and
        nested types (``DECIMAL(10,2)``, ``ARRAY<STRING>``, ``STRUCT<...>``)
        — the SDK ``type_name`` enum loses precision on those.
        """
        table_info = self._ws.tables.get(full_name=table_fqn)
        columns = table_info.columns or []
        ordered = sorted(columns, key=lambda c: c.position or 0)
        parts: list[str] = []
        for col in ordered:
            name = (col.name or "").strip()
            if not name:
                continue
            type_text = (getattr(col, "type_text", None) or "").strip()
            if not type_text and col.type_name:
                # Fallback for older SDKs that don't surface ``type_text``.
                type_text = col.type_name.value
            if not type_text:
                continue
            parts.append(f"{_quote_ddl_identifier(name)} {type_text}")
        return ", ".join(parts)

    def get_table_tags(self, catalog: str, schema: str, table: str) -> TableTags:
        """Get tags for a table and its columns from Unity Catalog."""
        full_name = f"{catalog}.{schema}.{table}"
        table_tags: list[str] = []
        column_tags: dict[str, list[str]] = {}

        try:
            # Get table info which includes column metadata
            table_info = self._ws.tables.get(full_name=full_name)

            # Extract table-level tags if available (SDK versions may lack the attribute)
            tbl_tags = getattr(table_info, "tags", None)
            if tbl_tags:
                for tag in tbl_tags:
                    if tag.key:
                        tag_str = f"{tag.key}={tag.value}" if tag.value else tag.key
                        table_tags.append(tag_str)

            # Extract column-level tags if available
            if table_info.columns:
                for col in table_info.columns:
                    col_name = col.name or ""
                    col_tags_attr = getattr(col, "tags", None)
                    if col_tags_attr:
                        col_tags = []
                        for tag in col_tags_attr:
                            if tag.key:
                                tag_str = f"{tag.key}={tag.value}" if tag.value else tag.key
                                col_tags.append(tag_str)
                        if col_tags:
                            column_tags[col_name] = col_tags

        except Exception as e:
            logger.warning(f"Failed to get tags for {full_name}: {e}")

        return TableTags(
            table_fqn=full_name,
            table_tags=table_tags,
            column_tags=column_tags,
        )

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

    @app_cache.cached("discovery:{_user}:tags:{catalog}:{schema}:{table}", ttl=_TAGS_TTL)
    async def get_table_tags_async(self, catalog: str, schema: str, table: str) -> TableTags:
        return await asyncio.to_thread(self.get_table_tags, catalog, schema, table)

    @app_cache.cached("discovery:{_user}:schema_ddl:{table_fqn}", ttl=_COLUMN_TTL)
    async def get_table_schema_ddl_async(self, table_fqn: str) -> str:
        return await asyncio.to_thread(self.get_table_schema_ddl, table_fqn)


# Identifiers with characters outside ``[A-Za-z0-9_]`` must be back-tick
# quoted in DDL, otherwise ``has_valid_schema`` will fail to parse the
# expected schema. Embedded back-ticks are doubled per Spark's grammar.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ddl_identifier(name: str) -> str:
    if _SAFE_IDENTIFIER.match(name):
        return name
    return "`" + name.replace("`", "``") + "`"
