"""Bulk, user-scoped ``information_schema`` snapshot reader for rule policies.

Reads tables, columns, tags and owners per catalog with a handful of set-based
queries (never one query per table) and returns :class:`TableSnapshot` objects
for the pure policy engine. Run it with an OBO executor so the result reflects
only what the calling user can see.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from databricks.labs.dqx.utils import is_sql_query_safe
from databricks.sdk.errors.base import DatabricksError

from databricks_labs_dqx_app.backend.policy_models import ALL, PolicyScope
from databricks_labs_dqx_app.backend.services.policy_engine import ColumnSnapshot, TableSnapshot, in_scope
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, quote_ident, validate_fqn, validate_identifier

logger = logging.getLogger(__name__)

MAX_SNAPSHOT_TABLES = 5000
_READ_ERRORS = (ValueError, RuntimeError, DatabricksError)  # RuntimeError covers SqlStatementError
_TABLE_TYPES = "('MANAGED','EXTERNAL','VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')"


@dataclass(frozen=True)
class SnapshotResult:
    tables: list[TableSnapshot]
    truncated: bool
    unreadable_catalogs: list[str]


@dataclass(frozen=True)
class _CatalogFilter:
    """Lower-cased restriction within one catalog; ``None`` means unrestricted."""

    schemas: frozenset[str] | None = None
    pairs: frozenset[tuple[str, str]] | None = None


def _literal(value: str) -> str:
    return f"'{escape_sql_string(value)}'"


def _schema_in(column: str, schemas: frozenset[str]) -> str:
    return f"lower({column}) IN ({', '.join(_literal(s) for s in sorted(schemas))})"


def _where(flt: _CatalogFilter, schema_col: str, table_col: str | None) -> str:
    """Build a ``WHERE`` clause excluding ``information_schema`` and applying *flt*."""
    clauses = [f"{schema_col} <> 'information_schema'"]
    if flt.schemas is not None:
        clauses.append(_schema_in(schema_col, flt.schemas))
    if flt.pairs is not None:
        if table_col is None:
            clauses.append(_schema_in(schema_col, frozenset(schema for schema, _ in flt.pairs)))
        else:
            ors = " OR ".join(
                f"(lower({schema_col}) = {_literal(schema)} AND lower({table_col}) = {_literal(table)})"
                for schema, table in sorted(flt.pairs)
            )
            clauses.append(f"({ors})")
    return " AND ".join(clauses)


def _tag(name: str | None, value: str | None) -> str | None:
    if not name:
        return None
    return f"{name}={value}" if value else name


def _tags(rows: Sequence[Sequence[str | None]], name_idx: int) -> tuple[str, ...]:
    out = (_tag(r[name_idx], r[name_idx + 1]) for r in rows if len(r) > name_idx + 1)
    return tuple(t for t in out if t)


class PolicySnapshotReader:
    """Builds :class:`TableSnapshot` lists for a policy scope or an explicit table list."""

    def __init__(self, sql: SqlExecutor, list_catalogs: Callable[[], list[str]]) -> None:
        self._sql = sql
        self._list_catalogs = list_catalogs

    def for_scope(self, scope: PolicyScope, *, limit: int = MAX_SNAPSHOT_TABLES) -> SnapshotResult:
        catalogs = self._list_catalogs() if scope.catalogs == ALL else list(scope.catalogs)
        plan: list[tuple[str, _CatalogFilter]] = []
        seen: set[str] = set()
        for catalog in catalogs:
            if catalog.lower() in seen:
                continue
            seen.add(catalog.lower())
            flt = self._scope_filter(scope, catalog)
            if flt is not None:
                plan.append((catalog, flt))
        return self._read(plan, limit, lambda fqn: in_scope(scope, fqn))

    def for_tables(self, table_fqns: list[str]) -> SnapshotResult:
        pairs_by_catalog: dict[str, set[tuple[str, str]]] = {}
        catalog_names: dict[str, str] = {}
        for fqn in table_fqns:
            try:
                validate_fqn(fqn)
            except ValueError:
                logger.warning("Skipping invalid table name in snapshot request")
                continue
            catalog, schema, table = fqn.split(".")
            key = catalog.lower()
            catalog_names.setdefault(key, catalog)
            pairs_by_catalog.setdefault(key, set()).add((schema.lower(), table.lower()))
        plan = [(catalog_names[key], _CatalogFilter(pairs=frozenset(pairs))) for key, pairs in pairs_by_catalog.items()]
        wanted = {fqn.lower() for fqn in table_fqns}
        return self._read(plan, MAX_SNAPSHOT_TABLES, lambda fqn: fqn.lower() in wanted)

    @staticmethod
    def _scope_filter(scope: PolicyScope, catalog: str) -> _CatalogFilter | None:
        """Restrict to the scope's schemas/tables inside *catalog*; ``None`` if nothing there is in scope."""
        key = catalog.lower()
        schemas: frozenset[str] | None = None
        pairs: frozenset[tuple[str, str]] | None = None
        if scope.schemas != ALL:
            parts = [s.lower().split(".") for s in scope.schemas]
            schemas = frozenset(p[1] for p in parts if len(p) == 2 and p[0] == key)
            if not schemas:
                return None
        if scope.tables != ALL:
            parts = [t.lower().split(".") for t in scope.tables]
            pairs = frozenset((p[1], p[2]) for p in parts if len(p) == 3 and p[0] == key)
            if not pairs:
                return None
        return _CatalogFilter(schemas=schemas, pairs=pairs)

    def _read(
        self, plan: list[tuple[str, _CatalogFilter]], limit: int, accept: Callable[[str], bool]
    ) -> SnapshotResult:
        tables: list[TableSnapshot] = []
        unreadable: list[str] = []
        truncated = False
        for catalog, flt in plan:
            try:
                validate_identifier(catalog)
                found, truncated = self._read_catalog(catalog, flt, limit - len(tables), accept)
            except _READ_ERRORS:
                logger.warning(f"Could not read information_schema for catalog (skipped): {_safe(catalog)}")
                unreadable.append(catalog)
                continue
            tables.extend(found)
            if truncated:
                break
        return SnapshotResult(tables=tables, truncated=truncated, unreadable_catalogs=unreadable)

    def _run(self, query: str) -> Sequence[Sequence[str | None]]:
        if not is_sql_query_safe(query):
            raise ValueError("Unsafe snapshot query")
        return self._sql.query(query)

    def _read_catalog(
        self, catalog: str, flt: _CatalogFilter, remaining: int, accept: Callable[[str], bool]
    ) -> tuple[list[TableSnapshot], bool]:
        base = f"{quote_ident(catalog)}.information_schema"
        rows = self._run(
            "SELECT table_schema, table_name, table_owner "
            f"FROM {base}.tables WHERE table_type IN {_TABLE_TYPES} "
            f"AND {_where(flt, 'table_schema', 'table_name')} "
            f"ORDER BY table_schema, table_name LIMIT {max(remaining, 0) + 1}"
        )
        rows = [r for r in rows if len(r) >= 2 and r[0] and r[1] and accept(f"{catalog}.{r[0]}.{r[1]}")]
        truncated = len(rows) > remaining
        rows = rows[: max(remaining, 0)]
        if not rows:
            return [], truncated
        owners = {(r[0] or "", r[1] or ""): (r[2] if len(r) > 2 else None) for r in rows}

        columns: dict[tuple[str, str], list[ColumnSnapshot]] = {}
        column_tags: dict[tuple[str, str, str], list[Sequence[str | None]]] = {}
        for r in self._run(
            "SELECT schema_name, table_name, column_name, tag_name, tag_value "
            f"FROM {base}.column_tags WHERE {_where(flt, 'schema_name', 'table_name')}"
        ):
            if len(r) >= 5 and r[0] and r[1] and r[2]:
                column_tags.setdefault((r[0], r[1], r[2]), []).append(r[3:5])
        for r in self._run(
            "SELECT table_schema, table_name, column_name, data_type "
            f"FROM {base}.columns WHERE {_where(flt, 'table_schema', 'table_name')} "
            "ORDER BY table_schema, table_name, ordinal_position"
        ):
            if len(r) >= 4 and r[0] and r[1] and r[2] and (r[0], r[1]) in owners:
                tags = _tags(column_tags.get((r[0], r[1], r[2]), []), 0)
                columns.setdefault((r[0], r[1]), []).append(ColumnSnapshot(r[2], r[3] or "", tags))

        table_tags: dict[tuple[str, str], list[Sequence[str | None]]] = {}
        for r in self._run(
            "SELECT schema_name, table_name, tag_name, tag_value "
            f"FROM {base}.table_tags WHERE {_where(flt, 'schema_name', 'table_name')}"
        ):
            if len(r) >= 4 and r[0] and r[1]:
                table_tags.setdefault((r[0], r[1]), []).append(r[2:4])
        schema_tags: dict[str, list[Sequence[str | None]]] = {}
        for r in self._run(
            "SELECT schema_name, tag_name, tag_value "
            f"FROM {base}.schema_tags WHERE {_where(flt, 'schema_name', None)}"
        ):
            if len(r) >= 3 and r[0]:
                schema_tags.setdefault(r[0], []).append(r[1:3])
        catalog_tags = _tags(self._run(f"SELECT tag_name, tag_value FROM {base}.catalog_tags"), 0)

        catalog_owner, schema_owners = self._owners(catalog, base, flt)
        snapshots = [
            TableSnapshot(
                table_fqn=f"{catalog}.{schema}.{table}",
                owner=owner,
                catalog_owner=catalog_owner,
                schema_owner=schema_owners.get(schema),
                catalog_tags=catalog_tags,
                schema_tags=_tags(schema_tags.get(schema, []), 0),
                table_tags=_tags(table_tags.get((schema, table), []), 0),
                columns=tuple(columns.get((schema, table), [])),
            )
            for (schema, table), owner in owners.items()
        ]
        return snapshots, truncated

    def _owners(self, catalog: str, base: str, flt: _CatalogFilter) -> tuple[str | None, dict[str, str | None]]:
        """Best-effort catalog/schema owners; any failure yields ``None`` owners."""
        catalog_owner: str | None = None
        schema_owners: dict[str, str | None] = {}
        try:
            for r in self._run(
                f"SELECT catalog_name, catalog_owner FROM {base}.catalogs "
                f"WHERE lower(catalog_name) = {_literal(catalog.lower())}"
            ):
                if len(r) >= 2 and r[0] and r[0].lower() == catalog.lower():
                    catalog_owner = r[1]
        except _READ_ERRORS:
            logger.warning(f"Catalog owner lookup failed: {_safe(catalog)}")
        try:
            for r in self._run(
                f"SELECT schema_name, schema_owner FROM {base}.schemata WHERE {_where(flt, 'schema_name', None)}"
            ):
                if len(r) >= 2 and r[0]:
                    schema_owners[r[0]] = r[1]
        except _READ_ERRORS:
            logger.warning(f"Schema owner lookup failed: {_safe(catalog)}")
        return catalog_owner, schema_owners


def _safe(value: str) -> str:
    """Strip control characters from a name before logging (CWE-117)."""
    return "".join(ch if ch.isprintable() else "?" for ch in value)
