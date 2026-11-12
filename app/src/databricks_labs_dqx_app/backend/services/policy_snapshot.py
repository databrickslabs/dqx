"""Bulk, user-scoped ``information_schema`` snapshot reader for rule policies.

Reads tables, columns, tags and owners per catalog with a handful of set-based
queries (never one query per table) and returns :class:`TableSnapshot` objects
for the pure policy engine. Run it with an OBO executor so the result reflects
only what the calling user can see.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from databricks.labs.dqx.utils import is_sql_query_safe
from databricks.sdk.errors.base import DatabricksError

from databricks_labs_dqx_app.backend.policy_models import ALL, PolicyScope
from databricks_labs_dqx_app.backend.sanitization import safe_log_value
from databricks_labs_dqx_app.backend.services.policy_engine import ColumnSnapshot, TableSnapshot, in_scope
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import (
    escape_sql_string_strict,
    quote_ident,
    validate_fqn,
    validate_identifier,
)

logger = logging.getLogger(__name__)

MAX_SNAPSHOT_TABLES = 5000
_READ_ERRORS = (ValueError, RuntimeError, OSError, DatabricksError)  # RuntimeError covers SqlStatementError
_SYSTEM_CATALOG = "system"
_INTERNAL_CATALOG_PREFIX = "__databricks_internal"
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
    # information_schema names are not validated FQNs, so backslashes must be escaped too.
    return f"'{escape_sql_string_strict(value)}'"


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


def _user_catalogs(names: list[str]) -> list[str]:
    """Drop the platform catalogs "All catalogs" never reaches (``system``, ``__databricks_internal*``)."""
    return [n for n in names if n.lower() != _SYSTEM_CATALOG and not n.lower().startswith(_INTERNAL_CATALOG_PREFIX)]


Row = dict[str, str | None]
_PAIR_CHUNK = 200


def _tag(name: str | None, value: str | None) -> str | None:
    if not name:
        return None
    return f"{name}={value}" if value else name


def _tags(rows: list[Row]) -> tuple[str, ...]:
    out = (_tag(r.get("tag_name"), r.get("tag_value")) for r in rows)
    return tuple(t for t in out if t)


class PolicySnapshotReader:
    """Builds :class:`TableSnapshot` lists for a policy scope or an explicit table list."""

    def __init__(self, sql: SqlExecutor, list_catalogs: Callable[[], list[str]]) -> None:
        self._sql = sql
        self._list_catalogs = list_catalogs

    def for_scope(self, scope: PolicyScope, *, limit: int = MAX_SNAPSHOT_TABLES) -> SnapshotResult:
        try:
            catalogs = _user_catalogs(self._list_catalogs()) if scope.catalogs == ALL else list(scope.catalogs)
        except _READ_ERRORS:
            logger.warning("Could not list catalogs for policy snapshot")
            return SnapshotResult(tables=[], truncated=False, unreadable_catalogs=[ALL])
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
                logger.warning(f"Could not read information_schema for catalog (skipped): {safe_log_value(catalog)}")
                unreadable.append(catalog)
                continue
            tables.extend(found)
            if truncated:
                break
        return SnapshotResult(tables=tables, truncated=truncated, unreadable_catalogs=unreadable)

    def _run(self, query: str) -> list[Row]:
        if not is_sql_query_safe(query):
            raise ValueError("Unsafe snapshot query")
        return self._sql.query_dicts(query, require_complete=True)

    @staticmethod
    def _followup_filters(flt: _CatalogFilter, candidates: list[tuple[str, str]]) -> list[_CatalogFilter]:
        """Filters limiting follow-up reads to the accepted candidates (pairs chunked when long)."""
        if flt.pairs is None:
            return [_CatalogFilter(schemas=frozenset(schema.lower() for schema, _ in candidates))]
        pairs = sorted({(schema.lower(), table.lower()) for schema, table in candidates})
        return [_CatalogFilter(pairs=frozenset(pairs[i : i + _PAIR_CHUNK])) for i in range(0, len(pairs), _PAIR_CHUNK)]

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
        rows = [r for r in rows if r.get("table_schema") and r.get("table_name")]
        rows = [r for r in rows if accept(f"{catalog}.{r['table_schema']}.{r['table_name']}")]
        truncated = len(rows) > remaining
        rows = rows[: max(remaining, 0)]
        if not rows:
            return [], truncated
        owners = {(str(r["table_schema"]), str(r["table_name"])): r.get("table_owner") for r in rows}
        followups = self._followup_filters(flt, list(owners))

        column_tags: dict[tuple[str, str, str], list[Row]] = {}
        columns: dict[tuple[str, str], list[ColumnSnapshot]] = {}
        table_tags: dict[tuple[str, str], list[Row]] = {}
        for sub in followups:
            for r in self._run(
                "SELECT schema_name, table_name, column_name, tag_name, tag_value "
                f"FROM {base}.column_tags WHERE {_where(sub, 'schema_name', 'table_name')}"
            ):
                sch, tbl, col = r.get("schema_name"), r.get("table_name"), r.get("column_name")
                if sch and tbl and col:
                    column_tags.setdefault((sch, tbl, col), []).append(r)
            for r in self._run(
                "SELECT table_schema, table_name, column_name, data_type "
                f"FROM {base}.columns WHERE {_where(sub, 'table_schema', 'table_name')} "
                "ORDER BY table_schema, table_name, ordinal_position"
            ):
                schema, table, column = r.get("table_schema"), r.get("table_name"), r.get("column_name")
                if schema and table and column and (schema, table) in owners:
                    tags = _tags(column_tags.get((schema, table, column), []))
                    columns.setdefault((schema, table), []).append(
                        ColumnSnapshot(column, r.get("data_type") or "", tags)
                    )
            for r in self._run(
                "SELECT schema_name, table_name, tag_name, tag_value "
                f"FROM {base}.table_tags WHERE {_where(sub, 'schema_name', 'table_name')}"
            ):
                sch, tbl = r.get("schema_name"), r.get("table_name")
                if sch and tbl:
                    table_tags.setdefault((sch, tbl), []).append(r)

        schema_filter = _CatalogFilter(schemas=frozenset(schema.lower() for schema, _ in owners))
        schema_tags: dict[str, list[Row]] = {}
        for r in self._run(
            "SELECT schema_name, tag_name, tag_value "
            f"FROM {base}.schema_tags WHERE {_where(schema_filter, 'schema_name', None)}"
        ):
            sch = r.get("schema_name")
            if sch:
                schema_tags.setdefault(sch, []).append(r)
        catalog_tags = _tags(self._run(f"SELECT tag_name, tag_value FROM {base}.catalog_tags"))

        catalog_owner, schema_owners = self._owners(catalog, base, schema_filter)
        snapshots = [
            TableSnapshot(
                table_fqn=f"{catalog}.{schema}.{table}",
                owner=owner,
                catalog_owner=catalog_owner,
                schema_owner=schema_owners.get(schema),
                catalog_tags=catalog_tags,
                schema_tags=_tags(schema_tags.get(schema, [])),
                table_tags=_tags(table_tags.get((schema, table), [])),
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
                name = r.get("catalog_name")
                if name and name.lower() == catalog.lower():
                    catalog_owner = r.get("catalog_owner")
        except _READ_ERRORS:
            logger.warning(f"Catalog owner lookup failed: {safe_log_value(catalog)}")
        try:
            for r in self._run(
                f"SELECT schema_name, schema_owner FROM {base}.schemata WHERE {_where(flt, 'schema_name', None)}"
            ):
                if r.get("schema_name"):
                    schema_owners[str(r["schema_name"])] = r.get("schema_owner")
        except _READ_ERRORS:
            logger.warning(f"Schema owner lookup failed: {safe_log_value(catalog)}")
        return catalog_owner, schema_owners
