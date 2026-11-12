"""Bulk, user-scoped ``information_schema`` snapshot reader for rule policies.

Reads tables, columns, tags and owners per catalog with a handful of set-based
queries (never one query per table) and returns :class:`TableSnapshot` objects
for the pure policy engine. Run it with an OBO executor so the result reflects
only what the calling user can see.

Every read is bounded by a deadline (``DEFAULT_DEADLINE_SECONDS`` per call):
when it passes, reading stops and the result is ``truncated``. ``for_tables``
can share a per-table, per-user TTL cache so opening the same table again (or
asking for its suggestions) does not re-read Unity Catalog.
"""

import functools
import hashlib
import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass

from databricks.labs.dqx.utils import is_sql_query_safe
from databricks.sdk.errors.base import DatabricksError

from databricks_labs_dqx_app.backend.cache import MISS, CacheFactory
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
DEFAULT_DEADLINE_SECONDS = 30.0
SNAPSHOT_CACHE_TTL_SECONDS = 300
_CACHE_PREFIX = "policy:snapshot"
# Databricks statement wait_timeout must be 0 or 5..50 s; the reader stays within its deadline.
_MIN_WAIT_SECONDS = 5
_MAX_WAIT_SECONDS = 30
_READ_ERRORS = (ValueError, RuntimeError, OSError, DatabricksError)  # RuntimeError covers SqlStatementError
_SYSTEM_CATALOG = "system"
_INTERNAL_CATALOG_PREFIX = "__databricks_internal"
_TABLE_TYPES = "('MANAGED','EXTERNAL','VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')"


@dataclass(frozen=True)
class SnapshotResult:
    tables: list[TableSnapshot]
    truncated: bool
    unreadable_catalogs: list[str]


class _DeadlineExceeded(Exception):
    """The snapshot ran out of time; deliberately not one of ``_READ_ERRORS``."""


def snapshot_cache_identity(user_email: str) -> str:
    """Stable, non-reversible cache key part for the user a snapshot is read for."""
    return hashlib.sha256(user_email.strip().lower().encode()).hexdigest()


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


def _valid_fqn(fqn: str) -> bool:
    try:
        validate_fqn(fqn)
    except ValueError:
        return False
    return True


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

    def __init__(
        self,
        sql: SqlExecutor,
        list_catalogs: Callable[[], list[str]],
        *,
        cache: CacheFactory | None = None,
        cache_identity: str = "",
        deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a reader.

        Args:
            sql: Executor the ``information_schema`` queries run on (OBO for user scoping).
            list_catalogs: Lists the catalogs "All catalogs" expands to.
            cache: Optional shared cache for :meth:`for_tables`; caching needs *cache_identity* too.
            cache_identity: Identity of the user the reads run as (see :func:`snapshot_cache_identity`).
            deadline_seconds: Time budget of each :meth:`for_scope` / :meth:`for_tables` call.
            clock: Monotonic clock (injectable for tests).
        """
        self._sql = sql
        self._list_catalogs = list_catalogs
        self._cache = cache if cache_identity else None
        self._cache_identity = cache_identity
        self._deadline_seconds = deadline_seconds
        self._clock = clock

    def for_scope(self, scope: PolicyScope, *, limit: int = MAX_SNAPSHOT_TABLES) -> SnapshotResult:
        deadline = self._clock() + self._deadline_seconds
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
        return self._read(plan, limit, lambda fqn: in_scope(scope, fqn), deadline)

    def for_tables(self, table_fqns: list[str]) -> SnapshotResult:
        """Snapshot specific tables, serving (and filling) the per-user cache when configured."""
        deadline = self._clock() + self._deadline_seconds
        cached: list[TableSnapshot] = []
        misses: list[str] = []
        for fqn in dict.fromkeys(table_fqns):
            hit = self._cache.peek(self._cache_key(fqn)) if self._cache is not None else MISS
            if hit is MISS:
                misses.append(fqn)
            elif isinstance(hit, TableSnapshot):
                cached.append(hit)
        if not misses:
            return SnapshotResult(tables=cached, truncated=False, unreadable_catalogs=[])
        result = self._read_tables(misses, deadline)
        self._remember(misses, result)
        return SnapshotResult(
            tables=cached + result.tables, truncated=result.truncated, unreadable_catalogs=result.unreadable_catalogs
        )

    def _cache_key(self, table_fqn: str) -> str:
        return f"{_CACHE_PREFIX}:{self._cache_identity}:{table_fqn.lower()}"

    def _remember(self, requested: list[str], result: SnapshotResult) -> None:
        """Cache what was read; a table not found is cached as ``None`` only when its read completed."""
        if self._cache is None:
            return
        found = {table.table_fqn.lower(): table for table in result.tables}
        unreadable = {catalog.lower() for catalog in result.unreadable_catalogs}
        for fqn in requested:
            table = found.get(fqn.lower())
            if table is not None:
                self._cache.put(self._cache_key(fqn), table, ttl=SNAPSHOT_CACHE_TTL_SECONDS)
            elif not result.truncated and _valid_fqn(fqn) and fqn.split(".")[0].lower() not in unreadable:
                self._cache.put(self._cache_key(fqn), None, ttl=SNAPSHOT_CACHE_TTL_SECONDS)

    def _read_tables(self, table_fqns: list[str], deadline: float) -> SnapshotResult:
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
        plan: list[tuple[str, _CatalogFilter]] = []
        for key, pairs in pairs_by_catalog.items():
            ordered = sorted(pairs)
            for start in range(0, len(ordered), _PAIR_CHUNK):  # keeps each tables query's predicate bounded
                plan.append((catalog_names[key], _CatalogFilter(pairs=frozenset(ordered[start : start + _PAIR_CHUNK]))))
        wanted = {fqn.lower() for fqn in table_fqns}
        return self._read(plan, MAX_SNAPSHOT_TABLES, lambda fqn: fqn.lower() in wanted, deadline)

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
        self, plan: list[tuple[str, _CatalogFilter]], limit: int, accept: Callable[[str], bool], deadline: float
    ) -> SnapshotResult:
        tables: list[TableSnapshot] = []
        seen: set[str] = set()
        unreadable: list[str] = []
        truncated = False
        for catalog, flt in plan:
            try:
                validate_identifier(catalog)
                found, truncated = self._read_catalog(catalog, flt, limit - len(tables), accept, deadline)
            except _DeadlineExceeded:
                truncated = True
            except _READ_ERRORS:
                logger.warning(f"Could not read information_schema for catalog (skipped): {safe_log_value(catalog)}")
                if catalog not in unreadable:
                    unreadable.append(catalog)
                truncated = self._clock() >= deadline  # a statement that hit the time limit
                if not truncated:
                    continue
            else:
                for table in found:
                    if table.table_fqn.lower() not in seen:
                        seen.add(table.table_fqn.lower())
                        tables.append(table)
            if truncated:
                break
        if truncated and self._clock() >= deadline:
            logger.info("Policy snapshot stopped at its time limit")
        return SnapshotResult(tables=tables, truncated=truncated, unreadable_catalogs=unreadable)

    def _run(self, query: str, deadline: float) -> list[Row]:
        if not is_sql_query_safe(query):
            raise ValueError("Unsafe snapshot query")
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise _DeadlineExceeded()
        wait = min(_MAX_WAIT_SECONDS, max(_MIN_WAIT_SECONDS, int(remaining)))
        poll = max(1, math.ceil(remaining) - wait)
        return self._sql.query_dicts(query, require_complete=True, timeout_seconds=poll, wait_timeout_seconds=wait)

    @staticmethod
    def _followup_filters(flt: _CatalogFilter, candidates: list[tuple[str, str]]) -> list[_CatalogFilter]:
        """Filters limiting follow-up reads to the accepted candidates (pairs chunked when long)."""
        if flt.pairs is None:
            return [_CatalogFilter(schemas=frozenset(schema.lower() for schema, _ in candidates))]
        pairs = sorted({(schema.lower(), table.lower()) for schema, table in candidates})
        return [_CatalogFilter(pairs=frozenset(pairs[i : i + _PAIR_CHUNK])) for i in range(0, len(pairs), _PAIR_CHUNK)]

    def _read_catalog(
        self, catalog: str, flt: _CatalogFilter, remaining: int, accept: Callable[[str], bool], deadline: float
    ) -> tuple[list[TableSnapshot], bool]:
        run = functools.partial(self._run, deadline=deadline)
        base = f"{quote_ident(catalog)}.information_schema"
        rows = run(
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
            for r in run(
                "SELECT schema_name, table_name, column_name, tag_name, tag_value "
                f"FROM {base}.column_tags WHERE {_where(sub, 'schema_name', 'table_name')}"
            ):
                sch, tbl, col = r.get("schema_name"), r.get("table_name"), r.get("column_name")
                if sch and tbl and col:
                    column_tags.setdefault((sch, tbl, col), []).append(r)
            for r in run(
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
            for r in run(
                "SELECT schema_name, table_name, tag_name, tag_value "
                f"FROM {base}.table_tags WHERE {_where(sub, 'schema_name', 'table_name')}"
            ):
                sch, tbl = r.get("schema_name"), r.get("table_name")
                if sch and tbl:
                    table_tags.setdefault((sch, tbl), []).append(r)

        schema_filter = _CatalogFilter(schemas=frozenset(schema.lower() for schema, _ in owners))
        schema_tags: dict[str, list[Row]] = {}
        for r in run(
            "SELECT schema_name, tag_name, tag_value "
            f"FROM {base}.schema_tags WHERE {_where(schema_filter, 'schema_name', None)}"
        ):
            sch = r.get("schema_name")
            if sch:
                schema_tags.setdefault(sch, []).append(r)
        catalog_tags = _tags(run(f"SELECT tag_name, tag_value FROM {base}.catalog_tags"))

        catalog_owner, schema_owners = self._owners(catalog, base, schema_filter, deadline)
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

    def _owners(
        self, catalog: str, base: str, flt: _CatalogFilter, deadline: float
    ) -> tuple[str | None, dict[str, str | None]]:
        """Best-effort catalog/schema owners; any read failure yields ``None`` owners."""
        run = functools.partial(self._run, deadline=deadline)
        catalog_owner: str | None = None
        schema_owners: dict[str, str | None] = {}
        try:
            for r in run(
                f"SELECT catalog_name, catalog_owner FROM {base}.catalogs "
                f"WHERE lower(catalog_name) = {_literal(catalog.lower())}"
            ):
                name = r.get("catalog_name")
                if name and name.lower() == catalog.lower():
                    catalog_owner = r.get("catalog_owner")
        except _READ_ERRORS:
            logger.warning(f"Catalog owner lookup failed: {safe_log_value(catalog)}")
        try:
            for r in run(
                f"SELECT schema_name, schema_owner FROM {base}.schemata WHERE {_where(flt, 'schema_name', None)}"
            ):
                if r.get("schema_name"):
                    schema_owners[str(r["schema_name"])] = r.get("schema_owner")
        except _READ_ERRORS:
            logger.warning(f"Schema owner lookup failed: {safe_log_value(catalog)}")
        return catalog_owner, schema_owners
