from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.cache import CacheFactory
from databricks_labs_dqx_app.backend.policy_models import PolicyScope
from databricks_labs_dqx_app.backend.services.policy_snapshot import MAX_SNAPSHOT_TABLES, PolicySnapshotReader
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

_COLUMNS = {
    "column_tags": ["schema_name", "table_name", "column_name", "tag_name", "tag_value"],
    "table_tags": ["schema_name", "table_name", "tag_name", "tag_value"],
    "schema_tags": ["schema_name", "tag_name", "tag_value"],
    "catalog_tags": ["tag_name", "tag_value"],
    "columns": ["table_schema", "table_name", "column_name", "data_type"],
    "schemata": ["schema_name", "schema_owner"],
    "catalogs": ["catalog_name", "catalog_owner"],
    "tables": ["table_schema", "table_name", "table_owner"],
}
_TABLE_KINDS = (
    ("information_schema.column_tags", "column_tags"),
    ("information_schema.table_tags", "table_tags"),
    ("information_schema.schema_tags", "schema_tags"),
    ("information_schema.catalog_tags", "catalog_tags"),
    ("information_schema.columns", "columns"),
    ("information_schema.schemata", "schemata"),
    ("information_schema.catalogs", "catalogs"),
    ("information_schema.tables", "tables"),
)


def _make_sql(data: dict[str, dict[str, list[list[str | None]]]], failing: set[str] | None = None):
    """Fake executor: *data* maps catalog -> kind -> rows; *failing* is a set of 'catalog' or 'catalog:kind'."""
    sql = create_autospec(SqlExecutor, instance=True)
    queries: list[str] = []

    def query(statement: str, **kwargs: object) -> list[dict[str, str | None]]:
        assert kwargs.get("require_complete") is True
        queries.append(statement)
        catalog = statement.split("FROM `", 1)[1].split("`", 1)[0].lower()
        kind = next(name for marker, name in _TABLE_KINDS if marker in statement)
        if catalog in (failing or set()) or f"{catalog}:{kind}" in (failing or set()):
            raise RuntimeError("denied")
        return [dict(zip(_COLUMNS[kind], row)) for row in data.get(catalog, {}).get(kind, [])]

    sql.query_dicts.side_effect = query
    return sql, queries


def _main_data() -> dict[str, dict[str, list[list[str | None]]]]:
    return {
        "main": {
            "tables": [["sales", "orders", "alice"], ["sales", "customers", None]],
            "columns": [
                ["sales", "orders", "id", "BIGINT"],
                ["sales", "orders", "email", "STRING"],
                ["sales", "customers", "name", "STRING"],
            ],
            "column_tags": [
                ["sales", "orders", "email", "class.pii", ""],
                ["sales", "orders", "email", "sensitivity", "high"],
            ],
            "table_tags": [["sales", "orders", "domain", "finance"]],
            "schema_tags": [["sales", "tier", ""]],
            "catalog_tags": [["env", "prod"]],
            "schemata": [["sales", "bob"]],
            "catalogs": [["main", "carol"]],
        }
    }


def _reader(sql, catalogs: list[str] | None = None) -> PolicySnapshotReader:
    return PolicySnapshotReader(sql, lambda: catalogs or [])


def test_snapshot_builds_tables_columns_tags_owners():
    sql, _ = _make_sql(_main_data())
    result = _reader(sql).for_scope(PolicyScope(catalogs=["main"]))

    assert not result.truncated and result.unreadable_catalogs == []
    by_fqn = {t.table_fqn: t for t in result.tables}
    orders = by_fqn["main.sales.orders"]
    assert (orders.owner, orders.schema_owner, orders.catalog_owner) == ("alice", "bob", "carol")
    assert orders.table_tags == ("domain=finance",)
    assert orders.schema_tags == ("tier",)
    assert orders.catalog_tags == ("env=prod",)
    columns = {c.name: c for c in orders.columns}
    assert columns["email"].type_name == "STRING"
    assert columns["email"].tags == ("class.pii", "sensitivity=high")
    assert columns["id"].tags == ()
    assert by_fqn["main.sales.customers"].owner is None
    assert by_fqn["main.sales.customers"].table_tags == ()


def test_owner_lookup_failure_keeps_snapshot_with_none_owners():
    sql, _ = _make_sql(_main_data(), failing={"main:schemata", "main:catalogs"})
    result = _reader(sql).for_scope(PolicyScope(catalogs=["main"]))

    assert result.unreadable_catalogs == []
    orders = next(t for t in result.tables if t.table_fqn == "main.sales.orders")
    assert orders.schema_owner is None and orders.catalog_owner is None and orders.owner == "alice"


def test_scope_lists_filter_with_escaped_in_lists():
    sql, queries = _make_sql(_main_data())
    scope = PolicyScope(catalogs=["main"], schemas=["main.sales", "main.Other"], tables=["main.sales.Orders"])
    result = _reader(sql).for_scope(scope)

    assert [t.table_fqn for t in result.tables] == ["main.sales.orders"]
    tables_query = next(q for q in queries if "information_schema.tables" in q)
    assert "information_schema" in tables_query
    assert "lower(table_schema) IN ('other', 'sales')" in tables_query
    assert "lower(table_name) = 'orders'" in tables_query
    assert "table_type IN ('MANAGED','EXTERNAL','VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')" in tables_query
    assert "ORDER BY table_schema, table_name" in tables_query


def test_wildcard_scope_has_no_schema_or_table_filter():
    sql, queries = _make_sql(_main_data())
    _reader(sql).for_scope(PolicyScope(catalogs=["main"]))
    tables_query = next(q for q in queries if "information_schema.tables" in q)
    assert "lower(" not in tables_query
    assert "table_schema <> 'information_schema'" in tables_query


def test_all_catalogs_uses_list_catalogs():
    data = _main_data()
    data["other"] = {"tables": [["s", "t", "x"]], "columns": [["s", "t", "c", "INT"]]}
    sql, _ = _make_sql(data)
    result = _reader(sql, ["main", "other"]).for_scope(PolicyScope())
    assert {t.table_fqn for t in result.tables} == {"main.sales.orders", "main.sales.customers", "other.s.t"}


def test_unreadable_catalog_is_skipped_and_reported():
    data = _main_data()
    data["other"] = {"tables": [["s", "t", "x"]]}
    sql, _ = _make_sql(data, failing={"main"})
    result = _reader(sql, ["main", "other"]).for_scope(PolicyScope())
    assert result.unreadable_catalogs == ["main"]
    assert [t.table_fqn for t in result.tables] == ["other.s.t"]


def test_tag_query_failure_marks_catalog_unreadable():
    sql, _ = _make_sql(_main_data(), failing={"main:column_tags"})
    result = _reader(sql).for_scope(PolicyScope(catalogs=["main"]))
    assert result.unreadable_catalogs == ["main"] and result.tables == []


def test_snapshot_cap_sets_truncated():
    rows = [["s", f"t{i}", None] for i in range(4)]
    sql, queries = _make_sql({"main": {"tables": rows}, "b": {"tables": [["s", "z", None]]}})
    result = _reader(sql, ["main", "b"]).for_scope(PolicyScope(), limit=3)

    assert result.truncated
    assert len(result.tables) == 3
    assert "LIMIT 4" in next(q for q in queries if "information_schema.tables" in q)
    assert not any("`b`" in q for q in queries)
    assert MAX_SNAPSHOT_TABLES == 5000


def test_snapshot_escapes_hostile_identifiers():
    data = {"main": {"tables": [["o'brien", "t", None]], "columns": [["o'brien", "t", "c", "INT"]]}}
    sql, queries = _make_sql(data)
    hostile = PolicyScope.model_construct(catalogs=["main", "bad`cat"], schemas="*", tables="*")
    result = _reader(sql).for_scope(hostile)
    assert result.unreadable_catalogs == ["bad`cat"]
    assert not any("bad" in q for q in queries)

    queries.clear()
    scoped = PolicyScope(catalogs=["main"], schemas=["main.o'brien"])
    result = _reader(sql).for_scope(scoped)
    assert [t.table_fqn for t in result.tables] == ["main.o'brien.t"]
    assert any("IN ('o''brien')" in q for q in queries)


def test_hostile_catalog_from_list_catalogs_is_rejected():
    sql, queries = _make_sql({})
    result = _reader(sql, ["we`ird"]).for_scope(PolicyScope())
    assert result.unreadable_catalogs == ["we`ird"] and queries == []


def test_for_tables_groups_by_catalog():
    data = _main_data()
    data["other"] = {"tables": [["s", "t", "x"], ["s", "skip", "x"]], "columns": [["s", "t", "c", "INT"]]}
    sql, queries = _make_sql(data)
    result = _reader(sql).for_tables(["main.sales.orders", "other.s.t", "main.sales.customers"])

    assert {t.table_fqn for t in result.tables} == {"main.sales.orders", "main.sales.customers", "other.s.t"}
    assert sum("information_schema.tables" in q for q in queries) == 2
    assert next(t for t in result.tables if t.table_fqn == "other.s.t").columns[0].type_name == "INT"


def test_for_tables_skips_malformed_fqns():
    sql, queries = _make_sql(_main_data())
    result = _reader(sql).for_tables(["not_an_fqn"])
    assert result.tables == [] and queries == []


def test_incomplete_result_marks_catalog_unreadable():
    sql, _ = _make_sql(_main_data())
    real = sql.query_dicts.side_effect

    def query(statement: str, **kwargs: object):
        if "information_schema.columns" in statement:
            raise RuntimeError("Could not verify complete SQL results.")
        return real(statement, **kwargs)

    sql.query_dicts.side_effect = query
    result = _reader(sql).for_scope(PolicyScope(catalogs=["main"]))
    assert result.unreadable_catalogs == ["main"] and result.tables == []


def test_os_errors_mark_catalog_unreadable():
    sql, _ = _make_sql(_main_data())
    sql.query_dicts.side_effect = TimeoutError("slow")
    result = _reader(sql).for_scope(PolicyScope(catalogs=["main"]))
    assert result.unreadable_catalogs == ["main"]


def test_list_catalogs_failure_returns_wildcard_unreadable():
    sql, queries = _make_sql({})

    def boom() -> list[str]:
        raise ConnectionError("down")

    result = PolicySnapshotReader(sql, boom).for_scope(PolicyScope())
    assert result.unreadable_catalogs == ["*"] and result.tables == [] and queries == []


def test_follow_up_queries_are_limited_to_candidate_tables():
    sql, queries = _make_sql(_main_data())
    _reader(sql).for_scope(PolicyScope(catalogs=["main"]))
    follow = [q for q in queries if "column_tags" in q or ".columns" in q or "table_tags" in q]
    assert follow and all("lower(" in q and "IN ('sales')" in q for q in follow)

    queries.clear()
    _reader(sql).for_scope(PolicyScope(catalogs=["main"], tables=["main.sales.orders"]))
    follow = [q for q in queries if ".columns" in q]
    assert "lower(table_name) = 'orders'" in follow[0] and "'customers'" not in follow[0]


def test_percent_and_semicolon_names_are_literal():
    data = {"main": {"tables": [["a%b;c", "t_1", None]], "columns": [["a%b;c", "t_1", "c", "INT"]]}}
    sql, queries = _make_sql(data)
    result = _reader(sql).for_scope(PolicyScope(catalogs=["main"], tables=["main.a%b;c.t_1"]))
    assert [t.table_fqn for t in result.tables] == ["main.a%b;c.t_1"]
    assert all("= 'a%b;c'" in q for q in queries if ".tables" in q)


def test_case_mismatched_scope_matches():
    sql, _ = _make_sql(_main_data())
    result = _reader(sql).for_scope(PolicyScope(catalogs=["MAIN"], schemas=["Main.Sales"]))
    assert {t.table_fqn for t in result.tables} == {"MAIN.sales.orders", "MAIN.sales.customers"}


def test_information_schema_literals_escape_backslashes():
    data = {"main": {"tables": [["back\\", "t", None]], "columns": [["back\\", "t", "c", "INT"]]}}
    sql, queries = _make_sql(data)
    result = _reader(sql).for_scope(PolicyScope(catalogs=["main"]))
    assert [t.table_fqn for t in result.tables] == ["main.back\\.t"]
    follow = [q for q in queries if ".columns" in q]
    assert "IN ('back\\\\')" in follow[0]


def test_all_catalogs_skips_system_and_internal_catalogs():
    sql, queries = _make_sql(_main_data())
    result = _reader(sql, ["main", "system", "SYSTEM", "__databricks_internal_x"]).for_scope(PolicyScope())
    assert {t.table_fqn for t in result.tables} == {"main.sales.orders", "main.sales.customers"}
    assert not any("`system`" in q.lower() or "__databricks_internal" in q for q in queries)


def test_explicit_system_catalog_is_still_read():
    sql, queries = _make_sql({})
    _reader(sql).for_scope(PolicyScope(catalogs=["system"]))
    assert any("`system`" in q for q in queries)


# ---------------------------------------------------------------------------
# Per-table cache and evaluation deadline
# ---------------------------------------------------------------------------


def _cached_reader(sql, cache: CacheFactory, identity: str = "alice") -> PolicySnapshotReader:
    return PolicySnapshotReader(sql, lambda: [], cache=cache, cache_identity=identity)


def test_for_tables_second_call_is_served_from_cache():
    sql, queries = _make_sql(_main_data())
    cache = CacheFactory()
    first = _cached_reader(sql, cache).for_tables(["main.sales.orders"])
    issued = len(queries)

    second = _cached_reader(sql, cache).for_tables(["main.sales.orders"])

    assert issued > 0 and len(queries) == issued
    assert second.tables == first.tables and not second.truncated


def test_cache_remembers_tables_the_user_cannot_see():
    sql, queries = _make_sql(_main_data())
    cache = CacheFactory()
    assert _cached_reader(sql, cache).for_tables(["main.sales.hidden"]).tables == []
    issued = len(queries)
    assert _cached_reader(sql, cache).for_tables(["main.sales.hidden"]).tables == []
    assert len(queries) == issued


def test_cache_is_per_user_and_reads_only_misses():
    sql, queries = _make_sql(_main_data())
    cache = CacheFactory()
    _cached_reader(sql, cache, "alice").for_tables(["main.sales.orders"])

    queries.clear()
    _cached_reader(sql, cache, "bob").for_tables(["main.sales.orders"])
    assert queries  # another user never sees alice's snapshot

    queries.clear()
    result = _cached_reader(sql, cache, "alice").for_tables(["main.sales.orders", "main.sales.customers"])
    assert {t.table_fqn for t in result.tables} == {"main.sales.orders", "main.sales.customers"}
    tables_query = next(q for q in queries if "information_schema.tables" in q)
    assert "'customers'" in tables_query and "'orders'" not in tables_query


def test_unreadable_catalog_is_not_cached():
    sql, queries = _make_sql(_main_data(), failing={"main"})
    cache = CacheFactory()
    assert _cached_reader(sql, cache).for_tables(["main.sales.orders"]).unreadable_catalogs == ["main"]
    queries.clear()
    _cached_reader(sql, cache).for_tables(["main.sales.orders"])
    assert queries


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_deadline_stops_the_snapshot_and_sets_truncated():
    data = _main_data()
    data["b"] = {"tables": [["s", "t", None]]}
    sql, queries = _make_sql(data)
    clock = _Clock()
    real = sql.query_dicts.side_effect
    timeouts: list[object] = []

    def slow(statement: str, **kwargs: object):
        timeouts.append(kwargs.get("timeout_seconds"))
        clock.now += 4.0
        return real(statement, **kwargs)

    sql.query_dicts.side_effect = slow
    reader = PolicySnapshotReader(sql, lambda: ["main", "b"], deadline_seconds=10.0, clock=clock)

    result = reader.for_scope(PolicyScope())

    assert result.truncated
    assert result.tables == []  # the catalog cut off part-way is dropped, never half-built
    assert not any("`b`" in q for q in queries)
    assert all(isinstance(t, int) and t <= 10 for t in timeouts)


def test_deadline_truncated_results_are_not_cached_as_missing():
    sql, queries = _make_sql(_main_data())
    clock = _Clock()
    real = sql.query_dicts.side_effect

    def slow(statement: str, **kwargs: object):
        clock.now += 100.0
        return real(statement, **kwargs)

    sql.query_dicts.side_effect = slow
    cache = CacheFactory()
    reader = PolicySnapshotReader(sql, lambda: [], cache=cache, cache_identity="a", deadline_seconds=30.0, clock=clock)
    assert reader.for_tables(["main.sales.orders"]).truncated

    sql.query_dicts.side_effect = real
    queries.clear()
    result = _cached_reader(sql, cache, "a").for_tables(["main.sales.orders"])
    assert queries and [t.table_fqn for t in result.tables] == ["main.sales.orders"]


def test_for_tables_chunks_long_table_lists():
    fqns = [f"main.s.t{i}" for i in range(450)]
    data = {"main": {"tables": [["s", f"t{i}", None] for i in range(450)]}}
    sql, queries = _make_sql(data)
    result = _reader(sql).for_tables(fqns)
    table_queries = [q for q in queries if "information_schema.tables" in q]
    assert len(table_queries) == 3
    assert all(q.count("lower(table_name) =") <= 200 for q in table_queries)
    assert len(result.tables) == 450
