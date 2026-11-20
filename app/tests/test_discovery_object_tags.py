"""Unit tests for the Unity Catalog object-tag reader and the monitored-table tags dependency."""

from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.dependencies import get_monitored_table_uc_tags
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService, read_object_tags
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService, TableUniverseRow
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _sql(rows_by_catalog: dict[str, list[list[str | None]]]) -> SqlExecutor:
    """A SqlExecutor whose query answers with the rows of the catalog the statement reads."""
    sql = create_autospec(SqlExecutor, instance=True)

    def query(statement: str) -> list[list[str | None]]:
        for catalog, rows in rows_by_catalog.items():
            if f"`{catalog}`.information_schema" in statement:
                return rows
        raise RuntimeError("permission denied")

    sql.query.side_effect = query
    return sql


def test_assigns_each_level_to_the_tables_under_it() -> None:
    sql = _sql(
        {
            "main": [
                ["catalog", None, None, "env", "prod"],
                ["schema", "sales", None, "pii", None],
                ["table", "sales", "orders", "tier", "gold"],
                ["table", "sales", "other_table", "tier", "bronze"],
            ]
        }
    )
    tags = read_object_tags(sql, ["main.sales.orders", "main.sales.items", "main.hr.people"])
    assert tags == {
        "main.sales.orders": {"catalog:env": "prod", "schema:pii": "", "table:tier": "gold"},
        "main.sales.items": {"catalog:env": "prod", "schema:pii": ""},
        "main.hr.people": {"catalog:env": "prod"},
    }


def test_runs_one_query_per_catalog_and_skips_a_failing_catalog() -> None:
    sql = _sql({"main": [["table", "sales", "orders", "tier", "gold"]]})
    tags = read_object_tags(sql, ["main.sales.orders", "locked.sales.orders"])
    assert tags == {"main.sales.orders": {"table:tier": "gold"}, "locked.sales.orders": {}}
    assert sql.query.call_count == 2


def test_skips_invalid_fqns_and_escapes_names() -> None:
    sql = _sql({"main": []})
    tags = read_object_tags(sql, ["not-an-fqn", "main.o'brien.orders"])
    assert tags == {"main.o'brien.orders": {}}
    statement = sql.query.call_args[0][0]
    assert "'o''brien'" in statement


async def test_monitored_table_uc_tags_reads_every_monitored_table_once() -> None:
    monitored = create_autospec(MonitoredTableService, instance=True)
    monitored.list_table_universe.return_value = [
        TableUniverseRow(binding_id=b, table_fqn=fqn, owner=None, owner_display_name=None, created_by=None, score=None)
        for b, fqn in (("b1", "main.sales.orders"), ("b2", "main.sales.items"), ("b3", "main.sales.orders"))
    ]
    discovery = create_autospec(DiscoveryService, instance=True)
    discovery.list_object_tags_async.return_value = {"main.sales.orders": {"table:tier": "gold"}}
    tags = await get_monitored_table_uc_tags(discovery, monitored)
    assert tags == {"main.sales.orders": {"table:tier": "gold"}}
    discovery.list_object_tags_async.assert_awaited_once_with(("main.sales.items", "main.sales.orders"))


async def test_monitored_table_uc_tags_degrade_to_none_on_failure() -> None:
    monitored = create_autospec(MonitoredTableService, instance=True)
    monitored.list_table_universe.side_effect = RuntimeError("lakebase down")
    discovery = create_autospec(DiscoveryService, instance=True)
    assert await get_monitored_table_uc_tags(discovery, monitored) == {}
