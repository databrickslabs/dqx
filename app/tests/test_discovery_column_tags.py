"""Unit tests for ``DiscoveryService.get_table_tags`` column-tag sourcing.

When a ``SqlExecutor`` is injected, column tags are read from
``<catalog>.information_schema.column_tags`` (the reliable source). When no
executor is injected, the method falls back to the legacy
``tables.get().columns[].tags`` scan so it still works without SQL.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService, read_column_tags
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _svc_with_sql(rows: list[list[str]]) -> DiscoveryService:
    ws = MagicMock()
    # tables.get is still used for table-level tags; give it no column tags.
    table_info = MagicMock()
    table_info.tags = None
    table_info.columns = []
    ws.tables.get.return_value = table_info
    sql = create_autospec(SqlExecutor, instance=True)
    sql.query.return_value = rows
    return DiscoveryService(ws=ws, user_id="tester", sql=sql)


class TestGetTableTagsFromSql:
    def test_column_tags_from_information_schema(self) -> None:
        svc = _svc_with_sql([["cut", "class.age", ""], ["carat", "class.pii", "x"]])
        result = svc.get_table_tags("cat", "sch", "diamonds")
        assert result.column_tags == {"cut": ["class.age"], "carat": ["class.pii=x"]}

    def test_read_column_tags_helper_formats_key_and_key_value(self) -> None:
        sql = create_autospec(SqlExecutor, instance=True)
        sql.query.return_value = [["cut", "class.age", ""], ["carat", "class.pii", "x"]]
        tags = read_column_tags(sql, "cat.sch.diamonds")
        assert tags == {"cut": ["class.age"], "carat": ["class.pii=x"]}


class TestGetTableTagsFallback:
    def test_falls_back_to_tables_get_when_no_sql(self) -> None:
        ws = MagicMock()
        table_info = MagicMock()
        table_info.tags = None
        col = MagicMock()
        col.name = "cut"
        tag = MagicMock()
        tag.key = "class.age"
        tag.value = None
        col.tags = [tag]
        table_info.columns = [col]
        ws.tables.get.return_value = table_info

        svc = DiscoveryService(ws=ws, user_id="tester")  # no sql injected
        result = svc.get_table_tags("cat", "sch", "diamonds")
        assert result.column_tags == {"cut": ["class.age"]}
