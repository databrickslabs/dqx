"""Unit tests for the apply-on-tag column reader (``dependencies._build_column_reader``).

The reader sources column NAMES + TYPES from ``ws.tables.get`` and column TAGS
from ``<catalog>.information_schema.column_tags`` via a ``SqlExecutor``. These
tests use a fake ``ws`` (``MagicMock`` ``tables.get``) and a fake ``SqlExecutor``
(``create_autospec`` with a scripted ``.query``) to assert per-column tag/type
merging and the best-effort degradation contract (never raises).
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.dependencies import _build_column_reader
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _column(name: str, type_name: str) -> MagicMock:
    col = MagicMock()
    col.name = name
    col.type_name = MagicMock()
    col.type_name.value = type_name
    return col


def _fake_ws(columns: list[MagicMock]) -> MagicMock:
    ws = create_autospec(WorkspaceClient, instance=True)
    table_info = MagicMock()
    table_info.columns = columns
    ws.tables.get.return_value = table_info
    return ws


def test_merges_tags_and_types_per_column() -> None:
    ws = _fake_ws([_column("cut", "STRING"), _column("carat", "DOUBLE")])
    sql = create_autospec(SqlExecutor, instance=True)
    sql.query.return_value = [["cut", "class.age", ""], ["carat", "class.pii", "x"]]

    reader = _build_column_reader(ws, sql)
    columns = reader("cat.sch.diamonds")

    by_name = {c.name: c for c in columns}
    assert by_name["cut"].tags == ["class.age"]
    assert by_name["cut"].type_name == "STRING"
    assert by_name["carat"].tags == ["class.pii=x"]
    assert by_name["carat"].type_name == "DOUBLE"
    # information_schema query targets the table's schema + name.
    query = sql.query.call_args.args[0]
    assert "information_schema.column_tags" in query
    assert "'sch'" in query
    assert "'diamonds'" in query


def test_column_without_tag_row_has_empty_tags() -> None:
    ws = _fake_ws([_column("cut", "STRING"), _column("price", "INT")])
    sql = create_autospec(SqlExecutor, instance=True)
    sql.query.return_value = [["cut", "class.age", ""]]

    reader = _build_column_reader(ws, sql)
    columns = reader("cat.sch.diamonds")

    by_name = {c.name: c for c in columns}
    assert by_name["cut"].tags == ["class.age"]
    assert by_name["price"].tags == []


def test_tag_query_raising_returns_columns_with_empty_tags() -> None:
    ws = _fake_ws([_column("cut", "STRING")])
    sql = create_autospec(SqlExecutor, instance=True)
    sql.query.side_effect = RuntimeError("warehouse down")

    reader = _build_column_reader(ws, sql)
    columns = reader("cat.sch.diamonds")

    assert len(columns) == 1
    assert columns[0].name == "cut"
    assert columns[0].type_name == "STRING"
    assert columns[0].tags == []


def test_tables_get_raising_returns_empty_list() -> None:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.tables.get.side_effect = RuntimeError("no such table")
    sql = create_autospec(SqlExecutor, instance=True)

    reader = _build_column_reader(ws, sql)
    columns = reader("cat.sch.missing")

    assert columns == []
    sql.query.assert_not_called()


def test_invalid_fqn_yields_no_tags_but_still_returns_columns() -> None:
    ws = _fake_ws([_column("cut", "STRING")])
    sql = create_autospec(SqlExecutor, instance=True)

    reader = _build_column_reader(ws, sql)
    columns = reader("not_a_three_part_name")

    assert len(columns) == 1
    assert columns[0].tags == []
    # An invalid FQN never reaches the warehouse.
    sql.query.assert_not_called()
