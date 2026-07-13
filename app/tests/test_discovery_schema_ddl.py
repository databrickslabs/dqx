"""Tests for ``DiscoveryService.get_table_schema_ddl``.

Covers the bits that are easy to get wrong: column ordering by
position, ``type_text`` preference over ``type_name``, identifier
quoting for non-standard column names, and skipping malformed columns.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.services.discovery import (
    DiscoveryService,
    _quote_ddl_identifier,
)


def _col(
    name: str,
    type_text: str | None = None,
    type_name: str | None = None,
    position: int = 0,
) -> SimpleNamespace:
    """Build a column duck-typed like Databricks SDK ColumnInfo."""
    return SimpleNamespace(
        name=name,
        type_text=type_text,
        type_name=SimpleNamespace(value=type_name) if type_name else None,
        position=position,
    )


def _service(columns: list[SimpleNamespace]) -> DiscoveryService:
    ws = MagicMock()
    ws.tables.get.return_value = SimpleNamespace(columns=columns)
    return DiscoveryService(ws=ws, user_id="tester")


class TestQuoteIdentifier:
    @pytest.mark.parametrize(
        "name",
        ["id", "order_id", "x1", "_internal", "A_B_c1"],
    )
    def test_safe_names_unquoted(self, name: str):
        assert _quote_ddl_identifier(name) == name

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("order id", "`order id`"),
            ("kebab-case", "`kebab-case`"),
            ("1starts_with_digit", "`1starts_with_digit`"),
            ("has`back`tick", "`has``back``tick`"),
        ],
    )
    def test_unsafe_names_quoted_and_escaped(self, name: str, expected: str):
        assert _quote_ddl_identifier(name) == expected


class TestGetTableOwner:
    def test_returns_owner_when_present(self):
        ws = MagicMock()
        ws.tables.get.return_value = SimpleNamespace(owner="alice@x")
        svc = DiscoveryService(ws=ws, user_id="tester")
        assert svc.get_table_owner("cat.sch.t") == "alice@x"

    def test_returns_group_or_sp_owner_verbatim(self):
        # UC owner may be a group or service principal, not a person — stored as-is.
        ws = MagicMock()
        ws.tables.get.return_value = SimpleNamespace(owner="data-eng-group")
        svc = DiscoveryService(ws=ws, user_id="tester")
        assert svc.get_table_owner("cat.sch.t") == "data-eng-group"

    def test_blank_owner_is_none(self):
        ws = MagicMock()
        ws.tables.get.return_value = SimpleNamespace(owner="   ")
        svc = DiscoveryService(ws=ws, user_id="tester")
        assert svc.get_table_owner("cat.sch.t") is None

    def test_none_owner_is_none(self):
        ws = MagicMock()
        ws.tables.get.return_value = SimpleNamespace(owner=None)
        svc = DiscoveryService(ws=ws, user_id="tester")
        assert svc.get_table_owner("cat.sch.t") is None

    def test_swallows_lookup_error_and_returns_none(self):
        # Permission denied / missing table -> graceful fallback (None), so the
        # route can default the steward to the creator.
        ws = MagicMock()
        ws.tables.get.side_effect = PermissionError("denied")
        svc = DiscoveryService(ws=ws, user_id="tester")
        assert svc.get_table_owner("cat.sch.t") is None


class TestGetTableSchemaDdl:
    def test_simple_three_column_table(self):
        svc = _service(
            [
                _col("id", type_text="INT", position=0),
                _col("name", type_text="STRING", position=1),
                _col("amount", type_text="DECIMAL(10,2)", position=2),
            ]
        )
        assert svc.get_table_schema_ddl("cat.sch.t") == "id INT, name STRING, amount DECIMAL(10,2)"

    def test_columns_sorted_by_position_not_input_order(self):
        # Out-of-order inputs ensure the position-based sort kicks in;
        # the SDK doesn't always guarantee returned order.
        svc = _service(
            [
                _col("c", type_text="STRING", position=2),
                _col("a", type_text="INT", position=0),
                _col("b", type_text="BIGINT", position=1),
            ]
        )
        assert svc.get_table_schema_ddl("c.s.t") == "a INT, b BIGINT, c STRING"

    def test_falls_back_to_type_name_when_type_text_missing(self):
        svc = _service(
            [
                _col("id", type_text=None, type_name="LONG", position=0),
            ]
        )
        assert svc.get_table_schema_ddl("c.s.t") == "id LONG"

    def test_skips_columns_with_no_name_or_type(self):
        svc = _service(
            [
                _col("id", type_text="INT", position=0),
                _col("", type_text="STRING", position=1),  # missing name
                _col("noise", type_text=None, type_name=None, position=2),  # missing both
                _col("ok", type_text="STRING", position=3),
            ]
        )
        assert svc.get_table_schema_ddl("c.s.t") == "id INT, ok STRING"

    def test_quotes_non_standard_column_names(self):
        svc = _service(
            [
                _col("order id", type_text="STRING", position=0),
                _col("price", type_text="DECIMAL(10,2)", position=1),
            ]
        )
        assert svc.get_table_schema_ddl("c.s.t") == "`order id` STRING, price DECIMAL(10,2)"

    def test_empty_table_returns_empty_string(self):
        svc = _service([])
        assert svc.get_table_schema_ddl("c.s.t") == ""

    def test_none_columns_returns_empty_string(self):
        ws = MagicMock()
        ws.tables.get.return_value = SimpleNamespace(columns=None)
        svc = DiscoveryService(ws=ws, user_id="tester")
        assert svc.get_table_schema_ddl("c.s.t") == ""

    def test_preserves_nested_types(self):
        svc = _service(
            [
                _col("tags", type_text="ARRAY<STRING>", position=0),
                _col(
                    "addr",
                    type_text="STRUCT<street: STRING, city: STRING>",
                    position=1,
                ),
                _col("flags", type_text="MAP<STRING, BOOLEAN>", position=2),
            ]
        )
        assert (
            svc.get_table_schema_ddl("c.s.t")
            == "tags ARRAY<STRING>, addr STRUCT<street: STRING, city: STRING>, flags MAP<STRING, BOOLEAN>"
        )
