"""Unit tests for the Python low-code compiler port (B2-132).

Mirrors the load-bearing cases in ``ui/lib/lowcodeCompile.test.ts`` so the
backend port stays faithful to the TypeScript source the visual builder uses:
the compiled ``body`` is what materializes and runs, so a wrong shape here
would ship a broken AI-proposed low-code rule.
"""

from __future__ import annotations

from typing import Any

from databricks_labs_dqx_app.backend.lowcode_compile import (
    compile_ast_to_sql,
    compile_lowcode_body,
    extract_slot_tokens,
    lowcode_is_usable,
)


def _ast(rows: list[dict[str, Any]], joins: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"rows": rows, "joins": joins or []}


def _row(**over: Any) -> dict[str, Any]:
    base = {"kind": "row", "combinator": None, "column_ref": "email", "operator": "is not null", "value": None}
    base.update(over)
    return base


class TestCompileAstToSql:
    def test_single_row_wraps_slot(self):
        assert compile_ast_to_sql(_ast([_row()])) == "{{email}} IS NOT NULL"

    def test_multiple_rows_join_with_combinator(self):
        rows = [
            _row(column_ref="a", operator="is not null"),
            _row(combinator="AND", column_ref="b", operator="is null"),
            _row(combinator="OR", column_ref="c", operator="is not null"),
        ]
        assert compile_ast_to_sql(_ast(rows)) == "{{a}} IS NOT NULL AND {{b}} IS NULL OR {{c}} IS NOT NULL"

    def test_qualified_ref_passes_through_raw(self):
        assert compile_ast_to_sql(_ast([_row(column_ref="orders.total", operator=">", value=5)])) == "orders.total > 5"

    def test_in_operator_quotes_each_literal(self):
        sql = compile_ast_to_sql(_ast([_row(column_ref="status", operator="in", value=["a", "b"])]))
        assert sql == "{{status}} IN ('a', 'b')"

    def test_between_uses_list_bounds(self):
        sql = compile_ast_to_sql(_ast([_row(column_ref="amount", operator="between", value=[0, 100])]))
        assert sql == "{{amount}} BETWEEN 0 AND 100"

    def test_unknown_operator_yields_empty(self):
        assert compile_ast_to_sql(_ast([_row(operator="frobnicate")])) == ""

    def test_aggregated_row_compiles_count(self):
        row = {"kind": "aggregated", "combinator": None, "aggregate": "count", "column_ref": "id", "operator": "<=", "value": 5}
        assert compile_ast_to_sql(_ast([row])) == "COUNT({{id}}) <= 5"


class TestCompileLowcodeBody:
    def test_simple_row_stack_is_predicate_body(self):
        body = compile_lowcode_body(_ast([_row(column_ref="amount", operator=">", value=0)]), "")
        assert body.predicate == "{{amount}} > 0"
        assert body.sql_query is None
        assert body.merge_columns is None

    def test_group_by_folds_into_sql_query(self):
        row = {"kind": "aggregated", "combinator": None, "aggregate": "count", "column_ref": "order_id", "operator": "<=", "value": 100}
        body = compile_lowcode_body(_ast([row]), "{{customer_id}}")
        assert body.predicate is None
        assert body.merge_columns == ["{{customer_id}}"]
        assert body.sql_query == (
            "SELECT {{customer_id}}, (NOT (COUNT({{order_id}}) <= 100)) AS condition "
            "FROM {{input_view}} GROUP BY {{customer_id}}"
        )

    def test_joins_only_merges_on_input_side_keys(self):
        joins = [
            {
                "join_type": "LEFT",
                "target_table": "cat.sch.dim",
                "keys": [{"joined_column": "id", "column_ref": "dim_id"}],
            }
        ]
        body = compile_lowcode_body(_ast([_row(column_ref="cat.sch.dim.valid", operator="is not null")], joins), "")
        assert body.merge_columns == ["{{dim_id}}"]
        assert body.sql_query is not None
        assert "LEFT JOIN cat.sch.dim ON cat.sch.dim.id = {{dim_id}}" in body.sql_query


class TestUsabilityAndTokens:
    def test_empty_ast_is_unusable(self):
        assert lowcode_is_usable(_ast([])) is False

    def test_single_row_ast_is_usable(self):
        assert lowcode_is_usable(_ast([_row()])) is True

    def test_extract_slot_tokens_first_appearance_order(self):
        tokens = extract_slot_tokens("{{b}} > 0 AND {{a}} < {{b}}", "SELECT {{a}}, {{c}} FROM {{input_view}}")
        # input_view is reserved and skipped; duplicates de-duped in order.
        assert tokens == ["b", "a", "c"]
