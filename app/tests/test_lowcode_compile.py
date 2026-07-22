"""Unit tests for the Python low-code compiler port (B2-132).

Mirrors the load-bearing cases in ``ui/lib/lowcodeCompile.test.ts`` so the
backend port stays faithful to the TypeScript source the visual builder uses:
the compiled ``body`` is what materializes and runs, so a wrong shape here
would ship a broken AI-proposed low-code rule.
"""

from __future__ import annotations

from typing import Any

import pytest

from databricks_labs_dqx_app.backend.lowcode_compile import (
    OPERATORS_BY_FAMILY,
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


class TestColumnRefValue:
    """A row value of ``{"$col": "<column>"}`` compiles the RHS to a column
    reference (item 42), mirroring ``valueSql`` in ``lowcodeCompile.ts`` — so a
    col-vs-col comparison yields ``{{a}} < {{b}}``, never a blank RHS."""

    def test_comparison_rhs_is_slot_placeholder(self):
        sql = compile_ast_to_sql(_ast([_row(column_ref="a", operator="<", value={"$col": "b"})]))
        assert sql == "{{a}} < {{b}}"

    def test_qualified_rhs_column_passes_through_raw(self):
        sql = compile_ast_to_sql(_ast([_row(column_ref="a", operator="=", value={"$col": "orders.total"})]))
        assert sql == "{{a}} = orders.total"

    def test_between_bounds_may_be_column_refs(self):
        sql = compile_ast_to_sql(
            _ast([_row(column_ref="x", operator="between", value=[{"$col": "lo"}, {"$col": "hi"}])])
        )
        assert sql == "{{x}} BETWEEN {{lo}} AND {{hi}}"

    def test_in_list_may_mix_columns_and_literals(self):
        sql = compile_ast_to_sql(_ast([_row(column_ref="s", operator="in", value=[{"$col": "b"}, "x"])]))
        assert sql == "{{s}} IN ({{b}}, 'x')"

    def test_rhs_column_slot_is_extracted_as_token(self):
        body = compile_lowcode_body(_ast([_row(column_ref="a", operator="<", value={"$col": "b"})]), "")
        assert body.predicate == "{{a}} < {{b}}"
        assert extract_slot_tokens(body.predicate) == ["a", "b"]


class TestExpandedOperatorCatalog:
    """The DQ-steward operator additions must compile identically to the
    frontend (``ui/lib/lowcodeCompile.ts``) so an AI-proposed rule using one
    round-trips through the visual builder unchanged."""

    @pytest.mark.parametrize(
        ("operator", "value", "expected"),
        [
            ("has length", 5, "length({{c}}) = 5"),
            ("is longer than", 3, "length({{c}}) > 3"),
            ("is shorter than", 8, "length({{c}}) < 8"),
            ("length between", [2, 4], "length({{c}}) BETWEEN 2 AND 4"),
            ("is not empty", None, "length(trim({{c}})) > 0"),
            ("is empty", None, "length(trim({{c}})) = 0"),
            ("does not match regex", "^a$", "NOT ({{c}} RLIKE '^a$')"),
            ("contains only digits", None, "{{c}} RLIKE '^[0-9]+$'"),
            ("is uppercase", None, "{{c}} = upper({{c}})"),
            ("is lowercase", None, "{{c}} = lower({{c}})"),
            ("is positive", None, "{{c}} > 0"),
            ("is negative", None, "{{c}} < 0"),
            ("is non-negative", None, "{{c}} >= 0"),
            ("is a whole number", None, "{{c}} = round({{c}})"),
            ("is a multiple of", 5, "mod({{c}}, 5) = 0"),
            ("is in the future", None, "{{c}} > current_timestamp()"),
            ("is in the past", None, "{{c}} < current_timestamp()"),
            ("is today", None, "to_date({{c}}) = current_date()"),
            ("has positive sentiment", None, "ai_analyze_sentiment({{c}}) = 'positive'"),
            ("has negative sentiment", None, "ai_analyze_sentiment({{c}}) = 'negative'"),
            (
                "passes luhn check",
                None,
                "length(regexp_replace({{c}}, '[^0-9]', '')) > 0 AND luhn_check(regexp_replace({{c}}, '[^0-9]', ''))",
            ),
        ],
    )
    def test_operator_compiles_to_expected_sql(self, operator, value, expected):
        assert compile_ast_to_sql(_ast([_row(column_ref="c", operator=operator, value=value)])) == expected

    def test_uuid_and_ipv4_regexes(self):
        assert compile_ast_to_sql(_ast([_row(column_ref="c", operator="is a valid uuid")])) == (
            "{{c}} RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'"
        )
        assert compile_ast_to_sql(_ast([_row(column_ref="c", operator="is a valid ipv4")])) == (
            "{{c}} RLIKE '^((25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])$'"
        )

    def test_every_catalog_operator_compiles_to_non_empty_sql(self):
        """No operator advertised to the AI can be one the compiler drops to ''
        (which would silently produce a broken rule)."""

        def sample(op: str) -> Any:
            if op in ("between", "length between"):
                return [1, 2]
            if op in ("in", "not in"):
                return ["a"]
            if op == "is in last":
                return {"number": 1, "unit": "days"}
            if op in ("is a valid", "is not a valid"):
                return "int"
            if op in ("has length", "is longer than", "is shorter than", "is a multiple of"):
                return 3
            return "x"

        seen = {op for ops in OPERATORS_BY_FAMILY.values() for op in ops}
        for op in seen:
            sql = compile_ast_to_sql(_ast([_row(column_ref="c", operator=op, value=sample(op))]))
            assert sql, f"operator {op!r} compiled to empty SQL"


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
        # merge_columns stay BARE — the library passes them verbatim to Spark's
        # df.join(on=...); an {{input_view}}-qualified value would be an invalid
        # bare identifier at runtime.
        assert body.merge_columns == ["{{dim_id}}"]
        assert body.sql_query is not None
        # ON own-side qualified to the input view (query text; unambiguous under join).
        assert "LEFT JOIN cat.sch.dim ON cat.sch.dim.id = {{input_view}}.{{dim_id}}" in body.sql_query
        # SELECT projects the qualified source aliased back to the bare merge name.
        assert "SELECT {{input_view}}.{{dim_id}} AS {{dim_id}}," in body.sql_query


class TestJoinColumnQualification:
    """Own-table columns are qualified to the input view when a rule has joins,
    so a joined table sharing the column name doesn't cause AMBIGUOUS_REFERENCE —
    WITHOUT breaking ``merge_columns`` (which must stay bare). Regression for the
    reported ``[AMBIGUOUS_REFERENCE] customer_id`` and the v1 fix's
    ``[INVALID_IDENTIFIER] {{input_view}}.customer_id`` follow-on.
    """

    _JOIN = [
        {
            "join_type": "INNER",
            "target_table": "dqx.dqx_studio_demo.customers",
            "keys": [{"joined_column": "customer_id", "column_ref": "customer_id"}],
        }
    ]

    def test_no_join_predicate_is_byte_identical(self):
        # Without joins nothing is qualified — the predicate stays exactly as before.
        body = compile_lowcode_body(_ast([_row(column_ref="amount", operator=">", value=0)]), "")
        assert body.predicate == "{{amount}} > 0"
        assert body.sql_query is None
        assert "input_view" not in (body.predicate or "")

    def test_join_predicate_own_column_qualified(self):
        body = compile_lowcode_body(_ast([_row(column_ref="customer_id", operator=">", value=0)], self._JOIN), "")
        assert body.sql_query is not None
        # predicate own column qualified to the input view
        assert "NOT ({{input_view}}.{{customer_id}} > 0)" in body.sql_query
        # ON own-side qualified; joined side raw
        assert (
            "ON dqx.dqx_studio_demo.customers.customer_id = {{input_view}}.{{customer_id}}" in body.sql_query
        )
        # SELECT projects qualified source aliased back to bare merge name
        assert "SELECT {{input_view}}.{{customer_id}} AS {{customer_id}}," in body.sql_query
        # merge_columns BARE
        assert body.merge_columns == ["{{customer_id}}"]

    def test_join_joined_table_column_stays_raw(self):
        body = compile_lowcode_body(
            _ast([_row(column_ref="dqx.dqx_studio_demo.customers.tier", operator="=", value="gold")], self._JOIN),
            "",
        )
        assert body.sql_query is not None
        assert "dqx.dqx_studio_demo.customers.tier" in body.sql_query
        # never double-qualify a dotted column
        assert "{{input_view}}.dqx.dqx_studio_demo.customers.tier" not in body.sql_query

    def test_col_ref_value_qualified_under_join(self):
        # item 42: a $col value on the RHS is an own column too -> qualified under a join.
        body = compile_lowcode_body(
            _ast([_row(column_ref="start_date", operator="before", value={"$col": "end_date"})], self._JOIN),
            "",
        )
        assert body.sql_query is not None
        assert "{{input_view}}.{{start_date}} < {{input_view}}.{{end_date}}" in body.sql_query

    @staticmethod
    def _simulate_substitution(sql_query: str, merge_columns: list[str]) -> tuple[str, list[str]]:
        """Reproduce BOTH runtime substitution layers to prove the final SQL is valid.

        Layer 1 (app materializer ``_substitute_text``): replace ``{{col}}`` slots
        in the query text AND merge_columns; it does NOT touch ``{{input_view}}``.
        Layer 2 (DQX library ``sql_query`` ``_replace_template``): replace
        ``{{input_view}}`` in the query TEXT ONLY. This is the end-to-end check the
        v1 fix lacked — v1 passed compiler-output unit tests but produced an
        invalid ``merge_columns`` at runtime.
        """
        import re

        cols = ["customer_id", "start_date", "end_date", "region"]

        def app(text: str) -> str:
            for c in cols:
                text = text.replace("{{" + c + "}}", c)
            return text

        q1 = app(sql_query)
        mc1 = [app(c) for c in merge_columns]
        view = "customer_id_query_condition_violation_input_view_deadbeef"
        q2 = re.sub(r"\{\{\s*input_view\s*\}\}", view, q1)
        return q2, mc1

    def test_end_to_end_substitution_yields_valid_sql(self):
        body = compile_lowcode_body(_ast([_row(column_ref="customer_id", operator=">", value=0)], self._JOIN), "")
        assert body.sql_query is not None
        final_query, final_merge = self._simulate_substitution(body.sql_query, body.merge_columns or [])
        # No residual placeholders anywhere in the executed query.
        assert "{{" not in final_query and "}}" not in final_query
        # merge_columns are BARE column names (valid for df.select / df.join(on=)).
        assert final_merge == ["customer_id"]
        assert all("{{" not in c and "." not in c for c in final_merge)
        # Own column resolved qualified against the unique view (no ambiguity).
        assert "customer_id_query_condition_violation_input_view_deadbeef.customer_id" in final_query


class TestUsabilityAndTokens:
    def test_empty_ast_is_unusable(self):
        assert lowcode_is_usable(_ast([])) is False

    def test_single_row_ast_is_usable(self):
        assert lowcode_is_usable(_ast([_row()])) is True

    def test_extract_slot_tokens_first_appearance_order(self):
        tokens = extract_slot_tokens("{{b}} > 0 AND {{a}} < {{b}}", "SELECT {{a}}, {{c}} FROM {{input_view}}")
        # input_view is reserved and skipped; duplicates de-duped in order.
        assert tokens == ["b", "a", "c"]
