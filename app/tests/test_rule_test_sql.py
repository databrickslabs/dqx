"""Unit tests for the pure Test-rule SQL builders (P22-E)."""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.rule_test_sql import (
    AdhocSource,
    TableSource,
    build_adhoc_sql,
    build_table_sql,
    parse_result,
    passed_expr,
    substitute_slots,
)


class TestSubstituteSlots:
    def test_replaces_placeholder_with_quoted_column(self):
        assert substitute_slots("{{amount}} > 0", {"amount": "price"}) == "`price` > 0"

    def test_multiple_slots(self):
        out = substitute_slots("{{a}} < {{b}}", {"a": "lo", "b": "hi"})
        assert out == "`lo` < `hi`"

    def test_repeated_placeholder(self):
        assert substitute_slots("{{c}} = {{c}}", {"c": "x"}) == "`x` = `x`"

    def test_doubles_internal_backtick(self):
        assert substitute_slots("{{c}}", {"c": "we`ird"}) == "`we``ird`"

    def test_unmapped_placeholder_left_untouched(self):
        assert substitute_slots("{{a}} {{b}}", {"a": "x"}) == "`x` {{b}}"


class TestPassedExpr:
    def test_pass_polarity_is_identity(self):
        assert passed_expr("col > 0", "pass") == "(col > 0)"

    def test_fail_polarity_negates(self):
        assert passed_expr("col IS NULL", "fail") == "(NOT (col IS NULL))"


class TestBuildTableSql:
    def test_records_sample_orders_by_rand(self):
        sql = build_table_sql(
            "{{col}} > 0",
            "pass",
            TableSource(table="c.s.t", column_mapping={"col": "amount"}, sample_kind="records", sample_value=1000),
        )
        assert "FROM `c`.`s`.`t` ORDER BY rand() LIMIT 1000" in sql
        assert "(`amount` > 0) AS __passed" in sql

    def test_percent_sample_uses_tablesample(self):
        sql = build_table_sql(
            "{{col}} > 0",
            "fail",
            TableSource(table="c.s.t", column_mapping={"col": "amount"}, sample_kind="percent", sample_value=10),
        )
        assert "TABLESAMPLE (10 PERCENT)" in sql
        assert "(NOT (`amount` > 0)) AS __passed" in sql

    def test_full_sample_has_no_sample_clause(self):
        sql = build_table_sql(
            "{{col}} > 0",
            "pass",
            TableSource(table="c.s.t", column_mapping={"col": "amount"}, sample_kind="full"),
        )
        assert "TABLESAMPLE" not in sql
        assert "ORDER BY rand()" not in sql

    def test_display_cap_applied(self):
        sql = build_table_sql(
            "{{col}} > 0",
            "pass",
            TableSource(table="c.s.t", column_mapping={"col": "amount"}, display_cap=42),
        )
        assert sql.rstrip().endswith("LIMIT 42")

    def test_invalid_fqn_raises(self):
        with pytest.raises(ValueError):
            build_table_sql("{{col}} > 0", "pass", TableSource(table="not_a_fqn", column_mapping={"col": "a"}))


class TestBuildAdhocSql:
    def test_row_idx_and_values(self):
        src = AdhocSource(
            columns=["amount"],
            rows=[["5"], ["-3"]],
            families={"amount": "numeric"},
            column_mapping={"amount": "amount"},
        )
        sql = build_adhoc_sql("{{amount}} > 0", "pass", src)
        assert "VALUES (0, '5'), (1, '-3')" in sql
        assert "TRY_CAST(`amount` AS DOUBLE) AS `amount`" in sql
        assert "CAST(__row_idx AS BIGINT) AS __row_idx" in sql
        assert "(`amount` > 0) AS __passed" in sql

    def test_null_and_empty_cells_become_null(self):
        src = AdhocSource(columns=["c"], rows=[[None], [""]], families={"c": "text"}, column_mapping={"c": "c"})
        sql = build_adhoc_sql("{{c}} IS NOT NULL", "pass", src)
        assert "(0, NULL)" in sql
        assert "(1, NULL)" in sql

    def test_ragged_rows_are_padded_and_trimmed(self):
        src = AdhocSource(
            columns=["a", "b"],
            rows=[["1"], ["1", "2", "3"]],
            families={},
            column_mapping={"a": "a", "b": "b"},
        )
        sql = build_adhoc_sql("{{a}} = {{b}}", "pass", src)
        # short row padded with NULL for b; overflow (3) dropped
        assert "(0, '1', NULL)" in sql
        assert "(1, '1', '2')" in sql

    def test_empty_rows_produce_where_false_shape(self):
        src = AdhocSource(columns=["a"], rows=[], families={}, column_mapping={"a": "a"})
        sql = build_adhoc_sql("{{a}} > 0", "pass", src)
        assert "WHERE 1=0" in sql

    def test_single_quotes_escaped(self):
        src = AdhocSource(columns=["a"], rows=[["O'Brien"]], families={"a": "text"}, column_mapping={"a": "a"})
        sql = build_adhoc_sql("{{a}} IS NOT NULL", "pass", src)
        assert "'O''Brien'" in sql


class TestParseResult:
    def test_strips_hidden_columns_and_reads_verdict(self):
        rows = [
            {"amount": "5", "__row_idx": "0", "__passed": "true"},
            {"amount": "-3", "__row_idx": "1", "__passed": "false"},
        ]
        result = parse_result(rows, display_cap=5000)
        assert result.columns == ["amount"]
        assert result.rows[0].cells == {"amount": "5"}
        assert result.rows[0].passed is True
        assert result.rows[0].row_idx == 0
        assert result.rows[1].passed is False
        assert result.rows[1].row_idx == 1

    def test_table_mode_has_no_row_idx(self):
        rows = [{"amount": "5", "__passed": "true"}]
        result = parse_result(rows, display_cap=5000)
        assert result.rows[0].row_idx is None

    def test_bool_verdict_coerced(self):
        rows = [{"c": "x", "__passed": True}]
        result = parse_result(rows, display_cap=5000)
        assert result.rows[0].passed is True

    def test_truncated_when_at_cap(self):
        rows = [{"c": "x", "__passed": "true"}]
        assert parse_result(rows, display_cap=1).truncated is True
        assert parse_result(rows, display_cap=2).truncated is False

    def test_empty_result(self):
        result = parse_result([], display_cap=5000)
        assert result.columns == []
        assert result.rows == []
