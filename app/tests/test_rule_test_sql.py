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

    def test_rejects_backtick_in_column_identifier(self):
        # A backtick is the quoting delimiter and is never a legitimate column
        # name — validate_identifier rejects it rather than relying on doubling.
        with pytest.raises(ValueError):
            substitute_slots("{{c}}", {"c": "we`ird"})

    def test_rejects_backslash_in_column_identifier(self):
        with pytest.raises(ValueError):
            substitute_slots("{{c}}", {"c": "col\\"})

    def test_allows_exotic_but_safe_column_identifier(self):
        # Spaces / hyphens / quotes are legitimate UC column characters and stay
        # inside the backtick quoting unharmed.
        assert substitute_slots("{{c}}", {"c": "my col-1"}) == "`my col-1`"

    def test_unmapped_placeholder_left_untouched(self):
        assert substitute_slots("{{a}} {{b}}", {"a": "x"}) == "`x` {{b}}"


class TestPassedExpr:
    def test_pass_polarity_is_identity(self):
        assert passed_expr("col > 0", "pass") == "(col > 0)"

    def test_fail_polarity_negates(self):
        assert passed_expr("col IS NULL", "fail") == "(NOT (col IS NULL))"

    def test_blank_filter_is_no_op(self):
        assert passed_expr("col > 0", "pass", "") == "(col > 0)"
        assert passed_expr("col > 0", "pass", None) == "(col > 0)"

    def test_filter_wraps_verdict_in_three_state_case(self):
        # A row the filter excludes (FALSE or NULL) is NOT evaluated -> NULL
        # verdict; an in-filter row keeps the exact pass/fail verdict.
        out = passed_expr("col > 0", "pass", "region = 'US'")
        assert out == "CASE WHEN NOT (region = 'US') OR (region = 'US') IS NULL THEN NULL ELSE (col > 0) END"

    def test_filter_with_fail_polarity(self):
        out = passed_expr("col IS NULL", "fail", "active")
        assert out == "CASE WHEN NOT (active) OR (active) IS NULL THEN NULL ELSE (NOT (col IS NULL)) END"


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

    def test_filter_slots_substituted_and_three_state(self):
        # The filter's {{slot}} placeholders are substituted the same way the
        # predicate's are, and the verdict becomes a three-state CASE.
        sql = build_table_sql(
            "{{col}} > 0",
            "pass",
            TableSource(table="c.s.t", column_mapping={"col": "amount", "reg": "region"}),
            row_filter="{{reg}} = 'US'",
        )
        assert "CASE WHEN NOT (`region` = 'US') OR (`region` = 'US') IS NULL THEN NULL ELSE (`amount` > 0) END AS __passed" in sql


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

    def test_trailing_backslash_is_doubled(self):
        # P22-E SQL-injection PoC: a cell ending in a backslash must NOT be able
        # to escape its own closing quote and splice the NEXT cell as raw SQL.
        # A single trailing backslash is doubled, so the literal stays closed and
        # the following cell is an inert quoted literal.
        src = AdhocSource(
            columns=["a", "b"],
            rows=[["foo\\", "'); DROP TABLE t; --"]],
            families={"a": "text", "b": "text"},
            column_mapping={"a": "a", "b": "b"},
        )
        sql = build_adhoc_sql("{{a}} IS NOT NULL", "pass", src)
        # backslash doubled → closing quote intact
        assert r"'foo\\'" in sql
        # the injection payload survives only as an escaped quoted literal
        assert "'''); DROP TABLE t; --'" in sql

    def test_quote_and_backslash_heavy_value(self):
        src = AdhocSource(columns=["a"], rows=[["O'Brien\\"]], families={"a": "text"}, column_mapping={"a": "a"})
        sql = build_adhoc_sql("{{a}} IS NOT NULL", "pass", src)
        assert r"'O''Brien\\'" in sql

    def test_control_chars_escaped_or_stripped(self):
        src = AdhocSource(
            columns=["a"],
            rows=[["line1\nline2\ttab\x00\x07bell"]],
            families={"a": "text"},
            column_mapping={"a": "a"},
        )
        sql = build_adhoc_sql("{{a}} IS NOT NULL", "pass", src)
        # newline/tab re-emitted as escape sequences; no raw control byte remains.
        assert r"line1\nline2\ttab" in sql
        assert "\x00" not in sql and "\x07" not in sql

    def test_rejects_invalid_column_identifier(self):
        src = AdhocSource(columns=["ev`il"], rows=[["1"]], families={}, column_mapping={"ev`il": "ev`il"})
        with pytest.raises(ValueError):
            build_adhoc_sql("{{ev`il}} IS NOT NULL", "pass", src)

    def test_filter_produces_three_state_verdict(self):
        src = AdhocSource(
            columns=["amount", "region"],
            rows=[["5", "US"], ["-3", "US"], ["9", "EU"]],
            families={"amount": "numeric", "region": "text"},
            column_mapping={"amount": "amount", "region": "region"},
        )
        sql = build_adhoc_sql("{{amount}} > 0", "pass", src, row_filter="{{region}} = 'US'")
        assert (
            "CASE WHEN NOT (`region` = 'US') OR (`region` = 'US') IS NULL THEN NULL ELSE (`amount` > 0) END AS __passed"
            in sql
        )


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

    def test_null_verdict_from_filtered_out_row_maps_to_none(self):
        # A row excluded by the rule's ROW FILTER carries a SQL NULL verdict; it
        # must map to None (not False) so the grid leaves it untinted.
        rows = [
            {"amount": "5", "__row_idx": "0", "__passed": "true"},
            {"amount": "9", "__row_idx": "2", "__passed": None},
            {"amount": "-3", "__row_idx": "3", "__passed": "null"},
        ]
        result = parse_result(rows, display_cap=5000)
        assert result.rows[0].passed is True
        assert result.rows[1].passed is None
        assert result.rows[2].passed is None

    def test_truncated_when_at_cap(self):
        rows = [{"c": "x", "__passed": "true"}]
        assert parse_result(rows, display_cap=1).truncated is True
        assert parse_result(rows, display_cap=2).truncated is False

    def test_empty_result(self):
        result = parse_result([], display_cap=5000)
        assert result.columns == []
        assert result.rows == []
