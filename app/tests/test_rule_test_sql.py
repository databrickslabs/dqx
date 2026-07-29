"""Unit tests for the pure Test-rule SQL builders (P22-E)."""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.rule_test_sql import (
    AdhocGrid,
    AdhocSource,
    TableSource,
    build_adhoc_query_sql,
    build_adhoc_sql,
    build_query_test_sql,
    build_table_sql,
    condition_passed_expr,
    find_table_refs,
    is_query_shaped,
    normalize_table_ref,
    parse_result,
    passed_expr,
    ref_cte_name,
    substitute_input_view,
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


_REF_TABLE = "main.sales.customers"
_ORPHAN_QUERY = (
    "SELECT {{order_id}}, (c.id IS NULL) AS condition "
    "FROM {{input_view}} LEFT JOIN main.sales.customers c ON c.id = {{customer_id}}"
)


class TestIsQueryShaped:
    def test_whole_select_is_a_query(self):
        assert is_query_shaped("SELECT (x) AS condition FROM {{input_view}}") is True

    def test_leading_comment_does_not_hide_the_select(self):
        assert is_query_shaped("-- checks orphans\nSELECT (x) AS condition FROM {{input_view}}") is True

    def test_bare_predicate_is_not_a_query(self):
        assert is_query_shaped("{{amount}} > 0") is False

    def test_predicate_mentioning_select_in_a_comment_is_not_a_query(self):
        assert is_query_shaped("-- SELECT was considered\n{{amount}} > 0") is False


class TestNormalizeTableRef:
    def test_plain_name_is_unchanged(self):
        assert normalize_table_ref("main.sales.customers") == "main.sales.customers"

    def test_backticks_and_spacing_are_the_authors_typing_not_the_name(self):
        assert normalize_table_ref("`main` . `sales`.`customers`") == "main.sales.customers"

    def test_two_part_name_is_kept_as_written(self):
        assert normalize_table_ref("sales.customers") == "sales.customers"

    def test_doubled_backtick_is_one_literal_backtick(self):
        assert normalize_table_ref("main.sales.`od``d`") == "main.sales.od`d"


class TestFindTableRefs:
    def test_finds_the_joined_table(self):
        assert find_table_refs(_ORPHAN_QUERY) == [_REF_TABLE]

    def test_input_view_and_bare_relations_are_not_tables(self):
        # `{{input_view}}` is the monitored data and `src` is the sample CTE —
        # neither is a table the author has to supply.
        assert find_table_refs("SELECT (x) AS condition FROM {{input_view}} JOIN src s ON s.k = 1") == []

    def test_a_quoted_reference_normalizes_to_the_same_table(self):
        query = "SELECT (x) AS condition FROM {{input_view}} JOIN `main`.`sales`.`customers` c ON c.id = 1"
        assert find_table_refs(query) == [_REF_TABLE]

    def test_repeated_reference_is_reported_once_regardless_of_case(self):
        query = (
            "SELECT (x) AS condition FROM {{input_view}} "
            "JOIN main.sales.customers c ON c.id = 1 "
            "LEFT JOIN MAIN.SALES.CUSTOMERS d ON d.id = 2"
        )
        assert find_table_refs(query) == [_REF_TABLE]

    def test_commented_out_reference_is_not_demanded(self):
        query = "SELECT (x) AS condition FROM {{input_view}}\n-- LEFT JOIN main.ref.old o ON o.id = 1"
        assert find_table_refs(query) == []

    def test_several_tables_keep_first_appearance_order(self):
        query = (
            "SELECT (x) AS condition FROM {{input_view}} " "JOIN main.ref.b b ON b.id = 1 JOIN main.ref.a a ON a.id = 2"
        )
        assert find_table_refs(query) == ["main.ref.b", "main.ref.a"]


class TestSubstituteInputView:
    def test_replaces_marker_with_sample_cte(self):
        assert substitute_input_view("FROM {{input_view}}") == "FROM src"

    def test_tolerates_inner_whitespace_like_dqx(self):
        assert substitute_input_view("FROM {{ input_view }}") == "FROM src"


class TestConditionPassedExpr:
    # A query's condition column flags a VIOLATION, and make_condition only fails
    # a row when the condition is TRUE — so NULL passes in both polarities, which
    # is what the asymmetric COALESCE defaults encode.
    def test_pass_polarity_fails_on_true_condition(self):
        assert condition_passed_expr("q.`condition`", "pass") == "(NOT COALESCE(q.`condition`, FALSE))"

    def test_fail_polarity_fails_on_false_condition(self):
        assert condition_passed_expr("q.`condition`", "fail") == "COALESCE(q.`condition`, TRUE)"


class TestBuildQueryTestSql:
    def _src(self, **kw):
        return TableSource(
            **{
                "table": "c.s.orders",
                "column_mapping": {"order_id": "order_id", "customer_id": "customer_id"},
                "sample_kind": "records",
                "sample_value": 100,
                **kw,
            }
        )

    def test_samples_monitored_table_into_the_cte(self):
        sql = build_query_test_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "WITH src AS (SELECT * FROM `c`.`s`.`orders` ORDER BY rand() LIMIT 100)" in sql

    def test_input_view_points_at_the_sample(self):
        sql = build_query_test_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "FROM src LEFT JOIN" in sql
        assert "{{input_view}}" not in sql

    def test_joined_table_stays_itself_while_column_slots_become_columns(self):
        # Table mode tests against real data, so the joined table resolves to the
        # real Unity Catalog table and needs no substitution at all.
        sql = build_query_test_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "LEFT JOIN main.sales.customers c ON c.id = `customer_id`" in sql
        assert "SELECT `order_id`," in sql

    def test_verdict_reads_the_condition_column(self):
        sql = build_query_test_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "(NOT COALESCE(q.`condition`, FALSE)) AS __passed" in sql

    def test_fail_polarity_inverts_the_verdict(self):
        sql = build_query_test_sql(_ORPHAN_QUERY, "fail", self._src())
        assert "COALESCE(q.`condition`, TRUE) AS __passed" in sql

    def test_display_cap_limits_the_outer_select(self):
        sql = build_query_test_sql(_ORPHAN_QUERY, "pass", self._src(display_cap=42))
        assert sql.rstrip().endswith("LIMIT 42")

    def test_full_sample_has_no_sample_clause(self):
        sql = build_query_test_sql(_ORPHAN_QUERY, "pass", self._src(sample_kind="full"))
        assert "ORDER BY rand()" not in sql
        assert "TABLESAMPLE" not in sql

    def test_query_without_condition_column_is_rejected(self):
        with pytest.raises(ValueError, match="condition"):
            build_query_test_sql("SELECT 1 AS x FROM {{input_view}}", "pass", self._src())

    def test_query_using_column_slots_without_the_input_view_is_rejected(self):
        # The shape a table-level author lands on: an aggregate over a column of
        # the monitored table with nothing to read it from. Spark would answer
        # UNRESOLVED_COLUMN; the builder says what is actually wrong instead.
        with pytest.raises(ValueError, match="never reads it"):
            build_query_test_sql("SELECT (COUNT_IF({{customer_id}} IS NULL) > 0) AS condition", "pass", self._src())

    def test_query_over_only_reference_tables_needs_no_input_view(self):
        sql = build_query_test_sql(
            "SELECT (COUNT(*) = 0) AS condition FROM main.sales.customers c", "pass", self._src()
        )
        assert _REF_TABLE in sql

    def test_reserved_input_view_slot_cannot_redirect_the_query(self):
        # An author who mistakenly declared {{input_view}} as a slot would other-
        # wise have the test read their bound column instead of the sample.
        src = TableSource(table="c.s.orders", column_mapping={"input_view": "elsewhere"})
        sql = build_query_test_sql("SELECT (x) AS condition FROM {{input_view}}", "pass", src)
        assert "FROM src" in sql
        assert "elsewhere" not in sql

    def test_dataset_level_aggregate_query_is_supported_unchanged(self):
        # No merge keys, one row out — the builder neither requires nor projects
        # keys, so a table-level rule tests the same way.
        query = (
            "SELECT (COUNT(*) > 0) AS condition FROM {{input_view}} "
            "LEFT JOIN main.sales.customers c ON c.id = {{customer_id}} WHERE c.id IS NULL"
        )
        sql = build_query_test_sql(query, "pass", self._src())
        assert "COUNT(*)" in sql
        assert _REF_TABLE in sql


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


class TestBuildAdhocQuerySql:
    """Manual test for a cross-table rule: every table it reads is a VALUES grid."""

    @staticmethod
    def _src(**kw: object) -> AdhocSource:
        base: dict[str, object] = {
            "columns": ["customer_id", "order_id"],
            "rows": [["CUST-1", "O-1"], ["CUST-9", "O-2"]],
            "families": {"customer_id": "text", "order_id": "text"},
            "column_mapping": {"customer_id": "customer_id", "order_id": "order_id"},
            "ref_grids": {
                _REF_TABLE: AdhocGrid(
                    columns=["id"],
                    rows=[["CUST-1"]],
                    families={"id": "text"},
                )
            },
        }
        return AdhocSource(**{**base, **kw})  # pyright: ignore[reportArgumentType]

    def test_input_grid_and_each_reference_grid_become_ctes(self):
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "WITH src AS (SELECT" in sql
        assert "`__ref_0_customers` AS (SELECT" in sql
        # input rows and reference rows both present as inline literals
        assert "'CUST-9'" in sql and "'CUST-1'" in sql

    def test_tables_and_placeholders_resolve_to_grids(self):
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "{{" not in sql
        assert "LEFT JOIN `__ref_0_customers` c" in sql
        assert "main.sales.customers" not in sql
        assert "FROM src" in sql

    def test_a_reference_written_quoted_or_spaced_still_matches_its_grid(self):
        query = (
            "SELECT (c.id IS NULL) AS condition FROM {{input_view}} "
            "LEFT JOIN `main`. `sales`.`customers` c ON c.id = {{customer_id}}"
        )
        sql = build_adhoc_query_sql(query, "pass", self._src())
        assert "LEFT JOIN `__ref_0_customers` c" in sql

    def test_a_reference_written_in_another_case_still_matches_its_grid(self):
        query = (
            "SELECT (c.id IS NULL) AS condition FROM {{input_view}} "
            "LEFT JOIN MAIN.SALES.CUSTOMERS c ON c.id = {{customer_id}}"
        )
        sql = build_adhoc_query_sql(query, "pass", self._src())
        # The CTE keeps the author's spelling; what matters is that the real table
        # no longer appears in the query.
        assert "LEFT JOIN `__ref_0_CUSTOMERS` c" in sql
        assert "MAIN.SALES.CUSTOMERS" not in sql

    def test_two_reference_tables_get_distinct_ctes(self):
        query = (
            "SELECT (a.id IS NULL) AS condition FROM {{input_view}} "
            "JOIN main.ref.a a ON a.id = {{customer_id}} JOIN main.ref.b b ON b.id = {{order_id}}"
        )
        src = self._src(
            ref_grids={
                "main.ref.a": AdhocGrid(columns=["id"], rows=[["CUST-1"]], families={}),
                "main.ref.b": AdhocGrid(columns=["id"], rows=[["O-1"]], families={}),
            }
        )
        sql = build_adhoc_query_sql(query, "pass", src)
        assert "JOIN `__ref_0_a` a" in sql
        assert "JOIN `__ref_1_b` b" in sql

    def test_reference_grid_has_no_row_idx(self):
        # A synthetic ordinal on a reference grid would leak into the rule's own
        # SELECT *; only the input grid gets one — and here not even that, since
        # the query decides which rows come back.
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "__row_idx" not in sql

    def test_verdict_reads_the_condition_column(self):
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", self._src())
        assert "(NOT COALESCE(q.`condition`, FALSE)) AS __passed" in sql

    def test_fail_polarity_inverts_the_verdict(self):
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "fail", self._src())
        assert "COALESCE(q.`condition`, TRUE) AS __passed" in sql

    def test_reference_grid_cells_are_typed(self):
        src = self._src(ref_grids={_REF_TABLE: AdhocGrid(columns=["id"], rows=[["7"]], families={"id": "numeric"})})
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", src)
        assert "TRY_CAST(`id` AS DOUBLE) AS `id`" in sql

    def test_reference_grid_cells_are_escaped(self):
        src = self._src(
            ref_grids={
                _REF_TABLE: AdhocGrid(
                    columns=["id"],
                    rows=[["'); DROP TABLE t; --"]],
                    families={"id": "text"},
                )
            }
        )
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", src)
        assert "'''); DROP TABLE t; --'" in sql

    def test_empty_reference_grid_is_a_typed_empty_relation(self):
        # Columns but no rows is legitimate — an orphan check where nothing matches.
        src = self._src(ref_grids={_REF_TABLE: AdhocGrid(columns=["id"], rows=[], families={"id": "text"})})
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", src)
        assert "WHERE 1=0" in sql

    def test_rejects_a_query_without_a_condition_column(self):
        with pytest.raises(ValueError, match="condition"):
            build_adhoc_query_sql("SELECT * FROM {{input_view}}", "pass", self._src(ref_grids={}))

    def test_rejects_a_query_using_column_slots_without_the_input_view(self):
        with pytest.raises(ValueError, match="never reads it"):
            build_adhoc_query_sql(
                "SELECT (COUNT_IF({{customer_id}} IS NULL) > 0) AS condition", "pass", self._src(ref_grids={})
            )

    def test_rejects_a_reference_table_with_no_grid(self):
        # Left unresolved, the reference would still point at the REAL table, so a
        # "manual" test would quietly run half on real data.
        with pytest.raises(ValueError, match="main.sales.customers"):
            build_adhoc_query_sql(_ORPHAN_QUERY, "pass", self._src(ref_grids={}))

    def test_rejects_a_reference_grid_with_no_columns(self):
        # Joining a grid with nothing in it compares against nothing, so every row
        # would silently "pass".
        src = self._src(ref_grids={_REF_TABLE: AdhocGrid(columns=[], rows=[], families={})})
        with pytest.raises(ValueError, match="main.sales.customers"):
            build_adhoc_query_sql(_ORPHAN_QUERY, "pass", src)

    def test_a_grid_for_a_table_the_query_never_reads_is_ignored(self):
        # The author edited the JOIN away; the stale grid must not become a CTE.
        src = self._src(
            ref_grids={
                _REF_TABLE: AdhocGrid(columns=["id"], rows=[["CUST-1"]], families={}),
                "main.ref.stale": AdhocGrid(columns=["id"], rows=[["X"]], families={}),
            }
        )
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", src)
        assert "stale" not in sql

    def test_input_view_cannot_be_hijacked_by_a_declared_slot(self):
        # {{input_view}} is reserved for the grid under test; a slot claiming that
        # name must not redirect it.
        src = self._src(
            column_mapping={"customer_id": "customer_id", "order_id": "order_id", "input_view": "nope"},
        )
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", src)
        assert "FROM src" in sql
        assert "nope" not in sql

    def test_a_reference_table_named_src_cannot_shadow_the_input_grid(self):
        query = "SELECT (COUNT(*) = 0) AS condition FROM {{input_view}} JOIN main.ref.src r ON r.k = {{customer_id}}"
        src = self._src(
            columns=["customer_id"],
            rows=[["CUST-1"]],
            column_mapping={"customer_id": "customer_id"},
            ref_grids={"main.ref.src": AdhocGrid(columns=["k"], rows=[["CUST-1"]], families={})},
        )
        sql = build_adhoc_query_sql(query, "pass", src)
        assert "JOIN `__ref_0_src` r" in sql
        assert "FROM src" in sql

    def test_dataset_level_aggregate_query_is_left_intact(self):
        query = (
            "SELECT (COUNT(*) > 0) AS condition FROM {{input_view}} "
            "JOIN main.sales.customers c ON c.id = {{customer_id}}"
        )
        sql = build_adhoc_query_sql(query, "pass", self._src())
        assert "COUNT(*) > 0" in sql
        assert "LIMIT 5000" in sql

    def test_display_cap_is_applied(self):
        sql = build_adhoc_query_sql(_ORPHAN_QUERY, "pass", self._src(display_cap=10))
        assert sql.endswith("LIMIT 10")


class TestRefCteName:
    def test_names_by_position_and_table_name(self):
        assert ref_cte_name(0, "main.sales.customers") == "`__ref_0_customers`"

    def test_exotic_characters_in_the_name_are_folded_away(self):
        assert ref_cte_name(1, "main.sales.cust omers!") == "`__ref_1_cust_omers_`"

    def test_position_keeps_same_named_tables_apart(self):
        assert ref_cte_name(0, "a.b.t") != ref_cte_name(1, "c.d.t")


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
