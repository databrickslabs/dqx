"""Tests for ``sql_utils`` — the front line of SQL/identifier injection defence."""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.sql_utils import (
    escape_sql_string,
    quote_fqn,
    validate_entity_type,
    validate_fqn,
    validate_schedule_name,
)


# ---------------------------------------------------------------------------
# escape_sql_string
# ---------------------------------------------------------------------------


class TestEscapeSqlString:
    def test_no_quotes_passes_through(self):
        assert escape_sql_string("hello world") == "hello world"

    def test_single_quote_is_doubled(self):
        assert escape_sql_string("O'Brien") == "O''Brien"

    def test_multiple_single_quotes(self):
        assert escape_sql_string("'a'b'c'") == "''a''b''c''"

    def test_classic_injection_payload(self):
        # Closing the outer quote is what makes injection work; doubling it
        # turns the would-be terminator into a literal apostrophe.
        payload = "'; DROP TABLE users; --"
        assert escape_sql_string(payload) == "''; DROP TABLE users; --"

    def test_does_not_touch_double_quotes_backslashes_or_unicode(self):
        # Databricks SQL uses doubled '' for escaping, never backslash; this
        # function must not mutate anything else.
        s = '\\\\""\u200b\n\t'
        assert escape_sql_string(s) == s

    def test_empty_string_round_trips(self):
        assert escape_sql_string("") == ""


# ---------------------------------------------------------------------------
# validate_fqn / quote_fqn
# ---------------------------------------------------------------------------


class TestValidateFqn:
    @pytest.mark.parametrize(
        "fqn",
        [
            "main.default.t",
            "my_catalog.my_schema.my_table",
            "a.b.c",
            "_underscore.start_ok.end_ok",
            "with-hyphens.also-ok.also-ok",
            "`backticked`.`parts`.`work_too`",
            "1.b.c",  # leading digit — legal for a backtick-quoted UC name
            "a.b.c d",  # space — legal for a backtick-quoted UC name
            "a.b.c;DROP",  # semicolon is inert once backtick-quoted, not SQL syntax
            # Real-world regression: Unity Catalog schema/table names created via
            # the REST API (bypassing the SQL parser) can contain literal quote
            # characters, e.g. a schema genuinely named "'ftr_mv_test'" — see
            # https://github.com/databrickslabs/dqx (Add table dialog bug report).
            "main.'ftr_mv_test'.'ftr_gold_mv_bkp'",
            "a.b.c'd",  # single quote — inert inside backticks, not a string literal
            "a.b./*x*/c",  # block-comment markers are just literal identifier text
            "a.b.c)union",  # parens/keywords are just literal identifier text
        ],
    )
    def test_valid_three_part_names(self, fqn):
        assert validate_fqn(fqn) == fqn

    def test_sql_check_prefix_is_allowed(self):
        assert validate_fqn("__sql_check__/my_check_name") == "__sql_check__/my_check_name"

    @pytest.mark.parametrize(
        "fqn",
        [
            "",
            "a",
            "a.b",
            "a.b.c.d",
            "a..b",
            "a.b.",
            ".b.c",
            "a.b.c`d",  # raw backtick — would break out of quote_fqn's identifier quoting
            "a.b.`",  # part is only a backtick — unwraps to empty
            "a.b.c\ninjected",  # newline — log injection (CWE-117)
            "a.b.c\x00d",  # NUL byte
            "a." + "x" * 256 + ".c",  # exceeds Unity Catalog's identifier length limit
            "__sql_check__/contains spaces",
            "__sql_check__/has;semi",
        ],
    )
    def test_invalid_fqns_raise(self, fqn):
        with pytest.raises(ValueError):
            validate_fqn(fqn)


class TestQuoteFqn:
    def test_wraps_each_part_in_backticks(self):
        assert quote_fqn("a.b.c") == "`a`.`b`.`c`"

    def test_strips_existing_backticks_before_re_quoting(self):
        assert quote_fqn("`a`.`b`.`c`") == "`a`.`b`.`c`"

    def test_handles_two_part_view_names(self):
        # quote_fqn doesn't validate; it's purely a quoter. Some callers pass
        # already-validated multi-part names plus a leaf, e.g. for tmp views.
        assert quote_fqn("a.b") == "`a`.`b`"

    def test_quotes_identifier_with_embedded_single_quotes(self):
        # Regression: a real Unity Catalog schema/table name containing
        # literal single quotes (e.g. "'ftr_mv_test'") must round-trip
        # through validate_fqn + quote_fqn and stay backtick-quoted as one
        # opaque identifier — the quotes are ordinary characters here, not
        # SQL string-literal delimiters.
        fqn = "main.'ftr_mv_test'.'ftr_gold_mv_bkp'"
        assert validate_fqn(fqn) == fqn
        assert quote_fqn(fqn) == "`main`.`'ftr_mv_test'`.`'ftr_gold_mv_bkp'`"

    def test_embedded_backtick_is_doubled_as_defense_in_depth(self):
        # validate_fqn() rejects raw backticks before this is ever reached in
        # production, but quote_fqn() must not silently produce a broken (or
        # injectable) identifier if it were ever called on unvalidated input.
        assert quote_fqn("a.weird`name.c") == "`a`.`weird``name`.`c`"


# ---------------------------------------------------------------------------
# validate_schedule_name
# ---------------------------------------------------------------------------


class TestValidateScheduleName:
    @pytest.mark.parametrize("name", ["nightly", "weekly_qc", "abc-123", "A" * 64])
    def test_valid(self, name):
        assert validate_schedule_name(name) == name

    @pytest.mark.parametrize(
        "name",
        ["", " ", "has spaces", "with;semi", "has'quote", "A" * 65, "name.with.dots"],
    )
    def test_invalid_raises(self, name):
        with pytest.raises(ValueError):
            validate_schedule_name(name)


# ---------------------------------------------------------------------------
# validate_entity_type
# ---------------------------------------------------------------------------


class TestValidateEntityType:
    def test_valid_passes_through(self):
        assert validate_entity_type("run", {"run", "rule"}) == "run"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            validate_entity_type("admin", {"run", "rule"})

    def test_empty_string_invalid(self):
        with pytest.raises(ValueError):
            validate_entity_type("", {"run", "rule"})
