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
            "1.b.c",  # part starting with digit
            "a.b.c d",  # space
            "a.b.c;DROP",  # semicolon → injection attempt
            "a.b.c'd",  # single quote
            "a.b./*x*/c",  # block comment
            "a.b.c)union",
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
