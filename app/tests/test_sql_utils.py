"""Tests for ``sql_utils`` — the front line of SQL/identifier injection defence."""

from __future__ import annotations

import json

import pytest

from databricks.labs.dqx.utils import is_sql_query_safe

from databricks_labs_dqx_app.backend.sql_utils import (
    escape_json_for_sql_string_literal,
    escape_sql_string,
    fqn_needs_quoting,
    quote_fqn,
    quote_ident,
    quote_object_fqn,
    strip_sql_line_comments,
    validate_entity_type,
    validate_fqn,
    validate_object_id,
    validate_schedule_name,
)


class TestStripSqlLineComments:
    """The comment stripper feeds the app-side SQL-safety gates (item 6)."""

    def test_leading_line_comment_removed_predicate_and_newline_kept(self):
        assert strip_sql_line_comments("-- explanation\n{{email}} IS NOT NULL") == "\n{{email}} IS NOT NULL"

    def test_multiple_leading_comment_lines_removed(self):
        assert strip_sql_line_comments("-- one\n-- two\n\n{{a}} > 0") == "\n\n\n{{a}} > 0"

    def test_comment_prose_with_forbidden_word_is_removed(self):
        stripped = strip_sql_line_comments("-- this deletes and updates rows\n{{a}} > 0")
        assert stripped == "\n{{a}} > 0"
        # And the whole point: a comment-bearing predicate now passes the gate.
        assert is_sql_query_safe(strip_sql_line_comments("-- delete old rows\n{{a}} > 0")) is True

    def test_trailing_line_comment_removed(self):
        assert strip_sql_line_comments("{{a}} > 0 -- note") == "{{a}} > 0 "

    def test_block_comment_removed(self):
        assert strip_sql_line_comments("{{a}} /* inline */ > 0") == "{{a}}  > 0"

    def test_security_dashes_inside_string_literal_are_not_a_comment(self):
        # ` OR DROP` after the string literal is live SQL and must survive so the
        # keyword scan still rejects it.
        sql = "{{a}} = 'a--b' OR DROP"
        assert strip_sql_line_comments(sql) == sql
        assert is_sql_query_safe(strip_sql_line_comments(sql)) is False

    def test_security_doubled_quote_escape_keeps_string_region(self):
        sql = "{{a}} = 'O''Brien -- x' AND {{b}} > 0"
        assert strip_sql_line_comments(sql) == sql

    def test_security_dashes_inside_backtick_identifier_not_a_comment(self):
        sql = "`weird--col` > 0"
        assert strip_sql_line_comments(sql) == sql

    def test_live_keyword_after_comment_line_survives(self):
        # The comment line is dropped but the next-line DROP remains -> rejected.
        stripped = strip_sql_line_comments("-- comment\nDROP TABLE x")
        assert stripped == "\nDROP TABLE x"
        assert is_sql_query_safe(stripped) is False

    def test_plain_predicate_unchanged(self):
        assert strip_sql_line_comments("{{a}} BETWEEN 1 AND 10") == "{{a}} BETWEEN 1 AND 10"


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


class TestEscapeJsonForSqlStringLiteral:
    def test_doubles_json_backslash_escapes_before_quoting(self):
        payload = json.dumps({"body": {"sql_query": "SELECT a\nFROM b"}})
        escaped = escape_json_for_sql_string_literal(payload)
        assert "\\n" in payload
        assert "\\\\n" in escaped
        assert escaped == escape_sql_string(payload.replace("\\", "\\\\"))

    def test_still_doubles_single_quotes(self):
        payload = json.dumps({"message": "it's fine"})
        assert escape_json_for_sql_string_literal(payload) == escape_sql_string(payload.replace("\\", "\\\\"))


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
            "a.b.c\\",  # trailing backslash — would escape the closing quote in a SQL string literal
            "a.b.c\\d",  # mid-part backslash — Delta treats it as a string-literal escape char
            "a.b.\\",  # part is only a backslash
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

    def test_quote_ident_quotes_a_single_part(self):
        assert quote_ident("prod-east") == "`prod-east`"

    def test_quote_ident_strips_one_existing_backtick_layer(self):
        assert quote_ident("`prod-east`") == "`prod-east`"

    def test_quote_ident_never_resplits_a_dotted_part(self):
        # The reason quote_ident exists: an exotic part containing a dot
        # must stay ONE identifier — quote_fqn would split it in two.
        assert quote_ident("team.data") == "`team.data`"

    def test_quote_ident_doubles_embedded_backticks(self):
        assert quote_ident("wei`rd") == "`wei``rd`"

    def test_quote_object_fqn_quotes_catalog_and_schema_only(self):
        # The object name is a trusted app constant and stays bare,
        # matching the view-DDL convention.
        assert quote_object_fqn("prod-east", "dqx-studio", "dq_metrics") == "`prod-east`.`dqx-studio`.dq_metrics"

    def test_embedded_backtick_is_doubled_as_defense_in_depth(self):
        # validate_fqn() rejects raw backticks before this is ever reached in
        # production, but quote_fqn() must not silently produce a broken (or
        # injectable) identifier if it were ever called on unvalidated input.
        assert quote_fqn("a.weird`name.c") == "`a`.`weird``name`.`c`"


# ---------------------------------------------------------------------------
# fqn_needs_quoting
# ---------------------------------------------------------------------------


class TestFqnNeedsQuoting:
    @pytest.mark.parametrize(
        "fqn",
        [
            "main.default.t",
            "my_catalog.my_schema.my_table",
            "_a._b._c",
            "a1.b2.c3",
        ],
    )
    def test_simple_names_do_not_need_quoting(self, fqn):
        assert fqn_needs_quoting(fqn) is False

    @pytest.mark.parametrize(
        "fqn",
        [
            "main.'ftr_mv_test'.'ftr_gold_mv_bkp'",  # embedded quotes
            "a.b.c d",  # space
            "1.b.c",  # leading digit
            "with-hyphens.a.b",  # hyphen
            "a.b",  # not three parts
            "__sql_check__/name",  # synthetic key (no dots)
        ],
    )
    def test_exotic_or_nonstandard_names_need_quoting(self, fqn):
        assert fqn_needs_quoting(fqn) is True


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


# ---------------------------------------------------------------------------
# validate_object_id
# ---------------------------------------------------------------------------


class TestValidateObjectId:
    @pytest.mark.parametrize(
        "object_id",
        [
            "r1",
            "a" * 32,  # uuid4().hex — registry_rule / monitored_table id shape
            "a" * 16,  # uuid4().hex[:16] — the truncated id shape some services use
            "abc-123_def",
            "A" * 128,  # exactly at the length cap
        ],
    )
    def test_valid_app_minted_ids_pass(self, object_id):
        assert validate_object_id(object_id) == object_id

    @pytest.mark.parametrize(
        "object_id",
        [
            "",
            "a" * 129,  # over the length cap
            "r1' OR '1'='1",
            "r1\\",  # trailing backslash — the string-literal break-out class
            "r1'; DROP TABLE dq_object_grants; --",
            "r1\n",  # control character — also a log-injection vector
            "r1 space",
            "r1/*comment*/",
        ],
    )
    def test_malicious_or_malformed_ids_raise(self, object_id):
        with pytest.raises(ValueError):
            validate_object_id(object_id)
