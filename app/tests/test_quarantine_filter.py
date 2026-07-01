"""Tests for the ``check_name`` filter on the quarantine routes.

We exercise the predicate builder + ``_query_quarantine`` directly with a
fake :class:`SqlExecutor` so we can assert on the SQL that would have been
sent to the warehouse — no warehouse needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.routes.v1.quarantine import (
    _CHECK_NAME_RE,
    _check_name_predicate,
    _query_quarantine,
)


class _AppConf:
    def __init__(self):
        self.catalog = "main"
        self.schema_name = "dqx"


def _sql(count: int = 0, rows: list | None = None):
    sql = MagicMock()
    sql.query.return_value = [[count]]
    sql.query_dicts.return_value = rows or []
    return sql


class TestCheckNameRegex:
    @pytest.mark.parametrize(
        "name",
        ["is_not_null", "_internal", "abc123", "Check42", "fare_amount_not_in_range"],
    )
    def test_accepts_valid(self, name: str):
        assert _CHECK_NAME_RE.match(name) is not None

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "1leading_digit",
            "name with space",
            "name'); DROP TABLE x;--",
            "kebab-case",
            "name.with.dots",
            "a" * 129,  # too long
        ],
    )
    def test_rejects_invalid(self, name: str):
        assert _CHECK_NAME_RE.match(name) is None


class TestCheckNamePredicate:
    def test_filters_both_errors_and_warnings(self):
        pred = _check_name_predicate("fare_amount_not_in_range")
        # The predicate must cover BOTH the errors column and the warnings
        # column; an OR between the two is the whole point — warning-only
        # rules write to ``warnings`` but never to ``errors``.
        assert "errors" in pred
        assert "warnings" in pred
        assert "fare_amount_not_in_range" in pred
        assert " OR " in pred

    def test_uses_typed_from_json_projection(self):
        # We deliberately project the VARIANT column to a typed array so
        # the higher-order ``exists`` can apply ``e -> e.name = ...``.
        # Locking that projection in stops a careless refactor from
        # falling back to a substring match (which would be unsafe).
        pred = _check_name_predicate("x")
        assert "from_json(to_json(errors), 'array<struct<name:string>>')" in pred
        assert "from_json(to_json(warnings), 'array<struct<name:string>>')" in pred
        assert "EXISTS(" in pred


class TestQueryQuarantine:
    def test_omits_check_name_clause_when_none(self):
        sql = _sql(count=42)
        _query_quarantine(sql, _AppConf(), "run-1", offset=0, limit=10)
        count_call = sql.query.call_args.args[0]
        data_call = sql.query_dicts.call_args.args[0]
        assert "exists" not in count_call.lower()
        assert "exists" not in data_call.lower()
        assert "run_id = 'run-1'" in count_call
        assert "ORDER BY created_at DESC LIMIT 10 OFFSET 0" in data_call

    def test_appends_check_name_predicate(self):
        sql = _sql(count=3)
        _query_quarantine(sql, _AppConf(), "run-1", offset=0, limit=10, check_name="fare_in_range")
        count_call = sql.query.call_args.args[0]
        data_call = sql.query_dicts.call_args.args[0]
        # Same WHERE on both queries — pagination relies on that
        # invariance, so check explicitly.
        assert "EXISTS(" in count_call
        assert "EXISTS(" in data_call
        assert "fare_in_range" in count_call
        assert "fare_in_range" in data_call

    def test_rejects_injection_attempt(self):
        sql = _sql()
        with pytest.raises(ValueError, match="Invalid check_name"):
            _query_quarantine(sql, _AppConf(), "run-1", check_name="x'); DROP TABLE quarantine;--")
        # SQL must never reach the warehouse.
        sql.query.assert_not_called()
        sql.query_dicts.assert_not_called()

    def test_run_id_is_escaped_via_escape_sql_string(self):
        # Sanity check that the existing escaping path is still wired in.
        sql = _sql(count=0)
        _query_quarantine(sql, _AppConf(), "run-with'quote", offset=0, limit=10)
        count_call = sql.query.call_args.args[0]
        assert "run-with''quote" in count_call

    def test_pagination_params_propagate_to_data_query(self):
        sql = _sql(count=100)
        _query_quarantine(sql, _AppConf(), "run-1", offset=20, limit=5, check_name="my_check")
        data_call = sql.query_dicts.call_args.args[0]
        assert "LIMIT 5 OFFSET 20" in data_call
