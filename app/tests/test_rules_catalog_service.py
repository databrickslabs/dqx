"""Tests for ``RulesCatalogService`` — pure helpers + the rule signature/dup logic.

The service hits a Delta table for most operations, so we focus on the
deterministic helpers (``_normalize_weight``, ``_check_signature``,
``_validate_sql_checks``) and the SQL-driven ``find_duplicates`` /
``set_status`` paths with a stubbed ``SqlExecutor``.
"""

from __future__ import annotations

import json

import pytest

from databricks_labs_dqx_app.backend.services.rules_catalog_service import (
    RuleCatalogEntry,
    RulesCatalogService,
)


@pytest.fixture
def svc(sql_executor_mock):
    return RulesCatalogService(sql=sql_executor_mock)


# ---------------------------------------------------------------------------
# _normalize_weight — top-level weight folds into user_metadata
# ---------------------------------------------------------------------------


class TestNormalizeWeight:
    def test_no_weight_passes_through(self):
        checks = [{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}]
        assert RulesCatalogService._normalize_weight(checks) == checks

    def test_top_level_weight_int_moves_into_user_metadata(self):
        checks = [{"check": {"function": "is_not_null"}, "weight": 3}]
        out = RulesCatalogService._normalize_weight(checks)
        assert "weight" not in out[0]
        assert out[0]["user_metadata"] == {"weight": "3"}

    def test_top_level_weight_float_stringified(self):
        checks = [{"check": {"function": "is_not_null"}, "weight": 1.5}]
        out = RulesCatalogService._normalize_weight(checks)
        assert out[0]["user_metadata"] == {"weight": "1.5"}

    def test_existing_user_metadata_weight_takes_precedence(self):
        # user_metadata.weight is the source of truth — the legacy top-level
        # weight should *not* clobber an explicit user_metadata.weight.
        checks = [
            {
                "check": {"function": "is_not_null"},
                "weight": 9,
                "user_metadata": {"weight": "5", "team": "data"},
            }
        ]
        out = RulesCatalogService._normalize_weight(checks)
        md = out[0]["user_metadata"]
        assert md["weight"] == "5"
        assert md["team"] == "data"

    def test_non_numeric_weight_dropped(self):
        # Strings/unknown types are dropped silently — the catalog only stores
        # numeric weights via user_metadata.
        checks = [{"check": {"function": "is_not_null"}, "weight": "high"}]
        out = RulesCatalogService._normalize_weight(checks)
        assert "weight" not in out[0]
        assert "user_metadata" not in out[0]

    def test_non_dict_user_metadata_replaced_with_clean_dict(self):
        # If user_metadata is malformed (not a dict), we still produce a
        # well-shaped {weight: "..."} dict rather than crashing.
        checks = [{"check": {"function": "is_not_null"}, "weight": 2, "user_metadata": "bogus"}]
        out = RulesCatalogService._normalize_weight(checks)
        assert out[0]["user_metadata"] == {"weight": "2"}

    def test_does_not_mutate_input(self):
        checks = [{"check": {"function": "is_not_null"}, "weight": 3}]
        before = json.dumps(checks)
        RulesCatalogService._normalize_weight(checks)
        assert json.dumps(checks) == before


# ---------------------------------------------------------------------------
# _check_signature — duplicate-detection identity
# ---------------------------------------------------------------------------


class TestCheckSignature:
    def test_only_identity_args_count(self):
        a = {"check": {"function": "is_not_null", "arguments": {"column": "id", "name": "rule-a"}}}
        b = {"check": {"function": "is_not_null", "arguments": {"column": "id", "name": "rule-b"}}}
        # Different `name` → still same signature, since `name` is not in
        # _IDENTITY_ARGS. This is the whole point of the helper.
        assert RulesCatalogService._check_signature(a) == RulesCatalogService._check_signature(b)

    def test_weight_does_not_affect_signature(self):
        a = {"check": {"function": "is_not_null", "arguments": {"column": "id", "weight": 3}}}
        b = {"check": {"function": "is_not_null", "arguments": {"column": "id", "weight": 99}}}
        assert RulesCatalogService._check_signature(a) == RulesCatalogService._check_signature(b)

    def test_criticality_does_not_affect_signature(self):
        a = {
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "id", "criticality": "error"},
            }
        }
        b = {
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "id", "criticality": "warn"},
            }
        }
        assert RulesCatalogService._check_signature(a) == RulesCatalogService._check_signature(b)

    def test_different_function_yields_different_signature(self):
        a = {"check": {"function": "is_not_null", "arguments": {"column": "id"}}}
        b = {"check": {"function": "is_unique", "arguments": {"column": "id"}}}
        assert RulesCatalogService._check_signature(a) != RulesCatalogService._check_signature(b)

    def test_different_column_yields_different_signature(self):
        a = {"check": {"function": "is_not_null", "arguments": {"column": "id"}}}
        b = {"check": {"function": "is_not_null", "arguments": {"column": "name"}}}
        assert RulesCatalogService._check_signature(a) != RulesCatalogService._check_signature(b)

    def test_case_insensitive(self):
        a = {"check": {"function": "is_not_null", "arguments": {"column": "ID"}}}
        b = {"check": {"function": "Is_Not_Null", "arguments": {"column": "id"}}}
        assert RulesCatalogService._check_signature(a) == RulesCatalogService._check_signature(b)

    def test_argument_order_independent(self):
        a = {
            "check": {
                "function": "is_in_range",
                "arguments": {"column": "x", "min_limit": 0, "max_limit": 10},
            }
        }
        b = {
            "check": {
                "function": "is_in_range",
                "arguments": {"max_limit": 10, "column": "x", "min_limit": 0},
            }
        }
        assert RulesCatalogService._check_signature(a) == RulesCatalogService._check_signature(b)

    def test_accepts_check_at_top_level(self):
        # Some callers pass the inner ``check`` body directly rather than
        # wrapping in {"check": ...}.
        wrapped = {"check": {"function": "is_not_null", "arguments": {"column": "x"}}}
        unwrapped = {"function": "is_not_null", "arguments": {"column": "x"}}
        assert RulesCatalogService._check_signature(wrapped) == RulesCatalogService._check_signature(unwrapped)


# ---------------------------------------------------------------------------
# _validate_sql_checks
# ---------------------------------------------------------------------------


class TestValidateSqlChecks:
    def test_safe_select_query_is_accepted(self):
        checks = [
            {
                "check": {
                    "function": "sql_query",
                    "arguments": {
                        "query": "SELECT * FROM main.default.t WHERE x > 0",
                        "condition": "row_count > 0",
                    },
                }
            }
        ]
        # Should not raise.
        RulesCatalogService._validate_sql_checks(checks)

    def test_drop_statement_is_rejected(self):
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        checks = [
            {
                "check": {
                    "function": "sql_query",
                    "arguments": {"query": "DROP TABLE main.default.t"},
                }
            }
        ]
        with pytest.raises(UnsafeSqlQueryError):
            RulesCatalogService._validate_sql_checks(checks)

    def test_non_sql_check_is_ignored(self):
        # Only sql_query checks are validated — is_not_null with a query-shaped
        # argument should pass through.
        checks = [
            {
                "check": {
                    "function": "is_not_null",
                    "arguments": {"column": "DROP TABLE x"},
                }
            }
        ]
        RulesCatalogService._validate_sql_checks(checks)

    def test_empty_query_is_skipped(self):
        # A missing/empty `query` arg does not get sent through is_sql_query_safe.
        checks = [{"check": {"function": "sql_query", "arguments": {"query": ""}}}]
        RulesCatalogService._validate_sql_checks(checks)


# ---------------------------------------------------------------------------
# find_duplicates — uses _check_signature against existing rules
# ---------------------------------------------------------------------------


class TestFindDuplicates:
    def test_returns_empty_when_table_has_no_rules(self, svc, sql_executor_mock):
        sql_executor_mock.query.return_value = []
        new = [{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}]
        assert svc.find_duplicates("a.b.c", new) == []

    def test_detects_existing_match(self, svc, sql_executor_mock):
        existing_check = {"check": {"function": "is_not_null", "arguments": {"column": "x"}}}
        # _row_to_entry expects 10 columns from _SELECT_COLS — emulate that.
        sql_executor_mock.query.return_value = [
            (
                "a.b.c",
                json.dumps([existing_check]),
                1,
                "draft",
                "u@x",
                "2024-01-01T00:00:00+00:00",
                "u@x",
                "2024-01-01T00:00:00+00:00",
                "ui",
                "rule-1",
            )
        ]
        new = [{"check": {"function": "is_not_null", "arguments": {"column": "x", "name": "different"}}}]
        dups = svc.find_duplicates("a.b.c", new)
        assert dups == new  # the new rule is a duplicate

    def test_rejected_rules_do_not_count(self, svc, sql_executor_mock):
        existing_check = {"check": {"function": "is_not_null", "arguments": {"column": "x"}}}
        sql_executor_mock.query.return_value = [
            (
                "a.b.c",
                json.dumps([existing_check]),
                1,
                "rejected",  # rejected → not a duplicate
                "u@x",
                "2024-01-01T00:00:00+00:00",
                "u@x",
                "2024-01-01T00:00:00+00:00",
                "ui",
                "rule-1",
            )
        ]
        assert svc.find_duplicates("a.b.c", [existing_check]) == []

    def test_exclude_rule_id_skips_self(self, svc, sql_executor_mock):
        existing_check = {"check": {"function": "is_not_null", "arguments": {"column": "x"}}}
        sql_executor_mock.query.return_value = [
            (
                "a.b.c",
                json.dumps([existing_check]),
                1,
                "approved",
                "u@x",
                "2024-01-01T00:00:00+00:00",
                "u@x",
                "2024-01-01T00:00:00+00:00",
                "ui",
                "self-rule",
            )
        ]
        # Excluding the rule's own id → it shouldn't be considered a dup.
        dups = svc.find_duplicates("a.b.c", [existing_check], exclude_rule_id="self-rule")
        assert dups == []


# ---------------------------------------------------------------------------
# Status transition validation surface
# ---------------------------------------------------------------------------


class TestStatusTransitions:
    @pytest.mark.parametrize(
        "from_status,to_status,allowed",
        [
            ("draft", "pending_approval", True),
            ("draft", "approved", False),
            ("pending_approval", "approved", True),
            ("pending_approval", "rejected", True),
            ("pending_approval", "draft", True),
            ("approved", "draft", True),
            ("approved", "rejected", False),
            ("rejected", "draft", True),
            ("rejected", "pending_approval", False),
        ],
    )
    def test_transition_table(self, from_status, to_status, allowed):
        valid = RulesCatalogService.VALID_TRANSITIONS.get(from_status, set())
        assert (to_status in valid) is allowed


# ---------------------------------------------------------------------------
# RuleCatalogEntry constructor sanity
# ---------------------------------------------------------------------------


class TestRuleCatalogEntry:
    def test_defaults(self):
        e = RuleCatalogEntry(table_fqn="a.b.c", checks=[])
        assert e.version == 1
        assert e.status == "draft"
        assert e.source == "ui"
        assert e.rule_id is None

    def test_round_trip_fields(self):
        e = RuleCatalogEntry(
            table_fqn="a.b.c",
            checks=[{"x": 1}],
            version=4,
            status="approved",
            rule_id="abc123",
        )
        assert e.version == 4
        assert e.status == "approved"
        assert e.rule_id == "abc123"
