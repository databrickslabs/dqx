"""Tests for native_test_predicate — compile dqx_native checks to SQL test predicates."""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.native_test_predicate import (
    NativeTestCompileError,
    NativeTestNotSupportedError,
    compile_native_test_predicate,
    is_native_rule_testable,
)


class TestIsNativeRuleTestable:
    def test_row_check_is_testable(self):
        assert is_native_rule_testable("is_not_null") is True

    def test_dataset_check_is_not_testable(self):
        assert is_native_rule_testable("is_unique") is False

    def test_geo_check_is_not_testable(self):
        assert is_native_rule_testable("is_point") is False


class TestCompileNativeTestPredicate:
    def test_is_in_list(self):
        pred = compile_native_test_predicate(
            "is_in_list",
            {"column": "{{status}}", "allowed": ["1", "2", "3"], "case_sensitive": False},
        )
        assert "IN (" in pred
        assert "{{status}}" in pred

    def test_is_in_list_rejects_missing_allowed(self):
        with pytest.raises(NativeTestCompileError, match="non-empty allowed list"):
            compile_native_test_predicate("is_in_list", {"column": "{{status}}"})

    def test_is_not_null(self):
        pred = compile_native_test_predicate("is_not_null", {"column": "{{column_1}}"})
        assert pred == "({{column_1}} IS NOT NULL)"

    def test_is_in_range(self):
        pred = compile_native_test_predicate(
            "is_in_range",
            {"column": "{{amount}}", "min_limit": 0, "max_limit": 100},
        )
        assert pred == "({{amount}} >= 0 AND {{amount}} <= 100)"

    def test_sql_expression(self):
        pred = compile_native_test_predicate(
            "sql_expression",
            {"expression": "{{column_1}} > 0"},
        )
        assert pred == "({{column_1}} > 0)"

    def test_dataset_check_rejected(self):
        with pytest.raises(NativeTestNotSupportedError):
            compile_native_test_predicate("is_unique", {"columns": ["{{a}}"]})

    def test_missing_slot_rejected(self):
        with pytest.raises(NativeTestCompileError):
            compile_native_test_predicate("is_not_null", {"column": "customer_id"})
