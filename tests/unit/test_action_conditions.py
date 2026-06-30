"""Unit tests for the safe AST condition evaluator (Task 2)."""

import pytest

from databricks.labs.dqx.actions.conditions import ConditionEvaluator
from databricks.labs.dqx.errors import InvalidConditionError


# ---------------------------------------------------------------------------
# Basic comparison tests
# ---------------------------------------------------------------------------


class TestBasicComparisons:
    def test_greater_than_true(self):
        assert ConditionEvaluator.evaluate("error_row_count > 0", {"error_row_count": 5}) is True

    def test_greater_than_false(self):
        assert ConditionEvaluator.evaluate("error_row_count > 0", {"error_row_count": 0}) is False

    def test_equal_true(self):
        assert ConditionEvaluator.evaluate("error_row_count == 5", {"error_row_count": 5}) is True

    def test_not_equal_true(self):
        assert ConditionEvaluator.evaluate("error_row_count != 0", {"error_row_count": 3}) is True

    def test_less_than_or_equal(self):
        assert ConditionEvaluator.evaluate("error_row_count <= 10", {"error_row_count": 10}) is True

    def test_greater_than_or_equal(self):
        assert ConditionEvaluator.evaluate("error_row_count >= 5", {"error_row_count": 4}) is False


# ---------------------------------------------------------------------------
# Boolean logic tests
# ---------------------------------------------------------------------------


class TestBooleanLogic:
    def test_or_first_true(self):
        assert ConditionEvaluator.evaluate("a > 1 or b > 1", {"a": 5, "b": 0}) is True

    def test_or_second_true(self):
        assert ConditionEvaluator.evaluate("a > 1 or b > 1", {"a": 0, "b": 5}) is True

    def test_or_both_false(self):
        assert ConditionEvaluator.evaluate("a > 1 or b > 1", {"a": 0, "b": 0}) is False

    def test_and_both_true(self):
        assert ConditionEvaluator.evaluate("a > 0 and b > 0", {"a": 1, "b": 1}) is True

    def test_and_one_false(self):
        assert ConditionEvaluator.evaluate("a > 0 and b > 0", {"a": 1, "b": 0}) is False

    def test_not_true(self):
        assert ConditionEvaluator.evaluate("not (a > 10)", {"a": 5}) is True

    def test_not_false(self):
        assert ConditionEvaluator.evaluate("not (a > 10)", {"a": 15}) is False


# ---------------------------------------------------------------------------
# Float comparisons
# ---------------------------------------------------------------------------


class TestFloatComparisons:
    def test_float_greater_than_true(self):
        assert ConditionEvaluator.evaluate("error_row_ratio > 0.10", {"error_row_ratio": 0.15}) is True

    def test_float_greater_than_false(self):
        assert ConditionEvaluator.evaluate("error_row_ratio > 0.10", {"error_row_ratio": 0.05}) is False

    def test_float_equal(self):
        assert ConditionEvaluator.evaluate("error_row_ratio == 0.10", {"error_row_ratio": 0.10}) is True


# ---------------------------------------------------------------------------
# Numeric string coercion
# ---------------------------------------------------------------------------


class TestNumericStringCoercion:
    def test_string_int_coercion(self):
        assert ConditionEvaluator.evaluate("x > 3", {"x": "5"}) is True

    def test_string_float_coercion(self):
        assert ConditionEvaluator.evaluate("x > 0.1", {"x": "0.5"}) is True

    def test_string_zero_coercion(self):
        assert ConditionEvaluator.evaluate("x > 0", {"x": "0"}) is False


# ---------------------------------------------------------------------------
# Arithmetic operations
# ---------------------------------------------------------------------------


class TestArithmeticOperations:
    def test_addition(self):
        assert ConditionEvaluator.evaluate("a + b > 5", {"a": 3, "b": 4}) is True

    def test_subtraction(self):
        assert ConditionEvaluator.evaluate("a - b > 0", {"a": 5, "b": 3}) is True

    def test_multiplication(self):
        assert ConditionEvaluator.evaluate("a * b > 10", {"a": 3, "b": 4}) is True

    def test_division(self):
        assert ConditionEvaluator.evaluate("a / b > 2", {"a": 10, "b": 4}) is True

    def test_floor_division(self):
        assert ConditionEvaluator.evaluate("a // b == 2", {"a": 10, "b": 4}) is True

    def test_modulo(self):
        assert ConditionEvaluator.evaluate("a % b == 0", {"a": 10, "b": 5}) is True

    def test_power(self):
        assert ConditionEvaluator.evaluate("a ** 2 > 20", {"a": 5}) is True

    def test_power_with_large_int_metrics_is_bounded(self):
        """`a ** b` with huge int metrics must not build an unbounded bigint (DoS); float
        coercion makes it overflow to InvalidConditionError instead of hanging the driver."""
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("a ** b > 0", {"a": 10**6, "b": 10**6})

    def test_power_with_non_numeric_operand_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("a ** 2 > 0", {"a": "not-a-number"})

    def test_unary_sub(self):
        assert ConditionEvaluator.evaluate("-a < 0", {"a": 5}) is True

    def test_unary_add(self):
        assert ConditionEvaluator.evaluate("+a > 0", {"a": 5}) is True


# ---------------------------------------------------------------------------
# Chained comparisons
# ---------------------------------------------------------------------------


class TestChainedComparisons:
    def test_chained_in_range(self):
        assert ConditionEvaluator.evaluate("0 < x < 10", {"x": 5}) is True

    def test_chained_out_of_range_low(self):
        assert ConditionEvaluator.evaluate("0 < x < 10", {"x": 0}) is False

    def test_chained_out_of_range_high(self):
        assert ConditionEvaluator.evaluate("0 < x < 10", {"x": 10}) is False


# ---------------------------------------------------------------------------
# Parentheses / complex expressions
# ---------------------------------------------------------------------------


class TestComplexExpressions:
    def test_parenthesized_expression(self):
        assert ConditionEvaluator.evaluate("(a + b) * 2 > 10", {"a": 2, "b": 4}) is True

    def test_nested_bool_ops(self):
        assert ConditionEvaluator.evaluate("(a > 0 and b > 0) or c > 10", {"a": 1, "b": 1, "c": 0}) is True

    def test_constants_only(self):
        assert ConditionEvaluator.evaluate("1 + 1 == 2", {}) is True


# ---------------------------------------------------------------------------
# Unknown metric names raise InvalidConditionError
# ---------------------------------------------------------------------------


class TestUnknownMetricRaisesError:
    def test_unknown_name_raises(self):
        with pytest.raises(InvalidConditionError, match="unknown metric"):
            ConditionEvaluator.evaluate("unknown_metric > 0", {"error_row_count": 5})

    def test_unknown_name_in_expression_raises(self):
        with pytest.raises(InvalidConditionError, match="unknown metric"):
            ConditionEvaluator.evaluate("error_row_count > 0 and missing_metric > 0", {"error_row_count": 5})


# ---------------------------------------------------------------------------
# Disallowed AST nodes raise InvalidConditionError
# ---------------------------------------------------------------------------


class TestDisallowedNodesRaiseError:
    def test_function_call_import_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("__import__('os')", {})

    def test_function_call_len_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("len(x)", {"x": [1, 2, 3]})

    def test_attribute_access_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("x.y", {"x": 5})

    def test_subscript_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("x[0]", {"x": [1, 2, 3]})

    def test_lambda_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("(lambda x: x)(1)", {})

    def test_list_comprehension_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("[x for x in items]", {"items": [1, 2, 3]})

    def test_list_literal_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("[1, 2, 3]", {})

    def test_dict_literal_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("{'a': 1}", {})


# ---------------------------------------------------------------------------
# Syntax errors raise InvalidConditionError
# ---------------------------------------------------------------------------


class TestSyntaxErrorsRaiseError:
    def test_incomplete_expression_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("1 +")

    def test_empty_string_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("")

    def test_statement_mode_raises(self):
        # Assignments are statements, not expressions
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("x = 1")


# ---------------------------------------------------------------------------
# Validate without metrics (at construction time)
# ---------------------------------------------------------------------------


class TestValidate:
    def test_validate_valid_expression(self):
        # Should not raise
        ConditionEvaluator.validate("error_row_count > 0 or warning_row_count > 0")

    def test_validate_disallowed_node_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("len(x) > 0")

    def test_validate_syntax_error_raises(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("1 +")

    def test_validate_does_not_require_metrics(self):
        # validate must work without knowing metric names — names are
        # only checked at evaluate() time
        ConditionEvaluator.validate("some_future_metric > 0")

    def test_validate_disallows_call_node(self):
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("__import__('os')")


# ---------------------------------------------------------------------------
# Full-tree pre-pass: short-circuit must NOT bypass allowlist (Fix round 1)
# ---------------------------------------------------------------------------


class TestFullTreePrePass:
    def test_validate_or_with_disallowed_right_raises(self):
        """validate must reject 'True or len(x) > 0' even though True short-circuits."""
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("True or len(x) > 0")

    def test_evaluate_or_with_disallowed_right_raises(self):
        """evaluate must raise even when the left branch short-circuits to True."""
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("True or len(x) > 0", {})

    def test_validate_and_with_disallowed_right_raises(self):
        """validate must reject '__import__' hidden in a False-short-circuit and branch."""
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.validate("False and __import__('os').system('x')")

    def test_evaluate_or_short_circuit_with_attribute_raises(self):
        """evaluate must raise on attribute access even when left branch is truthy."""
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("metric == 0 or x.y", {"metric": 0})


# ---------------------------------------------------------------------------
# Operator errors wrapped as InvalidConditionError (Fix round 1)
# ---------------------------------------------------------------------------


class TestOperatorErrorsWrapped:
    def test_division_by_zero_raises_invalid_condition(self):
        """ZeroDivisionError from '1/0 > 0' must surface as InvalidConditionError."""
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("1 / 0 > 0", {})

    def test_type_error_on_compare_raises_invalid_condition(self):
        """TypeError comparing non-numeric string to int must surface as InvalidConditionError."""
        with pytest.raises(InvalidConditionError):
            ConditionEvaluator.evaluate("x > 1", {"x": "abc"})
