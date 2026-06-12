"""Unit tests for SemanticValidator."""

import pytest

from databricks.labs.dqx.semantic_validator import SemanticValidator


# ---------------------------------------------------------------------------
# validate_sql_expression — happy path
# ---------------------------------------------------------------------------


def test_valid_simple_condition():
    """A plain filter expression should pass without raising."""
    SemanticValidator.validate_sql_expression("age >= 18 AND status = 'ACTIVE'")


def test_valid_expression_with_functions():
    """Expressions using SQL functions should be accepted."""
    SemanticValidator.validate_sql_expression("LOWER(email) LIKE '%@example.com'")


def test_valid_expression_containing_keyword_as_substring():
    """A word like 'dropout' must not trigger the DROP keyword check."""
    SemanticValidator.validate_sql_expression("category = 'dropout'")


# ---------------------------------------------------------------------------
# validate_sql_expression — forbidden keywords
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "expr",
    [
        "DROP TABLE users",
        "TRUNCATE TABLE orders",
        "DELETE FROM sales WHERE id = 1",
        "UPDATE accounts SET balance = 0",
        "INSERT INTO logs VALUES (1, 'x')",
        "ALTER TABLE customers ADD COLUMN phone STRING",
    ],
)
def test_forbidden_keywords_raise(expr: str):
    """Each destructive DML/DDL command must be rejected."""
    with pytest.raises(ValueError, match="Semantic validation failed"):
        SemanticValidator.validate_sql_expression(expr)


def test_forbidden_keyword_case_insensitive():
    """Keyword check must be case-insensitive."""
    with pytest.raises(ValueError, match="Semantic validation failed"):
        SemanticValidator.validate_sql_expression("drop table users")


def test_forbidden_keyword_mixed_case():
    """Mixed-case forbidden keywords must also be caught."""
    with pytest.raises(ValueError, match="Semantic validation failed"):
        SemanticValidator.validate_sql_expression("Drop Table users")


# ---------------------------------------------------------------------------
# validate_sql_expression — empty / invalid input
# ---------------------------------------------------------------------------


def test_empty_string_raises():
    with pytest.raises(ValueError, match="non-empty string"):
        SemanticValidator.validate_sql_expression("")


def test_whitespace_only_raises():
    with pytest.raises(ValueError, match="non-empty string"):
        SemanticValidator.validate_sql_expression("   ")


def test_non_string_raises():
    with pytest.raises(ValueError, match="non-empty string"):
        SemanticValidator.validate_sql_expression(None)  # type: ignore[arg-type]