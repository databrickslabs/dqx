"""Semantic validation for DQ rule expressions."""

import re


class SemanticValidator:
    """Provides semantic validation for data quality rule expressions.

    Ensures that SQL expressions used in DQ rules are well-formed and do not
    contain destructive SQL commands before the rules are executed.
    """

    # DML/DDL keywords that must never appear in a DQ rule expression.
    _FORBIDDEN_KEYWORDS = frozenset(["DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT", "ALTER"])

    @staticmethod
    def validate_sql_expression(expr: str) -> None:
        """Validate a SQL expression used in a DQ rule.

        Checks that the expression is a non-empty string and does not contain
        any destructive SQL commands (DROP, TRUNCATE, DELETE, UPDATE, INSERT, ALTER).

        Args:
            expr: The SQL expression string to validate.

        Raises:
            ValueError: If the expression is empty or contains a forbidden keyword.
        """
        if not isinstance(expr, str) or not expr.strip():
            raise ValueError("Rule expression must be a non-empty string.")

        upper_expr = expr.upper()
        for keyword in SemanticValidator._FORBIDDEN_KEYWORDS:
            if re.search(rf"\b{keyword}\b", upper_expr):
                raise ValueError(
                    f"Semantic validation failed: forbidden keyword '{keyword}' found in rule expression."
                )