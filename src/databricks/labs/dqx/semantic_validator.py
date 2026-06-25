"""Semantic validation for DQ rulesets."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class SemanticValidationMode:
    """Controls how semantic validation issues are surfaced."""

    WARN = "warn"  # Log warnings but continue
    FAIL = "fail"  # Raise an exception if any issues are found


class SemanticValidator:
    """Provides semantic validation for a collection of DQ rules.

    Detects ruleset-level issues such as:
    - Duplicate rules: two rules with the same function, arguments, criticality, and filter.
    - Conflicting rules: two rules targeting the same function and column but with
      different arguments (e.g. two ``is_in_range`` checks with different thresholds).

    Note:
        Rules that use raw Spark SQL expressions (via the ``sql_expression`` function)
        are not deeply inspected — only structured metadata (function name, column,
        arguments) is compared. Document this limitation when such checks are used.

    Usage::

        # Just get a list of issues:
        issues = SemanticValidator.validate_ruleset(checks)

        # Or apply with configurable behavior:
        SemanticValidator.apply(checks, mode=SemanticValidationMode.WARN)
        SemanticValidator.apply(checks, mode=SemanticValidationMode.FAIL)
    """

    @staticmethod
    def _get_function(check: dict) -> str | None:
        """Extract the function name from a check dict.

        Handles both nested form ``{"check": {"function": ...}}``
        and flat form ``{"function": ...}``.
        """
        if not isinstance(check, dict):
            return None
        inner = check.get("check", check)
        return inner.get("function")

    @staticmethod
    def _get_arguments(check: dict) -> dict:
        """Extract the arguments dict from a check dict."""
        if not isinstance(check, dict):
            return {}
        inner = check.get("check", check)
        return inner.get("arguments", {}) or {}

    @staticmethod
    def _full_key(check: dict) -> tuple | None:
        """Return a hashable key representing a rule's complete identity.

        Two rules with the same full key are exact duplicates.
        Key: (function, sorted_arguments, criticality, filter)
        """
        function = SemanticValidator._get_function(check)
        if not function:
            return None
        arguments = SemanticValidator._get_arguments(check)
        criticality = check.get("criticality", "error")
        filter_expr = check.get("filter")
        return (function, tuple(sorted(arguments.items())), criticality, filter_expr)

    @staticmethod
    def _conflict_key(check: dict) -> tuple | None:
        """Return a key grouping rules that target the same function and column.

        Used to detect rules that share a function and column but differ in
        other arguments (e.g. conflicting thresholds). Returns None if the
        check has no identifiable column to compare against.
        """
        function = SemanticValidator._get_function(check)
        if not function:
            return None
        arguments = SemanticValidator._get_arguments(check)
        column = arguments.get("col_name") or arguments.get("column")
        if not column:
            return None
        return (function, column)

    @staticmethod
    def detect_duplicates(checks: list[dict]) -> list[str]:
        """Detect rules that are completely identical.

        Two rules are duplicates when they share the same function, arguments,
        criticality, and filter expression.

        Args:
            checks: The ruleset to inspect.

        Returns:
            A list of issue message strings, empty if no duplicates found.
        """
        seen: dict[tuple, int] = {}
        issues: list[str] = []

        for idx, check in enumerate(checks):
            key = SemanticValidator._full_key(check)
            if key is None:
                continue
            if key in seen:
                issues.append(
                    f"Duplicate rule detected: rule at index {idx} is identical to "
                    f"rule at index {seen[key]} (function: '{key[0]}')."
                )
            else:
                seen[key] = idx

        return issues

    @staticmethod
    def detect_conflicts(checks: list[dict]) -> list[str]:
        """Detect rules targeting the same function and column with different arguments.

        For example, two ``is_in_range`` checks on ``age`` with different min/max
        thresholds would be flagged, as this is likely a misconfiguration.

        Args:
            checks: The ruleset to inspect.

        Returns:
            A list of issue message strings, empty if no conflicts found.
        """
        seen: dict[tuple, tuple[int, dict]] = {}
        issues: list[str] = []

        for idx, check in enumerate(checks):
            conflict_key = SemanticValidator._conflict_key(check)
            if conflict_key is None:
                continue

            arguments = SemanticValidator._get_arguments(check)

            if conflict_key in seen:
                prev_idx, prev_arguments = seen[conflict_key]
                if arguments != prev_arguments:
                    function, column = conflict_key
                    issues.append(
                        f"Conflicting rules detected: rule at index {idx} and rule at index {prev_idx} "
                        f"both apply '{function}' to column '{column}' but with different arguments "
                        f"(index {prev_idx}: {prev_arguments}, index {idx}: {arguments})."
                    )
            else:
                seen[conflict_key] = (idx, arguments)

        return issues

    @staticmethod
    def validate_ruleset(checks: list[dict]) -> list[str]:
        """Run all semantic checks and return a combined list of issue messages.

        Args:
            checks: The ruleset to inspect.

        Returns:
            A list of issue strings. Empty list means the ruleset is semantically clean.
        """
        issues: list[str] = []
        issues.extend(SemanticValidator.detect_duplicates(checks))
        issues.extend(SemanticValidator.detect_conflicts(checks))
        return issues

    @staticmethod
    def apply(checks: list[dict], mode: str = SemanticValidationMode.WARN) -> None:
        """Run semantic validation and surface issues according to the chosen mode.

        This is the main entry point called from ``validate_checks``, ``save_checks``,
        and ``load_checks`` with configurable behavior.

        Args:
            checks: The ruleset to inspect.
            mode: One of ``SemanticValidationMode.WARN`` (default) or
                ``SemanticValidationMode.FAIL``. In WARN mode, issues are logged
                as warnings and execution continues. In FAIL mode, a ``ValueError``
                is raised listing all issues found.

        Raises:
            ValueError: If ``mode`` is FAIL and any semantic issues are detected.
            ValueError: If an unsupported mode value is passed.
        """
        if mode not in (SemanticValidationMode.WARN, SemanticValidationMode.FAIL):
            raise ValueError(f"Unsupported semantic validation mode: '{mode}'. Use 'warn' or 'fail'.")

        issues = SemanticValidator.validate_ruleset(checks)
        if not issues:
            return

        if mode == SemanticValidationMode.WARN:
            for issue in issues:
                logger.warning("Semantic validation: %s", issue)
        else:
            raise ValueError(
                "Semantic validation failed with the following issues:\n"
                + "\n".join(f"  - {issue}" for issue in issues)
            )