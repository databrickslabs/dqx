"""Semantic (ruleset-level) validation for DQ checks."""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


class ChecksSemanticValidationMode:
    """Controls how semantic validation issues are surfaced."""

    WARN = "warn"  # Log warnings but continue
    FAIL = "fail"  # Raise an exception if any issues are found


class ChecksSemanticValidator:
    """Provides semantic validation for a collection of DQ rules.

    Detects ruleset-level issues such as:
    - Duplicate rules: two rules with the same function, arguments, criticality, and filter.
    - Conflicting rules: two rules targeting the same function and column but with
      different arguments (e.g. two *is_in_range* checks with different thresholds).

    Note:
        Rules that use raw Spark SQL expressions (via the *sql_expression* function)
        are not deeply inspected — only structured metadata (function name, column,
        arguments) is compared. Document this limitation when such checks are used.

    Usage::

        # Just get a list of issues:
        issues = ChecksSemanticValidator.validate_ruleset(checks)

        # Or apply with configurable behavior:
        ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.WARN)
        ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.FAIL)
    """

    @staticmethod
    def _inner_block(check: dict) -> dict | None:
        """Return the check block, handling both the nested and flat forms.

        The nested form wraps the definition under a *check* key; the flat form
        places *function*/*arguments* at the top level. Returns None for anything
        that is not a dict (malformed input is reported by structural validation).
        """
        if not isinstance(check, dict):
            return None
        inner = check.get("check", check)
        return inner if isinstance(inner, dict) else None

    @staticmethod
    def _get_function(check: dict) -> str | None:
        """Extract the function name from a check dict."""
        inner = ChecksSemanticValidator._inner_block(check)
        return inner.get("function") if inner is not None else None

    @staticmethod
    def _get_arguments(check: dict) -> dict:
        """Extract the arguments dict from a check dict.

        Returns an empty dict for malformed checks (e.g. a non-dict *check* block
        or non-dict *arguments*); structural validation reports those separately.
        """
        inner = ChecksSemanticValidator._inner_block(check)
        if inner is None:
            return {}
        arguments = inner.get("arguments", {})
        return arguments if isinstance(arguments, dict) else {}

    @staticmethod
    def _get_for_each_column(check: dict) -> object:
        """Extract the *for_each_column* value (a list of columns or list of column groups).

        Returns None when absent. This value lives in the check block alongside
        *function* and *arguments*, so it must be part of a rule's identity.
        """
        inner = ChecksSemanticValidator._inner_block(check)
        return inner.get("for_each_column") if inner is not None else None

    @staticmethod
    def _get_filter(check: dict) -> object:
        """Extract the rule *filter*.

        DQX accepts *filter* either at the top level of the check or nested inside
        the check block; the top-level value takes precedence when both are present.
        """
        if not isinstance(check, dict):
            return None
        if check.get("filter") is not None:
            return check.get("filter")
        inner = ChecksSemanticValidator._inner_block(check)
        return inner.get("filter") if inner is not None else None

    @staticmethod
    def _sorted_columns(columns: list) -> list:
        """Sort a column list by a stable serialized key (handles names and nested groups)."""
        return sorted(columns, key=lambda item: json.dumps(item, sort_keys=True, default=str))

    @staticmethod
    def _normalize_columns(value: object) -> object:
        """Return an order-insensitive canonical form for a column list.

        Column targeting (*columns*, *for_each_column*) is order-independent, so two
        rules listing the same columns in a different order are the same rule. Lists
        are sorted by a stable serialized key; non-list values are returned unchanged.
        """
        if isinstance(value, list):
            return ChecksSemanticValidator._sorted_columns(value)
        return value

    @staticmethod
    def _normalize_arguments(arguments: dict) -> dict:
        """Return a copy of *arguments* with the column-targeting list order normalized.

        Column order is not semantically significant, so the plural *columns* argument
        is sorted to a canonical order. This keeps duplicate and conflict detection
        consistent when comparing arguments.
        """
        normalized = dict(arguments)
        if "columns" in normalized:
            normalized["columns"] = ChecksSemanticValidator._normalize_columns(normalized["columns"])
        return normalized

    @staticmethod
    def _full_key(check: dict) -> tuple | None:
        """Return a hashable key representing a rule's complete identity.

        Two rules with the same full key are exact duplicates.
        Key: (function, arguments + for_each_column, criticality, filter)
        """
        function = ChecksSemanticValidator._get_function(check)
        if not function:
            return None
        arguments = ChecksSemanticValidator._get_arguments(check)
        criticality = check.get("criticality", "error")
        filter_expr = ChecksSemanticValidator._get_filter(check)
        for_each_column = ChecksSemanticValidator._normalize_columns(
            ChecksSemanticValidator._get_for_each_column(check)
        )
        # Normalize the column-targeting argument so that reordered column lists are
        # treated as the same rule (column order is not semantically significant).
        normalized_arguments = ChecksSemanticValidator._normalize_arguments(arguments)
        # Serialize the targeting parts (arguments + for_each_column) to a stable,
        # hashable string so that list- or dict-valued values (e.g. is_in_list
        # allowed=[...], for_each_column=[...]) do not raise "unhashable type" when
        # the key is used in a dict/set, and so rules targeting different columns via
        # for_each_column are not collapsed into the same identity.
        identity = {"arguments": normalized_arguments, "for_each_column": for_each_column}
        identity_key = json.dumps(identity, sort_keys=True, default=str)
        return (function, identity_key, criticality, filter_expr)

    @staticmethod
    def _conflict_key(check: dict) -> tuple | None:
        """Return a key grouping rules that target the same function and column(s).

        Used to detect rules that share a function and column but differ in
        other arguments (e.g. conflicting thresholds). Handles both the singular
        *column*/*col_name* arguments and the plural *columns* argument. Returns
        None if the check has no identifiable column to compare against.
        """
        function = ChecksSemanticValidator._get_function(check)
        if not function:
            return None
        arguments = ChecksSemanticValidator._get_arguments(check)
        column = arguments.get("col_name") or arguments.get("column") or arguments.get("columns")
        if not column:
            return None
        if isinstance(column, list):
            # Column order is not semantically significant; normalize so reordered
            # lists group together.
            column = tuple(ChecksSemanticValidator._sorted_columns(column))
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
            key = ChecksSemanticValidator._full_key(check)
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

        For example, two *is_in_range* checks on *age* with different min/max
        thresholds would be flagged, as this is likely a misconfiguration.

        Args:
            checks: The ruleset to inspect.

        Returns:
            A list of issue message strings, empty if no conflicts found.
        """
        seen: dict[tuple, tuple[int, dict]] = {}
        issues: list[str] = []

        for idx, check in enumerate(checks):
            conflict_key = ChecksSemanticValidator._conflict_key(check)
            if conflict_key is None:
                continue

            # Compare normalized arguments so that rules differing only by column order
            # are treated as identical (a duplicate), not as a conflict.
            arguments = ChecksSemanticValidator._normalize_arguments(ChecksSemanticValidator._get_arguments(check))

            if conflict_key in seen:
                prev_idx, prev_arguments = seen[conflict_key]
                if arguments != prev_arguments:
                    function, column = conflict_key
                    column_label = ", ".join(column) if isinstance(column, tuple) else column
                    issues.append(
                        f"Conflicting rules detected: rule at index {idx} and rule at index {prev_idx} "
                        f"both apply '{function}' to column '{column_label}' but with different arguments "
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
        issues.extend(ChecksSemanticValidator.detect_duplicates(checks))
        issues.extend(ChecksSemanticValidator.detect_conflicts(checks))
        return issues

    @staticmethod
    def apply(checks: list[dict], mode: str | None = ChecksSemanticValidationMode.WARN) -> None:
        """Run semantic validation and surface issues according to the chosen mode.

        This is the main entry point called from *validate_checks*, *save_checks*,
        and *load_checks* with configurable behavior.

        Args:
            checks: The ruleset to inspect.
            mode: One of *ChecksSemanticValidationMode.WARN* (default),
                *ChecksSemanticValidationMode.FAIL*, or *None*. In WARN mode, issues are
                logged as warnings and execution continues. In FAIL mode, a *ValueError*
                is raised listing all issues found. When *None*, semantic validation is
                skipped entirely.

        Raises:
            ValueError: If *mode* is FAIL and any semantic issues are detected.
            ValueError: If an unsupported mode value is passed.
        """
        if mode is None:
            return

        if mode not in (ChecksSemanticValidationMode.WARN, ChecksSemanticValidationMode.FAIL):
            raise ValueError(f"Unsupported semantic validation mode: '{mode}'. Use 'warn' or 'fail'.")

        issues = ChecksSemanticValidator.validate_ruleset(checks)
        if not issues:
            return

        if mode == ChecksSemanticValidationMode.WARN:
            for issue in issues:
                logger.warning(f"Semantic validation: {issue}")
        else:
            raise ValueError(
                "Semantic validation failed with the following issues:\n"
                + "\n".join(f"  - {issue}" for issue in issues)
            )
