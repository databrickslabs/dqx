"""Prepare supported reference checks using full, OBO-created temporary views.

Checks retain the original table name as their stable *ref_df_name*. Unsupported
checks and conflicting reference modes remain unchanged for Core validation.
The submission layer owns creation tracking and cleanup, including partial failures.
"""

import copy
from collections.abc import Callable
from typing import NamedTuple

from server.utils import sanitize_for_log, validate_and_quote_table_name

_REFERENCE_FUNCTIONS = frozenset({"foreign_key", "has_valid_schema", "compare_datasets", "aggr_matches_dataset"})


def _reference_arguments(check: dict[str, object]) -> tuple[str, dict[str, object]] | None:
    """Return eligible reference arguments without changing Core's mode validation."""
    inner = check.get("check")
    if not isinstance(inner, dict):
        return None
    function = inner.get("function")
    if not isinstance(function, str) or function not in _REFERENCE_FUNCTIONS:
        return None
    arguments = inner.get("arguments")
    if not isinstance(arguments, dict):
        return None
    ref = arguments.get("ref_table")
    if not isinstance(ref, str) or not ref.strip():
        return None

    # The aggregate check treats an empty ref_df_name as unset. The other three
    # treat every non-None value as supplied. Preserve wrong types for Core too.
    ref_df_name = arguments.get("ref_df_name")
    if function == "aggr_matches_dataset":
        if ref_df_name is not None and (not isinstance(ref_df_name, str) or ref_df_name):
            return None
    elif ref_df_name is not None:
        return None
    if function == "has_valid_schema" and arguments.get("expected_schema") is not None:
        return None
    return ref.strip(), arguments


class ReferenceViews(NamedTuple):
    """Prepared checks and the original-reference-name to temporary-view mapping."""

    checks: list[dict[str, object]]
    ref_views: dict[str, str]


def build_reference_views(
    checks: list[dict[str, object]],
    create_view: Callable[[str], str],
    *,
    monitored_table_fqn: str | None = None,
    monitored_view_fqn: str | None = None,
) -> ReferenceViews:
    """Create full reference views and adapt eligible checks in one pass.

    Args:
        checks: DQX metadata. The original checks are never modified.
        create_view: Creates a full OBO view and records it for caller-owned
            cleanup. The caller must also clean up after partial failures.
        monitored_table_fqn: Already exposed monitored table, if available.
        monitored_view_fqn: Full monitored view, reusable for self-references.

    Returns:
        Copied checks using original table names as *ref_df_name* keys, and the
        reference-to-view mapping. With no eligible references, returns the input.

    Raises:
        ValueError: A reference name is not a supported fully qualified identifier.
        RuntimeError: A reference view cannot be created. The message safely
            identifies the reference table and retains the underlying cause.
    """
    rewritten = copy.deepcopy(checks)
    ref_views: dict[str, str] = {}
    for check in rewritten:
        reference = _reference_arguments(check)
        if reference is None:
            continue
        ref, arguments = reference
        if ref not in ref_views:
            try:
                validate_and_quote_table_name(ref)
            except ValueError:
                guidance = "Use an unquoted catalog.schema.table name (letters, digits, and underscores)."
                raise ValueError(f"Invalid reference table name '{sanitize_for_log(ref)}'. {guidance}") from None
            if monitored_table_fqn and monitored_view_fqn and ref == monitored_table_fqn:
                ref_views[ref] = monitored_view_fqn
            else:
                try:
                    ref_views[ref] = create_view(ref)
                except Exception as exc:
                    guidance = "Check table existence, SELECT access, and SQL warehouse availability."
                    raise RuntimeError(
                        f"Cannot create a temporary view over reference table '{sanitize_for_log(ref)}'. {guidance}"
                    ) from exc
        del arguments["ref_table"]
        arguments["ref_df_name"] = ref
    return ReferenceViews(rewritten if ref_views else checks, ref_views)
