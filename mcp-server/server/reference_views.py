"""Expose supported reference checks through full, OBO-created temporary views.

Checks use the original table name as *ref_df_name*, while the runner resolves
that stable name through a name-to-view mapping. Random view names therefore do
not enter the rewritten checks. Unsupported checks and conflicting reference
modes remain unchanged so DQX Core can report their validation errors.
"""

import copy
import logging
from collections.abc import Callable

from server.utils import sanitize_for_log, validate_and_quote_table_name

logger = logging.getLogger(__name__)

_REFERENCE_FUNCTIONS = frozenset({"foreign_key", "has_valid_schema", "compare_datasets", "aggr_matches_dataset"})


def _ref_table_of(check: dict[str, object]) -> str | None:
    """Return an eligible reference table without changing Core's mode validation."""
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

    # The aggregate check uses truthiness; the other three checks treat even an
    # empty ref_df_name as supplied. Leave conflicting modes for Core to reject.
    ref_df_name = arguments.get("ref_df_name")
    if function == "aggr_matches_dataset":
        # An empty string is unset for this check, but a non-string value must
        # still reach Core's metadata type validation unchanged.
        if ref_df_name is not None and (not isinstance(ref_df_name, str) or ref_df_name):
            return None
    elif ref_df_name is not None:
        return None
    if function == "has_valid_schema" and arguments.get("expected_schema") is not None:
        return None
    return ref.strip()


def collect_reference_tables(checks: list[dict[str, object]]) -> list[str]:
    """Return distinct eligible reference tables in first-occurrence order.

    Args:
        checks: DQX check metadata, including any checks Core must reject.

    Returns:
        Reference table names for the four supported reference-check functions.
    """
    return list(dict.fromkeys(ref for check in checks if (ref := _ref_table_of(check)) is not None))


def rewrite_to_ref_df_name(checks: list[dict[str, object]], handled: set[str]) -> list[dict[str, object]]:
    """Copy eligible checks, replacing *ref_table* with a stable *ref_df_name*.

    Args:
        checks: DQX check metadata. The original collection is never mutated.
        handled: Reference tables exposed through temporary views.

    Returns:
        Copied metadata. Unsupported or conflicting checks are unchanged, even
        when another eligible check uses the same reference table.
    """
    rewritten = copy.deepcopy(checks)
    for check in rewritten:
        ref = _ref_table_of(check)
        if ref is None or ref not in handled:
            continue
        inner = check.get("check")
        if isinstance(inner, dict):
            arguments = inner.get("arguments")
            if isinstance(arguments, dict):
                del arguments["ref_table"]
                arguments["ref_df_name"] = ref
    return rewritten


class ReferenceViews:
    """Checks and reference views prepared for one runner job.

    Attributes:
        checks: Metadata using original table names as reference DataFrame keys.
        ref_views: Original table names mapped to full, unsampled view FQNs.
        created_view_fqns: Views created here, excluding a reused monitored view.
    """

    __slots__ = ("checks", "created_view_fqns", "ref_views")

    def __init__(
        self, checks: list[dict[str, object]], ref_views: dict[str, str], created_view_fqns: list[str]
    ) -> None:
        self.checks = checks
        self.ref_views = ref_views
        self.created_view_fqns = created_view_fqns


def build_reference_views(
    checks: list[dict[str, object]],
    create_view: Callable[[str], str],
    *,
    monitored_table_fqn: str | None = None,
    monitored_view_fqn: str | None = None,
    drop_view: Callable[[str], None] | None = None,
) -> ReferenceViews:
    """Create unsampled OBO reference views without repairing conflicting modes.

    Args:
        checks: DQX check metadata to prepare.
        create_view: Creates a full, unsampled OBO view over a reference table.
        monitored_table_fqn: Already exposed monitored table, if available.
        monitored_view_fqn: Full monitored view, reusable for self-references.
        drop_view: Best-effort cleanup for views created before a failure.

    Returns:
        Rewritten checks, the reference-to-view map, and newly created view FQNs.

    Raises:
        ValueError: A reference name is not a supported fully qualified identifier.
        RuntimeError: A reference view cannot be created. The error names the
            reference table without exposing control characters.
    """
    ref_tables = collect_reference_tables(checks)
    if not ref_tables:
        return ReferenceViews(checks, {}, [])

    view_by_table: dict[str, str] = {}
    created: list[str] = []
    try:
        for ref in ref_tables:
            try:
                validate_and_quote_table_name(ref)
            except ValueError:
                guidance = "Use an unquoted catalog.schema.table name (letters, digits, and underscores)."
                raise ValueError(f"Invalid reference table name '{sanitize_for_log(ref)}'. {guidance}") from None
            if monitored_table_fqn and monitored_view_fqn and ref == monitored_table_fqn:
                view_by_table[ref] = monitored_view_fqn
                continue
            try:
                view_fqn = create_view(ref)
            except Exception as exc:
                guidance = "Check table existence, SELECT access, and SQL warehouse availability."
                raise RuntimeError(
                    f"Cannot create a temporary view over reference table '{sanitize_for_log(ref)}'. {guidance}"
                ) from exc
            view_by_table[ref] = view_fqn
            created.append(view_fqn)
        rewritten = rewrite_to_ref_df_name(checks, set(view_by_table))
        return ReferenceViews(rewritten, view_by_table, created)
    except Exception:
        if drop_view is not None:
            for view_fqn in created:
                try:
                    drop_view(view_fqn)
                except Exception:
                    logger.warning(f"Could not clean up reference view '{sanitize_for_log(view_fqn)}'.")
        raise
