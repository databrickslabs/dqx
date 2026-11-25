"""Expose reference tables to the runner through full, OBO-created temporary views.

Checks reach DQX unchanged. The runner opts in to reading each check's *ref_table*
from reference DataFrames keyed by table name, so fingerprints and messages match a
direct DQX run. The submission layer owns creation tracking and cleanup.
"""

from collections.abc import Callable

from server.utils import sanitize_for_log, validate_and_quote_table_name


def _reference_table(check: object) -> str | None:
    """Return the check's *ref_table*, or None when it names none for DQX to validate."""
    inner = check.get("check") if isinstance(check, dict) else None
    arguments = inner.get("arguments") if isinstance(inner, dict) else None
    ref = arguments.get("ref_table") if isinstance(arguments, dict) else None
    return ref if isinstance(ref, str) and ref.strip() else None


def build_reference_views(
    checks: list[dict[str, object]],
    create_view: Callable[[str], str],
    *,
    monitored_table_fqn: str | None = None,
    monitored_view_fqn: str | None = None,
) -> dict[str, str]:
    """Create one full view per reference table the checks name.

    Args:
        checks: DQX metadata. Never modified.
        create_view: Creates a full OBO view and records it for caller-owned
            cleanup. The caller must also clean up after partial failures.
        monitored_table_fqn: Already exposed monitored table, if available.
        monitored_view_fqn: Full monitored view, reused for self-references.

    Returns:
        Each *ref_table* spelling mapped to its view. Unity Catalog names are
        case-insensitive, so spellings that differ only by case share one view.

    Raises:
        ValueError: A reference name is not a supported fully qualified identifier.
            The message identifies the check by position and never echoes the name.
        RuntimeError: A reference view cannot be created. The message identifies
            the validated reference table and retains the underlying cause.
    """
    ref_views: dict[str, str] = {}
    views_by_table: dict[str, str] = {}
    if monitored_table_fqn and monitored_view_fqn:
        views_by_table[monitored_table_fqn.lower()] = monitored_view_fqn
    for index, check in enumerate(checks):
        ref = _reference_table(check)
        if ref is None or ref in ref_views:
            continue
        try:
            validate_and_quote_table_name(ref)
        except ValueError:
            guidance = "Use an unquoted catalog.schema.table name (letters, digits, and underscores)."
            raise ValueError(f"Invalid reference table name in check at index {index}. {guidance}") from None
        table_key = ref.lower()
        if table_key not in views_by_table:
            try:
                views_by_table[table_key] = create_view(ref)
            except Exception as exc:
                guidance = "Check table existence, SELECT access, and SQL warehouse availability."
                raise RuntimeError(
                    f"Cannot create a temporary view over reference table '{sanitize_for_log(ref)}'. {guidance}"
                ) from exc
        ref_views[ref] = views_by_table[table_key]
    return ref_views
