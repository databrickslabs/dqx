"""Expose reference tables to the runner service principal via OBO temp views.

Reference checks — ``foreign_key``, ``has_valid_schema`` (reference-table mode),
``compare_datasets`` and ``aggr_matches_dataset`` — read a second table named by
the ``ref_table`` argument. The MCP server runs checks in a job as a
least-privilege runner service principal that reads the user's data only through
an OBO temporary view in the ``dqx_mcp_tmp`` schema; it holds no grant on base
tables. The monitored table is exposed this way, but the reference table was
not, so reference checks failed when DQX Core resolved ``ref_table`` with
``spark.table(...)`` as the runner service principal.

This module scans a checks list for reference tables, materialises one temp view
per distinct reference table (via an injected ``create_view`` callable, which the
tools wire to :func:`server.utils.create_temp_view` under the caller's OBO
token), and rewrites each check's ``ref_table`` to its view. The runner then
reads the reference through the temp view like any other input.

The ``create_view``/``drop_view`` callables are injected so the pure functions
and the orchestration can be unit tested without Spark or a workspace. Views
created by :func:`server.utils.create_temp_view` are always full (unsampled)
``SELECT *`` views, which is required: a sampled reference set would drop valid
keys and report false violations.
"""

import copy
from collections.abc import Callable
from typing import Any

_REF_TABLE_ARG = "ref_table"


def _ref_table_of(check: dict[str, Any]) -> str | None:
    """Return the non-empty ``ref_table`` argument of one check, or ``None``.

    Tolerates malformed/partial check dicts (missing ``check``/``arguments``) and
    checks that name an in-memory reference DataFrame via ``ref_df_name`` instead
    of a table — those carry no ``ref_table`` and are left untouched.
    """
    inner = check.get("check")
    if not isinstance(inner, dict):
        return None
    arguments = inner.get("arguments")
    if not isinstance(arguments, dict):
        return None
    ref = arguments.get(_REF_TABLE_ARG)
    if isinstance(ref, str) and ref.strip():
        return ref.strip()
    return None


def collect_reference_tables(checks: list[dict[str, Any]]) -> list[str]:
    """Return the distinct, non-empty ``ref_table`` FQNs referenced by *checks*.

    First-occurrence order is preserved so view creation and any error reporting
    are deterministic.
    """
    seen: dict[str, None] = {}
    for check in checks:
        ref = _ref_table_of(check)
        if ref is not None and ref not in seen:
            seen[ref] = None
    return list(seen)


def rewrite_reference_tables(checks: list[dict[str, Any]], view_by_table: dict[str, str]) -> list[dict[str, Any]]:
    """Return a deep copy of *checks* with each ``ref_table`` swapped for its view.

    *view_by_table* maps an original reference-table FQN to the temporary-view
    FQN that stands in for it. Checks whose ``ref_table`` is absent from the map
    (or that have none) are copied through unchanged. The input list is never
    mutated.
    """
    rewritten = copy.deepcopy(checks)
    for check in rewritten:
        inner = check.get("check")
        if not isinstance(inner, dict):
            continue
        arguments = inner.get("arguments")
        if not isinstance(arguments, dict):
            continue
        ref = arguments.get(_REF_TABLE_ARG)
        if isinstance(ref, str) and ref.strip() in view_by_table:
            arguments[_REF_TABLE_ARG] = view_by_table[ref.strip()]
    return rewritten


class ReferenceViews:
    """Outcome of exposing a checks list's reference tables as temp views.

    Attributes:
        checks: The checks list with every ``ref_table`` rewritten to its view
            FQN (the original list when there were no reference tables).
        created_view_fqns: View FQNs this call created, in creation order, so the
            caller can drop them if the job submission fails.
    """

    __slots__ = ("checks", "created_view_fqns")

    def __init__(self, checks: list[dict[str, Any]], created_view_fqns: list[str]) -> None:
        self.checks = checks
        self.created_view_fqns = created_view_fqns


def build_reference_views(
    checks: list[dict[str, Any]],
    create_view: Callable[[str], str],
    *,
    monitored_table_fqn: str | None = None,
    monitored_view_fqn: str | None = None,
    drop_view: Callable[[str], None] | None = None,
) -> ReferenceViews:
    """Materialise a temp view for every reference table in *checks* and rewrite them.

    Each distinct ``ref_table`` is passed to *create_view* (wired to
    :func:`server.utils.create_temp_view`, which produces a full, unsampled view
    under the caller's OBO token) and its ``ref_table`` is rewritten to the
    returned view FQN. When a reference table equals the already-materialised
    monitored table, its existing view is reused rather than creating a second
    identical one.

    Args:
        checks: The checks list to scan and rewrite.
        create_view: Maps a reference-table FQN to a temp-view FQN.
        monitored_table_fqn: FQN of the already-materialised monitored table, when
            known, enabling self-reference reuse.
        monitored_view_fqn: The monitored table's view FQN, reused when a
            reference table equals *monitored_table_fqn*.
        drop_view: Optional cleanup callable; when a later view creation fails,
            views created earlier in this call are dropped before re-raising so a
            partial failure never leaks views.

    Returns:
        A :class:`ReferenceViews` with the rewritten checks and the FQNs created.

    Raises:
        Propagates any error from *create_view* (after best-effort cleanup).
    """
    ref_tables = collect_reference_tables(checks)
    if not ref_tables:
        return ReferenceViews(checks, [])

    view_by_table: dict[str, str] = {}
    created: list[str] = []
    try:
        for ref in ref_tables:
            if monitored_table_fqn and monitored_view_fqn and ref == monitored_table_fqn:
                # Reuse the monitored view for a self-referential check; it is
                # itself an unsampled full view, so results stay correct.
                view_by_table[ref] = monitored_view_fqn
                continue
            view_fqn = create_view(ref)
            view_by_table[ref] = view_fqn
            created.append(view_fqn)
    except Exception:
        if drop_view is not None:
            for view_fqn in created:
                drop_view(view_fqn)
        raise

    return ReferenceViews(rewrite_reference_tables(checks, view_by_table), created)
