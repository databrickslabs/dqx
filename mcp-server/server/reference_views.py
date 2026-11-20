"""Expose reference tables to the runner service principal via OBO temp views.

Reference checks — ``foreign_key``, ``has_valid_schema`` (reference-table mode),
``compare_datasets`` and ``aggr_matches_dataset`` — read a second table named by
the ``ref_table`` argument. The MCP server runs checks in a job as a
least-privilege runner service principal that reads the user's data only through
an OBO temporary view in the ``dqx_mcp_tmp`` schema; it holds no grant on base
tables. The monitored table is exposed this way, but the reference table was
not, so reference checks failed when DQX Core resolved ``ref_table`` with
``spark.table(...)`` as the runner service principal.

This module materialises one temp view per distinct reference table (via an
injected ``create_view`` callable, wired to :func:`server.utils.create_temp_view`
under the caller's OBO token) and rewrites each check to read that view. Rather
than putting the view FQN into ``ref_table`` — which would leak the random
``tmp_view_…`` name into check messages and change the rule fingerprint every run
— each check is switched to ``ref_df_name`` keyed by the *original* table name,
and the runner supplies ``ref_dfs = {original_name: spark.table(view_fqn)}``. The
original name is therefore what appears in check output and stays stable across
runs.

Views produced by :func:`server.utils.create_temp_view` are always full
(unsampled) ``SELECT *`` views, which is required: a sampled reference set would
drop valid keys and report false violations. The ``create_view``/``drop_view``
callables are injected so the pure functions and the orchestration can be unit
tested without Spark or a workspace.
"""

import copy
from collections.abc import Callable
from typing import Any

_REF_TABLE_ARG = "ref_table"
_REF_DF_NAME_ARG = "ref_df_name"


def _ref_table_of(check: dict[str, Any]) -> str | None:
    """Return the non-empty ``ref_table`` argument of one check, or ``None``.

    Tolerates malformed/partial check dicts (missing ``check``/``arguments``) and
    checks that already name an in-memory reference DataFrame via ``ref_df_name``
    — those carry no ``ref_table`` and are left untouched.
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


def rewrite_to_ref_df_name(checks: list[dict[str, Any]], handled: set[str]) -> list[dict[str, Any]]:
    """Return a deep copy of *checks* with each handled ``ref_table`` swapped for ``ref_df_name``.

    For every check whose ``ref_table`` is in *handled*, the ``ref_table`` argument
    is removed and ``ref_df_name`` is set to the same (original) table name, so the
    runner can resolve it from a ``ref_dfs`` entry keyed by that name. DQX Core
    rejects a check that carries both, which is why ``ref_table`` is deleted. The
    input list is never mutated; checks not in *handled* are copied unchanged.
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
        if isinstance(ref, str) and ref.strip() in handled:
            del arguments[_REF_TABLE_ARG]
            arguments[_REF_DF_NAME_ARG] = ref.strip()
    return rewritten


class ReferenceViews:
    """Outcome of exposing a checks list's reference tables as temp views.

    Attributes:
        checks: The checks list with every handled ``ref_table`` rewritten to
            ``ref_df_name`` (the original list when there were no reference tables).
        ref_views: Map of original reference-table FQN to the temp-view FQN that
            stands in for it. Passed to the runner, which builds
            ``ref_dfs = {name: spark.table(view_fqn)}`` and drops the views when done.
        created_view_fqns: View FQNs this call *created* (excludes a reused
            monitored view), so the submitting caller can drop them if the job
            submission fails.
    """

    __slots__ = ("checks", "created_view_fqns", "ref_views")

    def __init__(self, checks: list[dict[str, Any]], ref_views: dict[str, str], created_view_fqns: list[str]) -> None:
        self.checks = checks
        self.ref_views = ref_views
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
    under the caller's OBO token). When a reference table equals the already
    materialised monitored table, its existing view is reused. The checks are then
    rewritten from ``ref_table`` to ``ref_df_name`` (see
    :func:`rewrite_to_ref_df_name`).

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
        A :class:`ReferenceViews` with the rewritten checks, the name→view map, and
        the FQNs created.

    Raises:
        RuntimeError: If a reference table's view cannot be created (e.g. the
            caller lacks SELECT on it); the message names the offending table.
    """
    ref_tables = collect_reference_tables(checks)
    if not ref_tables:
        return ReferenceViews(checks, {}, [])

    view_by_table: dict[str, str] = {}
    created: list[str] = []
    for ref in ref_tables:
        if monitored_table_fqn and monitored_view_fqn and ref == monitored_table_fqn:
            # Reuse the monitored view for a self-referential check; it is itself
            # an unsampled full view, so results stay correct.
            view_by_table[ref] = monitored_view_fqn
            continue
        try:
            view_fqn = create_view(ref)
        except Exception as e:
            if drop_view is not None:
                for created_fqn in created:
                    drop_view(created_fqn)
            raise RuntimeError(
                f"Cannot create a temporary view over reference table '{ref}'. " "The caller may lack SELECT on it."
            ) from e
        view_by_table[ref] = view_fqn
        created.append(view_fqn)

    rewritten = rewrite_to_ref_df_name(checks, set(view_by_table))
    return ReferenceViews(rewritten, view_by_table, created)
