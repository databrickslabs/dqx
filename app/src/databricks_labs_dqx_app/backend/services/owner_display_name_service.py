"""Best-effort SCIM resolver for owner identities.

Objects (registry rules, monitored tables, data products) each persist two
owner fields: ``owner`` (the identity — an email/username, or a group
name) and ``owner_display_name`` (a human-readable "Firstname Lastname").
The list pages and Permissions tab render ``owner_display_name || owner``,
so a friendly name shows whenever the column is populated.

Owners often arrive as free text (imported YAML / contracts), so this module
matches them against Databricks principals case-insensitively — users by
``userName`` or any email, then groups by name, then service principals by
application id — and returns the canonical identity plus display name.

Resolution is strictly best-effort and NEVER raises: a SCIM error leaves the
owner *unknown* (not cached, not reported as missing), whereas a successful
lookup with no match is a confirmed *not found* that the UI flags so the
owner can be corrected. An in-process TTL cache keeps repeated writes and
list reads from re-hitting SCIM; confirmed misses are cached longer because
nothing is written back for them, so they would otherwise be re-resolved on
every read.
"""

import logging
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import islice
from typing import Literal, Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest

from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

PrincipalKind = Literal["user", "group", "service_principal"]


@dataclass(frozen=True)
class ResolvedOwner:
    identity: str
    display_name: str | None
    kind: PrincipalKind


def _quote_scim(s: str) -> str:
    """Escape double quotes for SCIM filter strings."""
    return s.replace('"', '\\"')


# Maximum identities resolved per SCIM call batch (avoids very long filter strings).
_BATCH_SIZE = 25

# SCIM list endpoints omit displayName unless it is requested explicitly.
_USER_ATTRIBUTES = "userName,displayName,emails"
_GROUP_ATTRIBUTES = "displayName"
_SP_ATTRIBUTES = "applicationId,displayName"

# Keyed by the lower-cased owner. ``None`` = confirmed no matching principal.
_RESOLVE_CACHE_TTL_SECS = 300.0
_MISS_CACHE_TTL_SECS = 3600.0
_resolve_cache: dict[str, tuple[float, ResolvedOwner | None]] = {}

# Owners whose SCIM resolution is already queued, so concurrent reads don't
# schedule duplicate lookups for the same owner.
_inflight: set[str] = set()
_inflight_lock = threading.Lock()


def _key(owner: str) -> str:
    return owner.strip().lower()


def _list_users_by_key(filter_str: str, sp_ws: WorkspaceClient) -> dict[str, ResolvedOwner]:
    by_key: dict[str, ResolvedOwner] = {}
    for user in sp_ws.users.list(filter=filter_str, attributes=_USER_ATTRIBUTES, count=_BATCH_SIZE * 2):
        if not user.user_name:
            continue
        resolved = ResolvedOwner(user.user_name, user.display_name or None, "user")
        by_key[_key(user.user_name)] = resolved
        for email in user.emails or []:
            if email.value:
                by_key.setdefault(_key(email.value), resolved)
    return by_key


def _lookup_users(owners: list[str], sp_ws: WorkspaceClient) -> dict[str, ResolvedOwner | None]:
    found: dict[str, ResolvedOwner | None] = {}
    for chunk in _chunks(owners, _BATCH_SIZE):
        try:
            try:
                by_key = _list_users_by_key(
                    " or ".join(f'userName eq "{_quote_scim(o)}" or emails.value eq "{_quote_scim(o)}"' for o in chunk),
                    sp_ws,
                )
            except BadRequest:
                # Some workspaces reject filtering on ``emails.value``.
                by_key = _list_users_by_key(" or ".join(f'userName eq "{_quote_scim(o)}"' for o in chunk), sp_ws)
        except Exception:
            logger.warning("SCIM user lookup failed during owner resolution (non-fatal)", exc_info=True)
            continue
        for owner in chunk:
            found[owner] = by_key.get(_key(owner))
    return found


def _lookup_group(owner: str, sp_ws: WorkspaceClient) -> ResolvedOwner | None:
    for group in sp_ws.groups.list(
        filter=f'displayName eq "{_quote_scim(owner)}"', attributes=_GROUP_ATTRIBUTES, count=1
    ):
        if group.display_name:
            return ResolvedOwner(group.display_name, group.display_name, "group")
    return None


def _lookup_service_principal(owner: str, sp_ws: WorkspaceClient) -> ResolvedOwner | None:
    for sp in sp_ws.service_principals.list(
        filter=f'applicationId eq "{_quote_scim(owner)}"', attributes=_SP_ATTRIBUTES, count=1
    ):
        if sp.application_id:
            return ResolvedOwner(sp.application_id, sp.display_name or None, "service_principal")
    return None


def lookup_owners(owners: list[str], sp_ws: WorkspaceClient | None) -> dict[str, ResolvedOwner | None]:
    """Resolve *owners* to Databricks principals.

    Returns ``{owner: ResolvedOwner}`` for matches and ``{owner: None}`` for a
    confirmed miss. Owners whose lookup failed (SCIM error, no client) are
    absent from the result — callers must treat them as unknown, not missing.
    """
    if sp_ws is None:
        return {}
    now = time.time()
    result: dict[str, ResolvedOwner | None] = {}
    misses: list[str] = []
    for owner in dict.fromkeys(o.strip() for o in owners if o and o.strip()):
        hit = _resolve_cache.get(_key(owner))
        if hit is not None and hit[0] > now:
            result[owner] = hit[1]
        else:
            misses.append(owner)
    if not misses:
        return result

    looked_up = _lookup_users(misses, sp_ws)
    for owner in misses:
        if owner not in looked_up:
            continue
        resolved = looked_up[owner]
        if resolved is None and "@" not in owner:
            try:
                resolved = _lookup_group(owner, sp_ws) or _lookup_service_principal(owner, sp_ws)
            except Exception:
                logger.warning("SCIM group/SP lookup failed during owner resolution (non-fatal)", exc_info=True)
                continue
        ttl = _RESOLVE_CACHE_TTL_SECS if resolved is not None else _MISS_CACHE_TTL_SECS
        _resolve_cache[_key(owner)] = (now + ttl, resolved)
        result[owner] = resolved
    return result


def peek_owners(owners: list[str]) -> tuple[dict[str, ResolvedOwner | None], list[str]]:
    """Cache-only counterpart of :func:`lookup_owners`; never calls SCIM.

    Returns ``(cached, uncached)``: fresh cache entries keyed like
    :func:`lookup_owners`, plus the stripped owners with no fresh entry.
    """
    now = time.time()
    cached: dict[str, ResolvedOwner | None] = {}
    uncached: list[str] = []
    for owner in dict.fromkeys(o.strip() for o in owners if o and o.strip()):
        hit = _resolve_cache.get(_key(owner))
        if hit is not None and hit[0] > now:
            cached[owner] = hit[1]
        else:
            uncached.append(owner)
    return cached, uncached


def claim_owner_resolution(owners: list[str]) -> list[str]:
    """Mark *owners* as queued for resolution; returns only those not already queued."""
    with _inflight_lock:
        claimed = [o for o in owners if _key(o) not in _inflight]
        _inflight.update(_key(o) for o in claimed)
    return claimed


def release_owner_resolution(owners: list[str]) -> None:
    with _inflight_lock:
        _inflight.difference_update(_key(o) for o in owners)


def canonicalize_owner(owner: str | None, sp_ws: WorkspaceClient | None) -> tuple[str | None, str | None]:
    """Return ``(identity, display_name)`` for a free-text *owner*.

    A matched principal replaces *owner* with its canonical identity (e.g. a
    mis-cased email becomes the real ``userName``). Unmatched or unknown owners
    are kept verbatim with no display name.
    """
    if not owner or not owner.strip():
        return owner, None
    resolved = lookup_owners([owner], sp_ws).get(owner.strip())
    if resolved is None:
        return owner.strip(), None
    return resolved.identity, resolved.display_name if resolved.kind != "group" else None


def resolve_emails_to_display_names(emails: list[str], sp_ws: WorkspaceClient) -> dict[str, str]:
    """Resolve *emails* to user display names (``{email: display_name}``, matches only)."""
    return {
        email: resolved.display_name
        for email, resolved in _lookup_users(list(dict.fromkeys(emails)), sp_ws).items()
        if resolved is not None and resolved.display_name
    }


def resolve_owners_cached(owners: list[str], sp_ws: WorkspaceClient | None) -> dict[str, str]:
    """Resolve many *owners* to user display names via the shared TTL cache.

    Used for read-time enrichment of list endpoints so rows written before
    write-time resolution existed (or while SCIM was unavailable) self-heal.
    Returns only owners that resolved to a user with a display name.
    """
    return {
        owner: resolved.display_name
        for owner, resolved in lookup_owners(owners, sp_ws).items()
        if resolved is not None and resolved.kind != "group" and resolved.display_name
    }


def resolve_owner_display_name(owner: str | None, sp_ws: WorkspaceClient | None) -> str | None:
    """Resolve a single *owner* identity to its display name, or ``None``."""
    if not owner:
        return None
    return resolve_owners_cached([owner], sp_ws).get(owner.strip())


class _OwnedObject(Protocol):
    owner: str | None
    owner_display_name: str | None


class _SqlExecutor(Protocol):
    def execute(self, sql: str) -> object: ...


def fill_missing_owner_display_names(
    objects: Sequence[_OwnedObject],
    sp_ws: WorkspaceClient | None,
    sql: _SqlExecutor,
    table: str,
) -> None:
    """Resolve NULL ``owner_display_name`` values on read and persist them.

    Rows written before write-time resolution existed, or while SCIM was
    unavailable, keep a NULL display name and render as a raw email next to
    rows that show a friendly name. This fills *objects* in place (so the
    response is consistent immediately) and writes the names back to *table*
    so later reads skip SCIM. Only rows still missing a name are updated.
    Best-effort: lookup or write failures never break the read.
    """
    missing = [o.owner for o in objects if o.owner and not o.owner_display_name]
    if not missing:
        return
    resolved = resolve_owners_cached(missing, sp_ws)
    if not resolved:
        return
    for obj in objects:
        if obj.owner and not obj.owner_display_name:
            obj.owner_display_name = resolved.get(obj.owner.strip())
    stmt = owner_display_name_backfill_sql(table, resolved)
    if stmt is None:
        return
    try:
        sql.execute(stmt)
    except Exception:
        logger.warning("Owner display-name backfill failed for %s (non-fatal)", table, exc_info=True)


DeferredTaskRunner = Callable[[Callable[[], None]], None]
"""Runs a zero-argument task later (a background worker in production, inline in tests)."""

_background_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="owner-display-name")


def run_in_background(task: Callable[[], None]) -> None:
    """Default :data:`DeferredTaskRunner`: queue *task* on a small in-process worker pool."""
    _background_pool.submit(task)


def fill_owner_display_names_from_cache(
    objects: Sequence[_OwnedObject],
    sp_ws: WorkspaceClient | None,
    sql: _SqlExecutor,
    table: str,
    *,
    defer: DeferredTaskRunner = run_in_background,
) -> None:
    """Non-blocking counterpart of :func:`fill_missing_owner_display_names` for list reads.

    Fills NULL ``owner_display_name`` values on *objects* from the shared
    resolver cache only, so a list endpoint never waits on SCIM. Owners with no
    fresh cache entry are resolved through *defer* (off the request path),
    which writes their names back to *table* in one batched ``UPDATE`` so the
    next read has them. Concurrent reads never queue the same owner twice.
    """
    missing = [o.owner for o in objects if o.owner and not o.owner_display_name]
    if not missing:
        return
    cached, uncached = peek_owners(missing)
    for obj in objects:
        if not obj.owner or obj.owner_display_name:
            continue
        match = cached.get(obj.owner.strip())
        if match is not None and match.kind != "group" and match.display_name:
            obj.owner_display_name = match.display_name
    if sp_ws is None:
        return
    claimed = claim_owner_resolution(uncached)
    if claimed:
        defer(lambda: _resolve_and_backfill(claimed, sp_ws, sql, table))


def _resolve_and_backfill(owners: list[str], sp_ws: WorkspaceClient, sql: _SqlExecutor, table: str) -> None:
    """Resolve *owners* against SCIM and persist their display names to *table* (best-effort)."""
    try:
        stmt = owner_display_name_backfill_sql(table, resolve_owners_cached(owners, sp_ws))
        if stmt is not None:
            sql.execute(stmt)
    except Exception:
        logger.warning("Background owner display-name backfill failed for %s (non-fatal)", table, exc_info=True)
    finally:
        release_owner_resolution(owners)


def owner_display_name_backfill_sql(table: str, resolved: dict[str, str]) -> str | None:
    """One ``UPDATE`` that fills NULL/empty ``owner_display_name`` for every owner in *resolved*.

    Rows that already carry a name are never touched. Returns ``None`` when
    there is nothing to write.
    """
    pairs = [(escape_sql_string(o), escape_sql_string(n)) for o, n in resolved.items() if o and n]
    if not pairs:
        return None
    cases = " ".join(f"WHEN '{o}' THEN '{n}'" for o, n in pairs)
    owners = ", ".join(f"'{o}'" for o, _ in pairs)
    return (
        f"UPDATE {table} SET owner_display_name = CASE owner {cases} END "  # noqa: S608
        f"WHERE owner IN ({owners}) AND (owner_display_name IS NULL OR owner_display_name = '')"
    )


def _chunks(lst: list[str], size: int) -> Iterator[list[str]]:
    it = iter(lst)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk
