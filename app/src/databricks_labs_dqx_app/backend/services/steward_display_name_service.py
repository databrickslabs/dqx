"""Best-effort SCIM resolver for ``steward_display_name``.

Objects (registry rules, monitored tables, data products) each persist two
steward fields: ``steward`` (the identity — an email/username, or a group
name) and ``steward_display_name`` (a human-readable "Firstname Lastname").
The list pages and Permissions tab render ``steward_display_name || steward``,
so a friendly name shows whenever the column is populated.

This module resolves a steward identity to its SCIM display name at
**write time**: whenever a service sets ``steward`` and the caller did not
already supply a ``steward_display_name`` (the principal picker does), the
service calls :func:`resolve_steward_display_name` and persists the result
into the column. Resolution is strictly best-effort — a group name has no
SCIM user match, and any SCIM error is swallowed; both cases return ``None``
so the column is stored NULL (the frontend then shows the raw identity, which
for a group is its name). Resolution NEVER raises and NEVER blocks the write.

A short in-process TTL cache keeps repeated writes for the same steward
(e.g. bulk register, demo seed) from re-hitting SCIM.
"""

import logging
import time
from collections.abc import Iterator
from itertools import islice

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _quote_scim(s: str) -> str:
    """Escape double quotes for SCIM filter strings."""
    return s.replace('"', '\\"')

# Maximum emails resolved per SCIM call batch (avoids very long filter strings).
_BATCH_SIZE = 50

# Per-identity resolution cache. Maps a steward identity to
# ``(expires_at, display_name_or_none)``. A ``None`` value is cached too, so a
# group name / unresolvable email is not re-queried on every subsequent write.
_RESOLVE_CACHE_TTL_SECS = 300.0
_resolve_cache: dict[str, tuple[float, str | None]] = {}


def resolve_emails_to_display_names(
    emails: list[str],
    sp_ws: WorkspaceClient,
) -> dict[str, str]:
    """Resolve a batch of *emails* to SCIM display names.

    Returns a ``{email: display_name}`` dict for emails that matched a SCIM
    user. Unmatched emails are absent from the result. SCIM errors are logged
    and swallowed so a transient workspace outage never breaks a write.
    """
    result: dict[str, str] = {}
    for chunk in _chunks(emails, _BATCH_SIZE):
        try:
            filter_str = " or ".join(f'userName eq "{_quote_scim(e)}"' for e in chunk)
            for user in sp_ws.users.list(filter=filter_str, count=_BATCH_SIZE):
                if user.user_name and user.display_name:
                    result[user.user_name] = user.display_name
        except Exception:
            logger.warning("SCIM batch lookup failed during steward-name resolution (non-fatal)", exc_info=True)
    return result


def resolve_steward_display_name(steward: str | None, sp_ws: WorkspaceClient | None) -> str | None:
    """Resolve a single *steward* identity to its SCIM display name.

    Best-effort and cached: returns ``None`` (→ store NULL) when *steward* is
    empty, when no service-principal client is available, when *steward* is a
    group name / unresolvable email, or on any SCIM error. Never raises.
    """
    if not steward or sp_ws is None:
        return None
    now = time.time()
    hit = _resolve_cache.get(steward)
    if hit is not None and hit[0] > now:
        return hit[1]
    resolved = resolve_emails_to_display_names([steward], sp_ws).get(steward)
    _resolve_cache[steward] = (now + _RESOLVE_CACHE_TTL_SECS, resolved)
    return resolved


def _chunks(lst: list[str], size: int) -> Iterator[list[str]]:
    it = iter(lst)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk
