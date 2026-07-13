"""SCIM-backed principal search for the Steward and Permissions pickers (P22-D).

Ported from dqlake's ``routers/principals.py``. Searches workspace users and
groups via the SP ``WorkspaceClient`` (full SCIM read access), with a short
in-process cache so a steward typing into a picker doesn't re-hit SCIM on
every keystroke. Uses the index-friendly SCIM ``sw`` (starts-with) filter.
"""

from __future__ import annotations

import time
from itertools import islice
from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_sp_ws, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import PrincipalSearchOut

router = APIRouter()

# Any authenticated app role may search principals — the picker is used both by
# authors setting a steward and by grant-managers on the Permissions tab.
_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]

_CACHE_TTL_SECS = 60.0
_cache: dict[tuple[str, str, int], tuple[float, list[PrincipalSearchOut]]] = {}


def _quote_scim(s: str) -> str:
    """Escape double quotes for SCIM filter strings."""
    return s.replace('"', '\\"')


@router.get(
    "/search",
    operation_id="searchPrincipals",
    response_model=list[PrincipalSearchOut],
    dependencies=[require_role(*_ALL_ROLES)],
)
def search_principals(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    q: str = Query(min_length=1, max_length=128),
    limit: int = Query(default=20, le=50, ge=1),
) -> list[PrincipalSearchOut]:
    """Search workspace users and groups by name prefix.

    Returns up to ``limit`` matches (users first, then groups). Results are
    cached per (workspace, query, limit) for a short TTL. SCIM errors are
    swallowed so a partial result (e.g. users but not groups) is still useful.
    """
    key = (str(id(sp_ws)), q.strip().lower(), limit)
    now = time.time()
    hit = _cache.get(key)
    if hit is not None and hit[0] > now:
        return hit[1]

    out: list[PrincipalSearchOut] = []
    q_esc = _quote_scim(q)

    # ``sw`` (starts-with) uses the SCIM index; ``co`` (contains) does not.
    try:
        user_filter = f'displayName sw "{q_esc}" or userName sw "{q_esc}"'
        for u in islice(sp_ws.users.list(filter=user_filter, count=limit), limit):
            if not u.id:
                continue
            out.append(
                PrincipalSearchOut(
                    kind="user",
                    workspace_principal_id=u.id,
                    display_name=u.display_name or u.user_name or u.id,
                    secondary=u.user_name,
                )
            )
    except Exception:
        logger.warning("Principal user search failed (non-fatal)", exc_info=True)

    try:
        for g in islice(sp_ws.groups.list(filter=f'displayName sw "{q_esc}"', count=limit), limit):
            if not g.id:
                continue
            members = getattr(g, "members", None) or []
            out.append(
                PrincipalSearchOut(
                    kind="group",
                    workspace_principal_id=g.id,
                    display_name=g.display_name or g.id,
                    secondary=f"{len(members)} members",
                )
            )
    except Exception:
        logger.warning("Principal group search failed (non-fatal)", exc_info=True)

    result = out[:limit]
    _cache[key] = (now + _CACHE_TTL_SECS, result)
    return result
