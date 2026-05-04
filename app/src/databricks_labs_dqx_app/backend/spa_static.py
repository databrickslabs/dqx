"""StaticFiles subclass that returns index.html for SPA client-side routes.

When a request like ``/home`` or ``/rules/drafts`` arrives and no matching
static file exists, the standard ``StaticFiles`` returns 404.  For a
single-page application the correct behaviour is to serve ``index.html``
so the client-side router can handle the path.

API paths (``/api/...``) are never served by this mount so they are
unaffected.
"""

from starlette.exceptions import HTTPException
from starlette.staticfiles import StaticFiles
from starlette.types import Scope


_ASSET_EXTS = (
    ".js",
    ".mjs",
    ".css",
    ".map",
    ".png",
    ".jpg",
    ".jpeg",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
    ".json",
    ".webmanifest",
    ".txt",
    ".ttf",
    ".eot",
    ".gif",
    ".webp",
)


class SPAStaticFiles(StaticFiles):
    """Serve static assets with an ``index.html`` fallback for SPA routing."""

    async def get_response(self, path: str, scope: Scope):
        try:
            return await super().get_response(path, scope)
        except HTTPException as exc:
            if exc.status_code == 404:
                last_segment = path.rsplit("/", 1)[-1]
                if not last_segment.endswith(_ASSET_EXTS):
                    # ``index.html`` is intentionally relative — the parent's
                    # ``lookup_path`` joins it onto ``self.directory``. A
                    # leading slash would be treated as an absolute filesystem
                    # path and fail the security check, breaking the fallback.
                    return await super().get_response("index.html", scope)
            raise
