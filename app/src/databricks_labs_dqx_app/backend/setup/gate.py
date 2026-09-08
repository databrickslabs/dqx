"""FastAPI middleware that restricts APIs until setup is ready."""

from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from databricks_labs_dqx_app.backend.setup.models import SetupState
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime

_ALLOWED_BEFORE_READY = frozenset(
    {
        ("GET", "/api/health"),
        ("GET", "/api/v1/version"),
        ("GET", "/api/v1/current-user"),
        ("GET", "/api/v1/config/workspace-host"),
        ("GET", "/api/v1/setup/status"),
        ("POST", "/api/v1/setup/reconcile"),
    }
)


class SetupGateMiddleware(BaseHTTPMiddleware):
    """Reject database-backed API traffic until setup reaches READY."""

    def __init__(self, app: ASGIApp, runtime: SetupRuntime) -> None:
        super().__init__(app)
        self._runtime = runtime

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Apply the exact pre-readiness API allowlist."""
        path = request.url.path
        is_api_path = path == "/api" or path.startswith("/api/")
        allowed = (request.method, path) in _ALLOWED_BEFORE_READY
        if is_api_path and not allowed and self._runtime.report().state != SetupState.READY:
            return JSONResponse(status_code=503, content={"detail": "DQX Studio setup is required."})
        return await call_next(request)
