from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from .config import conf
from .logger import logger


def add_not_found_handler(app: FastAPI):
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        logger.info(
            f"HTTP exception handler called for request {request.url.path} with status code {exc.status_code}"
        )
        if exc.status_code == 404:
            path = request.url.path
            accept = request.headers.get("accept", "")

            is_api = path.startswith(conf.api_prefix)
            is_get_page_nav = request.method == "GET" and "text/html" in accept

            # Heuristic: if the last path segment looks like a file (has a dot), don't SPA-fallback
            looks_like_asset = "." in path.split("/")[-1]

            if (not is_api) and is_get_page_nav and (not looks_like_asset):
                # Let the SPA router handle it
                return FileResponse(conf.static_assets_path / "index.html")
        # Default: return the original HTTP error (JSON 404 for API, etc.)
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    app.exception_handler(StarletteHTTPException)(http_exception_handler)
