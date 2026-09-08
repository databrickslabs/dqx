"""FastAPI application entry point for DQX Studio."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from databricks_labs_dqx_app.backend.config import conf
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.routes import api_router
from databricks_labs_dqx_app.backend.setup.gate import SetupGateMiddleware
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime
from databricks_labs_dqx_app.backend.startup import StartupContext, start_studio, stop_studio
from databricks_labs_dqx_app.backend.utils import add_not_found_handler

_SCHEDULER_LOCK_PATH = Path("/tmp/.dqx_scheduler.lock")  # noqa: S108
_scheduler_lock_fd: int | None = None


def _try_acquire_scheduler_lease() -> bool:
    """Acquire and retain the single-host scheduler lease."""
    import fcntl

    global _scheduler_lock_fd
    try:
        file_descriptor = os.open(str(_SCHEDULER_LOCK_PATH), os.O_CREAT | os.O_RDWR)
        fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _scheduler_lock_fd = file_descriptor
        return True
    except OSError:
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Reconcile setup without aborting the restricted FastAPI surface."""
    context: StartupContext | None = None
    try:
        context = await start_studio(app)
        yield
    finally:
        await stop_studio(context)


app = FastAPI(title=conf.app_name, lifespan=lifespan)


@app.get("/api/health", include_in_schema=False)
async def health() -> dict[str, str]:
    """Return dependency-free process liveness."""
    return {"status": "ok"}


app.add_middleware(SetupGateMiddleware, runtime=setup_runtime)
app.include_router(api_router)

if conf.static_assets_path.exists():
    from databricks_labs_dqx_app.backend.spa_static import SPAStaticFiles

    ui = SPAStaticFiles(directory=conf.static_assets_path, html=True)
    app.mount("/", ui)
else:
    logger.warning("Static assets are unavailable; the Studio UI will not be served")

add_not_found_handler(app)
