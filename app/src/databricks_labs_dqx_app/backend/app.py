from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .config import conf
from .logger import logger
from .router import api
from .utils import add_not_found_handler


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting app with configuration:\n{conf.model_dump_json(indent=2)}")
    yield


app = FastAPI(title=f"{conf.app_name}", lifespan=lifespan)

# Configure route for the backend API (/api) using fastapi
app.include_router(api)

# Configure route for the UI (static files)
# Mount static files for the UI only if the dist directory exists (e.g., after build)
# This allows the API to work in test environments without requiring a frontend build
if conf.static_assets_path.exists():
    # serve static files for the UI
    ui = StaticFiles(directory=conf.static_assets_path, html=True)
    # configure main route: anything that is not API is considered to be UI
    app.mount("/", ui)
else:  # for testing in CI environments
    logger.warning(f"Static assets path {conf.static_assets_path} not found. UI will not be available.")

add_not_found_handler(app)
