from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from databricks.labs.dqx.app.config import conf, rt
from databricks.labs.dqx.app.api import app as api_app


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    rt.logger.info(f"Starting DQX App with instance {app_instance}")
    yield


app = FastAPI(title="DQX App", lifespan=lifespan)
ui_app = StaticFiles(directory=conf.static_assets_path, html=True)

if conf.dev_token:
    rt.logger.info("Adding CORS middleware for development")
    origins = [
        "http://localhost:8000",
        "http://localhost:5173",
        "http://0.0.0.0:5173",
    ]

    api_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# note the order of mounts!
app.mount("/api", api_app)
app.mount("/", ui_app)


@app.exception_handler(404)
async def client_side_routing(request: Request, exc: Exception):
    rt.logger.error(f"Not found: {exc} while handling request {request}, returning index.html")
    return FileResponse(conf.static_assets_path / "index.html")
