from functools import partial
from fastapi import FastAPI, Request

from databricks.labs.dqx.app.models import VersionView, ProfileView
from databricks.labs.dqx.app.dependencies import get_user_workspace_client
from databricks.labs.dqx.app.config import rt
from databricks.labs.dqx.app.utils import custom_openapi


version_view = VersionView.current()

app = FastAPI(
    title="DQX | UI",
    version=version_view.version,
)


@app.get("/version", response_model=VersionView, operation_id="Version")
async def version():
    return version_view


@app.get("/profile", response_model=ProfileView, operation_id="Profile")
async def profile(request: Request):
    try:
        ws = get_user_workspace_client(request)
        return ProfileView.from_ws(ws)
    except Exception as e:
        rt.logger.error(f"Error getting user workspace client: {e}")
        return ProfileView.from_request(request)


app.openapi = partial(custom_openapi, app)  # type: ignore
