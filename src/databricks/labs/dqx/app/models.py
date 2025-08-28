from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from fastapi import Request

from databricks.labs.dqx import __version__
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam


class CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, serialize_by_alias=True, populate_by_name=True)


class VersionView(CamelModel):
    """
    Model representing the version information of the package.
    """

    version: str

    @classmethod
    def current(cls) -> "VersionView":
        return cls(version=__version__)


class ProfileView(CamelModel):
    user: iam.User

    @classmethod
    def from_ws(cls, ws: WorkspaceClient) -> "ProfileView":
        return cls(user=ws.current_user.me())

    @classmethod
    def from_request(cls, request: Request) -> "ProfileView":
        # make user from request headers, all other fields are None
        return cls(
            user=iam.User(
                user_name=request.headers.get("X-Forwarded-Email"),
                display_name=request.headers.get("X-Forwarded-Preferred-Username"),
            )
        )
