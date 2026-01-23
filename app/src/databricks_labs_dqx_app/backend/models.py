from typing import Any

from databricks.labs.dqx.config import RunConfig, WorkspaceConfig
from pydantic import BaseModel, Field

from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


class ConfigOut(BaseModel):
    config: WorkspaceConfig


class ConfigIn(BaseModel):
    config: WorkspaceConfig


class RunConfigOut(BaseModel):
    config: RunConfig


class RunConfigIn(BaseModel):
    config: RunConfig


class ChecksOut(BaseModel):
    checks: list[dict[str, Any]]


class ChecksIn(BaseModel):
    checks: list[dict[str, Any]]


class InstallationSettings(BaseModel):
    install_folder: str = Field(description="Path to the folder containing config.yml")
    is_default: bool = Field(default=False, description="Whether this is the default location")
