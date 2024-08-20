import abc
import logging
from datetime import timedelta
from functools import cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import WorkspaceConfig

logger = logging.getLogger(__name__)


class GlobalContext(abc.ABC):
    def __init__(self, named_parameters: dict[str, str] | None = None):
        if not named_parameters:
            named_parameters = {}
        self._named_parameters = named_parameters

    def replace(self, **kwargs):
        """Replace cached properties for unit testing purposes."""
        for key, value in kwargs.items():
            self.__dict__[key] = value
        return self

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        raise ValueError("Workspace client not set")

    @cached_property
    def named_parameters(self) -> dict[str, str]:
        return self._named_parameters

    @cached_property
    def product_info(self):
        return ProductInfo.from_class(WorkspaceConfig)

    @cached_property
    def installation(self):
        return Installation.current(self.workspace_client, self.product_info.product_name())

    @cached_property
    def config(self) -> WorkspaceConfig:
        return self.installation.load(WorkspaceConfig)

    @cached_property
    def verify_timeout(self):
        return timedelta(minutes=2)

    @cached_property
    def wheels(self):
        return WheelsV2(self.installation, self.product_info)

    @cached_property
    def install_state(self):
        return InstallState.from_installation(self.installation)


class CliContext(GlobalContext, abc.ABC):
    @cached_property
    def prompts(self) -> Prompts:
        return Prompts()
