import abc
from functools import cached_property
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.installer.workflow_installer import DeployedWorkflows


class GlobalContext(abc.ABC):
    """
    GlobalContext class that provides a global context, including workspace client,
    """

    def __init__(self, named_parameters: dict[str, str] | None = None, install_folder: str | None = None):
        if not named_parameters:
            named_parameters = {}
        self._named_parameters = named_parameters
        self._install_folder = install_folder

    def replace(self, **kwargs):
        """
        Replace cached properties.

        Args:
            kwargs: Key-value pairs of properties to replace.

        Returns:
            The updated GlobalContext instance.
        """
        for key, value in kwargs.items():
            self.__dict__[key] = value
        return self

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        raise InvalidParameterError("Workspace client not set")

    @cached_property
    def named_parameters(self) -> dict[str, str]:
        return self._named_parameters

    @cached_property
    def product_info(self) -> ProductInfo:
        return ProductInfo.from_class(WorkspaceConfig)

    @cached_property
    def installation(self) -> Installation:
        if self._install_folder:
            return Installation(
                self.workspace_client, self.product_info.product_name(), install_folder=self._install_folder
            )
        return Installation.current(self.workspace_client, self.product_info.product_name())

    @cached_property
    def config(self) -> WorkspaceConfig:
        return self.installation.load(WorkspaceConfig)

    @cached_property
    def wheels(self) -> WheelsV2:
        return WheelsV2(self.installation, self.product_info)

    @cached_property
    def install_state(self) -> InstallState:
        return InstallState.from_installation(self.installation)

    @cached_property
    def deployed_workflows(self) -> DeployedWorkflows:
        return DeployedWorkflows(self.workspace_client, self.install_state)
