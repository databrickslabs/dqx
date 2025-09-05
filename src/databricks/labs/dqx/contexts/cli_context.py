import abc
from functools import cached_property
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.dqx.contexts.global_context import GlobalContext


class CliContext(GlobalContext, abc.ABC):
    """
    CliContext class that provides a global context for CLI operations, including prompts.
    """

    @cached_property
    def prompts(self) -> Prompts:
        return Prompts()
