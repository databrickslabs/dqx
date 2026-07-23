"""FailPipeline action for the DQX actions & alerting subsystem.

This module defines *FailPipeline*, a terminal action that raises
*PipelineFailedError* to abort the current DQX run.  Because
*PipelineFailedError* is a *TerminalActionError* subclass, the evaluator
defers this error until all other non-terminal actions have completed.
"""

from typing import Literal

from databricks.labs.dqx.actions.base import Action, ActionContext, ActionResult, ActionServices
from databricks.labs.dqx.errors import PipelineFailedError


class FailPipeline(Action):
    """Raises *PipelineFailedError* to terminate the current DQX run.

    When *execute* is called this action always raises *PipelineFailedError*
    and therefore never returns an *ActionResult*.  The evaluator catches
    *TerminalActionError* (the parent class of *PipelineFailedError*) after
    all other actions have run and then propagates the error.

    Attributes:
        type: Discriminator literal, always *"fail_pipeline"*.
        message: Custom failure message to embed in the raised error.  When
            *None* (the default) a message is generated automatically from the
            observed metrics in *context*.
        name: Logical identifier for this action; defaults to
            *"fail_pipeline"*.
    """

    type: Literal["fail_pipeline"] = "fail_pipeline"
    message: str | None = None
    name: str = "fail_pipeline"

    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        """Raise *PipelineFailedError* to abort the current DQX run.

        Builds the failure message from *self.message* when provided, or
        generates a default message that includes the action name and the
        observed metrics from *context*.

        Args:
            context: Immutable snapshot of run-time state including observed
                metrics, run identifiers, and location metadata.
            services: Injected services (unused by this action).

        Raises:
            PipelineFailedError: Always.  This method never returns normally.

        Returns:
            This method never returns — it always raises *PipelineFailedError*.
        """
        if self.message is not None:
            failure_message = self.message
        else:
            failure_message = f"Pipeline failed by DQX action '{self.name}'. " f"Observed metrics: {context.metrics}"
        raise PipelineFailedError(failure_message)


__all__ = ["FailPipeline"]
