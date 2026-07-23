"""No-op action for the DQX actions & alerting subsystem.

Defines *NoOpAction*, a minimal action that performs no side effect: when its
(optional) condition fires it just records the evaluation outcome. It is useful
for exercising the action machinery — condition gating, frequency / status-change
suppression, and event recording — without alerting or failing a pipeline, and as
an observe-only / dry-run mode that records what *would* have fired to the events
table without acting on it.
"""

from typing import Literal

from databricks.labs.dqx.actions.base import Action, ActionContext, ActionResult, ActionServices, ActionStatus


class NoOpAction(Action):
    """Action that fires without any side effect.

    *execute* returns an *ActionResult* with *fired=True* and *status=UNHEALTHY*
    (mirroring *DQAlert*: the action fired because its condition detected a
    violation), but it never notifies, writes, or raises — so it never gates a
    pipeline (only *FailPipeline* does). Because it is serializable (*type: noop*)
    it round-trips through the metadata form and can be persisted and auto-loaded
    like any other action.

    Attributes:
        type: Discriminator literal, always *"noop"*.
        name: Logical identifier for this action; defaults to *"noop"*.
    """

    type: Literal["noop"] = "noop"
    name: str = "noop"

    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        """Record the evaluation outcome without performing any side effect.

        Args:
            context: Immutable snapshot of run-time state. Unused; accepted to
                satisfy the *Action* interface.
            services: Injected services. Unused; accepted to satisfy the *Action*
                interface.

        Returns:
            An *ActionResult* with *fired=True* and *status=UNHEALTHY*.
        """
        return ActionResult(action_name=self.name, fired=True, status=ActionStatus.UNHEALTHY)


__all__ = ["NoOpAction"]
