"""Action evaluator orchestrator for the DQX actions & alerting subsystem.

This module provides *ActionEvaluator* — the top-level orchestrator that drives
all configured *DQAction* instances through their full lifecycle:

1. **Condition evaluation** — if *dq.condition* is not *None*, the condition is
   evaluated against *context.metrics* via *ConditionEvaluator*.  A *False*
   result short-circuits the action (not-fired event recorded, continue).

2. **Suppression check** — *ActionStateStore.should_fire* gates frequency and
   status-change suppression for alert actions.  A *False* result short-circuits
   the action (not-fired event recorded, continue).

3. **Polymorphic dispatch** — *dq.action.execute(context, services)* is called
   uniformly for all action types.  No ``isinstance`` checks are performed here;
   new action types extend the system without modifying this evaluator
   (Open/Closed principle).

4. **Terminal error deferral** — *TerminalActionError* (e.g. *PipelineFailedError*)
   is caught and deferred until after all actions have been evaluated.  This
   ensures every alert is delivered before the pipeline is terminated.
"""

from __future__ import annotations

import logging

from databricks.labs.dqx.actions.base import ActionContext, ActionResult, ActionServices, ActionStatus
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log as _sanitize
from databricks.labs.dqx.actions.conditions import ConditionEvaluator
from databricks.labs.dqx.actions.message import StandardMessageBuilder
from databricks.labs.dqx.actions.state import ActionStateStore, AlertEvent
from databricks.labs.dqx.errors import TerminalActionError

logger = logging.getLogger(__name__)


class ActionEvaluator:
    """Orchestrator that drives all configured *DQAction* instances.

    For each *DQAction* in *actions*, *evaluate* follows this pipeline:

    1. If *dq.condition* is not *None*, evaluate it.  On *False*: record a
       not-fired *AlertEvent* and continue.
    2. Call *state_store.should_fire*.  On *False*: record a not-fired
       *AlertEvent* and continue.
    3. Execute the action via *dq.action.execute(context, services)* —
       **no** ``isinstance`` checks, purely polymorphic dispatch.  Record
       a fired *AlertEvent* and append the *ActionResult*.
    4. Catch *TerminalActionError* into a deferred list.

    After all actions: raise the first deferred *TerminalActionError* (if any),
    so that all alerts are delivered before the run is aborted.

    Args:
        actions: Ordered list of *DQAction* configurations to evaluate.
        state_store: State manager for frequency/status suppression and event
            persistence.
        services: Injectable services passed through to each *Action.execute*.
        message_builder: Optional message builder; reserved for subclasses or
            future extensions.  Not used by this evaluator directly.
    """

    def __init__(
        self,
        actions: list[DQAction],
        *,
        state_store: ActionStateStore,
        services: ActionServices,
        message_builder: StandardMessageBuilder | None = None,
    ) -> None:
        self._actions = actions
        self._state_store = state_store
        self._services = services
        self._message_builder = message_builder

    def evaluate(self, context: ActionContext) -> list[ActionResult]:
        """Evaluate all configured actions against *context*.

        Iterates through every *DQAction* in *self._actions* and applies the
        condition/suppression/execute pipeline described in the class docstring.
        *TerminalActionError* exceptions are deferred until after all actions
        have been processed, then the first is re-raised.

        Args:
            context: Immutable run-time snapshot carrying *metrics*, *run_id*,
                *run_time*, and optional location fields.

        Returns:
            List of *ActionResult* instances for every action that actually
            fired.  Skipped/suppressed actions do not appear in the list.

        Raises:
            TerminalActionError: The first terminal error encountered across
                all actions, raised after the full action loop completes.
        """
        results: list[ActionResult] = []
        deferred: list[TerminalActionError] = []

        for dq_action in self._actions:
            safe_name = _sanitize(dq_action.name)

            # ------------------------------------------------------------------
            # Step 1: condition evaluation
            # ------------------------------------------------------------------
            if dq_action.condition is not None:
                safe_condition = _sanitize(dq_action.condition)
                condition_result = ConditionEvaluator.evaluate(dq_action.condition, context.metrics)
                if not condition_result:
                    logger.debug(f"Action '{safe_name}' skipped: condition '{safe_condition}' evaluated to False.")
                    self._state_store.record(
                        self._build_event(dq_action, context, fired=False, status=ActionStatus.HEALTHY)
                    )
                    continue
            else:
                condition_result = True

            # ------------------------------------------------------------------
            # Step 2: suppression check
            # ------------------------------------------------------------------
            if not self._state_store.should_fire(dq_action, context, condition_result=condition_result):
                logger.debug(f"Action '{safe_name}' suppressed by state store.")
                self._state_store.record(
                    self._build_event(dq_action, context, fired=False, status=ActionStatus.HEALTHY)
                )
                continue

            # ------------------------------------------------------------------
            # Step 3: execute — polymorphic dispatch, no isinstance
            # ------------------------------------------------------------------
            try:
                result = dq_action.action.execute(context, self._services)
                results.append(result)
                self._state_store.record(
                    self._build_event(
                        dq_action,
                        context,
                        fired=True,
                        status=result.status,
                        destination_errors=result.destination_errors,
                    )
                )
            except TerminalActionError as exc:
                logger.warning(f"Action '{safe_name}' raised a terminal error; deferring until loop completes.")
                deferred.append(exc)
                self._state_store.record(
                    self._build_event(dq_action, context, fired=True, status=ActionStatus.UNHEALTHY)
                )

        # ------------------------------------------------------------------
        # After loop: raise the first deferred terminal error (if any)
        # ------------------------------------------------------------------
        if deferred:
            raise deferred[0]

        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_event(
        dq_action: DQAction,
        context: ActionContext,
        *,
        fired: bool,
        status: ActionStatus,
        destination_errors: dict[str, str] | None = None,
    ) -> AlertEvent:
        """Construct an *AlertEvent* from the provided arguments.

        Args:
            dq_action: The *DQAction* being evaluated.
            context: Run-time snapshot providing metrics and run metadata.
            fired: Whether the action executed (*True*) or was skipped (*False*).
            status: The *ActionStatus* to record.
            destination_errors: Optional mapping of destination name to error
                message; defaults to an empty dict.

        Returns:
            A frozen *AlertEvent* capturing the evaluation outcome.
        """
        errors = destination_errors or {}
        return AlertEvent(
            action_name=dq_action.name,
            condition=dq_action.condition,
            fired=fired,
            status=status,
            observed_metrics=context.metrics,
            run_id=context.run_id,
            run_time=context.run_time,
            input_location=context.input_location,
            destinations=list(errors.keys()),
            delivery_errors=list(errors.values()),
        )


__all__ = ["ActionEvaluator"]
