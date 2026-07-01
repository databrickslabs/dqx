"""Public API for the DQX actions and alerting subsystem.

This package provides an extensible *action* abstraction that runs when data
checked by DQX violates a user-defined *condition* evaluated against the summary
metrics produced by *DQMetricsObserver*. The first concrete actions are *DQAlert*
(notifications to Slack, Microsoft Teams, generic webhooks, or in-process
callbacks) and *FailPipeline* (raises an exception to stop the current pipeline).

Adding a new action or destination type requires only subclassing the relevant
abstract base and registering it in the serializer; the evaluator, engine, and
storage layers remain unchanged.
"""

from databricks.labs.dqx.actions.base import (
    Action,
    ActionContext,
    ActionResult,
    ActionServices,
    ActionStatus,
)
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.destinations import (
    AlertDestination,
    CallbackDQAlertDestination,
    LogDQAlertDestination,
    SlackDQAlertDestination,
    TeamsDQAlertDestination,
    WebhookAlertDestination,
    WebhookDQAlertDestination,
)
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.actions.manager import DQActionManager
from databricks.labs.dqx.actions.serializer import ActionSerializer, deserialize_actions, serialize_actions
from databricks.labs.dqx.actions.state import ActionStateStore, AlertEvent

__all__ = [
    "Action",
    "ActionContext",
    "ActionEvaluator",
    "ActionResult",
    "ActionSerializer",
    "ActionServices",
    "ActionStateStore",
    "ActionStatus",
    "AlertDestination",
    "AlertEvent",
    "CallbackDQAlertDestination",
    "DQAction",
    "DQActionManager",
    "DQAlert",
    "DQAlertFrequency",
    "FailPipeline",
    "LogDQAlertDestination",
    "NotifyOn",
    "SlackDQAlertDestination",
    "TeamsDQAlertDestination",
    "WebhookAlertDestination",
    "WebhookDQAlertDestination",
    "deserialize_actions",
    "serialize_actions",
]
