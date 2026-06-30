"""Alert destination adapters for DQX actions.

Exposes the abstract base classes and concrete destination implementations
used by the DQX actions & alerting subsystem.

Classes:
    AlertDestination: Abstract base class for all alert destinations.
    WebhookAlertDestination: Abstract dataclass base for webhook destinations.
    SlackDQAlertDestination: Slack Block Kit webhook destination.
    TeamsDQAlertDestination: Microsoft Teams MessageCard webhook destination.
    WebhookDQAlertDestination: Generic HTTPS webhook destination with optional
        Basic-auth support.
    CallbackDQAlertDestination: In-process callback destination.
"""

from __future__ import annotations

from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.destinations.callback import CallbackDQAlertDestination
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.destinations.teams import TeamsDQAlertDestination
from databricks.labs.dqx.actions.destinations.union import AnyDestination
from databricks.labs.dqx.actions.destinations.webhook import WebhookDQAlertDestination
from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination

__all__ = [
    "AlertDestination",
    "AnyDestination",
    "WebhookAlertDestination",
    "SlackDQAlertDestination",
    "TeamsDQAlertDestination",
    "WebhookDQAlertDestination",
    "CallbackDQAlertDestination",
]
