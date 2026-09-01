"""Alert destination adapters for DQX actions.

Exposes the abstract base classes and concrete destination implementations
used by the DQX actions & alerting subsystem.

Classes:
    AlertDestination: Abstract base class for all alert destinations.
    WebhookAlertDestination: Abstract dataclass base for webhook destinations.
    DQSlackAlertDestination: Slack Block Kit webhook destination.
    DQTeamsAlertDestination: Microsoft Teams MessageCard webhook destination.
    DQWebhookAlertDestination: Generic HTTPS webhook destination with optional
        Basic-auth support.
    DQLogAlertDestination: Serializable destination that logs the alert to the
        driver logger (no external system).
    DQCallbackAlertDestination: In-process callback destination.
"""

from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.destinations.callback import DQCallbackAlertDestination
from databricks.labs.dqx.actions.destinations.log import DQLogAlertDestination
from databricks.labs.dqx.actions.destinations.slack import DQSlackAlertDestination
from databricks.labs.dqx.actions.destinations.teams import DQTeamsAlertDestination
from databricks.labs.dqx.actions.destinations.union import AnyDestination
from databricks.labs.dqx.actions.destinations.webhook import DQWebhookAlertDestination
from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination

__all__ = [
    "AlertDestination",
    "AnyDestination",
    "WebhookAlertDestination",
    "DQSlackAlertDestination",
    "DQTeamsAlertDestination",
    "DQWebhookAlertDestination",
    "DQLogAlertDestination",
    "DQCallbackAlertDestination",
]
