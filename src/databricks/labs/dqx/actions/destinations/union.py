"""Discriminated union over the concrete alert destination types.

*AnyDestination* is the Pydantic discriminated union used as the field type for
*DQAlert.destinations*.  Discrimination is keyed on the literal *type* field
that each concrete destination declares.  *DQCallbackAlertDestination* is a
member of the union (it is a valid runtime destination) even though the
serializer excludes it from persisted output, since it holds a live callable.
"""

from typing import Annotated

from pydantic import Field

from databricks.labs.dqx.actions.destinations.callback import DQCallbackAlertDestination
from databricks.labs.dqx.actions.destinations.log import DQLogAlertDestination
from databricks.labs.dqx.actions.destinations.slack import DQSlackAlertDestination
from databricks.labs.dqx.actions.destinations.teams import DQTeamsAlertDestination
from databricks.labs.dqx.actions.destinations.webhook import DQWebhookAlertDestination

AnyDestination = Annotated[
    DQSlackAlertDestination
    | DQTeamsAlertDestination
    | DQWebhookAlertDestination
    | DQLogAlertDestination
    | DQCallbackAlertDestination,
    Field(discriminator="type"),
]

__all__ = ["AnyDestination"]
