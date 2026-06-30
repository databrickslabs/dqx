"""Discriminated union over the concrete alert destination types.

*AnyDestination* is the Pydantic discriminated union used as the field type for
*DQAlert.destinations*.  Discrimination is keyed on the literal *type* field
that each concrete destination declares.  *CallbackDQAlertDestination* is a
member of the union (it is a valid runtime destination) even though the
serializer excludes it from persisted output, since it holds a live callable.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from databricks.labs.dqx.actions.destinations.callback import CallbackDQAlertDestination
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.destinations.teams import TeamsDQAlertDestination
from databricks.labs.dqx.actions.destinations.webhook import WebhookDQAlertDestination

AnyDestination = Annotated[
    SlackDQAlertDestination | TeamsDQAlertDestination | WebhookDQAlertDestination | CallbackDQAlertDestination,
    Field(discriminator="type"),
]

__all__ = ["AnyDestination"]
