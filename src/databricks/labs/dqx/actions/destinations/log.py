"""Log alert destination for DQX actions.

Delivers DQX alert messages by writing them to the standard Python logger on the
Spark driver. Unlike webhook destinations it contacts no external system, which
makes it ideal for local development, demos, and end-to-end tests where the
alerting mechanism must be exercised without a live Slack / Teams / webhook
endpoint. Unlike *CallbackDQAlertDestination* it is fully serializable, so it has
a metadata (*type: log*) form and can be persisted and loaded like any other
destination.
"""

from __future__ import annotations

import logging
from typing import ClassVar, Literal

from pydantic import model_validator

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.errors import InvalidActionError

logger = logging.getLogger(__name__)


class LogDQAlertDestination(AlertDestination):
    """Alert destination that logs the alert message to the driver logger.

    Writes a single, sanitized log record summarizing the alert — title,
    condition, severity, table, run identifiers, and observed metrics — at the
    configured *level*. It performs no network I/O, so it never triggers SSRF
    validation and cannot stall a streaming micro-batch. Because it is
    serializable, it round-trips through the metadata (*type: log*) form and can
    be persisted alongside webhook destinations.

    Attributes:
        type: Discriminator literal, always *"log"*.
        name: Logical name for this destination instance.
        level: Logging level used to emit the alert. One of *debug*, *info*,
            *warning*, *error*, or *critical* (case-insensitive); defaults to
            *warning*.
    """

    type: Literal["log"] = "log"
    level: str = "warning"

    _LEVELS: ClassVar[dict[str, int]] = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    @model_validator(mode="after")
    def _validate_level(self) -> "LogDQAlertDestination":
        """Validate that *level* names a supported logging level.

        Returns:
            This destination instance, with *level* normalized to lower case.

        Raises:
            InvalidActionError: If *level* is not a recognized logging level.
        """
        normalized = self.level.lower()
        if normalized not in self._LEVELS:
            supported = ", ".join(sorted(self._LEVELS))
            raise InvalidActionError(f"LogDQAlertDestination.level must be one of: {supported}; got {self.level!r}.")
        self.level = normalized
        return self

    def deliver(self, message: AlertMessage, _context: ActionContext, _services: ActionServices) -> None:
        """Log *message* at the configured level.

        The rendered line is sanitized (CWE-117) because it embeds
        user-influenced values (condition, table, observed metrics).

        The *_context* and *_services* parameters are unused; they are accepted to
        satisfy the *AlertDestination* interface.

        Args:
            message: Immutable alert message payload assembled by *StandardMessageBuilder*.
        """
        metrics = ", ".join(f"{name}={value}" for name, value in message.observed_metrics.items())
        rendered = (
            f"[DQX alert] {message.title} | severity={message.severity} | "
            f"condition={message.condition} | table={message.table} | "
            f"run_id={message.run_id} | run_time={message.run_time.isoformat()} | "
            f"metrics: {metrics} | {message.summary}"
        )
        logger.log(self._LEVELS[self.level], sanitize_for_log(rendered))


__all__ = ["LogDQAlertDestination"]
