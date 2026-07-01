"""Core action types for the DQX actions & alerting subsystem.

This module defines the foundational building blocks used throughout the
*databricks.labs.dqx.actions* package:

- *ActionStatus* — outcome enum (healthy / unhealthy).
- *ActionContext* — frozen snapshot of run-time state passed to every action.
- *ActionResult* — frozen record of a single action's outcome.
- *ActionServices* — container for injectable services (secret resolver,
  webhook client, workspace client, Spark session).
- *Action* — abstract Pydantic base class that concrete actions extend.

The *DQAction* binding lives in *actions/dq_action.py* rather than here: its
*action* field is the discriminated union over the concrete action classes,
which import this module, so declaring it here would create an import cycle.

Forward-reference strategy
--------------------------
*WebhookClient* (defined in *actions/delivery.py*) and *SparkSession*
(from *pyspark.sql*) would introduce heavy or cyclic imports if resolved at
module load time.  Both are therefore imported **only** inside the
*TYPE_CHECKING* guard so that static type checkers can resolve them while the
module remains importable in any environment — including unit-test environments
where neither *delivery.py* nor PySpark exists.
"""

from __future__ import annotations

import abc
import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.secrets import SecretResolver

if TYPE_CHECKING:
    from databricks.labs.dqx.actions.delivery import WebhookClient
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# ActionStatus
# ---------------------------------------------------------------------------


class ActionStatus(enum.Enum):
    """Outcome of a triggered DQX action.

    Attributes:
        HEALTHY: The action completed without detecting a quality violation.
        UNHEALTHY: The action detected a quality violation or encountered an
            unrecoverable error.
    """

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


# ---------------------------------------------------------------------------
# ActionContext
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActionContext:
    """Immutable snapshot of run-time state passed to every *Action.execute* call.

    All location fields are optional — populate only the ones that are
    meaningful for a given run.

    Attributes:
        metrics: Mapping of metric name to observed value (for example, the
            metric *error_row_count* with a value of 12).
        run_id: Unique identifier for the DQX run that produced these metrics.
        run_time: Timestamp when the DQX run executed.
        run_name: Human-readable name for the run; defaults to *"dqx"*.
        input_location: Source path/URI of the data being checked, or *None*.
        output_location: Destination path/URI of checked output, or *None*.
        quarantine_location: Path/URI where quarantined rows are written, or *None*.
        checks_location: Path/URI of the checks definition file, or *None*.
        rule_set_fingerprint: Fingerprint of the rule set applied, or *None*.
        user_metadata: Arbitrary string-valued metadata supplied by the caller,
            or *None* when not provided.
    """

    metrics: dict[str, object]
    run_id: str
    run_time: datetime
    run_name: str = "dqx"
    input_location: str | None = None
    output_location: str | None = None
    quarantine_location: str | None = None
    checks_location: str | None = None
    rule_set_fingerprint: str | None = None
    user_metadata: dict[str, str] | None = None


# ---------------------------------------------------------------------------
# ActionResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActionResult:
    """Immutable record of a single action's outcome.

    Attributes:
        action_name: Logical name of the action that was executed.
        fired: Whether the action's condition evaluated to *True* (and the
            action was therefore executed).
        status: Aggregate outcome of the action execution.
        destination_errors: Mapping of destination name to error message for
            any delivery failures.  Empty when all deliveries succeeded.
    """

    action_name: str
    fired: bool
    status: ActionStatus
    destination_errors: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ActionServices
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActionServices:
    """Frozen container of injectable services available to action implementations.

    *WebhookClient* and *SparkSession* are typed via *TYPE_CHECKING*-only
    imports to avoid heavy or cyclic imports at module load time.  At runtime
    the type annotations are strings (due to *from __future__ import
    annotations*), so the dataclass fields accept any value assignable to
    those types without requiring the actual classes to be present.

    Attributes:
        secret_resolver: Resolver for plain-string or *DQSecret* credentials.
        webhook_client: HTTP client for delivering webhook-based notifications.
        ws: An authenticated *WorkspaceClient*, or *None* when workspace access
            is not required by this action.
        spark: An active *SparkSession*, or *None* when Spark is not required.
    """

    secret_resolver: SecretResolver
    webhook_client: WebhookClient
    ws: WorkspaceClient | None = None
    spark: SparkSession | None = None


# ---------------------------------------------------------------------------
# Action (abstract base class)
# ---------------------------------------------------------------------------


class Action(BaseModel, abc.ABC):
    """Abstract Pydantic base class for all DQX action implementations.

    Subclasses must declare a literal *type* discriminator field and override
    *execute*.  Construction-time validation of a subclass's own configuration
    is performed by Pydantic validators on the subclass (for example, a
    *model_validator* that raises *InvalidActionError*) rather than a separate
    *validate* method.

    Attributes:
        name: Logical identifier for this action instance.  Default is an empty
            string; concrete subclasses set a meaningful value.
    """

    model_config = {"arbitrary_types_allowed": True}

    name: str = ""

    @abc.abstractmethod
    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        """Execute this action and return its result.

        Args:
            context: Immutable snapshot of run-time state including observed
                metrics, run identifiers, and location metadata.
            services: Injected services (secret resolver, webhook client,
                workspace client, Spark session).

        Returns:
            An *ActionResult* describing whether the action fired and its
            aggregate outcome.
        """


__all__ = [
    "Action",
    "ActionContext",
    "ActionResult",
    "ActionServices",
    "ActionStatus",
]
