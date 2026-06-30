"""Core action types for the DQX actions & alerting subsystem.

This module defines the foundational building blocks used throughout the
``databricks.labs.dqx.actions`` package:

- *ActionStatus* — outcome enum (healthy / unhealthy).
- *ActionContext* — frozen snapshot of run-time state passed to every action.
- *ActionResult* — frozen record of a single action's outcome.
- *ActionServices* — container for injectable services (secret resolver,
  webhook client, workspace client, Spark session).
- *Action* — abstract base class that concrete actions extend.
- *DQAction* — dataclass that binds an *Action* to an optional gating condition
  and a logical name.

Forward-reference strategy
--------------------------
*WebhookClient* (defined in ``actions/delivery.py``, Task 6) and *SparkSession*
(from ``pyspark.sql``) would introduce heavy or cyclic imports if resolved at
module load time.  Both are therefore imported **only** inside the
``TYPE_CHECKING`` guard so that static type checkers can resolve them while the
module remains importable in any environment — including unit-test environments
where neither ``delivery.py`` nor PySpark exists.
"""

from __future__ import annotations

import abc
import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.conditions import ConditionEvaluator
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
        run_name: Human-readable name for the run; defaults to ``"dqx"``.
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

    *WebhookClient* and *SparkSession* are typed via ``TYPE_CHECKING``-only
    imports to avoid heavy or cyclic imports at module load time.  At runtime
    the type annotations are strings (due to ``from __future__ import
    annotations``), so the dataclass fields accept any value assignable to
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


class Action(abc.ABC):
    """Abstract base class for all DQX action implementations.

    Subclasses must override *execute*.  They may optionally override
    *validate* to perform construction-time validation of their own
    configuration fields; the default implementation is a no-op.

    Class / instance attribute:
        name: Logical identifier for this action type.  Default is an empty
            string; concrete subclasses should set a meaningful value.
    """

    name: str = ""

    def validate(self) -> None:
        """Validate this action's configuration at construction time.

        The default implementation is a no-op.  Override in subclasses to
        raise *InvalidActionError* when the action's own configuration is
        invalid.

        Raises:
            InvalidActionError: If the action configuration is invalid.
        """

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


# ---------------------------------------------------------------------------
# DQAction
# ---------------------------------------------------------------------------


@dataclass
class DQAction:
    """Binds an *Action* to an optional gating condition and a logical name.

    The required field *action* is listed first so callers may omit it by
    position when the action is the only argument::

        DQAction(action=MyAction())
        DQAction(condition="error_row_count > 0", action=MyAction())

    A ``None`` *condition* means the action fires unconditionally after every
    DQX run.  A non-``None`` *condition* is validated at construction time via
    *ConditionEvaluator.validate* — a malformed expression surfaces immediately
    rather than at evaluation time.

    The *action*'s own *validate()* is also called during construction, so
    configuration errors in the action itself are surfaced early.

    Name derivation (applied when *name* is empty):

    1. Use *action.name* if non-empty.
    2. Otherwise, if *condition* is set, derive a compact label from the
       condition string.
    3. Otherwise, use the action's class name.

    Attributes:
        action: The *Action* instance to execute.
        condition: Optional gating expression.  When *None* the action fires
            unconditionally.
        name: Logical name for this *DQAction* configuration entry.  Derived
            automatically when left empty.
    """

    action: Action
    condition: str | None = None
    name: str = ""

    def __post_init__(self) -> None:
        if self.condition is not None:
            ConditionEvaluator.validate(self.condition)

        self.action.validate()

        if not self.name:
            if self.action.name:
                self.name = self.action.name
            elif self.condition is not None:
                # Compact label: strip whitespace, replace spaces with underscores,
                # truncate to keep names readable.
                label = self.condition.strip().replace(" ", "_")[:64]
                self.name = label
            else:
                self.name = type(self.action).__name__


__all__ = [
    "Action",
    "ActionContext",
    "ActionResult",
    "ActionServices",
    "ActionStatus",
    "DQAction",
]
