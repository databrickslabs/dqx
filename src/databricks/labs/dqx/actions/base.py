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
"""

import abc
import enum
from dataclasses import dataclass, field
from datetime import datetime

from pydantic import BaseModel
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.secrets import SecretResolver


# ---------------------------------------------------------------------------
# ActionStatus
# ---------------------------------------------------------------------------


class ActionStatus(enum.Enum):
    """Outcome of a triggered DQX action.

    Attributes:
        HEALTHY: The action completed without detecting a quality violation.
        UNHEALTHY: The action detected a quality violation.
        CONFIG_ERROR: The action could not be evaluated because of a configuration
            problem (e.g. its condition failed to evaluate against the observed
            metrics). This is distinct from *UNHEALTHY*: the data is not known to be
            bad, the action itself is misconfigured.
    """

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    CONFIG_ERROR = "config_error"


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
        condition: The gating condition expression of the action being executed, or *None* when the
            action fires unconditionally. Set per-action by the evaluator so an action (e.g. an alert
            message) can report *why* it fired; the engine leaves it *None* on the shared run context.
        extras: Mapping keyed by producing action name to the *dict[str, str]* payload that action
            returned via *ActionResult.extras*, or *None* when no producer has contributed yet.
            Populated by the evaluator as each action completes: the payload is copied before
            insertion, and the outer *ActionContext* is rebuilt via *dataclasses.replace* for the
            next action, so downstream actions cannot observe post-execute mutations by the producer.
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
    condition: str | None = None
    extras: dict[str, dict[str, str]] | None = None

    def get_extras(self, action_name: str) -> dict[str, str]:
        """Return the *extras* payload produced by *action_name*, or an empty dict if absent.

        Both levels of the *extras* structure are optional (outer ``None`` = no producer has run
        yet; missing key = that producer did not contribute). This accessor collapses both cases
        into an empty ``dict[str, str]`` so callers can write
        ``context.get_extras("collect_lineage").get("lineage_location")`` without the
        ``(context.extras or {}).get(...) or {}`` dance.

        Args:
            action_name: The producing action's *name* to look up.

        Returns:
            The producer's *dict[str, str]* payload, or an empty dict when there is none.
        """
        if self.extras is None:
            return {}
        return self.extras.get(action_name) or {}


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
        extras: Optional *dict[str, str]* payload produced by this action for consumption by later
            actions in the evaluator loop. *None* (the default) means the action produced no
            payload. The evaluator copies this value before inserting it into the next
            *ActionContext.extras* under the action's name, so authors do not need to defensively
            copy the payload themselves.
    """

    action_name: str
    fired: bool
    status: ActionStatus
    destination_errors: dict[str, str] = field(default_factory=dict)
    extras: dict[str, str] | None = None


# ---------------------------------------------------------------------------
# ActionServices
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActionServices:
    """Frozen container of injectable services available to action implementations.

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
