"""The *DQAction* binding and the *AnyAction* discriminated union.

This module binds a concrete *Action* (currently *DQAlert* or *FailPipeline*)
to an optional gating condition and a logical name.

*DQAction* lives in its own module — separate from *actions/base.py* — because
its *action* field is typed as the discriminated union *AnyAction*, which must
import the concrete action classes.  *base.py* is imported by those concrete
actions, so declaring *DQAction* there would create an import cycle.  Keeping
the binding here imports in a single direction
(*dq_action* depends on *alert* and *fail_pipeline*, which depend on *base*).
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.conditions import ConditionEvaluator
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.errors import InvalidActionError

# Discriminated union of all persistable concrete actions, keyed on the literal
# *type* field each concrete action declares.
AnyAction = Annotated[DQAlert | FailPipeline, Field(discriminator="type")]


class DQAction(BaseModel):
    """Binds an *Action* to an optional gating condition and a logical name.

    A *None* *condition* means the action fires unconditionally after every
    DQX run.  A non-*None* *condition* is validated at construction time via
    *ConditionEvaluator.validate* — a malformed expression surfaces immediately
    rather than at evaluation time.

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

    model_config = {"arbitrary_types_allowed": True}

    action: AnyAction
    condition: str | None = None
    name: str = ""

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise InvalidActionError(str(exc)) from exc

    @field_validator("condition")
    @classmethod
    def _validate_condition(cls, value: str | None) -> str | None:
        """Validate the gating condition expression when present.

        Args:
            value: The condition expression, or *None*.

        Returns:
            The validated condition expression unchanged.

        Raises:
            InvalidConditionError: If the expression is malformed.
        """
        if value is not None:
            ConditionEvaluator.validate(value)
        return value

    @model_validator(mode="after")
    def _derive_name(self) -> "DQAction":
        """Derive *name* when it was left empty.

        Returns:
            This *DQAction* instance, with *name* populated.
        """
        if not self.name:
            if self.action.name:
                self.name = self.action.name
            elif self.condition is not None:
                # Compact label: strip whitespace, replace spaces with underscores,
                # truncate to keep names readable.
                self.name = self.condition.strip().replace(" ", "_")[:64]
            else:
                self.name = type(self.action).__name__
        return self


__all__ = ["AnyAction", "DQAction"]
