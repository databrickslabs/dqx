"""The *DQAction* binding.

This module binds a concrete *Action* (any registered type — the built-in *DQAlert*,
*FailPipeline*, and *NoOpAction*, plus any custom action registered via *register_action*) to an
optional gating condition and a logical name.

*DQAction* lives in its own module — separate from *actions/base.py* — because resolving the
*action* field imports the action registry, which imports the concrete action classes. *base.py* is
imported by those concrete actions, so declaring *DQAction* there would create an import cycle.

Extensibility: instead of a closed discriminated union of the built-ins, the *action* field accepts
any *Action* subclass registered in *ACTION_REGISTRY*. A metadata dict is resolved to its concrete
class by the *type* discriminator via the registry, so custom actions round-trip through metadata
exactly like the built-ins (their defining module must be imported first so registration has run).
"""

from typing import Any

from pydantic import BaseModel, SerializeAsAny, ValidationError, field_validator, model_validator

from databricks.labs.dqx.actions.base import Action
from databricks.labs.dqx.actions.conditions import ConditionEvaluator
from databricks.labs.dqx.actions.registry import resolve_action_type
from databricks.labs.dqx.errors import InvalidActionError

# The built-in actions register themselves via @register_action when their modules are imported; the
# actions package __init__ imports all three, so ACTION_REGISTRY is populated before any DQAction is
# constructed. This mirrors how check functions self-register via @register_rule into CHECK_FUNC_REGISTRY.


class DQAction(BaseModel):
    """Binds an *Action* to an optional gating condition and a logical name.

    A *None* *condition* means the action fires unconditionally after every
    DQX run.  A non-*None* *condition* is validated at construction time via
    *ConditionEvaluator.validate* — a malformed expression surfaces immediately
    rather than at evaluation time.

    The *action* may be any registered *Action* subclass (see *register_action*), so custom actions
    work both programmatically and as metadata.

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

    # SerializeAsAny so model_dump() serializes the *concrete* action subclass (its own 'type' and
    # fields), not just the base Action schema — otherwise custom fields are lost on round-trip.
    action: SerializeAsAny[Action]
    condition: str | None = None
    name: str = ""

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise InvalidActionError(str(exc)) from exc

    @field_validator("action", mode="before")
    @classmethod
    def _resolve_action(cls, value: object) -> object:
        """Resolve *action* from a metadata dict via the action registry.

        A dict carrying a *type* discriminator (the metadata form) is resolved to its registered
        concrete *Action* subclass and constructed. An already-constructed *Action* instance is
        returned unchanged (its type is validated by *_validate_action_registered* afterwards). Any
        other shape is passed through for Pydantic to reject.

        Raises:
            InvalidActionError: If a dict has no *type*, or *type* is not a registered action.
        """
        if isinstance(value, dict):
            action_type = value.get("type")
            if not isinstance(action_type, str) or not action_type:
                raise InvalidActionError("Action metadata must include a non-empty 'type' discriminator.")
            action_cls = resolve_action_type(action_type)
            # Construct the concrete action from the remaining fields (Pydantic ignores extra 'type').
            return action_cls.model_validate(value)
        return value

    @model_validator(mode="after")
    def _validate_action_registered(self) -> "DQAction":
        """Ensure the bound action's concrete class is the one registered under its *type*.

        Guards the programmatic path: an instance of an unregistered custom *Action* subclass would
        otherwise silently bypass the registry (and could not be persisted/reloaded). Checking the
        class identity — not merely that the *type* string is known — also rejects an unregistered
        subclass that reuses a registered *type* discriminator; such an instance would otherwise be
        silently substituted for the registered class on round-trip, dropping its extra fields.

        Raises:
            InvalidActionError: If the action declares no *type*, its *type* is not registered, or a
                *different* class is registered under that *type*.
        """
        action_type = getattr(self.action, "type", None)
        if not isinstance(action_type, str) or not action_type:
            raise InvalidActionError(
                f"Action {type(self.action).__name__} must declare a non-empty literal 'type' field."
            )
        registered_cls = resolve_action_type(action_type)  # raises InvalidActionError if not registered
        # Exact class identity (not isinstance): an unregistered subclass reusing a registered 'type'
        # must be rejected, since it would be silently substituted for the registered class on reload.
        bound_cls = self.action.__class__
        if bound_cls is not registered_cls:
            raise InvalidActionError(
                f"Action type '{action_type}' is registered to {registered_cls.__name__}, but the bound "
                f"action is a {bound_cls.__name__}. Register {bound_cls.__name__} under a "
                "unique 'type' via @register_action before using it."
            )
        return self

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


__all__ = ["DQAction"]
