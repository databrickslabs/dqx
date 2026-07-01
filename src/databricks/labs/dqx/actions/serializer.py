"""Serializer for *DQAction* instances.

*ActionSerializer* is a thin facade over Pydantic: it converts *DQAction*
objects to plain Python dicts (suitable for JSON / YAML persistence) and back.
Serialization and validation are driven entirely by the Pydantic models
(*DQAction*, *DQAlert*, *FailPipeline*, and the destination types) and their
discriminated unions — no per-type registries are needed.

Wire format
-----------
- Enums (*DQAlertFrequency*, *NotifyOn*) serialize to their string *value*.
- A *DQSecret* credential serializes to a tagged dict mapping the key *secret*
  to a *scope/key* reference (via *SecretOrStr*), so it round-trips losslessly
  without being confused with a plain string.
- *CallbackDQAlertDestination* instances are skipped during *to_dict* with a
  *WARNING*-level log message because they hold a live Python callable that
  cannot be persisted (see *DQAlert._serialize_destinations*).
- The *"condition"* field is omitted from the output of *to_dict* when it is
  *None*, and defaults back to *None* when absent on *from_dict*.

Security
--------
User-supplied names are sanitized before they appear in log messages to
prevent log injection (CWE-117).
"""

from __future__ import annotations

import pydantic

from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.errors import InvalidActionError


class ActionSerializer:
    """Converts *DQAction* instances to plain dicts and back via Pydantic.

    Adding a new action or destination type requires only declaring the new
    Pydantic model with its literal *type* discriminator and registering it in
    the relevant discriminated union (*AnyAction* or *AnyDestination*); the
    serializer itself needs no changes.

    *DQSecret* values round-trip as a tagged dict (key *secret* mapped to a
    *scope/key* reference) so the reference survives JSON / YAML without being
    confused with a plain string.

    *CallbackDQAlertDestination* instances are skipped during *to_dict* because
    they hold a live Python callable that cannot be persisted.  The
    *"condition"* field is omitted when *None*.
    """

    @staticmethod
    def to_dict(action: DQAction) -> dict[str, object]:
        """Serialize *action* to a plain Python dict.

        Args:
            action: The *DQAction* to serialize.

        Returns:
            A JSON-serializable dict representing *action*.  The *"condition"*
            key is omitted when the condition is *None*.
        """
        result: dict[str, object] = action.model_dump(mode="json")
        if action.condition is None:
            result.pop("condition", None)
        return result

    @staticmethod
    def from_dict(raw: dict[str, object]) -> DQAction:
        """Deserialize a plain dict into a *DQAction*.

        Args:
            raw: Dict produced by *to_dict* (or loaded from JSON / YAML).

        Returns:
            A fully reconstructed *DQAction*.

        Raises:
            InvalidActionError: If the payload is invalid — for example an
                unknown action or destination *"type"*, or a missing required
                field.
        """
        try:
            return DQAction.model_validate(raw)
        except pydantic.ValidationError as exc:
            raise InvalidActionError(str(exc)) from exc


__all__ = ["ActionSerializer"]
