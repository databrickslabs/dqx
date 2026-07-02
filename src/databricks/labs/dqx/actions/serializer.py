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


def serialize_actions(actions: list[DQAction]) -> list[dict[str, object]]:
    """Serialize a list of *DQAction* instances to plain Python dicts.

    Convenience wrapper around *ActionSerializer.to_dict* for operating on
    a whole list at once.  The output is suitable for YAML or JSON
    persistence.

    Args:
        actions: List of *DQAction* instances to serialize.

    Returns:
        List of JSON-serializable dicts, one per action.
    """
    return [ActionSerializer.to_dict(a) for a in actions]


def deserialize_actions(metadata: list[dict[str, object]]) -> list[DQAction]:
    """Deserialize a list of plain dicts into *DQAction* instances.

    Convenience wrapper around *ActionSerializer.from_dict* for operating on
    a whole list at once.

    Args:
        metadata: List of dicts produced by *serialize_actions* (or loaded
            from YAML / JSON).  Each element must be a *dict*; passing a
            non-dict element raises *InvalidActionError*.

    Returns:
        List of fully reconstructed *DQAction* instances.

    Raises:
        InvalidActionError: If any element is not a *dict*, or if a dict
            cannot be validated as a *DQAction* (unknown *type*, missing
            required field, etc.).
    """
    result: list[DQAction] = []
    for i, item in enumerate(metadata):
        if not isinstance(item, dict):
            raise InvalidActionError(f"Expected a dict at index {i} of the actions list, got {type(item).__name__}")
        result.append(ActionSerializer.from_dict(item))
    return result


__all__ = ["ActionSerializer", "deserialize_actions", "serialize_actions"]
