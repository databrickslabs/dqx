"""Serializer for *DQAction* instances.

*ActionSerializer* converts *DQAction* objects to plain Python dicts
(suitable for JSON / YAML persistence) and back.  It uses two internal
type registries so that adding a new action or destination type requires
only a single registry entry — no conditional branching needed in the
core logic (Open/Closed Principle).

Registry keys
-------------
- *_ACTION_BUILDERS*: maps the ``"type"`` string in a serialized action
  dict to a callable that reconstructs the *Action* instance from that dict.
  Currently supported keys: ``"alert"``, ``"fail_pipeline"``.
- *_DESTINATION_BUILDERS*: maps the ``"type"`` string of a serialized
  destination dict to a callable that reconstructs the *AlertDestination*
  instance.  Currently supported keys: ``"slack"``, ``"teams"``,
  ``"webhook"``.

Adding a new type
-----------------
Register a new entry in the appropriate dict at module level — no other
code changes are required::

    _ACTION_BUILDERS["my_action"] = _build_my_action
    _DESTINATION_BUILDERS["my_dest"] = _build_my_destination

*DQSecret* serialization
------------------------
A *DQSecret* value is serialized as a tagged dict ``{"secret": "scope/key"}``
so that it can be distinguished from a plain string after a round-trip.  On
deserialization, any dict with a single ``"secret"`` key is converted back to
a *DQSecret*.

*CallbackDQAlertDestination*
----------------------------
Callback destinations are not serializable (they hold a live Python
callable).  *to_dict* skips them with a ``WARNING``-level log message.
If all destinations in an alert are callbacks, the serialized
``destinations`` list is empty.

Security
--------
User-supplied names are sanitized before they appear in log messages to
prevent log injection (CWE-117).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable

from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.base import Action, DQAction
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.destinations.callback import CallbackDQAlertDestination
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.destinations.teams import TeamsDQAlertDestination
from databricks.labs.dqx.actions.destinations.webhook import WebhookDQAlertDestination
from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.errors import InvalidActionError

logger = logging.getLogger(__name__)

# Pre-compiled pattern for sanitizing control characters (CWE-117).
_CONTROL_CHAR_RE = re.compile(r"[\r\n\t\x00-\x1f\x7f]")


def _sanitize(text: str) -> str:
    """Strip newlines and control characters from *text* to prevent log injection (CWE-117).

    Args:
        text: Raw string that may contain control characters.

    Returns:
        A copy of *text* with all control characters replaced by a space.
    """
    return _CONTROL_CHAR_RE.sub(" ", text)


# ---------------------------------------------------------------------------
# DQSecret helpers
# ---------------------------------------------------------------------------


def _serialize_secret_or_str(value: str | DQSecret | None) -> str | dict[str, str] | None:
    """Serialize a *str | DQSecret | None* value for storage.

    A *DQSecret* is encoded as ``{"secret": "scope/key"}`` so that the tagged
    form survives a JSON / YAML round-trip without being confused with a plain
    string.  Plain strings and *None* are passed through unchanged.

    Args:
        value: The value to serialize.

    Returns:
        A plain string, a tagged dict, or *None*.
    """
    if isinstance(value, DQSecret):
        return {"secret": value.as_reference()}
    return value


def _deserialize_secret_or_str(value: object) -> str | DQSecret | None:
    """Reconstruct a *str | DQSecret | None* from its serialized form.

    A dict with a single ``"secret"`` key is parsed back into a *DQSecret*.
    Strings are returned as-is.  *None* is returned for absent / null values.

    Args:
        value: Serialized value from the dict.

    Returns:
        A plain string, a *DQSecret*, or *None*.
    """
    if isinstance(value, dict) and set(value.keys()) == {"secret"}:
        return DQSecret.from_reference(str(value["secret"]))
    if isinstance(value, str):
        return value
    return None


# ---------------------------------------------------------------------------
# Destination serializers / deserializers
# ---------------------------------------------------------------------------


def _serialize_destination(dest: AlertDestination) -> dict[str, object] | None:
    """Serialize a single *AlertDestination* to a plain dict.

    *CallbackDQAlertDestination* instances are not serializable — this
    function returns *None* for them (the caller logs a warning and skips
    the entry).

    Args:
        dest: The destination to serialize.

    Returns:
        A serialization dict, or *None* when the destination cannot be
        serialized.
    """
    if isinstance(dest, CallbackDQAlertDestination):
        return None

    if isinstance(dest, WebhookDQAlertDestination):
        dest_dict: dict[str, object] = {
            "type": dest.type,
            "name": dest.name,
            "webhook_url": _serialize_secret_or_str(dest.webhook_url),
        }
        if dest.username is not None:
            dest_dict["username"] = _serialize_secret_or_str(dest.username)
        if dest.password is not None:
            dest_dict["password"] = _serialize_secret_or_str(dest.password)
        return dest_dict

    # SlackDQAlertDestination and TeamsDQAlertDestination both inherit from
    # WebhookAlertDestination which provides webhook_url.
    if isinstance(dest, WebhookAlertDestination):
        return {
            "type": dest.type,
            "name": dest.name,
            "webhook_url": _serialize_secret_or_str(dest.webhook_url),
        }

    # Unrecognized destination type — cannot serialize; return None so caller skips it.
    logger.warning(
        f"Destination '{_sanitize(dest.name)}' has an unrecognized type '{_sanitize(type(dest).__name__)}'; skipping."
    )
    return None


def _build_slack(raw: dict[str, object]) -> SlackDQAlertDestination:
    return SlackDQAlertDestination(
        name=str(raw["name"]),
        webhook_url=_deserialize_secret_or_str(raw["webhook_url"]) or "",
    )


def _build_teams(raw: dict[str, object]) -> TeamsDQAlertDestination:
    return TeamsDQAlertDestination(
        name=str(raw["name"]),
        webhook_url=_deserialize_secret_or_str(raw["webhook_url"]) or "",
    )


def _build_webhook(raw: dict[str, object]) -> WebhookDQAlertDestination:
    return WebhookDQAlertDestination(
        name=str(raw["name"]),
        webhook_url=_deserialize_secret_or_str(raw["webhook_url"]) or "",
        username=_deserialize_secret_or_str(raw.get("username")),
        password=_deserialize_secret_or_str(raw.get("password")),
    )


# Registry: destination type string → builder callable
_DESTINATION_BUILDERS: dict[str, Callable[[dict[str, object]], AlertDestination]] = {
    "slack": _build_slack,
    "teams": _build_teams,
    "webhook": _build_webhook,
}


def _deserialize_destination(raw: dict[str, object]) -> AlertDestination:
    """Reconstruct an *AlertDestination* from its serialized dict.

    Args:
        raw: Serialized destination dict (must contain a ``"type"`` key).

    Returns:
        A concrete *AlertDestination* instance.

    Raises:
        InvalidActionError: If the ``"type"`` value is not registered.
    """
    dest_type = str(raw.get("type", ""))
    builder = _DESTINATION_BUILDERS.get(dest_type)
    if builder is None:
        raise InvalidActionError(
            f"Unknown destination type '{_sanitize(dest_type)}'. " f"Supported types: {sorted(_DESTINATION_BUILDERS)}"
        )
    return builder(raw)


# ---------------------------------------------------------------------------
# Action serializers / deserializers
# ---------------------------------------------------------------------------


def _serialize_alert(alert: DQAlert) -> dict[str, object]:
    """Serialize a *DQAlert* action to a plain dict.

    Args:
        alert: The alert action to serialize.

    Returns:
        Dict representation of the alert (without the top-level ``"type"`` key;
        the caller adds it).
    """
    serialized_destinations: list[dict[str, object]] = []
    for dest in alert.destinations:
        dest_dict = _serialize_destination(dest)
        if dest_dict is None:
            safe_name = _sanitize(dest.name)
            logger.warning(
                f"Destination '{safe_name}' is a CallbackDQAlertDestination and cannot be serialized; skipping."
            )
        else:
            serialized_destinations.append(dest_dict)

    return {
        "type": "alert",
        "name": alert.name,
        "alert_frequency": alert.alert_frequency.value,
        "notify_on": alert.notify_on.value,
        "severity": alert.severity,
        "destinations": serialized_destinations,
    }


def _build_alert(raw: dict[str, object]) -> DQAlert:
    raw_destinations = raw.get("destinations")
    destinations_list: list[dict[str, object]] = []
    if isinstance(raw_destinations, list):
        for item in raw_destinations:
            if isinstance(item, dict):
                destinations_list.append(item)

    destinations = [_deserialize_destination(dest_raw) for dest_raw in destinations_list]

    raw_freq = raw.get("alert_frequency", DQAlertFrequency.ALWAYS.value)
    raw_notify = raw.get("notify_on", NotifyOn.EACH.value)

    return DQAlert(
        destinations=destinations,
        name=str(raw.get("name", "")),
        alert_frequency=DQAlertFrequency(raw_freq),
        notify_on=NotifyOn(raw_notify),
        severity=str(raw.get("severity", "error")),
    )


def _serialize_fail_pipeline(fail_action: FailPipeline) -> dict[str, object]:
    result: dict[str, object] = {
        "type": "fail_pipeline",
        "name": fail_action.name,
    }
    if fail_action.message is not None:
        result["message"] = fail_action.message
    return result


def _build_fail_pipeline(raw: dict[str, object]) -> FailPipeline:
    message_raw = raw.get("message")
    message = str(message_raw) if message_raw is not None else None
    return FailPipeline(
        message=message,
        name=str(raw.get("name", "fail_pipeline")),
    )


# Registry: action type string → builder callable
_ACTION_BUILDERS: dict[str, Callable[[dict[str, object]], Action]] = {
    "alert": _build_alert,
    "fail_pipeline": _build_fail_pipeline,
}


# ---------------------------------------------------------------------------
# ActionSerializer
# ---------------------------------------------------------------------------


class ActionSerializer:
    """Converts *DQAction* instances to plain dicts and back.

    Uses two internal type registries (*_ACTION_BUILDERS* and
    *_DESTINATION_BUILDERS*) keyed by the ``"type"`` string.  To add support
    for a new action or destination type, add a single entry to the
    corresponding registry dict at module level — no other changes are needed.

    *DQSecret* values are serialized as ``{"secret": "scope/key"}`` tagged
    dicts so that the secret reference survives a round-trip through JSON or
    YAML without being confused with a plain string.

    *CallbackDQAlertDestination* instances are skipped during *to_dict* with
    a ``WARNING``-level log message because they hold a live Python callable
    that cannot be serialized.  If all destinations are callbacks the
    resulting ``"destinations"`` list is empty.

    The ``"condition"`` field is omitted from the output of *to_dict* when it
    is *None*, and defaults back to *None* when absent on *from_dict*.
    """

    @staticmethod
    def to_dict(action: DQAction) -> dict[str, object]:
        """Serialize *action* to a plain Python dict.

        The top-level dict contains:

        - ``"name"`` — logical name of the *DQAction*.
        - ``"action"`` — nested dict describing the concrete *Action*.
        - ``"condition"`` (optional) — omitted when *None*.

        Args:
            action: The *DQAction* to serialize.

        Returns:
            A JSON-serializable dict representing *action*.

        Raises:
            InvalidActionError: If the underlying *Action* type is not
                registered in *_ACTION_BUILDERS*.
        """
        inner = action.action

        if isinstance(inner, DQAlert):
            action_dict = _serialize_alert(inner)
        elif isinstance(inner, FailPipeline):
            action_dict = _serialize_fail_pipeline(inner)
        else:
            action_type = type(inner).__name__
            raise InvalidActionError(f"Action type '{_sanitize(action_type)}' is not supported by ActionSerializer.")

        result: dict[str, object] = {
            "name": action.name,
            "action": action_dict,
        }
        if action.condition is not None:
            result["condition"] = action.condition
        return result

    @staticmethod
    def from_dict(raw: dict[str, object]) -> DQAction:
        """Deserialize a plain dict into a *DQAction*.

        Args:
            raw: Dict produced by *to_dict* (or loaded from JSON / YAML).

        Returns:
            A fully reconstructed *DQAction*.

        Raises:
            InvalidActionError: If the ``"type"`` field in the ``"action"``
                sub-dict is missing or not registered in *_ACTION_BUILDERS*,
                or if a destination ``"type"`` is not registered.
        """
        action_dict_raw = raw.get("action", {})
        action_dict: dict[str, object] = action_dict_raw if isinstance(action_dict_raw, dict) else {}

        action_type = str(action_dict.get("type", ""))
        builder = _ACTION_BUILDERS.get(action_type)
        if builder is None:
            raise InvalidActionError(
                f"Unknown action type '{_sanitize(action_type)}'. " f"Supported types: {sorted(_ACTION_BUILDERS)}"
            )

        concrete_action = builder(action_dict)

        condition_raw = raw.get("condition")
        condition = str(condition_raw) if condition_raw is not None else None

        name_raw = raw.get("name", "")
        name = str(name_raw) if name_raw is not None else ""

        return DQAction(action=concrete_action, condition=condition, name=name)


__all__ = ["ActionSerializer"]
