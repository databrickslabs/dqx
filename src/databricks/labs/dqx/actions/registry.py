"""Registry of DQX action types, enabling custom (user-defined) actions.

An *Action* subclass becomes usable by *DQAction* — both programmatically and from YAML/JSON
metadata — once it is registered here, keyed on its literal *type* discriminator. This follows the
same registry idea as *CHECK_FUNC_REGISTRY* / *register_rule* for check functions, but with stricter
collision handling: whereas *register_rule* is unconditionally last-write-wins, this registry *raises*
*InvalidActionError* when a **genuinely different** class (different *__module__* / *__qualname__*)
tries to claim a *type* that is already taken, so an accidental clash between two distinct actions
surfaces loudly instead of one silently shadowing the other. Re-registering the same class — including
the same definition re-run in a notebook cell or reloaded module, which yields a new class object with
identical identity — is allowed and simply overwrites the previous registration, so interactive
iteration on a custom action works without restarting the Python session.

Built-in actions register themselves at import time. A custom action registers via the
*register_action* decorator (or *register_action_class*); the module defining it must be imported
before the action is constructed or a metadata definition referencing its *type* is deserialized, so
that registration has run.
"""

from databricks.labs.dqx.actions.base import Action
from databricks.labs.dqx.errors import InvalidActionError

__all__ = ["ACTION_REGISTRY", "register_action", "register_action_class", "resolve_action_type"]

# Maps the literal *type* discriminator to the concrete Action subclass that implements it.
ACTION_REGISTRY: dict[str, type[Action]] = {}


def register_action_class(action_cls: type[Action]) -> type[Action]:
    """Register *action_cls* under its declared *type* discriminator.

    Args:
        action_cls: An *Action* subclass declaring a non-empty literal *type* field.

    Returns:
        The same *action_cls*, so this can be used as a decorator.

    Raises:
        InvalidActionError: If *action_cls* has no resolvable *type*, or if a *genuinely different*
            class (different *__module__* / *__qualname__*) is already registered under the same
            *type*. Re-registering the same class — or the same class redefined by a notebook cell
            re-run or a module reload, which produces a new class object with identical identity — is
            allowed and overwrites the previous registration.
    """
    action_type = _extract_action_type(action_cls)
    existing = ACTION_REGISTRY.get(action_type)
    if existing is not None and not _same_class_identity(existing, action_cls):
        raise InvalidActionError(
            f"Action type '{action_type}' is already registered to {existing.__module__}.{existing.__qualname__}; "
            f"cannot re-register it for {action_cls.__module__}.{action_cls.__qualname__}. "
            "Choose a unique 'type' for the new action."
        )
    ACTION_REGISTRY[action_type] = action_cls
    return action_cls


def _same_class_identity(existing: type[Action], candidate: type[Action]) -> bool:
    """Whether two classes share import identity (*__module__* + *__qualname__*).

    A notebook cell re-run or a module reload produces a *new* class object for the same source
    definition; such redefinitions share module and qualified name, so they are treated as the same
    class (and may overwrite each other) rather than a genuine clash between two distinct actions.
    """
    return (existing.__module__, existing.__qualname__) == (candidate.__module__, candidate.__qualname__)


def register_action(action_cls: type[Action]) -> type[Action]:
    """Decorator form of *register_action_class*.

    Example:
        >>> @register_action
        ... class MyAction(Action):
        ...     type: Literal["my_action"] = "my_action"
        ...     def execute(self, context, services): ...
    """
    return register_action_class(action_cls)


def resolve_action_type(action_type: str) -> type[Action]:
    """Return the registered *Action* subclass for *action_type*.

    Args:
        action_type: The literal *type* discriminator to look up.

    Returns:
        The registered *Action* subclass.

    Raises:
        InvalidActionError: If no action is registered under *action_type*.
    """
    action_cls = ACTION_REGISTRY.get(action_type)
    if action_cls is None:
        known = ", ".join(sorted(ACTION_REGISTRY)) or "<none>"
        raise InvalidActionError(
            f"Unknown action type '{action_type}'. Known types: [{known}]. If this is a custom action, "
            "import the module that registers it (via @register_action) before loading."
        )
    return action_cls


def _extract_action_type(action_cls: type[Action]) -> str:
    """Extract the literal *type* discriminator value from an *Action* subclass.

    The *type* is read from the field's Pydantic default (concrete actions declare it as
    ``type: Literal["..."] = "..."``).
    """
    field = action_cls.model_fields.get("type")
    default = getattr(field, "default", None) if field is not None else None
    if not isinstance(default, str) or not default:
        raise InvalidActionError(
            f"Action class {action_cls.__name__} must declare a non-empty literal 'type' field to be registered."
        )
    return default
