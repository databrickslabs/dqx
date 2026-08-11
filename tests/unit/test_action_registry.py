"""Unit tests for the custom-action registry (register_action / ACTION_REGISTRY).

Covers: built-ins self-register; a registered custom action works programmatically and round-trips
through metadata; unknown/unregistered/typeless actions are rejected; duplicate type registration is
rejected while re-registering the same class is a no-op.
"""

from typing import Literal

import pytest

from databricks.labs.dqx.actions import (
    ACTION_REGISTRY,
    Action,
    ActionContext,
    ActionResult,
    ActionServices,
    ActionStatus,
    DQAction,
    deserialize_actions,
    register_action,
    serialize_actions,
)
from databricks.labs.dqx.actions.registry import register_action_class, resolve_action_type
from databricks.labs.dqx.errors import InvalidActionError


class _CustomAction(Action):
    """A minimal registered custom action used across the tests below."""

    type: Literal["custom_test_action"] = "custom_test_action"
    threshold: int = 10

    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        return ActionResult(action_name=self.name or self.type, fired=True, status=ActionStatus.UNHEALTHY)


@pytest.fixture(autouse=True)
def _register_custom_action():
    """Register _CustomAction for each test and remove it afterwards to keep the registry clean."""
    register_action_class(_CustomAction)
    yield
    ACTION_REGISTRY.pop("custom_test_action", None)


def test_builtins_are_registered() -> None:
    for action_type in ("alert", "fail_pipeline", "noop"):
        assert action_type in ACTION_REGISTRY


def test_register_action_decorator_returns_class() -> None:
    # register_action is usable as a decorator and returns the class unchanged.
    assert register_action(_CustomAction) is _CustomAction


def test_custom_action_works_programmatically() -> None:
    binding = DQAction(action=_CustomAction(threshold=5), condition="error_row_count > 0", name="c1")
    assert isinstance(binding.action, _CustomAction)
    assert binding.action.threshold == 5
    assert binding.name == "c1"


def test_custom_action_round_trips_through_metadata() -> None:
    binding = DQAction(action=_CustomAction(threshold=7), condition="error_row_count > 0", name="c2")

    raw = serialize_actions([binding])
    # The serialized action must carry its discriminator and its custom fields.
    action_dict = raw[0]["action"]
    assert isinstance(action_dict, dict)
    assert action_dict["type"] == "custom_test_action"
    assert action_dict["threshold"] == 7

    restored = deserialize_actions(raw)
    assert isinstance(restored[0].action, _CustomAction)
    assert restored[0].action.threshold == 7
    assert restored[0].condition == "error_row_count > 0"
    assert restored[0].name == "c2"


def test_custom_action_from_metadata_dict_directly() -> None:
    binding = DQAction.model_validate(
        {"action": {"type": "custom_test_action", "threshold": 3}, "condition": "error_row_count > 0"}
    )
    assert isinstance(binding.action, _CustomAction)
    assert binding.action.threshold == 3


def test_unknown_action_type_in_metadata_raises() -> None:
    with pytest.raises(InvalidActionError, match="Unknown action type 'does_not_exist'"):
        deserialize_actions([{"action": {"type": "does_not_exist"}}])


def test_metadata_without_type_raises() -> None:
    with pytest.raises(InvalidActionError, match="must include a non-empty 'type'"):
        DQAction.model_validate({"action": {"threshold": 1}})


def test_resolve_action_type_returns_registered_class() -> None:
    assert resolve_action_type("custom_test_action") is _CustomAction


def test_register_duplicate_type_for_different_class_raises() -> None:
    # A genuinely different class (distinct __qualname__) reusing a registered type must raise.
    class _Clashing(Action):
        type: Literal["custom_test_action"] = "custom_test_action"

        def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
            return ActionResult(action_name="x", fired=False, status=ActionStatus.HEALTHY)

    with pytest.raises(InvalidActionError, match="already registered"):
        register_action_class(_Clashing)


def test_reregistering_same_class_is_noop() -> None:
    # Registering the identical class again must not raise (idempotent).
    register_action_class(_CustomAction)
    assert ACTION_REGISTRY["custom_test_action"] is _CustomAction


def test_reregistering_same_identity_overwrites() -> None:
    # A notebook cell re-run / module reload yields a NEW class object with the same import identity
    # (__module__ + __qualname__). Such a redefinition must overwrite rather than raise, so interactive
    # iteration works without restarting the session.
    def define() -> type[Action]:
        class _Reloaded(Action):
            type: Literal["reloaded_test_action"] = "reloaded_test_action"

            def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
                return ActionResult(action_name="x", fired=True, status=ActionStatus.UNHEALTHY)

        return _Reloaded

    first = define()
    second = define()  # distinct class object, identical __module__ + __qualname__
    assert second is not first
    try:
        register_action_class(first)
        register_action_class(second)  # must not raise
        assert ACTION_REGISTRY["reloaded_test_action"] is second
    finally:
        ACTION_REGISTRY.pop("reloaded_test_action", None)


def test_registering_action_without_type_raises() -> None:
    class _NoType(Action):
        def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
            return ActionResult(action_name="x", fired=False, status=ActionStatus.HEALTHY)

    with pytest.raises(InvalidActionError, match="non-empty literal 'type'"):
        register_action_class(_NoType)


def test_unregistered_subclass_reusing_registered_type_is_rejected() -> None:
    # An unregistered subclass that reuses a registered 'type' (here the built-in 'noop') must be
    # rejected at construction: otherwise it would be silently substituted for the registered class
    # on round-trip, dropping its extra fields.
    class _SneakyNoop(Action):
        type: Literal["noop"] = "noop"
        extra: str = "dropped-on-round-trip"

        def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
            return ActionResult(action_name=self.name or self.type, fired=True, status=ActionStatus.UNHEALTHY)

    with pytest.raises(InvalidActionError, match="is registered to NoOpAction"):
        DQAction(action=_SneakyNoop(), condition="error_row_count > 0")
