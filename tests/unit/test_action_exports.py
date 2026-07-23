"""Unit tests for the public databricks.labs.dqx.actions API surface."""

from databricks.labs.dqx import actions
from databricks.labs.dqx.actions import (
    ActionContext,
    CallbackDQAlertDestination,
    DQAction,
    DQActionManager,
    DQAlert,
    DQAlertFrequency,
    FailPipeline,
    NotifyOn,
    SlackDQAlertDestination,
    TeamsDQAlertDestination,
    WebhookDQAlertDestination,
)

# Core names the PRD/usage examples depend on, referenced as symbols (not a
# duplicated string list) so the check tracks the real objects.
_CORE_PUBLIC_SYMBOLS = (
    ActionContext,
    CallbackDQAlertDestination,
    DQAction,
    DQActionManager,
    DQAlert,
    DQAlertFrequency,
    FailPipeline,
    NotifyOn,
    SlackDQAlertDestination,
    TeamsDQAlertDestination,
    WebhookDQAlertDestination,
)


def test_core_public_symbols_are_exported() -> None:
    missing = [symbol.__name__ for symbol in _CORE_PUBLIC_SYMBOLS if symbol.__name__ not in actions.__all__]
    assert not missing, f"missing from actions.__all__: {missing}"


def test_all_is_sorted_and_unique() -> None:
    assert actions.__all__ == sorted(actions.__all__)
    assert len(actions.__all__) == len(set(actions.__all__))


def test_every_exported_name_is_importable() -> None:
    for name in actions.__all__:
        assert hasattr(actions, name), f"actions.{name} is not importable"


def test_public_classes_are_usable() -> None:
    """Smoke test confirming the names resolve to the right objects."""
    dq_action = DQAction(action=FailPipeline(message="boom"))
    assert dq_action.condition is None
    assert dq_action.name
