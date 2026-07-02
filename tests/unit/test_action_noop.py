"""Unit tests for the no-op action.

Covers that *NoOpAction* fires without side effect, resolves through the action
discriminated union, and round-trips through its metadata (*type: noop*) form.
The *action_context* / *action_services* fixtures come from tests/unit/conftest.py.
"""

from databricks.labs.dqx.actions import DQAction, NoOpAction
from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus


def test_execute_fires_unhealthy_without_side_effect(
    action_context: ActionContext, action_services: ActionServices
) -> None:
    result = NoOpAction(name="record_only").execute(action_context, action_services)
    assert result.fired is True
    assert result.status is ActionStatus.UNHEALTHY
    assert result.action_name == "record_only"
    assert not result.destination_errors


def test_defaults() -> None:
    action = NoOpAction()
    assert action.type == "noop"
    assert action.name == "noop"


def test_metadata_round_trip() -> None:
    original = NoOpAction(name="audit")
    dumped = original.model_dump(mode="json")
    assert dumped == {"type": "noop", "name": "audit"}
    assert NoOpAction.model_validate(dumped) == original


def test_resolves_from_action_union_via_dqaction() -> None:
    # A metadata dict with type "noop" must deserialize to a NoOpAction through the AnyAction union.
    dq_action = DQAction(condition="error_row_count > 0", action={"type": "noop", "name": "record_only"})
    assert isinstance(dq_action.action, NoOpAction)
    assert dq_action.name == "record_only"
