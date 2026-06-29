"""Unit tests for databricks.labs.dqx.actions.base.

These tests verify:
- ActionStatus enum values
- ActionContext / ActionResult are frozen dataclasses
- Action ABC and DQAction construction/validation
- Forward-reference isolation: base.py imports cleanly even though delivery.py does not exist
"""

from __future__ import annotations

import dataclasses
import enum
import inspect
import sys
from datetime import datetime, timezone

import pytest

from databricks.labs.dqx.actions.base import (
    Action,
    ActionContext,
    ActionResult,
    ActionServices,
    ActionStatus,
    DQAction,
)
from databricks.labs.dqx.errors import InvalidActionError, InvalidConditionError


# ---------------------------------------------------------------------------
# Local test doubles
# ---------------------------------------------------------------------------


class DummyAction(Action):
    """Concrete Action subclass for testing — always succeeds."""

    name: str = "dummy_action"

    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        return ActionResult(action_name=self.name, fired=True, status=ActionStatus.HEALTHY)


class ValidatingAction(DummyAction):
    """Action whose validate() raises InvalidActionError."""

    name: str = "bad_action"

    def validate(self) -> None:
        raise InvalidActionError("bad_action is always invalid")


class UnnamedAction(DummyAction):
    """Action with an empty name, for name-derivation tests."""

    name: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(**kwargs: object) -> ActionContext:
    defaults: dict[str, object] = {
        "metrics": {"error_row_count": 0},
        "run_id": "run-001",
        "run_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }
    defaults.update(kwargs)
    return ActionContext(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Verify the module imports cleanly WITHOUT delivery.py / SparkSession
# ---------------------------------------------------------------------------


def test_module_imports_without_delivery() -> None:
    """base.py must import successfully even when delivery.py does not exist.

    The top-level imports of this module already prove this: if delivery.py
    were required at import time every test in this file would fail at
    collection.  This function is an explicit marker so the intent is
    documented in the test report.
    """
    # Confirm the import is accessible via sys.modules (already imported above).
    assert "databricks.labs.dqx.actions.base" in sys.modules


# ---------------------------------------------------------------------------
# ActionStatus
# ---------------------------------------------------------------------------


def test_action_status_values() -> None:
    assert ActionStatus.HEALTHY.value == "healthy"
    assert ActionStatus.UNHEALTHY.value == "unhealthy"


def test_action_status_is_enum() -> None:
    assert isinstance(ActionStatus.HEALTHY, enum.Enum)


# ---------------------------------------------------------------------------
# ActionContext
# ---------------------------------------------------------------------------


def test_action_context_construction() -> None:
    ctx = _make_context()
    assert ctx.run_id == "run-001"
    assert ctx.run_name == "dqx"
    assert ctx.input_location is None
    assert ctx.output_location is None
    assert ctx.quarantine_location is None
    assert ctx.checks_location is None
    assert ctx.rule_set_fingerprint is None
    assert ctx.user_metadata is None


def test_action_context_is_frozen() -> None:
    ctx = _make_context()
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.run_id = "new-id"  # type: ignore[misc]


def test_action_context_custom_run_name() -> None:
    ctx = _make_context(run_name="my_pipeline")
    assert ctx.run_name == "my_pipeline"


def test_action_context_optional_fields() -> None:
    ctx = _make_context(
        input_location="dbfs:/input",
        output_location="dbfs:/output",
        quarantine_location="dbfs:/quarantine",
        checks_location="dbfs:/checks",
        rule_set_fingerprint="abc123",
        user_metadata={"env": "prod"},
    )
    assert ctx.input_location == "dbfs:/input"
    assert ctx.output_location == "dbfs:/output"
    assert ctx.quarantine_location == "dbfs:/quarantine"
    assert ctx.checks_location == "dbfs:/checks"
    assert ctx.rule_set_fingerprint == "abc123"
    assert ctx.user_metadata == {"env": "prod"}


# ---------------------------------------------------------------------------
# ActionResult
# ---------------------------------------------------------------------------


def test_action_result_is_frozen() -> None:
    result = ActionResult(action_name="x", fired=True, status=ActionStatus.HEALTHY)
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.fired = False  # type: ignore[misc]


def test_action_result_default_destination_errors() -> None:
    result = ActionResult(action_name="x", fired=False, status=ActionStatus.UNHEALTHY)
    assert not result.destination_errors


def test_action_result_destination_errors_independent() -> None:
    """Two ActionResult instances must not share the same destination_errors dict."""
    result_a = ActionResult(action_name="a", fired=True, status=ActionStatus.HEALTHY)
    result_b = ActionResult(action_name="b", fired=True, status=ActionStatus.HEALTHY)
    assert result_a.destination_errors is not result_b.destination_errors


# ---------------------------------------------------------------------------
# Action ABC
# ---------------------------------------------------------------------------


def test_action_abc_cannot_be_instantiated_directly() -> None:
    assert inspect.isabstract(Action)
    assert "execute" in Action.__abstractmethods__


def test_dummy_action_validate_noop() -> None:
    action = DummyAction()
    action.validate()  # must not raise


# ---------------------------------------------------------------------------
# DQAction construction
# ---------------------------------------------------------------------------


def test_dqaction_with_condition_constructs_and_sets_name() -> None:
    dqa = DQAction(condition="error_row_count > 0", action=DummyAction())
    assert dqa.condition == "error_row_count > 0"
    assert dqa.name
    assert isinstance(dqa.name, str)


def test_dqaction_without_condition_is_valid() -> None:
    dqa = DQAction(action=DummyAction())
    assert dqa.condition is None


def test_dqaction_without_condition_has_name() -> None:
    dqa = DQAction(action=DummyAction())
    assert dqa.name  # non-empty — derived from action.name


def test_dqaction_bad_condition_raises_invalid_condition_error() -> None:
    with pytest.raises(InvalidConditionError):
        DQAction(condition="import os; os.system('rm -rf /')", action=DummyAction())


def test_dqaction_syntax_error_condition_raises_invalid_condition_error() -> None:
    with pytest.raises(InvalidConditionError):
        DQAction(condition="??? not valid", action=DummyAction())


def test_dqaction_invalid_action_propagates_error() -> None:
    with pytest.raises(InvalidActionError):
        DQAction(action=ValidatingAction())


def test_dqaction_explicit_name_takes_precedence() -> None:
    dqa = DQAction(action=DummyAction(), name="my_custom_name")
    assert dqa.name == "my_custom_name"


def test_dqaction_name_derived_from_action_name_when_empty() -> None:
    dqa = DQAction(action=DummyAction())
    # DummyAction.name == "dummy_action" — must appear in derived name
    assert "dummy_action" in dqa.name


def test_dqaction_name_derived_from_condition_when_action_name_empty() -> None:
    """When action.name is '' and no explicit name given, name is derived from condition."""
    dqa = DQAction(condition="error_row_count > 0", action=UnnamedAction())
    assert dqa.name  # non-empty, derived from condition


def test_dqaction_name_derived_from_class_name_when_all_empty() -> None:
    """Fallback: when action.name is '' and condition is None, use action's class name."""
    dqa = DQAction(action=UnnamedAction())
    assert dqa.name  # non-empty, derived from class name
    assert "UnnamedAction" in dqa.name
