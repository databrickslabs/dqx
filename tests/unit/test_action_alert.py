"""Unit tests for databricks.labs.dqx.actions.alert.

TDD step 1: write failing tests before implementation.

Tests cover:
- DQAlertFrequency and NotifyOn enum values.
- DQAlert.validate raises InvalidActionError on empty destinations.
- DQAlert.validate calls validate() on each destination.
- DQAlert.execute calls deliver() on EVERY destination.
- DQAlert.execute returns ActionResult with fired=True.
- When ONE destination raises, others still receive deliver() and the
  failing destination is recorded in ActionResult.destination_errors.
- Recorded error text in destination_errors has no newlines.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import ClassVar
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus
from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.errors import InvalidActionError


# ---------------------------------------------------------------------------
# Fake destinations
# ---------------------------------------------------------------------------


@dataclass
class _RecordingDestination(AlertDestination):
    """Records calls to deliver() without side effects."""

    type: ClassVar[str] = "recording"
    name: str
    deliver_calls: list[AlertMessage] = field(default_factory=list)

    def deliver(self, message: AlertMessage, context: ActionContext, services: ActionServices) -> None:
        self.deliver_calls.append(message)


@dataclass
class _FailingDestination(AlertDestination):
    """Always raises on deliver()."""

    type: ClassVar[str] = "failing"
    name: str
    error_message: str = "delivery failed\nwith newline"

    def deliver(self, message: AlertMessage, context: ActionContext, services: ActionServices) -> None:
        raise RuntimeError(self.error_message)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(*, metrics: dict[str, object] | None = None) -> ActionContext:
    return ActionContext(
        metrics=metrics or {"error_row_count": 5},
        run_id="run-test-001",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        input_location="catalog.schema.my_table",
    )


def _make_services() -> ActionServices:
    spec_resolver = create_autospec(SecretResolver, instance=True)
    spec_client = create_autospec(WebhookClient, instance=True)
    return ActionServices(secret_resolver=spec_resolver, webhook_client=spec_client)


# ---------------------------------------------------------------------------
# Enum tests
# ---------------------------------------------------------------------------


def test_dq_alert_frequency_enum_values() -> None:
    assert DQAlertFrequency.ALWAYS.value == "always"
    assert DQAlertFrequency.HOURLY.value == "hourly"
    assert DQAlertFrequency.DAILY.value == "daily"


def test_dq_alert_frequency_is_enum() -> None:
    assert isinstance(DQAlertFrequency.ALWAYS, enum.Enum)


def test_notify_on_enum_values() -> None:
    assert NotifyOn.EACH.value == "each"
    assert NotifyOn.STATUS_CHANGE.value == "status_change"


def test_notify_on_is_enum() -> None:
    assert isinstance(NotifyOn.EACH, enum.Enum)


# ---------------------------------------------------------------------------
# DQAlert.validate tests
# ---------------------------------------------------------------------------


def test_dq_alert_validate_raises_for_empty_destinations() -> None:
    alert = DQAlert(destinations=[])
    with pytest.raises(InvalidActionError):
        alert.validate()


def test_dq_alert_validate_calls_each_destination_validate() -> None:
    dest_a = create_autospec(_RecordingDestination, instance=True)
    dest_b = create_autospec(_RecordingDestination, instance=True)
    # 'name' is a reserved Mock kwarg, so set it explicitly (and distinctly, so the
    # uniqueness check in validate() passes).
    dest_a.name = "dest_a"
    dest_b.name = "dest_b"
    alert = DQAlert(destinations=[dest_a, dest_b])
    alert.validate()
    dest_a.validate.assert_called_once()
    dest_b.validate.assert_called_once()


def test_dq_alert_validate_propagates_destination_error() -> None:
    """A destination whose validate() raises must propagate out of DQAlert.validate."""

    @dataclass
    class _BadDestination(AlertDestination):
        type: ClassVar[str] = "bad"
        name: str

        def validate(self) -> None:
            raise InvalidActionError("bad destination")

        def deliver(self, message: AlertMessage, context: ActionContext, services: ActionServices) -> None:
            pass

    alert = DQAlert(destinations=[_BadDestination(name="bad")])
    with pytest.raises(InvalidActionError):
        alert.validate()


def test_dq_alert_validate_passes_for_valid_destinations() -> None:
    dest = _RecordingDestination(name="good")
    alert = DQAlert(destinations=[dest])
    alert.validate()  # must not raise


def test_dq_alert_validate_raises_for_duplicate_destination_names() -> None:
    """Duplicate destination names would clobber each other in destination_errors."""
    alert = DQAlert(destinations=[_RecordingDestination(name="ops"), _FailingDestination(name="ops")])
    with pytest.raises(InvalidActionError, match="unique"):
        alert.validate()


# ---------------------------------------------------------------------------
# DQAlert.execute — happy path
# ---------------------------------------------------------------------------


def test_dq_alert_execute_calls_deliver_on_all_destinations() -> None:
    dest_a = _RecordingDestination(name="dest_a")
    dest_b = _RecordingDestination(name="dest_b")
    alert = DQAlert(destinations=[dest_a, dest_b], name="my_alert")
    context = _make_context()
    services = _make_services()

    result = alert.execute(context, services)

    assert len(dest_a.deliver_calls) == 1
    assert len(dest_b.deliver_calls) == 1
    assert result.fired is True


def test_dq_alert_execute_returns_action_result_with_action_name() -> None:
    dest = _RecordingDestination(name="dest")
    alert = DQAlert(destinations=[dest], name="named_alert")
    result = alert.execute(_make_context(), _make_services())
    assert result.action_name == "named_alert"


def test_dq_alert_execute_result_has_no_destination_errors_on_success() -> None:
    dest = _RecordingDestination(name="ok_dest")
    alert = DQAlert(destinations=[dest])
    result = alert.execute(_make_context(), _make_services())
    assert not result.destination_errors


def test_dq_alert_execute_returns_unhealthy_status() -> None:
    """DQAlert.execute always returns UNHEALTHY (the action fired = quality issue detected)."""
    dest = _RecordingDestination(name="d")
    alert = DQAlert(destinations=[dest])
    result = alert.execute(_make_context(), _make_services())
    assert result.status == ActionStatus.UNHEALTHY


# ---------------------------------------------------------------------------
# DQAlert.execute — isolated destination failure
# ---------------------------------------------------------------------------


def test_dq_alert_execute_other_destinations_still_called_when_one_fails() -> None:
    """A failing destination must not prevent other destinations from receiving deliver()."""
    good_dest = _RecordingDestination(name="good")
    bad_dest = _FailingDestination(name="bad")
    alert = DQAlert(destinations=[bad_dest, good_dest], name="mixed_alert")
    context = _make_context()
    services = _make_services()

    result = alert.execute(context, services)

    # Good destination was still called.
    assert len(good_dest.deliver_calls) == 1
    # Bad destination is recorded in destination_errors.
    assert "bad" in result.destination_errors


def test_dq_alert_execute_failing_destination_recorded_in_errors() -> None:
    bad_dest = _FailingDestination(name="failing_dest", error_message="boom")
    good_dest = _RecordingDestination(name="good_dest")
    alert = DQAlert(destinations=[bad_dest, good_dest])
    result = alert.execute(_make_context(), _make_services())
    assert "failing_dest" in result.destination_errors
    assert result.destination_errors["failing_dest"]  # non-empty string


def test_dq_alert_execute_error_text_has_no_newlines() -> None:
    """Sanitization: stored error text must contain no newline characters (CWE-117)."""
    bad_dest = _FailingDestination(name="nl_dest", error_message="line1\nline2\r\nline3\ttab\x1b[31mansi\x00null")
    alert = DQAlert(destinations=[bad_dest])
    result = alert.execute(_make_context(), _make_services())
    error_text = result.destination_errors.get("nl_dest", "")
    for control_char in ("\n", "\r", "\t", "\x1b", "\x00"):
        assert control_char not in error_text


def test_dq_alert_execute_fired_true_even_when_all_destinations_fail() -> None:
    bad_dest = _FailingDestination(name="bad")
    alert = DQAlert(destinations=[bad_dest])
    result = alert.execute(_make_context(), _make_services())
    assert result.fired is True


# ---------------------------------------------------------------------------
# DQAlert defaults
# ---------------------------------------------------------------------------


def test_dq_alert_default_name_is_alert() -> None:
    dest = _RecordingDestination(name="d")
    alert = DQAlert(destinations=[dest])
    assert alert.name == "alert"


def test_dq_alert_default_frequency_is_always() -> None:
    dest = _RecordingDestination(name="d")
    alert = DQAlert(destinations=[dest])
    assert alert.alert_frequency == DQAlertFrequency.ALWAYS


def test_dq_alert_default_notify_on_is_each() -> None:
    dest = _RecordingDestination(name="d")
    alert = DQAlert(destinations=[dest])
    assert alert.notify_on == NotifyOn.EACH


def test_dq_alert_default_severity_is_error() -> None:
    dest = _RecordingDestination(name="d")
    alert = DQAlert(destinations=[dest])
    assert alert.severity == "error"
