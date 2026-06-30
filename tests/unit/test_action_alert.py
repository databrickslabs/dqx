"""Unit tests for databricks.labs.dqx.actions.alert.

Tests cover:
- DQAlertFrequency and NotifyOn enum values.
- DQAlert construction raises InvalidActionError on empty destinations.
- DQAlert construction raises InvalidActionError on duplicate destination names.
- DQAlert.execute calls deliver() on EVERY destination.
- DQAlert.execute returns ActionResult with fired=True.
- When ONE destination raises, others still receive deliver() and the
  failing destination is recorded in ActionResult.destination_errors.
- Recorded error text in destination_errors has no newlines.
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus
from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.destinations.callback import CallbackDQAlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.errors import InvalidActionError


# ---------------------------------------------------------------------------
# Real destination helpers (callbacks are members of the AnyDestination union)
# ---------------------------------------------------------------------------


def _recording_destination(name: str) -> tuple[CallbackDQAlertDestination, list[AlertMessage]]:
    """Return a callback destination that records every delivered message."""
    received: list[AlertMessage] = []

    def _record(message: AlertMessage, _context: ActionContext) -> None:
        received.append(message)

    return CallbackDQAlertDestination(name=name, callback=_record), received


def _failing_destination(name: str, error_message: str = "delivery failed\nwith newline") -> CallbackDQAlertDestination:
    """Return a callback destination that always raises on delivery."""

    def _fail(_message: AlertMessage, _context: ActionContext) -> None:
        raise RuntimeError(error_message)

    return CallbackDQAlertDestination(name=name, callback=_fail)


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
# DQAlert construction-time validation
# ---------------------------------------------------------------------------


def test_dq_alert_raises_for_empty_destinations() -> None:
    with pytest.raises(InvalidActionError):
        DQAlert(destinations=[])


def test_dq_alert_propagates_destination_error() -> None:
    """A destination whose own validator fails raises InvalidActionError at its construction."""
    with pytest.raises(InvalidActionError):
        # An empty destination name fails the base AlertDestination validator.
        DQAlert(destinations=[CallbackDQAlertDestination(name="", callback=lambda message, context: None)])


def test_dq_alert_passes_for_valid_destinations() -> None:
    dest, _ = _recording_destination("good")
    alert = DQAlert(destinations=[dest])
    assert alert.name == "alert"


def test_dq_alert_raises_for_duplicate_destination_names() -> None:
    """Duplicate destination names would clobber each other in destination_errors."""
    dest_a, _ = _recording_destination("ops")
    with pytest.raises(InvalidActionError, match="unique"):
        DQAlert(destinations=[dest_a, _failing_destination("ops")])


# ---------------------------------------------------------------------------
# DQAlert.execute — happy path
# ---------------------------------------------------------------------------


def test_dq_alert_execute_calls_deliver_on_all_destinations() -> None:
    dest_a, received_a = _recording_destination("dest_a")
    dest_b, received_b = _recording_destination("dest_b")
    alert = DQAlert(destinations=[dest_a, dest_b], name="my_alert")

    result = alert.execute(_make_context(), _make_services())

    assert len(received_a) == 1
    assert len(received_b) == 1
    assert result.fired is True


def test_dq_alert_execute_returns_action_result_with_action_name() -> None:
    dest, _ = _recording_destination("dest")
    alert = DQAlert(destinations=[dest], name="named_alert")
    result = alert.execute(_make_context(), _make_services())
    assert result.action_name == "named_alert"


def test_dq_alert_execute_result_has_no_destination_errors_on_success() -> None:
    dest, _ = _recording_destination("ok_dest")
    alert = DQAlert(destinations=[dest])
    result = alert.execute(_make_context(), _make_services())
    assert not result.destination_errors


def test_dq_alert_execute_returns_unhealthy_status() -> None:
    """DQAlert.execute always returns UNHEALTHY (the action fired = quality issue detected)."""
    dest, _ = _recording_destination("d")
    alert = DQAlert(destinations=[dest])
    result = alert.execute(_make_context(), _make_services())
    assert result.status == ActionStatus.UNHEALTHY


# ---------------------------------------------------------------------------
# DQAlert.execute — isolated destination failure
# ---------------------------------------------------------------------------


def test_dq_alert_execute_other_destinations_still_called_when_one_fails() -> None:
    """A failing destination must not prevent other destinations from receiving deliver()."""
    good_dest, received = _recording_destination("good")
    bad_dest = _failing_destination("bad")
    alert = DQAlert(destinations=[bad_dest, good_dest], name="mixed_alert")

    result = alert.execute(_make_context(), _make_services())

    # Good destination was still called.
    assert len(received) == 1
    # Bad destination is recorded in destination_errors.
    assert "bad" in result.destination_errors


def test_dq_alert_execute_failing_destination_recorded_in_errors() -> None:
    bad_dest = _failing_destination("failing_dest", error_message="boom")
    good_dest, _ = _recording_destination("good_dest")
    alert = DQAlert(destinations=[bad_dest, good_dest])
    result = alert.execute(_make_context(), _make_services())
    assert "failing_dest" in result.destination_errors
    assert result.destination_errors["failing_dest"]  # non-empty string


def test_dq_alert_execute_error_text_has_no_newlines() -> None:
    """Sanitization: stored error text must contain no newline characters (CWE-117)."""
    bad_dest = _failing_destination("nl_dest", error_message="line1\nline2\r\nline3\ttab\x1b[31mansi\x00null")
    alert = DQAlert(destinations=[bad_dest])
    result = alert.execute(_make_context(), _make_services())
    error_text = result.destination_errors.get("nl_dest", "")
    for control_char in ("\n", "\r", "\t", "\x1b", "\x00"):
        assert control_char not in error_text


def test_dq_alert_execute_fired_true_even_when_all_destinations_fail() -> None:
    bad_dest = _failing_destination("bad")
    alert = DQAlert(destinations=[bad_dest])
    result = alert.execute(_make_context(), _make_services())
    assert result.fired is True


# ---------------------------------------------------------------------------
# DQAlert defaults
# ---------------------------------------------------------------------------


def test_dq_alert_default_name_is_alert() -> None:
    dest, _ = _recording_destination("d")
    alert = DQAlert(destinations=[dest])
    assert alert.name == "alert"


def test_dq_alert_default_frequency_is_always() -> None:
    dest, _ = _recording_destination("d")
    alert = DQAlert(destinations=[dest])
    assert alert.alert_frequency == DQAlertFrequency.ALWAYS


def test_dq_alert_default_notify_on_is_each() -> None:
    dest, _ = _recording_destination("d")
    alert = DQAlert(destinations=[dest])
    assert alert.notify_on == NotifyOn.EACH


def test_dq_alert_default_severity_is_error() -> None:
    dest, _ = _recording_destination("d")
    alert = DQAlert(destinations=[dest])
    assert alert.severity == "error"
