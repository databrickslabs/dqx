"""Unit tests for callback alert destination.

Tests cover:
- deliver() calls the provided callable exactly once with the same AlertMessage
  and ActionContext objects.
- validate() raises InvalidActionError when name is empty.
- validate() raises InvalidActionError when callback is not callable.
- validate() does not raise for a valid destination.
- type class variable equals "callback".
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import cast
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.destinations.callback import CallbackDQAlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.errors import InvalidActionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message() -> AlertMessage:
    return AlertMessage(
        title="DQX alert: test",
        summary="Test summary",
        condition="error_row_count > 0",
        table="catalog.schema.table",
        observed_metrics={"error_row_count": 5},
        run_id="run-abc",
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        severity="error",
        fields={"condition": "error_row_count > 0", "run_id": "run-abc"},
    )


def _make_context() -> ActionContext:
    return ActionContext(
        metrics={"error_row_count": 5},
        run_id="run-abc",
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
    )


def _make_services() -> ActionServices:
    spec_resolver = create_autospec(SecretResolver, instance=True)
    spec_client = create_autospec(WebhookClient, instance=True)
    return ActionServices(secret_resolver=spec_resolver, webhook_client=spec_client)


# ---------------------------------------------------------------------------
# type class variable
# ---------------------------------------------------------------------------


def test_callback_type_discriminator() -> None:
    dest = CallbackDQAlertDestination(name="cb_dest", callback=lambda msg, ctx: None)
    assert dest.type == "callback"


# ---------------------------------------------------------------------------
# deliver() tests
# ---------------------------------------------------------------------------


def test_deliver_calls_callback_exactly_once() -> None:
    captured: list[tuple[AlertMessage, ActionContext]] = []

    def my_callback(msg: AlertMessage, ctx: ActionContext) -> None:
        captured.append((msg, ctx))

    message = _make_message()
    context = _make_context()
    services = _make_services()

    dest = CallbackDQAlertDestination(name="cb_dest", callback=my_callback)
    dest.deliver(message, context, services)

    assert len(captured) == 1, "callback must be called exactly once"


def test_deliver_passes_same_message_and_context_objects() -> None:
    captured: list[tuple[AlertMessage, ActionContext]] = []

    def my_callback(msg: AlertMessage, ctx: ActionContext) -> None:
        captured.append((msg, ctx))

    message = _make_message()
    context = _make_context()
    services = _make_services()

    dest = CallbackDQAlertDestination(name="cb_dest", callback=my_callback)
    dest.deliver(message, context, services)

    received_msg, received_ctx = captured[0]
    assert received_msg is message, "deliver must forward the exact AlertMessage instance"
    assert received_ctx is context, "deliver must forward the exact ActionContext instance"


class _CallbackSpec:
    """Concrete callable spec for create_autospec — matches the callback signature."""

    def __call__(self, message: AlertMessage, context: ActionContext) -> None:
        pass  # pragma: no cover


def test_deliver_with_mock_callable() -> None:
    mock_cb = create_autospec(_CallbackSpec)
    message = _make_message()
    context = _make_context()
    services = _make_services()

    dest = CallbackDQAlertDestination(name="cb_dest", callback=mock_cb)
    dest.deliver(message, context, services)

    mock_cb.assert_called_once_with(message, context)


# ---------------------------------------------------------------------------
# Construction-time validation (Pydantic model_validator)
# ---------------------------------------------------------------------------


def test_validate_raises_for_empty_name() -> None:
    def noop(_msg: AlertMessage, _ctx: ActionContext) -> None:
        pass

    with pytest.raises(InvalidActionError):
        CallbackDQAlertDestination(name="", callback=noop)


def test_validate_raises_for_non_callable_callback() -> None:
    # Use typing.cast to pass a non-callable value without a type-ignore suppression.
    bad: Callable[[AlertMessage, ActionContext], None] = cast(Callable[[AlertMessage, ActionContext], None], 123)
    with pytest.raises(InvalidActionError):
        CallbackDQAlertDestination(name="cb_dest", callback=bad)


def test_validate_passes_for_valid_destination() -> None:
    def my_callback(_msg: AlertMessage, _ctx: ActionContext) -> None:
        pass

    dest = CallbackDQAlertDestination(name="cb_dest", callback=my_callback)
    assert dest.name == "cb_dest"


def test_validate_passes_for_lambda_callback() -> None:
    dest = CallbackDQAlertDestination(
        name="lambda_dest",
        callback=lambda msg, ctx: None,
    )
    assert callable(dest.callback)
