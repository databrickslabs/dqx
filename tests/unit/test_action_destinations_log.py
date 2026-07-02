"""Unit tests for the log alert destination.

Tests cover:
- deliver() logs one record at the configured level, including the alert title and metrics.
- deliver() sanitizes control characters in user-influenced fields (CWE-117).
- level defaults to "warning" and is normalized to lower case.
- an invalid level raises InvalidActionError.
- an empty name raises InvalidActionError (inherited AlertDestination contract).
- the destination round-trips through its metadata (type: log) form.
- it is a member of the AnyDestination discriminated union.
"""

import logging
from datetime import datetime, timezone
from typing import cast

import pytest
from pydantic import TypeAdapter

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.destinations.log import LogDQAlertDestination
from databricks.labs.dqx.actions.destinations.union import AnyDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.errors import InvalidActionError

_LOG = "databricks.labs.dqx.actions.destinations.log"


def _make_message(summary: str = "Action 'notify_on_errors' triggered on table 'catalog.schema.t'.") -> AlertMessage:
    return AlertMessage(
        title="DQX alert: notify_on_errors",
        summary=summary,
        condition="error_row_count > 0",
        table="catalog.schema.t",
        observed_metrics={"error_row_count": 5, "warning_row_count": 0},
        run_id="run-abc",
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        severity="error",
        fields={"condition": "error_row_count > 0", "run_id": "run-abc"},
    )


# ---------------------------------------------------------------------------
# deliver()  (action_context / action_services come from tests/unit/conftest.py)
# ---------------------------------------------------------------------------


def test_deliver_logs_one_record_at_configured_level(
    caplog: pytest.LogCaptureFixture, action_context: ActionContext, action_services: ActionServices
) -> None:
    destination = LogDQAlertDestination(name="log", level="error")

    with caplog.at_level(logging.DEBUG, logger=_LOG):
        destination.deliver(_make_message(), action_context, action_services)

    records = [r for r in caplog.records if r.name == _LOG]
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.ERROR
    assert "DQX alert: notify_on_errors" in record.message
    assert "error_row_count=5" in record.message
    assert "condition=error_row_count > 0" in record.message


def test_deliver_defaults_to_warning_level(
    caplog: pytest.LogCaptureFixture, action_context: ActionContext, action_services: ActionServices
) -> None:
    destination = LogDQAlertDestination(name="log")

    with caplog.at_level(logging.DEBUG, logger=_LOG):
        destination.deliver(_make_message(), action_context, action_services)

    assert destination.level == "warning"
    assert caplog.records[-1].levelno == logging.WARNING


def test_deliver_sanitizes_control_characters(
    caplog: pytest.LogCaptureFixture, action_context: ActionContext, action_services: ActionServices
) -> None:
    # A newline-laced summary must not forge a second log line (CWE-117).
    forged = _make_message(summary="line1\nINJECTED forged log line\r\tmore")

    destination = LogDQAlertDestination(name="log")
    with caplog.at_level(logging.DEBUG, logger=_LOG):
        destination.deliver(forged, action_context, action_services)

    message = caplog.records[-1].message
    assert "\n" not in message
    assert "\r" not in message
    assert "\t" not in message
    assert "INJECTED forged log line" in message  # content preserved, control chars replaced


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def test_level_is_normalized_to_lower_case() -> None:
    assert LogDQAlertDestination(name="log", level="ERROR").level == "error"


def test_invalid_level_raises() -> None:
    with pytest.raises(InvalidActionError, match="level must be one of"):
        LogDQAlertDestination(name="log", level="verbose")


def test_empty_name_raises() -> None:
    with pytest.raises(InvalidActionError, match="non-empty"):
        LogDQAlertDestination(name="")


def test_type_discriminator_is_log() -> None:
    assert LogDQAlertDestination(name="log").type == "log"


# ---------------------------------------------------------------------------
# serialization / union membership
# ---------------------------------------------------------------------------


def test_metadata_round_trip() -> None:
    original = LogDQAlertDestination(name="audit-log", level="info")
    dumped = original.model_dump(mode="json")
    assert dumped == {"type": "log", "name": "audit-log", "level": "info"}

    restored = LogDQAlertDestination.model_validate(dumped)
    assert restored == original


def test_resolves_from_any_destination_union() -> None:
    adapter: TypeAdapter[AlertDestination] = TypeAdapter(AnyDestination)
    resolved = adapter.validate_python({"type": "log", "name": "log", "level": "warning"})
    assert isinstance(resolved, LogDQAlertDestination)
    assert cast(LogDQAlertDestination, resolved).level == "warning"
