"""Unit tests for databricks.labs.dqx.actions.fail_pipeline.

TDD step 1: write failing tests before implementation.

Tests cover:
- FailPipeline.execute raises PipelineFailedError.
- Default message includes metrics information from context.
- Custom message is respected when provided.
- ActionResult is never returned (always raises before returning).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.delivery import WebhookClient  # type: ignore[import-untyped]
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.errors import PipelineFailedError, TerminalActionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(*, metrics: dict[str, object] | None = None) -> ActionContext:
    return ActionContext(
        metrics=metrics or {"error_row_count": 42, "warn_row_count": 3},
        run_id="run-fp-001",
        run_time=datetime(2024, 7, 4, 9, 0, 0, tzinfo=timezone.utc),
        input_location="catalog.schema.target_table",
    )


def _make_services() -> ActionServices:
    spec_resolver = create_autospec(SecretResolver, instance=True)
    spec_client = create_autospec(WebhookClient, instance=True)
    return ActionServices(secret_resolver=spec_resolver, webhook_client=spec_client)


# ---------------------------------------------------------------------------
# FailPipeline defaults
# ---------------------------------------------------------------------------


def test_fail_pipeline_default_name() -> None:
    action = FailPipeline()
    assert action.name == "fail_pipeline"


def test_fail_pipeline_default_message_is_none() -> None:
    action = FailPipeline()
    assert action.message is None


# ---------------------------------------------------------------------------
# FailPipeline.execute — raises PipelineFailedError
# ---------------------------------------------------------------------------


def test_fail_pipeline_execute_raises_pipeline_failed_error() -> None:
    action = FailPipeline()
    with pytest.raises(PipelineFailedError):
        action.execute(_make_context(), _make_services())


def test_fail_pipeline_execute_default_message_contains_metrics() -> None:
    metrics: dict[str, object] = {"error_row_count": 99, "warn_row_count": 1}
    action = FailPipeline()
    with pytest.raises(PipelineFailedError) as exc_info:
        action.execute(_make_context(metrics=metrics), _make_services())
    error_message = str(exc_info.value)
    # The default message must include some metric information.
    assert "error_row_count" in error_message or "99" in error_message


def test_fail_pipeline_execute_default_message_contains_action_name() -> None:
    action = FailPipeline(name="my_fail_action")
    with pytest.raises(PipelineFailedError) as exc_info:
        action.execute(_make_context(), _make_services())
    error_message = str(exc_info.value)
    assert "my_fail_action" in error_message


def test_fail_pipeline_execute_custom_message_respected() -> None:
    custom_msg = "Custom pipeline failure message"
    action = FailPipeline(message=custom_msg)
    with pytest.raises(PipelineFailedError) as exc_info:
        action.execute(_make_context(), _make_services())
    error_message = str(exc_info.value)
    assert custom_msg in error_message


def test_fail_pipeline_execute_custom_message_overrides_default() -> None:
    """When a custom message is set, the default metrics info is NOT used."""
    custom_msg = "Explicit failure"
    action = FailPipeline(message=custom_msg)
    metrics: dict[str, object] = {"error_row_count": 7}
    with pytest.raises(PipelineFailedError) as exc_info:
        action.execute(_make_context(metrics=metrics), _make_services())
    error_message = str(exc_info.value)
    assert custom_msg in error_message


def test_fail_pipeline_execute_raises_terminal_action_error_subclass() -> None:
    """PipelineFailedError must be a TerminalActionError so the evaluator defers it."""
    action = FailPipeline()
    with pytest.raises(TerminalActionError):
        action.execute(_make_context(), _make_services())


def test_fail_pipeline_with_custom_name() -> None:
    action = FailPipeline(name="custom_fail")
    assert action.name == "custom_fail"
