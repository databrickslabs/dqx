"""Tests for process-wide setup state publication."""

import pytest
from pydantic import ValidationError

from databricks_labs_dqx_app.backend.setup.models import (
    SetupActionId,
    SetupReport,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
)
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime


def test_runtime_starts_checking_and_publishes_immutable_reports() -> None:
    runtime = SetupRuntime()
    assert runtime.report().state == SetupState.CHECKING

    report = SetupReport(
        state=SetupState.SETUP_REQUIRED,
        current_step=SetupStepId.VOLUME,
        steps=(
            SetupStep(
                id=SetupStepId.VOLUME,
                state=StepState.ACTION_REQUIRED,
                code="volume_missing",
                summary="A wheels volume is required.",
                instructions=("Bind a Unity Catalog volume.",),
                actions=(SetupActionId.VERIFY_AGAIN,),
            ),
        ),
    )
    runtime.publish(report)

    assert runtime.report() is report
    assert runtime.report().step(SetupStepId.VOLUME).code == "volume_missing"
    with pytest.raises(ValidationError):
        report.state = SetupState.READY


def test_report_step_rejects_unknown_step() -> None:
    report = SetupReport(state=SetupState.CHECKING, steps=())

    with pytest.raises(LookupError, match="task_runner"):
        report.step(SetupStepId.TASK_RUNNER)


@pytest.mark.asyncio
async def test_activation_lock_serializes_reconciliation() -> None:
    runtime = SetupRuntime()

    async with runtime.activation_lock:
        assert runtime.activation_lock.locked()
