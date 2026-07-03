"""Alert channel management and Teams webhook notification endpoints."""

from __future__ import annotations

import logging
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_conf,
    get_job_service,
    require_role,
    require_runner,
)
from databricks_labs_dqx_app.backend.models import (
    AlertChannelIn,
    AlertChannelOut,
    NotifyResultIn,
    NotifyRunsIn,
    NotifyRunsOut,
    TestWebhookIn,
    TestWebhookOut,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.teams_notifier import TeamsNotifier

logger = logging.getLogger(__name__)

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_ADMIN_ONLY = [UserRole.ADMIN]


@router.get(
    "/channels",
    response_model=list[AlertChannelOut],
    operation_id="listAlertChannels",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_alert_channels(
    settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> list[AlertChannelOut]:
    """Return all configured alert channels."""
    raw = settings.get_alert_channels()
    return [AlertChannelOut(**ch) for ch in raw]


@router.post(
    "/channels",
    response_model=AlertChannelOut,
    operation_id="createAlertChannel",
    dependencies=[require_role(*_ADMIN_ONLY)],
)
def create_alert_channel(
    body: AlertChannelIn,
    settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> AlertChannelOut:
    """Create a new alert channel."""
    channels = settings.get_alert_channels()
    channel_id = uuid4().hex[:16]
    new_channel = {
        "channel_id": channel_id,
        "name": body.name,
        "webhook_url": body.webhook_url,
        "trigger": body.trigger,
        "enabled": body.enabled,
        "notify_dry_runs": body.notify_dry_runs,
    }
    channels.append(new_channel)
    settings.save_alert_channels(channels)
    return AlertChannelOut(**new_channel)


@router.put(
    "/channels/{channel_id}",
    response_model=AlertChannelOut,
    operation_id="updateAlertChannel",
    dependencies=[require_role(*_ADMIN_ONLY)],
)
def update_alert_channel(
    channel_id: str,
    body: AlertChannelIn,
    settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> AlertChannelOut:
    """Update an existing alert channel."""
    channels = settings.get_alert_channels()
    for i, ch in enumerate(channels):
        if ch.get("channel_id") == channel_id:
            channels[i] = {
                "channel_id": channel_id,
                "name": body.name,
                "webhook_url": body.webhook_url,
                "trigger": body.trigger,
                "enabled": body.enabled,
                "notify_dry_runs": body.notify_dry_runs,
            }
            settings.save_alert_channels(channels)
            return AlertChannelOut(**channels[i])
    raise HTTPException(status_code=404, detail=f"Alert channel '{channel_id}' not found")


@router.delete(
    "/channels/{channel_id}",
    status_code=204,
    operation_id="deleteAlertChannel",
    dependencies=[require_role(*_ADMIN_ONLY)],
)
def delete_alert_channel(
    channel_id: str,
    settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> None:
    """Delete an alert channel."""
    channels = settings.get_alert_channels()
    updated = [ch for ch in channels if ch.get("channel_id") != channel_id]
    if len(updated) == len(channels):
        raise HTTPException(status_code=404, detail=f"Alert channel '{channel_id}' not found")
    settings.save_alert_channels(updated)


@router.post(
    "/test",
    response_model=TestWebhookOut,
    operation_id="testAlertWebhook",
    dependencies=[require_role(*_ADMIN_ONLY)],
)
def test_alert_webhook(body: TestWebhookIn) -> TestWebhookOut:
    """Send a test message to a Teams webhook to verify it works."""
    try:
        TeamsNotifier().send_test(body.webhook_url)
        return TestWebhookOut(success=True, message="Test message sent successfully")
    except Exception as exc:
        logger.warning("Webhook test failed for %s: %s", body.webhook_url[:40], exc)
        return TestWebhookOut(success=False, message=str(exc))


@router.post(
    "/notify",
    response_model=NotifyRunsOut,
    operation_id="notifyRuns",
    dependencies=[require_runner()],
)
def notify_runs(
    body: NotifyRunsIn,
    settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    conf: Annotated[AppConfig, Depends(get_conf)],
) -> NotifyRunsOut:
    """Send Teams notifications for the given run IDs.

    Reads run results from ``dq_validation_runs``, then posts to every
    enabled channel whose trigger matches the supplied ``trigger`` value.
    """
    channels = settings.get_alert_channels()
    active_channels = [
        ch for ch in channels
        if ch.get("enabled", True) and _trigger_matches(ch.get("trigger", "all_runs"), body.trigger)
    ]

    if not active_channels:
        return NotifyRunsOut(notified=0, skipped=len(channels))

    runs_table = f"{conf.catalog}.{conf.schema_name}.dq_validation_runs"
    runs: list[dict] = []
    for run_id in body.run_ids:
        row = job_svc.get_run_result_row(runs_table, run_id)
        if row:
            runs.append(row)

    if not runs:
        logger.info("notifyRuns: no run results found for %s", body.run_ids)
        return NotifyRunsOut(notified=0, skipped=len(channels))

    notifier = TeamsNotifier()
    notified = 0
    errors: list[str] = []
    skipped = len(channels) - len(active_channels)

    for ch in active_channels:
        try:
            notifier.send_run_notification(runs, ch["webhook_url"], trigger=body.trigger)
            notified += 1
            logger.info("Sent run notification to channel '%s'", ch.get("name", ch["channel_id"]))
        except Exception as exc:
            msg = f"Channel '{ch.get('name')}': {exc}"
            logger.warning("Failed to send Teams notification: %s", msg)
            errors.append(msg)
            skipped += 1

    return NotifyRunsOut(notified=notified, skipped=skipped, errors=errors)


@router.post(
    "/notify-result",
    response_model=NotifyRunsOut,
    operation_id="notifyDryRunResult",
    dependencies=[require_role(*_ALL_ROLES)],
)
def notify_dry_run_result(
    body: NotifyResultIn,
    settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> NotifyRunsOut:
    """Send a Teams notification for a completed dry run.

    Posts to every enabled channel that has ``notify_dry_runs`` set.
    The result data is passed in the request body so the caller doesn't
    need a ``run_id`` stored in ``dq_validation_runs``.
    """
    channels = settings.get_alert_channels()
    active_channels = [
        ch for ch in channels
        if ch.get("enabled", True) and ch.get("notify_dry_runs", False)
    ]

    if not active_channels:
        return NotifyRunsOut(notified=0, skipped=len(channels))

    run_data = {
        "source_table_fqn": body.source_table_fqn,
        "status": body.status,
        "total_rows": body.total_rows,
        "valid_rows": body.valid_rows,
        "error_rows": body.error_rows,
        "warning_rows": body.warning_rows,
    }

    notifier = TeamsNotifier()
    notified = 0
    errors: list[str] = []
    skipped = len(channels) - len(active_channels)

    for ch in active_channels:
        try:
            notifier.send_run_notification([run_data], ch["webhook_url"], trigger="dry_run")
            notified += 1
            logger.info("Sent dry-run notification to channel '%s'", ch.get("name", ch["channel_id"]))
        except Exception as exc:
            msg = f"Channel '{ch.get('name')}': {exc}"
            logger.warning("Failed to send Teams dry-run notification: %s", msg)
            errors.append(msg)
            skipped += 1

    return NotifyRunsOut(notified=notified, skipped=skipped, errors=errors)


def _trigger_matches(channel_trigger: str, run_trigger: str) -> bool:
    """Return True if the channel's trigger setting matches the run trigger."""
    if channel_trigger == "all_runs":
        return True
    if channel_trigger == "manual_only" and run_trigger == "manual":
        return True
    if channel_trigger == "scheduled_only" and run_trigger == "scheduled":
        return True
    return False
