"""Alert channel management and Teams webhook notification endpoints."""

from __future__ import annotations

import logging
from typing import Annotated
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_conf,
    get_job_service,
    get_sp_ws,
    require_role,
    require_runner,
)
from databricks_labs_dqx_app.backend.models import (
    AlertChannelIn,
    AlertChannelOut,
    AlertStatusOut,
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


def _channel_dict(body: AlertChannelIn, channel_id: str) -> dict:
    return {
        "channel_id": channel_id,
        "name": body.name,
        "webhook_url": body.webhook_url,
        "trigger": body.trigger,
        "enabled": body.enabled,
        "notify_dry_runs": body.notify_dry_runs,
        "scope_mode": body.scope_mode,
        "scope_tables": body.scope_tables,
    }


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
    new_channel = _channel_dict(body, channel_id)
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
            channels[i] = _channel_dict(body, channel_id)
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
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
) -> NotifyRunsOut:
    """Send Teams notifications for the given run IDs.

    Reads run results from ``dq_validation_runs``, then posts to every
    enabled channel whose trigger and scope match.
    """
    channels = settings.get_alert_channels()
    workspace_url = _workspace_url(sp_ws, conf)

    runs_table = f"{conf.catalog}.{conf.schema_name}.dq_validation_runs"
    runs: list[dict] = []
    for run_id in body.run_ids:
        row = job_svc.get_run_result_row(runs_table, run_id)
        if row:
            runs.append(dict(row))

    if not runs:
        logger.info("notifyRuns: no run results found for %s", body.run_ids)
        return NotifyRunsOut(notified=0, skipped=len(channels))

    notifier = TeamsNotifier()
    notified = 0
    errors: list[str] = []
    skipped = 0

    for ch in channels:
        if not ch.get("enabled", True):
            skipped += 1
            continue
        if not _trigger_matches(ch.get("trigger", "all_runs"), body.trigger):
            skipped += 1
            continue
        # Filter runs to those matching this channel's scope
        scoped_runs = [r for r in runs if _scope_matches(ch, r.get("source_table_fqn", ""))]
        if not scoped_runs:
            skipped += 1
            continue
        try:
            notifier.send_run_notification(
                scoped_runs, ch["webhook_url"],
                trigger=body.trigger,
                workspace_url=workspace_url,
                job_id=conf.job_id,
            )
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
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    conf: Annotated[AppConfig, Depends(get_conf)],
) -> NotifyRunsOut:
    """Send a Teams notification for a completed dry run."""
    channels = settings.get_alert_channels()
    workspace_url = _workspace_url(sp_ws, conf)

    run_data = {
        "source_table_fqn": body.source_table_fqn,
        "status": body.status,
        "total_rows": body.total_rows,
        "valid_rows": body.valid_rows,
        "error_rows": body.error_rows,
        "warning_rows": body.warning_rows,
        "created_at": body.created_at,
        "requesting_user": body.requesting_user,
        "checks_json": body.checks_json,
        "error_message": body.error_message,
    }

    notifier = TeamsNotifier()
    notified = 0
    errors: list[str] = []
    skipped = 0

    for ch in channels:
        if not ch.get("enabled", True) or not ch.get("notify_dry_runs", False):
            skipped += 1
            continue
        if not _scope_matches(ch, body.source_table_fqn):
            skipped += 1
            continue
        try:
            notifier.send_run_notification(
                [run_data], ch["webhook_url"],
                trigger="dry_run",
                workspace_url=workspace_url,
                job_id=conf.job_id,
            )
            notified += 1
            logger.info("Sent dry-run notification to channel '%s'", ch.get("name", ch["channel_id"]))
        except Exception as exc:
            msg = f"Channel '{ch.get('name')}': {exc}"
            logger.warning("Failed to send Teams dry-run notification: %s", msg)
            errors.append(msg)
            skipped += 1

    return NotifyRunsOut(notified=notified, skipped=skipped, errors=errors)


def _status_out(row: dict) -> AlertStatusOut:
    return AlertStatusOut(
        status=row.get("status") or "UNKNOWN",
        source_table_fqn=row.get("source_table_fqn") or "",
        run_id=row.get("run_id") or "",
        error_rows=row.get("error_rows"),
        warning_rows=row.get("warning_rows"),
        updated_at=row.get("updated_at"),
    )


def _raise_for_status(row: dict) -> AlertStatusOut:
    """Return the status body on pass, or raise 503 on fail.

    Pass = a completed run with no error rows. Anything else (FAILED,
    CANCELED, or SUCCESS with error_rows > 0) is reported as a failure so
    an uptime monitor sees it as "down".
    """
    out = _status_out(row)
    error_rows = row.get("error_rows") or 0
    if out.status == "SUCCESS" and error_rows == 0:
        return out
    raise HTTPException(status_code=503, detail=out.model_dump())


@router.get(
    "/status/table/{table_fqn:path}",
    response_model=AlertStatusOut,
    operation_id="getAlertStatusByTable",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_alert_status_by_table(
    table_fqn: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    conf: Annotated[AppConfig, Depends(get_conf)],
) -> AlertStatusOut:
    """Return the latest run's pass/fail status for a table.

    Meant for external polling (e.g. a Site24x7 REST monitor authenticating
    with a Databricks OAuth token) — 200 on a clean run, 503 on a failed one.
    """
    runs_table = f"{conf.catalog}.{conf.schema_name}.dq_validation_runs"
    row = job_svc.get_latest_run_result_row(runs_table, table_fqn)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No validation run recorded for '{table_fqn}'")
    return _raise_for_status(row)


@router.get(
    "/status/run/{run_id}",
    response_model=AlertStatusOut,
    operation_id="getAlertStatusByRun",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_alert_status_by_run(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    conf: Annotated[AppConfig, Depends(get_conf)],
) -> AlertStatusOut:
    """Return a specific run's pass/fail status.

    Same 200/503 contract as ``getAlertStatusByTable`` but keyed by
    ``run_id`` instead of table name.
    """
    runs_table = f"{conf.catalog}.{conf.schema_name}.dq_validation_runs"
    row = job_svc.get_run_result_row(runs_table, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _raise_for_status(row)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _workspace_url(sp_ws: WorkspaceClient, conf: AppConfig) -> str:
    """Return the workspace host URL (no trailing slash)."""
    try:
        return sp_ws.config.host.rstrip("/")
    except Exception:
        return ""


def _trigger_matches(channel_trigger: str, run_trigger: str) -> bool:
    if channel_trigger == "all_runs":
        return True
    if channel_trigger == "manual_only" and run_trigger == "manual":
        return True
    if channel_trigger == "scheduled_only" and run_trigger == "scheduled":
        return True
    return False


def _scope_matches(channel: dict, source_table_fqn: str) -> bool:
    """Return True if the table is within the channel's configured scope."""
    scope_mode = channel.get("scope_mode", "all")
    if scope_mode == "all":
        return True
    if scope_mode == "tables":
        return source_table_fqn in (channel.get("scope_tables") or [])
    return True
