"""Teams Adaptive Card notification sender for DQX run results."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_MAX_RULE_NAMES = 8   # show at most this many rule names in the card


class TeamsNotifier:
    """Sends Microsoft Teams Adaptive Card notifications via incoming webhooks."""

    _TIMEOUT = 10.0

    def send_test(self, webhook_url: str) -> None:
        """Send a sample notification to verify the webhook URL works."""
        payload = _build_payload(
            runs=[],
            title="DQX Studio — Test Notification",
            subtitle="This is a test message from DQX Studio. Your Teams channel is configured correctly.",
            workspace_url="",
            job_id="",
        )
        _post(webhook_url, payload, self._TIMEOUT)

    def send_run_notification(
        self,
        runs: list[dict[str, Any]],
        webhook_url: str,
        *,
        trigger: str = "manual",
        workspace_url: str = "",
        job_id: str = "",
    ) -> None:
        """Send run results as an Adaptive Card to a Teams webhook."""
        if not runs:
            return
        if trigger == "dry_run":
            subtitle = "Dry run results"
        elif trigger == "scheduled":
            subtitle = "Scheduled run results"
        else:
            subtitle = "Manual run results"
        payload = _build_payload(
            runs=runs,
            title="DQX Data Quality Run Results",
            subtitle=subtitle,
            workspace_url=workspace_url,
            job_id=job_id,
        )
        _post(webhook_url, payload, self._TIMEOUT)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _post(webhook_url: str, payload: dict[str, Any], timeout: float) -> None:
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(webhook_url, json=payload)
        resp.raise_for_status()


def _build_payload(
    runs: list[dict[str, Any]],
    title: str,
    subtitle: str,
    workspace_url: str,
    job_id: str,
) -> dict[str, Any]:
    """Build the Teams-compatible Adaptive Card JSON payload."""
    body: list[dict[str, Any]] = [
        {
            "type": "TextBlock",
            "text": title,
            "weight": "Bolder",
            "size": "Large",
            "wrap": True,
        },
        {
            "type": "TextBlock",
            "text": subtitle,
            "isSubtle": True,
            "wrap": True,
            "spacing": "None",
        },
    ]

    for run in runs:
        _append_run_block(body, run, workspace_url=workspace_url, job_id=job_id)

    card: dict[str, Any] = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    }

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card,
            }
        ],
    }


def _append_run_block(
    body: list[dict[str, Any]],
    run: dict[str, Any],
    *,
    workspace_url: str,
    job_id: str,
) -> None:
    """Append Adaptive Card blocks for a single run result."""
    table = run.get("source_table_fqn") or "unknown"
    error_rows = run.get("error_rows") or 0
    warning_rows = run.get("warning_rows") or 0
    total = run.get("total_rows")
    valid = run.get("valid_rows")
    created_at = run.get("created_at")
    requesting_user = run.get("requesting_user")
    job_run_id = run.get("job_run_id")
    checks_json_raw = run.get("checks_json")
    error_message = run.get("error_message")

    is_failure = int(error_rows or 0) > 0
    status_label = "FAILURE" if is_failure else "SUCCESS"
    status_color = "Attention" if is_failure else "Good"

    # Pass rate
    pass_rate_str: str | None = None
    if total and int(total) > 0:
        rate = round((int(valid or 0) / int(total)) * 100, 1)
        pass_rate_str = f"{rate}%"

    # Separator + table header
    body.append({"type": "TextBlock", "text": " ", "spacing": "Medium"})
    body.append(
        {
            "type": "TextBlock",
            "text": f"**{table.split('.')[-1]}**",
            "color": status_color,
            "wrap": True,
        }
    )
    body.append(
        {
            "type": "TextBlock",
            "text": table,
            "isSubtle": True,
            "size": "Small",
            "spacing": "None",
            "wrap": True,
        }
    )

    # Status + row counts fact set
    facts: list[dict[str, str]] = [
        {"title": "Status", "value": status_label},
    ]
    if created_at is not None:
        facts.append({"title": "Triggered", "value": str(created_at)})
    if requesting_user:
        facts.append({"title": "User", "value": str(requesting_user)})
    if total is not None:
        facts.append({"title": "Total rows", "value": str(total)})
    if pass_rate_str is not None:
        facts.append({"title": "Pass rate", "value": pass_rate_str})
    if valid is not None:
        facts.append({"title": "Valid rows", "value": str(valid)})
    if error_rows is not None:
        facts.append({"title": "Error rows", "value": str(error_rows)})
    if warning_rows is not None:
        facts.append({"title": "Warning rows", "value": str(warning_rows)})

    body.append({"type": "FactSet", "facts": facts})

    # Rule descriptions from checks_json
    if checks_json_raw:
        rule_labels = _extract_rule_labels(checks_json_raw)
        if rule_labels:
            shown = rule_labels[:_MAX_RULE_NAMES]
            more = len(rule_labels) - len(shown)
            label_text = "\n\n".join(f"- {r}" for r in shown)
            if more > 0:
                label_text += f"\n\n- *(+{more} more)*"
            body.append(
                {
                    "type": "TextBlock",
                    "text": "**Rules applied:**",
                    "weight": "Bolder",
                    "size": "Small",
                    "spacing": "Small",
                    "wrap": True,
                }
            )
            body.append(
                {
                    "type": "TextBlock",
                    "text": label_text,
                    "size": "Small",
                    "wrap": True,
                    "spacing": "None",
                }
            )

    # Exception — shown when error_message is present
    if error_message:
        body.append(
            {
                "type": "TextBlock",
                "text": "**Exception:**",
                "weight": "Bolder",
                "size": "Small",
                "color": "Attention",
                "spacing": "Small",
                "wrap": True,
            }
        )
        body.append(
            {
                "type": "TextBlock",
                "text": str(error_message)[:500],
                "size": "Small",
                "wrap": True,
                "color": "Attention",
                "spacing": "None",
            }
        )

    # Link to job run (only when this was a workflow-triggered run)
    if job_run_id and job_id and workspace_url:
        run_url = f"{workspace_url}/jobs/{job_id}/runs/{job_run_id}"
        body.append(
            {
                "type": "ActionSet",
                "spacing": "Small",
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "View Run in Databricks",
                        "url": run_url,
                    }
                ],
            }
        )


def _extract_rule_labels(checks_json_raw: str) -> list[str]:
    """Parse checks_json and return human-readable rule descriptions."""
    try:
        checks = json.loads(checks_json_raw)
        if not isinstance(checks, list):
            return []
    except (json.JSONDecodeError, TypeError):
        return []

    labels: list[str] = []
    for check in checks:
        if not isinstance(check, dict):
            continue

        # Use the user-defined name if available
        name = check.get("name") or ""

        # Extract the check function and column from nested "check" dict
        check_inner = check.get("check") or {}
        func = check_inner.get("function") or ""
        args = check_inner.get("arguments") or {}
        col = args.get("col_name") or args.get("columns") or ""

        criticality = check.get("criticality") or "error"
        crit_tag = "⚠️" if criticality in ("warn", "warning") else "❌"

        if name:
            label = f"{crit_tag} {name}"
            if col and not name.lower().replace(" ", "_").endswith(str(col).lower()):
                label += f" ({col})"
        elif func and col:
            label = f"{crit_tag} {func}({col})"
        elif func:
            label = f"{crit_tag} {func}"
        else:
            continue

        labels.append(label)

    return labels
