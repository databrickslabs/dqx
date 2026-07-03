"""Teams Adaptive Card notification sender for DQX run results."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class TeamsNotifier:
    """Sends Microsoft Teams Adaptive Card notifications via incoming webhooks."""

    _TIMEOUT = 10.0

    def send_test(self, webhook_url: str) -> None:
        """Send a sample notification to verify the webhook URL works."""
        payload = self._build_card(
            runs=[],
            title="DQX Studio — Test Notification",
            subtitle="This is a test message from DQX Studio. Your Teams channel is configured correctly.",
        )
        self._post(webhook_url, payload)

    def send_run_notification(
        self,
        runs: list[dict[str, Any]],
        webhook_url: str,
        *,
        trigger: str = "manual",
    ) -> None:
        """Send run results as an Adaptive Card to a Teams webhook."""
        if not runs:
            return
        subtitle = "Manual run results" if trigger == "manual" else "Scheduled run results"
        payload = self._build_card(runs=runs, title="DQX Data Quality Run Results", subtitle=subtitle)
        self._post(webhook_url, payload)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post(self, webhook_url: str, payload: dict[str, Any]) -> None:
        with httpx.Client(timeout=self._TIMEOUT) as client:
            resp = client.post(webhook_url, json=payload)
            resp.raise_for_status()

    def _build_card(
        self,
        runs: list[dict[str, Any]],
        title: str,
        subtitle: str,
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
            table = run.get("source_table_fqn", "unknown")
            status = run.get("status", "unknown")
            total = run.get("total_rows")
            errors = run.get("error_rows")
            warnings = run.get("warning_rows")
            valid = run.get("valid_rows")

            status_color = "Attention" if (errors or 0) > 0 else "Good"
            if status in ("failed", "error"):
                status_color = "Attention"

            facts: list[dict[str, str]] = [
                {"title": "Status", "value": status.upper() if status else "UNKNOWN"},
            ]
            if total is not None:
                facts.append({"title": "Total rows", "value": str(total)})
            if valid is not None:
                facts.append({"title": "Valid rows", "value": str(valid)})
            if errors is not None:
                facts.append({"title": "Error rows", "value": str(errors)})
            if warnings is not None:
                facts.append({"title": "Warning rows", "value": str(warnings)})

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
            body.append(
                {
                    "type": "FactSet",
                    "facts": facts,
                }
            )

        card: dict[str, Any] = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": body,
        }

        # Teams workflow-compatible envelope
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card,
                }
            ],
        }
