"""Settings-backed store for the long-running demo-seed job status.

The status is persisted as a JSON blob under *DEMO_STATUS_KEY* in the
*dq_app_settings* key/value store so the admin UI can poll it and it
survives an app restart.
"""

import dataclasses
import json
import logging
from dataclasses import dataclass

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

logger = logging.getLogger(__name__)

DEMO_STATUS_KEY = "demo_content_status"


@dataclass
class DemoStatus:
    """Snapshot of the demo-seed job's current state.

    Args:
        state: One of ``idle``, ``running``, ``succeeded``, or ``failed``.
        phase: Human-readable phase label (e.g. *datagen*, *rules*).
        message: Free-form status message for display in the UI.
        started_at: ISO-8601 timestamp string when the job started.
        updated_at: ISO-8601 timestamp string of the last status update.
    """

    state: str
    phase: str
    message: str
    started_at: str
    updated_at: str


def _idle_default() -> DemoStatus:
    return DemoStatus(state="idle", phase="", message="", started_at="", updated_at="")


class DemoStatusStore:
    """Persists and retrieves the demo-seed job status via *AppSettingsService*.

    Args:
        app_settings: The application settings service used for key/value persistence.
    """

    def __init__(self, app_settings: AppSettingsService) -> None:
        self._app_settings = app_settings

    def get(self) -> DemoStatus:
        """Return the current demo status, defaulting to *idle* when unset or unparseable.

        Never raises — a corrupt or missing blob degrades gracefully to the idle default
        so a wedged store cannot block the admin UI.
        """
        raw = self._app_settings.get_setting(DEMO_STATUS_KEY)
        if raw is None:
            return _idle_default()
        try:
            data = json.loads(raw)
            return DemoStatus(**data)
        except (ValueError, TypeError, KeyError):
            logger.warning("demo status blob is unparseable; returning idle default")
            return _idle_default()

    def set(self, status: DemoStatus, *, user_email: str | None = None) -> None:
        """Persist the given status to the settings store.

        Args:
            status: The *DemoStatus* to persist.
            user_email: Optional email of the user triggering the update, recorded for
                audit purposes.
        """
        json_str = json.dumps(dataclasses.asdict(status))
        self._app_settings.save_setting(DEMO_STATUS_KEY, json_str, user_email=user_email)

    def is_running(self) -> bool:
        """Return *True* when the demo-seed job is currently in the *running* state."""
        return self.get().state == "running"
