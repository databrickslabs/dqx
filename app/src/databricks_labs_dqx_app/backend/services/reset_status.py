"""Settings-backed store for the long-running "Reset database" job status.

Mirrors :mod:`backend.demo.status`: the reset now runs on a background daemon
thread (32 cross-backend DELETEs plus a full Genie space reprovision can outlive
the Databricks Apps gateway idle timeout), so the UI polls this store to drive
its spinner and terminal toast rather than blocking on the request. The status is
persisted as a JSON blob under *RESET_STATUS_KEY* in the *dq_app_settings*
key/value store so it survives an app restart and every worker sees the same
state.
"""

import dataclasses
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

logger = logging.getLogger(__name__)

RESET_STATUS_KEY = "reset_database_status"

# A ``running`` status whose ``updated_at`` is older than this is treated as
# STALE, i.e. not actually running. The reset runs in-process on a daemon thread
# and writes a terminal ``succeeded`` / ``failed`` status when it ends — but if
# the app process is restarted mid-reset (e.g. a redeploy), that thread dies
# WITHOUT writing a terminal status, leaving the persisted status wedged at
# ``running`` forever and blocking every future reset / demo deploy with a 409.
# A run that has not advanced within this window is therefore considered dead so
# an interrupted reset self-heals. The bound is generous: a reset takes seconds
# to a couple of minutes (the Genie reprovision is the slow step), so 30 minutes
# has wide margin over the realistic wall-clock while still self-healing quickly
# relative to the demo seed's 2-hour window.
_STALE_RUNNING_AFTER_SECONDS = 30 * 60  # 30 minutes


@dataclass
class ResetStatus:
    """Snapshot of the database-reset job's current state.

    Args:
        state: One of ``idle``, ``running``, ``succeeded``, or ``failed``.
        message: Free-form status message for display in the UI (on ``failed``
            this carries the sanitized error message).
        started_at: Timestamp string of when the job started.
        updated_at: Timestamp string of the last status update.
        cleared_count: Number of app tables cleared (populated on ``succeeded``).
        failed_count: Number of tables/steps that failed (populated on ``succeeded``).
    """

    state: str
    message: str
    started_at: str
    updated_at: str
    cleared_count: int = 0
    failed_count: int = 0


def _idle_default() -> ResetStatus:
    return ResetStatus(state="idle", message="", started_at="", updated_at="")


class ResetStatusStore:
    """Persists and retrieves the database-reset job status via *AppSettingsService*.

    Args:
        app_settings: The application settings service used for key/value persistence.
    """

    def __init__(self, app_settings: AppSettingsService) -> None:
        self._app_settings = app_settings

    def get(self) -> ResetStatus:
        """Return the current reset status, healing a stale ``running`` to terminal.

        Never raises — a corrupt or missing blob degrades gracefully to the idle
        default so a wedged store cannot block the admin UI.

        A ``running`` status whose ``updated_at`` is older than
        :data:`_STALE_RUNNING_AFTER_SECONDS` (or unparseable) is downgraded to a
        terminal ``failed`` here, on the read path — not only inside
        :meth:`is_running`. The reset thread writes a terminal status when it
        ends, but if the app process is restarted mid-reset that thread dies
        WITHOUT writing one, leaving the persisted status wedged at ``running``
        forever. Since the status endpoint returns whatever :meth:`get` yields,
        without this heal the Danger Zone spinner would wedge indefinitely for
        any admin who opens Settings after such a restart.
        """
        status = self._load()
        if self._running_is_stale(status):
            return ResetStatus(
                state="failed",
                message=(
                    "The previous reset did not report an outcome — the app was likely restarted "
                    "while it was running. Its result is unknown; re-run the reset if needed."
                ),
                started_at=status.started_at,
                updated_at=status.updated_at,
            )
        return status

    def _load(self) -> ResetStatus:
        """Load the raw persisted status, defaulting to *idle* when unset or unparseable."""
        raw = self._app_settings.get_setting(RESET_STATUS_KEY)
        if raw is None:
            return _idle_default()
        try:
            data = json.loads(raw)
            return ResetStatus(**data)
        except (ValueError, TypeError, KeyError):
            logger.warning("reset status blob is unparseable; returning idle default")
            return _idle_default()

    def _running_is_stale(self, status: ResetStatus) -> bool:
        """Whether *status* is a ``running`` status too old to still be live.

        A non-``running`` status is never stale. A ``running`` status is stale
        when its ``updated_at`` is older than :data:`_STALE_RUNNING_AFTER_SECONDS`
        or cannot be parsed (an interrupted run that never wrote a terminal
        status) — so a wedged status cannot block new resets or the UI forever.
        """
        if status.state != "running":
            return False
        try:
            updated = datetime.fromisoformat(status.updated_at)
        except (ValueError, TypeError):
            return True
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - updated).total_seconds()
        return age_seconds >= _STALE_RUNNING_AFTER_SECONDS

    def set(self, status: ResetStatus, *, user_email: str | None = None) -> None:
        """Persist the given status to the settings store.

        Args:
            status: The *ResetStatus* to persist.
            user_email: Optional email of the user triggering the update, recorded for
                audit purposes.
        """
        json_str = json.dumps(dataclasses.asdict(status))
        self._app_settings.save_setting(RESET_STATUS_KEY, json_str, user_email=user_email)

    def is_running(self) -> bool:
        """Return *True* when a reset job is genuinely still running.

        A status is only "running" if its state is ``running`` AND recent. Because
        :meth:`get` already downgrades a stale ``running`` (an interrupted run
        that never wrote a terminal status) to ``failed``, this reduces to reading
        the healed state — so the 409 gate and the status endpoint agree, and a
        wedged status can never block new resets forever.
        """
        return self.get().state == "running"
