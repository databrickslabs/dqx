"""Remember setup steps an administrator chose to continue past without Studio confirming them.

Studio can only see grants made directly to a principal. Access given through a parent
group, or set up by hand in the workspace UI, often looks missing or unreadable to it.
An override records that an administrator confirmed such a step, so setup does not
stay blocked. Each override is tied to a fingerprint of what was confirmed (the step,
the Studio storage and audience, and any step-specific subject such as the warehouse).
Changing any of these makes the override stop applying.
"""

import hashlib
import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.setup.configuration import SetupSettings
from databricks_labs_dqx_app.backend.setup.models import SetupStepId
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources

_KEY_PREFIX = "setup_override_"
_CLEARED = ""
logger = logging.getLogger(__name__)

OVERRIDABLE_STEPS = frozenset(
    {
        SetupStepId.UNITY_CATALOG,
        SetupStepId.WAREHOUSE,
        SetupStepId.TASK_RUNNER,
        SetupStepId.ACCESS,
        SetupStepId.AI,
        SetupStepId.APP_SHARING,
    }
)


def override_fingerprint(step_id: SetupStepId, resources: ActiveResources, subject: str = "") -> str:
    """Return a stable fingerprint of what an override confirms.

    Args:
        step_id: Overridden step.
        resources: Studio storage and audience the override applies to.
        subject: Step-specific detail, for example the SQL warehouse ID.

    Returns:
        The hexadecimal SHA-256 digest.
    """
    canonical = {
        "step": step_id.value,
        "catalog": resources.volume.catalog,
        "schema": resources.volume.schema,
        "uc_principals": sorted(principal.casefold() for principal in resources.audience.uc_principals),
        "workspace_principals": sorted(principal.casefold() for principal in resources.audience.workspace_principals),
        "subject": subject,
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class SetupOverrides:
    """Persist administrator overrides per setup step.

    Store failures never raise: an unreadable override counts as absent, so the step
    blocks again, and a failed write is logged.

    Args:
        settings: Application settings store used for persistence.
        clock: Returns the current time; defaults to UTC now.
    """

    def __init__(self, settings: SetupSettings, *, clock: Callable[[], datetime] | None = None) -> None:
        self._settings = settings
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def is_overridden(self, step_id: SetupStepId, fingerprint: str) -> bool:
        """Return whether *step_id* has an override matching *fingerprint*."""
        try:
            stored = self._settings.get_setting(_KEY_PREFIX + step_id.value)
        except Exception:
            logger.warning("Could not read setup overrides; the step is checked normally.")
            return False
        if not stored:
            return False
        try:
            record = json.loads(stored)
        except ValueError:
            return False
        return isinstance(record, dict) and record.get("fingerprint") == fingerprint

    def record(self, step_id: SetupStepId, fingerprint: str, *, user_email: str | None) -> bool:
        """Record an override for *step_id*.

        Returns:
            Whether the override was saved.
        """
        value = json.dumps({"fingerprint": fingerprint, "overridden_at": self._clock().isoformat()})
        try:
            self._settings.save_setting(_KEY_PREFIX + step_id.value, value, user_email=user_email)
        except Exception:
            logger.warning("Could not save a setup override.")
            return False
        return True

    def clear(self, step_id: SetupStepId) -> None:
        """Remove any override for *step_id*, for example once its check passes on its own."""
        try:
            if self._settings.get_setting(_KEY_PREFIX + step_id.value):
                self._settings.save_setting(_KEY_PREFIX + step_id.value, _CLEARED)
        except Exception:
            logger.warning("Could not clear a setup override.")
