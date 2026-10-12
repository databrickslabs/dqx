"""Check that the configured AI model endpoints exist, and turn AI off when they don't.

The check only confirms each configured serving endpoint is listed in the workspace, first
as the app service principal and then as the setup administrator. It never blocks
readiness: a missing endpoint is reported as a warning. Only a check an administrator
runs changes the AI setting, so an unattended restart (which may not be able to see an
endpoint the administrator can) never flips it: unless an administrator chose the AI
setting themselves, an attended check turns AI features off when an endpoint is missing,
and back on once every endpoint is listed again.
"""

import logging
from typing import Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks_labs_dqx_app.backend.services.app_settings_service import AiEnabledSource
from databricks_labs_dqx_app.backend.setup.checks import instruction_identifier
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState

# Most severe first: a missing endpoint needs a settings change; a failed lookup may clear up.
_PROBLEM_ORDER = ("ai_endpoint_missing", "ai_endpoint_unverified")
logger = logging.getLogger(__name__)


class AiSettings(Protocol):
    """AI kill-switch and endpoint settings used by the AI check."""

    def get_ai_enabled(self) -> bool: ...

    def get_ai_enabled_source(self) -> AiEnabledSource: ...

    def save_ai_enabled(
        self, enabled: bool, *, user_email: str | None = None, source: AiEnabledSource = "admin"
    ) -> bool: ...

    def get_ai_endpoint_name(self) -> str: ...

    def get_embedding_endpoint_name(self) -> str: ...


class AiAccess:
    """Check that the configured AI model endpoints are listed in the workspace.

    Args:
        workspace: App service principal workspace client.
        settings: AI settings, including the kill-switch.
    """

    def __init__(self, *, workspace: WorkspaceClient, settings: AiSettings) -> None:
        self._workspace = workspace
        self._settings = settings

    def check_ai_access(self, reader_ws: WorkspaceClient | None = None) -> SetupStep:
        """Confirm every configured AI endpoint exists.

        Args:
            reader_ws: Setup administrator's client. Endpoints the app service principal
                can't see are looked up again with it, and its presence marks an attended
                check, the only kind that changes the AI setting.

        Returns:
            A passed step, or a non-blocking warning naming the endpoints Studio couldn't find.
        """
        try:
            enabled = self._settings.get_ai_enabled()
            setup_owned = self._settings.get_ai_enabled_source() != "admin"
            endpoints = self.endpoint_names()
        except Exception:
            logger.warning("Could not read the AI settings during setup.")
            return SetupStep(
                id=SetupStepId.AI,
                state=StepState.WARNING,
                code="ai_settings_unreadable",
                summary="Studio couldn't read its AI settings. Click Verify again in a minute.",
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        if not enabled and not setup_owned:
            return _passed("AI features are turned off in Settings.")

        attended = reader_ws is not None
        problems = [problem for name in endpoints if (problem := self._endpoint_problem(name, reader_ws))]
        if not problems:
            if not enabled and attended and self._save_enabled(True):
                return _passed("The AI models are available again, so AI features are back on.")
            return _passed("The AI models are available.")

        code = min((problem_code for problem_code, _ in problems), key=_PROBLEM_ORDER.index)
        instructions = tuple(instruction for _, instruction in problems)
        if setup_owned and attended:
            if enabled:
                self._save_enabled(False)
            summary = (
                "AI features are turned off because Studio couldn't find the AI model. "
                "Fix it below, then click Verify again, or turn AI back on in Settings."
            )
        elif not enabled:
            summary = (
                "AI features are off because Studio couldn't find the AI model. Fix it below, then click Verify again."
            )
        elif attended:
            summary = "AI features are on, but Studio couldn't find the AI model."
        else:
            summary = (
                "Studio couldn't find the AI model. AI features stay as they are until an administrator "
                "clicks Verify again."
            )
        return SetupStep(
            id=SetupStepId.AI,
            state=StepState.WARNING,
            code=code,
            summary=summary,
            instructions=instructions,
            actions=(SetupActionId.VERIFY_AGAIN, SetupActionId.OVERRIDE),
        )

    def keep_enabled(self, *, user_email: str | None) -> None:
        """Turn AI features on as an administrator's choice, so setup leaves them on."""
        self._settings.save_ai_enabled(True, user_email=user_email, source="admin")

    def endpoint_names(self) -> tuple[str, ...]:
        """Return the configured AI and embedding endpoint names, sorted and de-duplicated."""
        names = {self._settings.get_ai_endpoint_name(), self._settings.get_embedding_endpoint_name()}
        return tuple(sorted(name.strip() for name in names if name and name.strip()))

    def _save_enabled(self, enabled: bool) -> bool:
        try:
            self._settings.save_ai_enabled(enabled, source="setup")
        except Exception:
            logger.warning("Could not update the AI setting during setup.")
            return False
        return True

    def _endpoint_problem(self, name: str, reader_ws: WorkspaceClient | None) -> tuple[str, str] | None:
        """Return a problem code and instruction for endpoint *name*, or None when it is listed.

        The app service principal may lack permission to view a custom endpoint, so a
        failed lookup is retried as the setup administrator before reporting a problem.
        """
        endpoint_label = instruction_identifier(name)
        clients = (self._workspace, reader_ws) if reader_ws is not None else (self._workspace,)
        missing = False
        for client in clients:
            try:
                client.serving_endpoints.get(name)
            except NotFound:
                missing = True
            except Exception:
                logger.warning("Could not look up an AI model endpoint during setup.")
            else:
                return None
        if missing:
            return (
                "ai_endpoint_missing",
                f"AI model endpoint {endpoint_label} doesn't exist in this workspace, or Studio can't see it. "
                "Choose another one in Settings, or give the app's service principal Can view on it.",
            )
        return (
            "ai_endpoint_unverified",
            f"Studio couldn't look up AI model endpoint {endpoint_label}. Click Verify again in a minute.",
        )


def _passed(summary: str) -> SetupStep:
    return SetupStep(id=SetupStepId.AI, state=StepState.PASSED, summary=summary)
