"""Check that Studio users can query the AI model endpoints, and turn AI off when they cannot.

AI calls run as the signed-in user. Custom serving endpoints have their own permissions,
so every audience group needs Can query on them: setup grants it on a best-effort basis,
then re-reads the endpoint permissions. Databricks-hosted foundation-model endpoints have
no per-endpoint permissions (they have no endpoint ID); they pass when listed, and when an
administrator verifies setup, a one-token test call as that administrator confirms the
model answers. The check never blocks readiness: when access is missing or cannot
be confirmed it reports a warning and, unless an administrator chose the AI setting
themselves, turns AI features off. It turns them back on once access is confirmed.
"""

import logging
from collections.abc import Sequence
from typing import Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole,
    ServingEndpointAccessControlRequest,
    ServingEndpointPermissionLevel,
)

from databricks_labs_dqx_app.backend.services.app_settings_service import AiEnabledSource
from databricks_labs_dqx_app.backend.setup.acl import group_levels, missing_principals
from databricks_labs_dqx_app.backend.setup.checks import instruction_identifier
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources

_QUERY_LEVELS = frozenset({"CAN_QUERY", "CAN_MANAGE"})
# Most severe first: a missing endpoint needs a settings change, missing access needs a
# grant, and unverified access may already be fine.
_PROBLEM_ORDER = ("ai_endpoint_missing", "ai_endpoint_unreachable", "ai_access_missing", "ai_access_unverified")
_EMBEDDINGS_TASK = "llm/v1/embeddings"
logger = logging.getLogger(__name__)


class AiSettings(Protocol):
    """AI kill-switch and endpoint settings used by the AI access check."""

    def get_ai_enabled(self) -> bool: ...

    def get_ai_enabled_source(self) -> AiEnabledSource: ...

    def save_ai_enabled(
        self, enabled: bool, *, user_email: str | None = None, source: AiEnabledSource = "admin"
    ) -> bool: ...

    def get_ai_endpoint_name(self) -> str: ...

    def get_embedding_endpoint_name(self) -> str: ...


class AiAccess:
    """Verify Studio users can use the configured AI model endpoints.

    Args:
        resources: Resolved Studio resources, including the audience.
        workspace: App service principal workspace client.
        settings: AI settings, including the kill-switch.
    """

    def __init__(self, *, resources: ActiveResources, workspace: WorkspaceClient, settings: AiSettings) -> None:
        self._resources = resources
        self._workspace = workspace
        self._settings = settings

    def check_ai_access(self, reader_ws: WorkspaceClient | None = None) -> SetupStep:
        """Grant, then verify, Can query on the AI endpoints for every audience group.

        Args:
            reader_ws: Setup administrator's client, used to read endpoint permissions
                when the app service principal cannot.

        Returns:
            A passed step, or a non-blocking warning describing what Studio users lack.
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

        problems = [problem for name in endpoints if (problem := self._endpoint_problem(name, reader_ws))]
        if not problems:
            if not enabled and self._save_enabled(True):
                return _passed("Studio users can use the AI models again, so AI features are back on.")
            return _passed("Studio users can use the AI models.")

        code = min((problem_code for problem_code, _ in problems), key=_PROBLEM_ORDER.index)
        instructions = tuple(instruction for _, lines in problems for instruction in lines)
        if setup_owned:
            if enabled:
                self._save_enabled(False)
            summary = (
                "AI features are turned off because Studio users may not be able to use the AI model. "
                "Fix the access below, then click Verify again, or turn AI back on in Settings."
            )
        else:
            summary = "AI features are on, but Studio users may not be able to use the AI model."
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

    def _endpoint_problem(self, name: str, reader_ws: WorkspaceClient | None) -> tuple[str, tuple[str, ...]] | None:
        """Return a problem code and instructions for endpoint *name*, or None when users can query it."""
        endpoint_label = instruction_identifier(name)
        try:
            endpoint = self._workspace.serving_endpoints.get(name)
        except NotFound:
            return "ai_endpoint_missing", (
                f"AI model endpoint {endpoint_label} doesn't exist in this workspace. Choose another one in Settings.",
            )
        except Exception:
            return "ai_access_unverified", (
                f"Studio couldn't look up AI model endpoint {endpoint_label}. Click Verify again in a minute.",
            )
        endpoint_id = endpoint.id
        if not endpoint_id:
            # Databricks-hosted model: no per-endpoint permissions to read or grant.
            if reader_ws is None or self._answers_test_call(reader_ws, name, endpoint.task):
                return None
            return "ai_endpoint_unreachable", (
                f"AI model endpoint {endpoint_label} didn't answer a test call. Check that the model is "
                "available in this workspace and not blocked by its AI Gateway rate limits, then click Verify again.",
            )
        principals = self._resources.audience.workspace_principals
        absent = self._absent_principals(endpoint_id, principals, reader_ws)
        if absent:
            try:
                self._workspace.serving_endpoints.update_permissions(
                    serving_endpoint_id=endpoint_id,
                    access_control_list=[
                        ServingEndpointAccessControlRequest(
                            group_name=group, permission_level=ServingEndpointPermissionLevel.CAN_QUERY
                        )
                        for group in absent
                    ],
                )
            except Exception:
                logger.warning("Could not give Studio users access to an AI model endpoint; verifying instead.")
            absent = self._absent_principals(endpoint_id, principals, reader_ws)
        if absent == ():
            return None
        grants = tuple(
            f"Give group {instruction_identifier(group)} Can query on AI model endpoint {endpoint_label} "
            "(Serving > the endpoint > Permissions)."
            for group in (absent or principals)
        )
        if absent is None:
            return "ai_access_unverified", (
                f"Studio couldn't read who can use AI model endpoint {endpoint_label}.",
                *grants,
            )
        return "ai_access_missing", grants

    @staticmethod
    def _answers_test_call(reader_ws: WorkspaceClient, name: str, task: str | None) -> bool:
        """Make the smallest possible call to *name* as the administrator; True when it answers."""
        try:
            if task == _EMBEDDINGS_TASK:
                reader_ws.serving_endpoints.query(name=name, input=["ok"])
            else:
                reader_ws.serving_endpoints.query(
                    name=name, messages=[ChatMessage(role=ChatMessageRole.USER, content="ok")], max_tokens=1
                )
        except Exception:
            logger.warning("An AI model endpoint did not answer the setup test call.")
            return False
        return True

    def _absent_principals(
        self,
        endpoint_id: str,
        principals: Sequence[str],
        reader_ws: WorkspaceClient | None,
    ) -> tuple[str, ...] | None:
        """Return the principals lacking Can query, or None when the permissions can't be read."""
        entries = _read_acl(self._workspace, endpoint_id)
        if entries is None and reader_ws is not None:
            entries = _read_acl(reader_ws, endpoint_id)
        if entries is None:
            return None
        return missing_principals(group_levels(entries), principals, _QUERY_LEVELS)


def _read_acl(client: WorkspaceClient, endpoint_id: str) -> list[object] | None:
    try:
        return list(client.serving_endpoints.get_permissions(endpoint_id).access_control_list or [])
    except Exception:
        return None


def _passed(summary: str) -> SetupStep:
    return SetupStep(id=SetupStepId.AI, state=StepState.PASSED, summary=summary)
