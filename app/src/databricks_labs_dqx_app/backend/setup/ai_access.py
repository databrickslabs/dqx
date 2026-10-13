"""Check that Studio users can use the AI models, and turn AI off when they cannot.

AI calls go to Unity Gateway model services (for example ``system.ai.claude-sonnet-4-5``),
which are Unity Catalog securables. AI features run as the signed-in user, so every audience
group needs ``EXECUTE`` on each configured model service plus ``USE_CATALOG`` and
``USE_SCHEMA`` on its catalog and schema. The app service principal needs the same on the
embedding model, because it embeds rules when they are published. Databricks grants all of
this on ``system.ai`` to account users by default, but a metastore admin can revoke it.

Studio never grants these itself (only a metastore admin can grant on ``system.ai``); it reads
the effective grants and lists the exact GRANT statements to run. The check never blocks
readiness: when access is missing or cannot be confirmed it reports a warning and, unless an
administrator chose the AI setting themselves, turns AI features off. It turns them back on
once access is confirmed.
"""

import logging
from collections.abc import Sequence
from typing import Protocol

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.app_settings_service import AiEnabledSource
from databricks_labs_dqx_app.backend.services.model_services import ModelServices
from databricks_labs_dqx_app.backend.setup.checks import instruction_identifier
from databricks_labs_dqx_app.backend.setup.grants import effective_privilege_names, has_privilege
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources

# Most severe first: a missing model needs a settings change, missing access needs a
# grant, and unverified access may already be fine.
_PROBLEM_ORDER = ("ai_endpoint_missing", "ai_access_missing", "ai_access_unverified")
_GRANT_GUIDANCE = (
    "Some Studio users can't use the AI models. A metastore admin (or anyone with MANAGE on the "
    "model services) needs to run these statements in a SQL editor, then click Verify again:"
)
logger = logging.getLogger(__name__)


class AiSettings(Protocol):
    """AI kill-switch and model settings used by the AI access check."""

    def get_ai_enabled(self) -> bool: ...

    def get_ai_enabled_source(self) -> AiEnabledSource: ...

    def save_ai_enabled(
        self, enabled: bool, *, user_email: str | None = None, source: AiEnabledSource = "admin"
    ) -> bool: ...

    def get_ai_endpoint_name(self) -> str: ...

    def get_embedding_endpoint_name(self) -> str: ...


class AiAccess:
    """Verify Studio users can use the configured AI model services.

    Args:
        resources: Resolved Studio resources, including the audience.
        workspace: App service principal workspace client.
        settings: AI settings, including the kill-switch.
        app_sp_id: Application ID of the app service principal; empty skips its check.
    """

    def __init__(
        self,
        *,
        resources: ActiveResources,
        workspace: WorkspaceClient,
        settings: AiSettings,
        app_sp_id: str = "",
    ) -> None:
        self._resources = resources
        self._workspace = workspace
        self._settings = settings
        self._app_sp = app_sp_id.strip()

    def check_ai_access(self, reader_ws: WorkspaceClient | None = None) -> SetupStep:
        """Verify every audience group can use the AI models, and report the grants they lack.

        Args:
            reader_ws: Setup administrator's client, used to read grants when the app
                service principal cannot.

        Returns:
            A passed step, or a non-blocking warning listing the GRANT statements to run.
        """
        try:
            enabled = self._settings.get_ai_enabled()
            setup_owned = self._settings.get_ai_enabled_source() != "admin"
            chat_model = self._settings.get_ai_endpoint_name().strip()
            embedding_model = self._settings.get_embedding_endpoint_name().strip()
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

        problems: list[tuple[str, tuple[str, ...]]] = []
        statements: list[str] = []
        for name in self.model_names():
            problem, missing = self._model_problem(name, self._principals_for(name, embedding_model), reader_ws)
            if problem:
                problems.append(problem)
            statements.extend(statement for statement in missing if statement not in statements)
        if statements:
            problems.append(("ai_access_missing", (f"{_GRANT_GUIDANCE}\n" + "\n".join(statements),)))
        if not chat_model:
            problems.append(("ai_endpoint_missing", ("No AI model is selected. Choose one in Settings > AI.",)))
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
                "AI features are turned off because Studio users may not be able to use the AI models. "
                "Fix the access below, then click Verify again, or turn AI back on in Settings."
            )
        else:
            summary = "AI features are on, but Studio users may not be able to use the AI models."
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

    def model_names(self) -> tuple[str, ...]:
        """Return the configured AI and embedding model service names, sorted and de-duplicated."""
        names = {self._settings.get_ai_endpoint_name(), self._settings.get_embedding_endpoint_name()}
        return tuple(sorted(name.strip() for name in names if name and name.strip()))

    def _principals_for(self, name: str, embedding_model: str) -> tuple[str, ...]:
        principals = list(self._resources.audience.uc_principals)
        if self._app_sp and name == embedding_model and self._app_sp not in principals:
            principals.append(self._app_sp)
        return tuple(principals)

    def _save_enabled(self, enabled: bool) -> bool:
        try:
            self._settings.save_ai_enabled(enabled, source="setup")
        except Exception:
            logger.warning("Could not update the AI setting during setup.")
            return False
        return True

    def _model_problem(
        self, name: str, principals: Sequence[str], reader_ws: WorkspaceClient | None
    ) -> tuple[tuple[str, tuple[str, ...]] | None, list[str]]:
        """Inspect model service *name* for *principals*.

        Returns:
            A problem code with instructions (None when nothing but grants is wrong), and the
            GRANT statements the principals still need.
        """
        label = instruction_identifier(name)
        parts = name.split(".")
        if len(parts) != 3 or not all(parts):
            return (
                "ai_endpoint_missing",
                (f"AI model {label} isn't a Unity Gateway model service. Choose a model in Settings > AI.",),
            ), []
        exists = self._exists(name, reader_ws)
        if exists is None:
            return (
                "ai_access_unverified",
                (self._unconfirmed_model_instruction(name, label),),
            ), []
        if not exists:
            return (
                "ai_endpoint_missing",
                (f"AI model {label} doesn't exist in this workspace. Choose another one in Settings > AI.",),
            ), []

        catalog, schema = parts[0], ".".join(parts[:2])
        requirements = (
            ("CATALOG", catalog, "USE_CATALOG", _quote_name(catalog)),
            ("SCHEMA", schema, "USE_SCHEMA", _quote_name(schema)),
            ("MODEL_SERVICE", name, "EXECUTE", _quote_name(name)),
        )
        statements: list[str] = []
        unreadable: list[str] = []
        for principal in principals:
            for kind, full_name, privilege, quoted in requirements:
                privileges = self._privileges(kind, full_name, principal, reader_ws)
                if privileges is None:
                    if principal not in unreadable:
                        unreadable.append(principal)
                    continue
                if not has_privilege(privileges, privilege):
                    statements.append(
                        f"GRANT {privilege} ON {kind.replace('_', ' ')} {quoted} TO {instruction_identifier(principal)};"
                    )
        if not unreadable:
            return None, statements
        who = ", ".join(instruction_identifier(principal) for principal in unreadable)
        return (
            "ai_access_unverified",
            (
                f"Studio couldn't read who can use AI model {label}. Check that {who} have EXECUTE on the "
                f"model service and USE_CATALOG / USE_SCHEMA on {_quote_name(schema)} (Catalog Explorer > "
                "the model service > Permissions).",
            ),
        ), statements

    def _exists(self, name: str, reader_ws: WorkspaceClient | None) -> bool | None:
        """Return whether model service *name* exists, or None when that can't be confirmed.

        The app service principal only sees model services it can access, so its miss is
        not proof the model is missing; only the setup administrator's client can confirm
        that. At startup there is no administrator client, so a miss stays unconfirmed.
        """
        try:
            if ModelServices(self._workspace).exists(name):
                return True
        except Exception:
            pass
        if reader_ws is None:
            return None
        try:
            return ModelServices(reader_ws).exists(name)
        except Exception:
            return None

    def _unconfirmed_model_instruction(self, name: str, label: str) -> str:
        """Explain an unconfirmed model lookup and how to make it visible to Studio."""
        if not self._app_sp:
            return f"Studio couldn't look up AI model {label}. Click Verify again in a minute."
        return (
            f"Studio's service principal can't see AI model {label}, so Studio can't confirm it exists. "
            "Check the model in Settings > AI. If it's correct, a metastore admin can let Studio see it by "
            f"running: GRANT READ_METADATA ON MODEL SERVICE {_quote_name(name)} TO "
            f"{instruction_identifier(self._app_sp)}; then click Verify again."
        )

    def _privileges(
        self, kind: str, full_name: str, principal: str, reader_ws: WorkspaceClient | None
    ) -> frozenset[str] | None:
        """Return *principal*'s effective privileges on a securable, or None when unreadable."""
        for client in (self._workspace, reader_ws):
            if client is None:
                continue
            try:
                return effective_privilege_names(client.grants.get_effective(kind, full_name, principal=principal))
            except Exception:
                continue
        return None


def _quote_name(full_name: str) -> str:
    return ".".join(instruction_identifier(part) for part in full_name.split("."))


def _passed(summary: str) -> SetupStep:
    return SetupStep(id=SetupStepId.AI, state=StepState.PASSED, summary=summary)
