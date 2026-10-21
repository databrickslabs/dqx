"""Check that Studio users can use the AI models, and turn AI off when they cannot.

AI calls go to Unity Gateway model services (for example ``system.ai.claude-sonnet-4-5``),
which are Unity Catalog securables. AI features run as the signed-in user, so every audience
group needs ``EXECUTE`` on each configured model service plus ``USE_CATALOG`` and
``USE_SCHEMA`` on its catalog and schema. The app service principal needs the same on the
embedding model, because it embeds rules when they are published. Databricks grants all of
this on ``system.ai`` to account users by default, but a metastore admin can revoke it.

Studio never grants these itself (only a metastore admin can grant on ``system.ai``); it reads
the effective grants and lists the exact GRANT statements to run. The check never blocks
readiness: it reports a warning when access is missing or cannot be confirmed. Only a check an
administrator runs changes the AI setting, so an unattended restart never flips it. Unless an
administrator chose the AI setting themselves, an attended check turns AI features off when a
model is missing or access is missing, and back on once access is confirmed. Access that only
couldn't be confirmed never turns AI off.
"""

import logging
from collections.abc import Sequence
from typing import Protocol

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.app_settings_service import AiEnabledSource
from databricks_labs_dqx_app.backend.services.model_services import ModelServices
from databricks_labs_dqx_app.backend.setup.checks import (
    grant_statement,
    instruction_identifier,
    unsafe_name_instruction,
)
from databricks_labs_dqx_app.backend.setup.grants import GrantInspector, effective_privilege_names, has_privilege
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources

# Most severe first: a missing model needs a settings change, missing access needs a
# grant, and unverified access may already be fine.
_PROBLEM_ORDER = ("ai_endpoint_missing", "ai_access_missing", "ai_access_unverified")
_GRANT_GUIDANCE = (
    "Some Studio users can't use the AI models. A metastore admin (or anyone with MANAGE on the "
    "model services) needs to run the commands below, then click Verify again."
)
logger = logging.getLogger(__name__)
_GrantCache = dict[tuple[str, str, str], frozenset[str] | None]


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

    def check_ai_access(self, reader_ws: WorkspaceClient | None = None, *, inspect_only: bool = False) -> SetupStep:
        """Verify every audience group can use the AI models, and report the grants they lack.

        Args:
            reader_ws: Setup administrator's client, used to read grants when the app
                service principal cannot.
            inspect_only: Only report, like an unattended check: never turn AI features on or
                off. Used to re-derive the GRANT statements just before running them.

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

        # Effective privileges per (kind, securable, principal), shared by every model in this run:
        # both default models live in system.ai, so catalog and schema grants are read once.
        grant_cache: _GrantCache = {}
        problems: list[tuple[str, tuple[str, ...]]] = []
        statements: list[str] = []
        for name in self.model_names():
            problem, missing = self._model_problem(
                name, self._principals_for(name, embedding_model), reader_ws, grant_cache
            )
            if problem:
                problems.append(problem)
            statements.extend(statement for statement in missing if statement not in statements)
        if statements:
            problems.append(("ai_access_missing", (_GRANT_GUIDANCE,)))
        if not chat_model:
            problems.append(("ai_endpoint_missing", ("No AI model is selected. Choose one in Settings > AI.",)))
        # Only an administrator's own check may change the AI setting.
        attended = reader_ws is not None and not inspect_only
        if not problems:
            if not enabled and attended and self._save_enabled(True):
                return _passed("Studio users can use the AI models again, so AI features are back on.")
            if not enabled:
                return SetupStep(
                    id=SetupStepId.AI,
                    state=StepState.WARNING,
                    code="ai_reenable_pending",
                    summary="Studio users can use the AI models again. Click Verify again to turn AI features back on.",
                    actions=(SetupActionId.VERIFY_AGAIN,),
                )
            return _passed("Studio users can use the AI models.")

        code = min((problem_code for problem_code, _ in problems), key=_PROBLEM_ORDER.index)
        instructions = tuple(instruction for _, lines in problems for instruction in lines)
        # Unconfirmed access may already be fine, so it never turns AI off.
        confirmed = code != "ai_access_unverified"
        if setup_owned and attended and confirmed and enabled and not self._save_enabled(False):
            summary = (
                "Studio users may not be able to use the AI models, and Studio couldn't turn AI features off, "
                "so they're still on. Fix the access below, then click Verify again, or turn AI off in Settings."
            )
        elif setup_owned and attended and confirmed:
            summary = (
                "AI features are turned off because Studio users may not be able to use the AI models. "
                "Fix the access below, then click Verify again, or turn AI back on in Settings."
            )
        elif not enabled:
            summary = (
                "AI features are off because Studio users may not be able to use the AI models. "
                "Fix the access below, then click Verify again."
            )
        elif attended or not setup_owned:
            summary = "AI features are on, but Studio users may not be able to use the AI models."
        else:
            summary = (
                "Studio users may not be able to use the AI models. AI features stay as they are until an "
                "administrator clicks Verify again."
            )
        return SetupStep(
            id=SetupStepId.AI,
            state=StepState.WARNING,
            code=code,
            summary=summary,
            instructions=instructions,
            grant_statements=tuple(statements),
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
        self, name: str, principals: Sequence[str], reader_ws: WorkspaceClient | None, grant_cache: _GrantCache
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
        sp_sees = self._sp_sees(name)
        exists = True if sp_sees else self._administrator_sees(name, reader_ws)
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
            ("CATALOG", catalog, "USE_CATALOG", tuple(parts[:1])),
            ("SCHEMA", schema, "USE_SCHEMA", tuple(parts[:2])),
            ("MODEL_SERVICE", name, "EXECUTE", tuple(parts)),
        )
        statements: list[str] = []
        unreadable: list[str] = []
        unsafe = False
        for principal in principals:
            for kind, full_name, privilege, securable in requirements:
                privileges = self._privileges(kind, full_name, principal, reader_ws, grant_cache)
                if privileges is None:
                    if principal not in unreadable:
                        unreadable.append(principal)
                    continue
                if not has_privilege(privileges, privilege):
                    statement = grant_statement(privilege, kind.replace("_", " "), securable, principal)
                    if statement is None:
                        unsafe = True
                    else:
                        statements.append(statement)
        if unsafe:
            # Left out rather than quoted unsafely, so access can't be confirmed until it's given by hand.
            return ("ai_access_unverified", (unsafe_name_instruction("AI model", label),)), statements
        # Only owners and MANAGE holders can read grants on system.ai, so on most installs
        # they are unreadable. The app SP only sees model services it can access, and it gets
        # that access the same way users do (account users), so its visibility stands in.
        if not unreadable or sp_sees:
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

    def _sp_sees(self, name: str) -> bool | None:
        """Return whether the app service principal can see model service *name*, or None on error.

        The app service principal only sees model services it can access, so a True here
        both proves the model exists and that the SP has access to it.
        """
        try:
            return ModelServices(self._workspace).exists(name)
        except Exception:
            return None

    @staticmethod
    def _administrator_sees(name: str, reader_ws: WorkspaceClient | None) -> bool | None:
        """Return whether the setup administrator can see model service *name*, or None when unknown.

        The app SP's miss is not proof the model is missing; only the administrator's client
        can confirm that. At startup there is no administrator client, so a miss stays unknown.
        """
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
        self, kind: str, full_name: str, principal: str, reader_ws: WorkspaceClient | None, grant_cache: _GrantCache
    ) -> frozenset[str] | None:
        """Return *principal*'s effective privileges on a securable, or None when unreadable.

        Reads as the app service principal, then the setup administrator; cached for the run.
        """
        key = (kind, full_name, principal)
        if key not in grant_cache:
            privileges = None
            for client in (self._workspace, reader_ws):
                if client is None:
                    continue
                response = GrantInspector(client).effective_permissions(kind, full_name, principal)
                if response is not None:
                    privileges = effective_privilege_names(response)
                    break
            grant_cache[key] = privileges
        return grant_cache[key]


def _quote_name(full_name: str) -> str:
    return ".".join(instruction_identifier(part) for part in full_name.split("."))


def _passed(summary: str) -> SetupStep:
    return SetupStep(id=SetupStepId.AI, state=StepState.PASSED, summary=summary)
