"""Apply and verify audience and administrator access to Studio-managed resources.

Unity Catalog grants on the temporary, Genie and demo schemas, and workspace ACLs on
the configured Genie space and dashboard, are applied on a best-effort basis and then
re-read. Verification alone decides readiness, and anything that cannot be inspected
blocks readiness (fail closed).
"""

import logging
from collections.abc import Sequence

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.services.entitlement_service import FAILING_ROWS_VIEW_NAME
from databricks_labs_dqx_app.backend.services.genie_space_service import SETTING_SPACE_ID
from databricks_labs_dqx_app.backend.services.metadata_dim_service import (
    DIM_MONITORED_TABLES_TABLE_NAME,
    DIM_RULES_TABLE_NAME,
)
from databricks_labs_dqx_app.backend.services.score_view_service import (
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
)
from databricks_labs_dqx_app.backend.setup.acl import AccessStatus, group_levels, missing_principals
from databricks_labs_dqx_app.backend.setup.checks import instruction_identifier
from databricks_labs_dqx_app.backend.setup.configuration import SetupSettings
from databricks_labs_dqx_app.backend.setup.grants import GrantInspector, has_privilege
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import validate_identifier

GENIE_ALLOWLIST: tuple[str, ...] = (
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
    FAILING_ROWS_VIEW_NAME,
    DIM_RULES_TABLE_NAME,
    DIM_MONITORED_TABLES_TABLE_NAME,
)

_GENIE_RUN_LEVELS = frozenset({"CAN_RUN", "CAN_EDIT", "CAN_MANAGE", "IS_OWNER"})
_DASHBOARD_READ_LEVELS = frozenset({"CAN_READ", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE", "IS_OWNER"})
_APP_USE_LEVELS = frozenset({"CAN_USE", "CAN_MANAGE"})
logger = logging.getLogger(__name__)


class AudienceAccess:
    """Reconcile the access Studio users and the custom administrator group need.

    Args:
        resources: Resolved Studio resources, including the audience.
        workspace: App service principal workspace client.
        sql: App service principal SQL executor used to apply grants.
        settings: Application settings holding the provisioned Genie space ID.
        app_name: Databricks App name, used when verifying app sharing.
        dashboard_id: Configured default dashboard ID; empty when none is configured.
    """

    def __init__(
        self,
        *,
        resources: ActiveResources,
        workspace: WorkspaceClient,
        sql: SqlExecutor,
        settings: SetupSettings,
        app_name: str,
        dashboard_id: str,
    ) -> None:
        self._resources = resources
        self._workspace = workspace
        self._sql = sql
        self._settings = settings
        self._app_name = app_name
        self._dashboard_id = dashboard_id.strip()

    def required_uc_grants(self) -> tuple[tuple[str, str, str], ...]:
        """Return the Unity Catalog grants every UC audience principal needs.

        Returns:
            (kind, full_name, privilege) triples: USE SCHEMA and CREATE TABLE on the
            temporary schema, USE SCHEMA on the Genie schema, SELECT on each Genie
            allowlist object, and USE SCHEMA and SELECT on the demo schema.
        """
        catalog = self._resources.volume.catalog
        tmp = f"{catalog}.{self._resources.tmp_schema}"
        genie = f"{catalog}.{self._resources.genie_schema}"
        demo = f"{catalog}.{self._resources.demo_schema}"
        return (
            ("SCHEMA", tmp, "USE_SCHEMA"),
            ("SCHEMA", tmp, "CREATE_TABLE"),
            ("SCHEMA", genie, "USE_SCHEMA"),
            *(("TABLE", f"{genie}.{name}", "SELECT") for name in GENIE_ALLOWLIST),
            ("SCHEMA", demo, "USE_SCHEMA"),
            ("SCHEMA", demo, "SELECT"),
        )

    def reconcile_access(self, reader_sql: SqlExecutor | None = None) -> SetupStep:
        """Apply, then verify, audience grants and shared-resource ACLs.

        Grants and ACL updates are best effort; the re-read state decides the result.
        Uninspectable state takes precedence over missing state.

        Args:
            reader_sql: Administrator SQL executor enabling the SHOW GRANTS fallback
                when effective permissions cannot be read.

        Returns:
            A passed access step, or an action-required step describing what is missing.
        """
        triples = self.required_uc_grants()
        principals = self._resources.audience.uc_principals
        self._apply_uc_grants(triples, principals)
        uc_unknown, uc_missing = self._verify_uc_grants(triples, principals, reader_sql)

        not_applicable: list[str] = []
        shared_unknown: list[str] = []
        genie_missing: list[str] = []
        dashboard_missing: list[str] = []
        genie_space_id = self._genie_space_id()
        if genie_space_id is None:
            shared_unknown.append("Verify that the app service principal can read the Studio settings.")
        elif not genie_space_id:
            not_applicable.append("Genie space sharing")
        else:
            status, missing = self._reconcile_acl("genie", genie_space_id, PermissionLevel.CAN_RUN, _GENIE_RUN_LEVELS)
            if status == "unknown":
                shared_unknown.append(_acl_check_instruction("Genie space", genie_space_id))
            genie_missing.extend(_acl_instruction("CAN RUN", "Genie space", genie_space_id, group) for group in missing)
        if not self._dashboard_id:
            not_applicable.append("dashboard sharing")
        else:
            status, missing = self._reconcile_acl(
                "dashboards", self._dashboard_id, PermissionLevel.CAN_READ, _DASHBOARD_READ_LEVELS
            )
            if status == "unknown":
                shared_unknown.append(_acl_check_instruction("dashboard", self._dashboard_id))
            dashboard_missing.extend(
                _acl_instruction("CAN READ", "dashboard", self._dashboard_id, group) for group in missing
            )

        instructions = (*uc_unknown, *shared_unknown, *uc_missing, *genie_missing, *dashboard_missing)
        if uc_unknown:
            return _blocked("audience_grant_check_failed", "Could not verify Studio user access.", instructions)
        if shared_unknown:
            return _blocked(
                "shared_resource_check_failed", "Could not verify sharing of Studio resources.", instructions
            )
        if uc_missing:
            return _blocked(
                "audience_grants_missing", "Studio users or administrators need Unity Catalog grants.", instructions
            )
        if genie_missing:
            return _blocked("genie_space_sharing_missing", "Studio users cannot run the DQ Genie space.", instructions)
        if dashboard_missing:
            return _blocked("dashboard_sharing_missing", "Studio users cannot view the Studio dashboard.", instructions)
        summary = "Studio users and administrators have the required access."
        if not_applicable:
            summary += f" Not configured, so not applicable: {', '.join(not_applicable)}."
        return SetupStep(id=SetupStepId.ACCESS, state=StepState.PASSED, summary=summary)

    def check_app_sharing(self, reader_ws: WorkspaceClient | None = None) -> SetupStep:
        """Verify the audience and administrators can use the Databricks App; never writes its ACL.

        The ACL is read as the app service principal, then as the setup administrator. When
        no identity can read it, a non-blocking warning carries manual sharing instructions.

        Args:
            reader_ws: Setup administrator's client for reading the app ACL.

        Returns:
            A passed step, a blocking step naming the groups lacking CAN USE, or a warning
            when the ACL cannot be read.
        """
        principals = self._resources.audience.workspace_principals
        entries = self._read_app_acl(self._workspace)
        if entries is None and reader_ws is not None:
            entries = self._read_app_acl(reader_ws)
        if entries is None:
            return SetupStep(
                id=SetupStepId.APP_SHARING,
                state=StepState.WARNING,
                code="app_sharing_unverified",
                summary="Could not verify that Studio users can open the app. Share it manually if needed.",
                instructions=tuple(self._app_sharing_instruction(group) for group in principals),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        absent = missing_principals(group_levels(entries), principals, _APP_USE_LEVELS)
        if absent:
            return SetupStep(
                id=SetupStepId.APP_SHARING,
                state=StepState.ACTION_REQUIRED,
                code="app_sharing_missing",
                summary="Studio users or administrators cannot open the app.",
                instructions=tuple(self._app_sharing_instruction(group) for group in absent),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return SetupStep(
            id=SetupStepId.APP_SHARING,
            state=StepState.PASSED,
            summary="Studio users and administrators can use the app.",
        )

    def _read_app_acl(self, client: WorkspaceClient) -> list[object] | None:
        try:
            return list(client.apps.get_permissions(self._app_name).access_control_list or [])
        except Exception:
            return None

    def _app_sharing_instruction(self, group: str) -> str:
        return (
            f"Share the app {instruction_identifier(self._app_name)} with group "
            f"{instruction_identifier(group)} (CAN USE) in Compute > Apps > Permissions."
        )

    def _apply_uc_grants(self, triples: Sequence[tuple[str, str, str]], principals: Sequence[str]) -> None:
        for kind, full_name, privilege in triples:
            try:
                securable = ".".join(self._sql.q(validate_identifier(part)) for part in full_name.split("."))
            except Exception:
                logger.warning("Could not apply an audience Unity Catalog grant; verifying access instead.")
                continue
            for principal in principals:
                try:
                    self._sql.execute_no_schema(
                        f"GRANT {privilege.replace('_', ' ')} ON {kind} {securable} TO {self._sql.q(principal)}"
                    )
                except Exception:
                    logger.warning("Could not apply an audience Unity Catalog grant; verifying access instead.")

    def _verify_uc_grants(
        self,
        triples: Sequence[tuple[str, str, str]],
        principals: Sequence[str],
        reader_sql: SqlExecutor | None,
    ) -> tuple[list[str], list[str]]:
        inspector = GrantInspector(self._workspace, reader_sql)
        objects: dict[tuple[str, str], list[str]] = {}
        for kind, full_name, privilege in triples:
            objects.setdefault((kind, full_name), []).append(privilege)
        unknown: list[str] = []
        missing: list[str] = []
        for principal in principals:
            quoted_principal = instruction_identifier(principal)
            for (kind, full_name), required in objects.items():
                quoted_name = ".".join(instruction_identifier(part) for part in full_name.split("."))
                privileges = inspector.privileges(kind, full_name, principal, required=frozenset(required))
                if privileges is None:
                    guidance = (
                        "Verify setup as an administrator with ownership or READ METADATA on "
                        f"{kind} {quoted_name}, or as a metastore administrator, to inspect Studio user grants."
                    )
                    if guidance not in unknown:
                        unknown.append(guidance)
                    continue
                missing.extend(
                    f"GRANT {privilege.replace('_', ' ')} ON {kind} {quoted_name} TO {quoted_principal};"
                    for privilege in required
                    if not has_privilege(privileges, privilege)
                )
        return unknown, missing

    def _genie_space_id(self) -> str | None:
        """Return the provisioned Genie space ID, an empty string when none, or None when unreadable."""
        try:
            value = self._settings.get_setting(SETTING_SPACE_ID)
        except Exception:
            return None
        return (value or "").strip()

    def _reconcile_acl(
        self,
        object_type: str,
        object_id: str,
        level: PermissionLevel,
        sufficient: frozenset[str],
    ) -> tuple[AccessStatus, tuple[str, ...]]:
        """Additively grant *level* to workspace principals lacking it, then re-read the ACL."""
        principals = self._resources.audience.workspace_principals
        if replace_control_characters(object_id) != object_id:
            return "unknown", ()
        absent = self._missing_acl_principals(object_type, object_id, principals, sufficient)
        if absent is None:
            return "unknown", ()
        if not absent:
            return "granted", ()
        try:
            self._workspace.permissions.update(
                object_type,
                object_id,
                access_control_list=[
                    AccessControlRequest(group_name=group, permission_level=level) for group in absent
                ],
            )
        except Exception:
            logger.warning("Could not share a Studio resource with the audience; verifying access instead.")
        absent = self._missing_acl_principals(object_type, object_id, principals, sufficient)
        if absent is None:
            return "unknown", ()
        return ("missing", absent) if absent else ("granted", ())

    def _missing_acl_principals(
        self,
        object_type: str,
        object_id: str,
        principals: Sequence[str],
        sufficient: frozenset[str],
    ) -> tuple[str, ...] | None:
        try:
            permissions = self._workspace.permissions.get(object_type, object_id)
        except Exception:
            return None
        return missing_principals(group_levels(permissions.access_control_list or []), principals, sufficient)


def _blocked(code: str, summary: str, instructions: tuple[str, ...]) -> SetupStep:
    return SetupStep(
        id=SetupStepId.ACCESS,
        state=StepState.ACTION_REQUIRED,
        code=code,
        summary=summary,
        instructions=instructions,
        actions=(SetupActionId.VERIFY_AGAIN,),
    )


def _acl_instruction(level: str, label: str, object_id: str, group: str) -> str:
    return f"Grant {level} on {label} {instruction_identifier(object_id)} to group {instruction_identifier(group)}."


def _acl_check_instruction(label: str, object_id: str) -> str:
    return (
        f"Verify that the app service principal can manage {label} {instruction_identifier(object_id)}, "
        "or share it with the Studio audience manually."
    )
