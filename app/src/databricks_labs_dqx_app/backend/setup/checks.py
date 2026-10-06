"""Deployment-agnostic capability checks for DQX Studio setup resources."""

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EffectivePermissionsList

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.grants import GrantInspector, has_privilege, missing_privileges
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import validate_identifier

_VOLUME_PRIVILEGES = frozenset({"READ_VOLUME", "WRITE_VOLUME"})
_CATALOG_PRIVILEGES = frozenset({"USE_CATALOG", "CREATE_SCHEMA"})
_SCHEMA_PRIVILEGES = frozenset({"USE_SCHEMA", "CREATE_TABLE"})
logger = logging.getLogger(__name__)


class ResourceCheckers:
    """Check and reconcile capabilities of already-resolved setup resources."""

    def __init__(
        self,
        *,
        resources: ActiveResources,
        workspace: WorkspaceClient,
        sql: SqlExecutor,
        compute: ComputeService,
        audience_groups: tuple[str, ...] = (),
    ) -> None:
        self._resources = resources
        self._workspace = workspace
        self._sql = sql
        self._compute = compute
        self._audience_groups = audience_groups
        self._inspector = GrantInspector(workspace)
        self._app_sp: str | None = None
        self._app_sp_resolved = False

    def check_volume(self) -> SetupStep:
        """Verify that the app SP can read and write the wheels volume."""
        app_sp = self._app_sp_id()
        if not app_sp:
            return _identity_required(SetupStepId.STORAGE)

        response = self._effective_permissions("VOLUME", self._volume_full_name(), app_sp)
        if response is None:
            return _action_required(
                SetupStepId.STORAGE,
                "volume_permission_check_failed",
                "Could not verify app service principal access to the wheels volume.",
            )
        missing = missing_privileges(response, _VOLUME_PRIVILEGES)
        if missing and not self._is_owner("VOLUME", self._volume_full_name(), app_sp):
            return SetupStep(
                id=SetupStepId.STORAGE,
                state=StepState.ACTION_REQUIRED,
                code="volume_permissions_missing",
                summary="The app service principal needs access to the wheels volume.",
                instructions=_required_volume_grants(app_sp, self._resources),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return _passed(SetupStepId.STORAGE, "The app service principal can access the wheels volume.")

    def check_unity_catalog(self, reader_sql: SqlExecutor | None = None) -> SetupStep:
        """Verify catalog and main-schema privileges needed by DQX Studio.

        Args:
            reader_sql: Administrator SQL executor for grant inspection. Accepted for
                interface compatibility; the app service principal's own effective
                permissions are inspected.
        """
        app_sp = self._app_sp_id()
        if not app_sp:
            return _identity_required(SetupStepId.UNITY_CATALOG)

        catalog_response = self._effective_permissions("CATALOG", self._resources.volume.catalog, app_sp)
        schema_response = self._effective_permissions("SCHEMA", self._main_schema_full_name(), app_sp)
        if catalog_response is None or schema_response is None:
            return _action_required(
                SetupStepId.UNITY_CATALOG,
                "catalog_permission_check_failed",
                "Could not verify the required Unity Catalog permissions.",
            )
        missing_catalog = missing_privileges(catalog_response, _CATALOG_PRIVILEGES)
        missing_schema = missing_privileges(schema_response, _SCHEMA_PRIVILEGES)
        if missing_catalog and self._is_owner("CATALOG", self._resources.volume.catalog, app_sp):
            missing_catalog = frozenset()
        if missing_schema and self._is_owner("SCHEMA", self._main_schema_full_name(), app_sp):
            missing_schema = frozenset()
        if missing_catalog or missing_schema:
            return SetupStep(
                id=SetupStepId.UNITY_CATALOG,
                state=StepState.ACTION_REQUIRED,
                code="catalog_permissions_missing",
                summary="The app service principal needs additional Unity Catalog permissions.",
                instructions=required_catalog_grants(app_sp, self._resources),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return _passed(SetupStepId.UNITY_CATALOG, "Required Unity Catalog permissions are available.")

    def check_runner_access(
        self,
        job_id: int,
        reader_ws: WorkspaceClient | None = None,
        *,
        reader_sql: SqlExecutor | None = None,
        include_outputs: bool = False,
    ) -> SetupStep:
        """Verify wheel access, schema usage, and main-schema data access.

        Missing grants are reported for an administrator to apply; this check
        never grants the runner write or administrative privileges.

        Args:
            job_id: Resolved task-runner job whose run-as identity is checked.
            reader_ws: Setup administrator's OBO client for read-only ownership
                inspection, or the app client during unattended startup.
            reader_sql: Administrator's SQL executor for SHOW GRANTS using the
                supported Apps SQL scope instead of the grants REST API.
            include_outputs: Check main-schema SELECT/MODIFY after migrations.
                Schema grants cover current and future outputs; source access
                remains run-specific. Runner Lakebase access is not checked.
        """
        app_sp = self._app_sp_id()
        try:
            job = self._workspace.jobs.get(job_id)
            run_as = getattr(getattr(job, "settings", None), "run_as", None)
            principal = getattr(run_as, "service_principal_name", None)
        except Exception:
            # Setup must remain action-required when the Jobs API cannot resolve the identity.
            principal = None
        if (
            not app_sp
            or not isinstance(principal, str)
            or not principal.strip()
            or _has_control_characters(principal)
            or principal.casefold() == app_sp.casefold()
        ):
            return _action_required(
                SetupStepId.TASK_RUNNER,
                "task_runner_identity_unresolved",
                "Could not resolve a task-runner service principal distinct from the app identity.",
            )
        volume = self._resources.volume
        catalog = _instruction_identifier(volume.catalog)
        schema = f"{catalog}.{_instruction_identifier(volume.schema)}"
        quoted_volume = f"{schema}.{_instruction_identifier(volume.volume)}"
        quoted_principal = _instruction_identifier(principal)
        requirements = [
            ("CATALOG", volume.catalog, "USE_CATALOG", "USE CATALOG", catalog),
            ("SCHEMA", self._main_schema_full_name(), "USE_SCHEMA", "USE SCHEMA", schema),
            (
                "SCHEMA",
                f"{volume.catalog}.{self._resources.tmp_schema}",
                "USE_SCHEMA",
                "USE SCHEMA",
                f"{catalog}.{_instruction_identifier(self._resources.tmp_schema)}",
            ),
            ("VOLUME", self._volume_full_name(), "READ_VOLUME", "READ VOLUME", quoted_volume),
        ]
        if include_outputs:
            requirements.extend(
                ("SCHEMA", self._main_schema_full_name(), privilege, privilege, schema)
                for privilege in ("SELECT", "MODIFY")
            )
        instructions: list[str] = []
        unknown: list[str] = []
        inspected: dict[tuple[str, str], frozenset[str] | None] = {}
        inspector = GrantInspector(self._workspace, reader_sql)
        for kind, full_name, privilege, grant, quoted_name in requirements:
            key = (kind, full_name)
            if key not in inspected:
                required = frozenset(item[2] for item in requirements if item[:2] == key)
                inspected[key] = inspector.privileges(kind, full_name, principal, required=required)
            privileges = inspected[key]
            if privileges is not None and has_privilege(privileges, privilege):
                continue
            # Container ownership does not imply data access to child tables.
            if privilege not in {"SELECT", "MODIFY"} and (
                self._is_owner(kind, full_name, principal)
                or (reader_ws is not None and self._is_owner(kind, full_name, principal, reader_ws=reader_ws))
            ):
                continue
            if privileges is None:
                unknown.append(
                    f"Verify setup as an administrator with ownership or READ METADATA on {kind} {quoted_name}, "
                    "or as a metastore administrator, to inspect the runner's grants. "
                    "SQL verification also requires CAN_USE on the bound warehouse and USE CATALOG/USE SCHEMA "
                    "on the temporary schema and the object's parent containers."
                )
                continue
            instructions.append(f"GRANT {grant} ON {kind} {quoted_name} TO {quoted_principal};")
        if unknown:
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_permission_check_failed",
                summary="Could not verify task-runner access to required Studio resources.",
                instructions=(*unknown, *instructions),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        if instructions:
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_permissions_missing",
                summary="The task-runner service principal needs access to required Studio resources.",
                instructions=tuple(instructions),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return _passed(SetupStepId.TASK_RUNNER, "The task-runner service principal has the required Studio access.")

    def ensure_storage(self, *, provision: bool) -> SetupStep:
        """Verify the wheels volume, then create and verify the sibling schemas.

        Args:
            provision: Whether Studio owns provisioning of the storage. Accepted for
                interface compatibility; sibling schemas are always created idempotently.

        Returns:
            The first step that did not pass, or a passed storage step.
        """
        for check in (self.check_volume, self.ensure_sibling_schemas):
            step = check()
            if step.state != StepState.PASSED:
                return step
        return _passed(SetupStepId.STORAGE, "Studio storage is available.")

    def ensure_sibling_schemas(self) -> SetupStep:
        """Create sibling schemas and verify the app can create views in them."""
        app_sp = self._app_sp_id()
        if not app_sp:
            return _identity_required(SetupStepId.STORAGE)
        try:
            catalog = _validated_identifier(self._resources.volume.catalog)
            schemas = (
                _validated_identifier(self._resources.tmp_schema),
                _validated_identifier(self._resources.genie_schema),
            )
            for schema in schemas:
                self._sql.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS {self._sql.q(catalog)}.{self._sql.q(schema)}")
        except Exception:
            return _action_required(
                SetupStepId.STORAGE,
                "sibling_schema_creation_failed",
                "Could not create the required application schemas.",
                action=SetupActionId.RECONCILE,
            )
        for schema in schemas:
            full_name = f"{catalog}.{schema}"
            response = self._effective_permissions("SCHEMA", full_name, app_sp)
            if response is None:
                return _action_required(
                    SetupStepId.STORAGE,
                    "sibling_schema_permission_check_failed",
                    "Could not verify the app service principal's sibling-schema permissions.",
                )
            missing = missing_privileges(response, _SCHEMA_PRIVILEGES)
            if missing and not self._is_owner("SCHEMA", full_name, app_sp):
                quoted_schema = f"{_instruction_identifier(catalog)}.{_instruction_identifier(schema)}"
                principal = _instruction_identifier(app_sp)
                return SetupStep(
                    id=SetupStepId.STORAGE,
                    state=StepState.ACTION_REQUIRED,
                    code="sibling_schema_permissions_missing",
                    summary="The app service principal needs permission to create views in a sibling schema.",
                    instructions=(f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA {quoted_schema} TO {principal};",),
                    actions=(SetupActionId.VERIFY_AGAIN,),
                )
        for group in self._audience_groups:
            quoted_schema = f"{self._sql.q(catalog)}.{self._sql.q(schemas[0])}"
            try:
                self._sql.execute_no_schema(
                    f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA {quoted_schema} TO {_instruction_identifier(group)}"
                )
            except Exception:
                logger.warning("Could not grant a configured audience group access to the temporary schema.")
        return _passed(SetupStepId.STORAGE, "Required application schemas are available.")

    def check_warehouse(
        self,
        warehouse_id: str | None = None,
        reader_ws: WorkspaceClient | None = None,
    ) -> SetupStep:
        """Verify that the app SP has CAN_USE on a configured SQL warehouse.

        Args:
            warehouse_id: Candidate warehouse ID, or the bound warehouse when omitted.
            reader_ws: Client permitted to inspect the candidate's access controls,
                or the app service principal client when omitted.
        """
        effective_warehouse_id = (warehouse_id or self._resources.warehouse_id).strip()
        effective_reader_ws = reader_ws or self._workspace
        try:
            status = self._compute.warehouse_access_status(effective_warehouse_id, reader_ws=effective_reader_ws)
        except Exception:
            return _action_required(
                SetupStepId.WAREHOUSE,
                "warehouse_permission_check_failed",
                "Could not verify app service principal access to the SQL warehouse.",
            )
        if status == "granted":
            return _passed(SetupStepId.WAREHOUSE, "The app service principal can use the SQL warehouse.")
        if status == "missing":
            warehouse = _instruction_identifier(effective_warehouse_id)
            return SetupStep(
                id=SetupStepId.WAREHOUSE,
                state=StepState.ACTION_REQUIRED,
                code="warehouse_permissions_missing",
                summary="The app service principal needs CAN_USE on the SQL warehouse.",
                instructions=(f"Grant CAN_USE on SQL warehouse {warehouse} to the app service principal.",),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        if warehouse_id is None:
            try:
                self._sql.query("SELECT 1")
            except Exception:
                pass
            else:
                return _passed(SetupStepId.WAREHOUSE, "The app service principal can use the SQL warehouse.")
        return _action_required(
            SetupStepId.WAREHOUSE,
            "warehouse_permission_unknown",
            "Could not determine app service principal access to the SQL warehouse.",
        )

    def _app_sp_id(self) -> str:
        if self._app_sp_resolved:
            return self._app_sp or ""
        self._app_sp_resolved = True
        try:
            identity = self._workspace.current_user.me()
            candidate = (identity.user_name or identity.id or "").strip()
        except Exception:
            return ""
        if _has_control_characters(candidate):
            return ""
        self._app_sp = candidate
        return candidate

    def _effective_permissions(
        self,
        securable_type: str,
        full_name: str,
        app_sp: str,
    ) -> EffectivePermissionsList | None:
        return self._inspector.effective_permissions(securable_type, full_name, app_sp)

    def _is_owner(
        self,
        securable_type: str,
        full_name: str,
        app_sp: str,
        reader_ws: WorkspaceClient | None = None,
    ) -> bool:
        owner = self._inspector.owner(securable_type, full_name, reader_ws)
        return owner is not None and owner.casefold() == app_sp.casefold()

    def _volume_full_name(self) -> str:
        volume = self._resources.volume
        return ".".join((volume.catalog, volume.schema, volume.volume))

    def _main_schema_full_name(self) -> str:
        volume = self._resources.volume
        return f"{volume.catalog}.{volume.schema}"


def required_catalog_grants(app_sp: str, resources: ActiveResources) -> tuple[str, ...]:
    """Return safe, administrator-run grants for the required catalog capabilities."""
    catalog = _instruction_identifier(resources.volume.catalog)
    schema = _instruction_identifier(resources.volume.schema)
    principal = _instruction_identifier(app_sp)
    return (
        f"GRANT USE CATALOG, CREATE SCHEMA ON CATALOG {catalog} TO {principal};",
        f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA {catalog}.{schema} TO {principal};",
    )


def _required_volume_grants(app_sp: str, resources: ActiveResources) -> tuple[str, ...]:
    volume = resources.volume
    full_name = ".".join(
        (
            _instruction_identifier(volume.catalog),
            _instruction_identifier(volume.schema),
            _instruction_identifier(volume.volume),
        )
    )
    principal = _instruction_identifier(app_sp)
    return (f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {full_name} TO {principal};",)


def _passed(step_id: SetupStepId, summary: str) -> SetupStep:
    return SetupStep(id=step_id, state=StepState.PASSED, summary=summary)


def _action_required(
    step_id: SetupStepId,
    code: str,
    summary: str,
    *,
    action: SetupActionId = SetupActionId.VERIFY_AGAIN,
) -> SetupStep:
    return SetupStep(
        id=step_id,
        state=StepState.ACTION_REQUIRED,
        code=code,
        summary=summary,
        actions=(action,),
    )


def _identity_required(step_id: SetupStepId) -> SetupStep:
    return _action_required(
        step_id,
        "app_identity_unresolved",
        "Could not resolve the app service principal identity.",
    )


def _validated_identifier(value: str) -> str:
    return validate_identifier(value)


def _instruction_identifier(value: str) -> str:
    sanitized = replace_control_characters(value)
    return "`" + sanitized.replace("`", "``") + "`"


def _has_control_characters(value: str) -> bool:
    return replace_control_characters(value) != value
