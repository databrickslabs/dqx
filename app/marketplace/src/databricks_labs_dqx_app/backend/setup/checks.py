"""Deployment-agnostic capability checks for DQX Studio setup resources."""

import unicodedata

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EffectivePermissionsList

from databricks_labs_dqx_app.backend.pg_executor import PgExecutor
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import validate_identifier

_VOLUME_PRIVILEGES = frozenset({"READ_VOLUME", "WRITE_VOLUME"})
_CATALOG_PRIVILEGES = frozenset({"USE_CATALOG", "CREATE_SCHEMA"})
_SCHEMA_PRIVILEGES = frozenset({"USE_SCHEMA", "CREATE_TABLE"})


class ResourceCheckers:
    """Check and reconcile capabilities of already-resolved setup resources."""

    def __init__(
        self,
        *,
        resources: ActiveResources,
        workspace: WorkspaceClient,
        sql: SqlExecutor,
        pg: PgExecutor,
        compute: ComputeService,
    ) -> None:
        self._resources = resources
        self._workspace = workspace
        self._sql = sql
        self._pg = pg
        self._compute = compute
        self._app_sp: str | None = None
        self._app_sp_resolved = False

    def check_app_identity(self) -> SetupStep:
        """Verify that the app service principal identity can be resolved."""
        if self._app_sp_id():
            return _passed(SetupStepId.IDENTITY, "The app service principal identity is available.")
        return SetupStep(
            id=SetupStepId.IDENTITY,
            state=StepState.ACTION_REQUIRED,
            code="app_identity_unresolved",
            summary="Could not resolve the app service principal identity.",
            instructions=("Verify the Databricks App service principal binding.",),
            actions=(SetupActionId.VERIFY_AGAIN,),
        )

    def check_volume(self) -> SetupStep:
        """Verify that the app SP can read and write the wheels volume."""
        app_sp = self._app_sp_id()
        if not app_sp:
            return _identity_required(SetupStepId.VOLUME)

        response = self._effective_permissions("VOLUME", self._volume_full_name(), app_sp)
        if response is None:
            return _action_required(
                SetupStepId.VOLUME,
                "volume_permission_check_failed",
                "Could not verify app service principal access to the wheels volume.",
            )
        missing = _VOLUME_PRIVILEGES - _privileges(response)
        if missing:
            return SetupStep(
                id=SetupStepId.VOLUME,
                state=StepState.ACTION_REQUIRED,
                code="volume_permissions_missing",
                summary="The app service principal needs access to the wheels volume.",
                instructions=_required_volume_grants(app_sp, self._resources),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return _passed(SetupStepId.VOLUME, "The app service principal can access the wheels volume.")

    def check_unity_catalog(self) -> SetupStep:
        """Verify catalog and main-schema privileges needed by DQX Studio."""
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
        missing_catalog = _CATALOG_PRIVILEGES - _privileges(catalog_response)
        missing_schema = _SCHEMA_PRIVILEGES - _privileges(schema_response)
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

    def ensure_sibling_schemas(self) -> SetupStep:
        """Create the app-owned temporary and Genie schemas if they are absent."""
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
                SetupStepId.SCHEMAS,
                "sibling_schema_creation_failed",
                "Could not create the required application schemas.",
                action=SetupActionId.RECONCILE,
            )
        return _passed(SetupStepId.SCHEMAS, "Required application schemas are available.")

    def check_lakebase(self) -> SetupStep:
        """Verify non-mutating connectivity to the configured Lakebase database."""
        try:
            self._pg.query("SELECT 1")
        except Exception:
            return _action_required(
                SetupStepId.LAKEBASE,
                "lakebase_connectivity_failed",
                "Could not connect to the configured Lakebase database.",
            )
        return _passed(SetupStepId.LAKEBASE, "Lakebase connectivity is available.")

    def ensure_lakebase_schema(self) -> SetupStep:
        """Create the validated Lakebase schema if it is absent before migrations run."""
        try:
            schema = _validated_identifier(self._resources.lakebase.schema)
            self._pg.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS {self._pg.q(schema)}")
        except Exception:
            return _action_required(
                SetupStepId.LAKEBASE,
                "lakebase_schema_creation_failed",
                "Could not create the required Lakebase schema.",
                action=SetupActionId.RECONCILE,
            )
        return _passed(SetupStepId.LAKEBASE, "The required Lakebase schema is available.")

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
        try:
            return self._workspace.grants.get_effective(securable_type, full_name, principal=app_sp)
        except Exception:
            return None

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


def _privileges(response: EffectivePermissionsList) -> frozenset[str]:
    privileges: set[str] = set()
    for assignment in response.privilege_assignments or []:
        for effective_privilege in assignment.privileges or []:
            privilege = effective_privilege.privilege
            value = getattr(privilege, "value", privilege)
            if isinstance(value, str):
                privileges.add(value)
    return frozenset(privileges)


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
    sanitized = "".join(" " if _is_control_character(character) else character for character in value)
    return "`" + sanitized.replace("`", "``") + "`"


def _has_control_characters(value: str) -> bool:
    return any(_is_control_character(character) for character in value)


def _is_control_character(character: str) -> bool:
    return unicodedata.category(character) == "Cc"
