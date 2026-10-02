"""Deployment-agnostic capability checks for DQX Studio setup resources."""

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EffectivePermissionsList

from databricks_labs_dqx_app.backend.pg_executor import PgExecutor
from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, quote_fqn, validate_identifier

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
        pg: PgExecutor,
        compute: ComputeService,
        audience_groups: tuple[str, ...] = (),
        runner_postgres_role: str = "",
    ) -> None:
        self._resources = resources
        self._workspace = workspace
        self._sql = sql
        self._pg = pg
        self._compute = compute
        self._audience_groups = audience_groups
        self._runner_postgres_role = runner_postgres_role
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
        missing = _missing_privileges(response, _VOLUME_PRIVILEGES)
        if missing and not self._is_owner("VOLUME", self._volume_full_name(), app_sp):
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
        missing_catalog = _missing_privileges(catalog_response, _CATALOG_PRIVILEGES)
        missing_schema = _missing_privileges(schema_response, _SCHEMA_PRIVILEGES)
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
        """Verify wheel and schema access, then fixed outputs and Lakebase access.

        Missing grants are reported for an administrator to apply; this check
        never grants the runner write or administrative privileges.

        Args:
            job_id: Resolved task-runner job whose run-as identity is checked.
            reader_ws: Setup administrator's OBO client for read-only ownership
                inspection, or the app client during unattended startup.
            reader_sql: Administrator's SQL executor for SHOW GRANTS using the
                supported Apps SQL scope instead of the grants REST API.
            include_outputs: Check output-table SELECT/MODIFY and staged-config
                Lakebase access after migrations. Source access remains run-specific.
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
        if self._runner_postgres_role and self._runner_postgres_role.casefold() != principal.casefold():
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_lakebase_identity_mismatch",
                summary="The configured Lakebase runner role does not match the job's run-as identity.",
                instructions=(
                    "Remove DQX_TASK_RUNNER_POSTGRES_ROLE or set it to the job's run-as service principal client ID.",
                ),
                actions=(SetupActionId.VERIFY_AGAIN,),
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
                ("TABLE", f"{self._main_schema_full_name()}.{table}", privilege, privilege, f"{schema}.`{table}`")
                for table in ("dq_validation_runs", "dq_profiling_results", "dq_metrics", "dq_quarantine_records")
                for privilege in ("SELECT", "MODIFY")
            )
        instructions: list[str] = []
        unknown: list[str] = []
        inspected: dict[tuple[str, str], frozenset[str] | None] = {}
        for kind, full_name, privilege, grant, quoted_name in requirements:
            key = (kind, full_name)
            if key not in inspected:
                inspected[key] = self._runner_privileges(kind, full_name, principal, reader_sql)
            privileges = inspected[key]
            if privileges is not None and (privilege in privileges or "ALL_PRIVILEGES" in privileges):
                continue
            if self._is_owner(kind, full_name, principal) or (
                reader_ws is not None and self._is_owner(kind, full_name, principal, reader_ws=reader_ws)
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
        if include_outputs:
            database_step = self._check_runner_lakebase(principal)
            if database_step.state != StepState.PASSED:
                return database_step
        return _passed(SetupStepId.TASK_RUNNER, "The task-runner service principal has the required Studio access.")

    def _check_runner_lakebase(self, principal: str) -> SetupStep:
        role = escape_sql_string(principal)
        database = self._resources.lakebase.database
        schema = self._resources.lakebase.schema
        table = f'{self._pg.q(schema)}."dq_run_configs"'
        query = (
            "SELECT rolcanlogin AS can_login, "
            f"has_database_privilege(oid, '{escape_sql_string(database)}', 'CONNECT') AS can_connect, "
            f"has_schema_privilege(oid, '{escape_sql_string(schema)}', 'USAGE') AS can_use, "
            f"has_table_privilege(oid, '{escape_sql_string(table)}', 'SELECT') AS can_select, "
            f"has_table_privilege(oid, '{escape_sql_string(table)}', 'DELETE') AS can_delete "
            f"FROM pg_catalog.pg_roles WHERE rolname = '{role}'"
        )
        try:
            rows = self._pg.query_dicts(query)
        except Exception:
            return _action_required(
                SetupStepId.TASK_RUNNER,
                "task_runner_lakebase_permission_check_failed",
                "Could not inspect the runner's Lakebase role and effective permissions.",
            )
        if not rows or not _pg_boolean(rows[0].get("can_login")):
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_lakebase_role_missing",
                summary="Create a Lakebase OAuth login role for the task-runner service principal.",
                instructions=(
                    "In the bound Lakebase project's branch, use Roles & Databases > Add role > OAuth "
                    "and select the job's run-as service principal; do not grant superuser membership.",
                    f"Alternatively, as a Lakebase role administrator: "
                    f"CREATE EXTENSION IF NOT EXISTS databricks_auth; "
                    f"SELECT databricks_create_role('{role}', 'SERVICE_PRINCIPAL');",
                ),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        quoted_role = self._pg.q(principal)
        grants = {
            "can_connect": f"GRANT CONNECT ON DATABASE {self._pg.q(database)} TO {quoted_role};",
            "can_use": f"GRANT USAGE ON SCHEMA {self._pg.q(schema)} TO {quoted_role};",
            "can_select": f"GRANT SELECT ON TABLE {table} TO {quoted_role};",
            "can_delete": f"GRANT DELETE ON TABLE {table} TO {quoted_role};",
        }
        missing = tuple(statement for key, statement in grants.items() if not _pg_boolean(rows[0].get(key)))
        if missing:
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_lakebase_permissions_missing",
                summary="The task-runner service principal needs narrowly scoped Lakebase access.",
                instructions=(
                    "Run these statements in the bound Lakebase database as an authorized administrator:",
                    *missing,
                ),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return _passed(SetupStepId.TASK_RUNNER, "The runner can read and delete staged Lakebase configs.")

    def _runner_privileges(
        self, kind: str, full_name: str, principal: str, reader_sql: SqlExecutor | None
    ) -> frozenset[str] | None:
        if reader_sql is None:
            response = self._effective_permissions(kind, full_name, principal)
            return _privileges(response) if response is not None else None
        try:
            for part in full_name.split("."):
                validate_identifier(part)
            validate_identifier(principal)
            rows = reader_sql.query_dicts(
                f"SHOW GRANTS {_instruction_identifier(principal)} ON {kind} {quote_fqn(full_name)}"
            )
            return frozenset(
                action.upper().replace(" ", "_")
                for row in rows
                if (row.get("principal") or "").casefold() == principal.casefold()
                if (action := row.get("actionType"))
            )
        except Exception:
            return None

    def ensure_sibling_schemas(self) -> SetupStep:
        """Create sibling schemas and verify the app can create views in them."""
        app_sp = self._app_sp_id()
        if not app_sp:
            return _identity_required(SetupStepId.SCHEMAS)
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
        for schema in schemas:
            full_name = f"{catalog}.{schema}"
            response = self._effective_permissions("SCHEMA", full_name, app_sp)
            if response is None:
                return _action_required(
                    SetupStepId.SCHEMAS,
                    "sibling_schema_permission_check_failed",
                    "Could not verify the app service principal's sibling-schema permissions.",
                )
            missing = _missing_privileges(response, _SCHEMA_PRIVILEGES)
            if missing and not self._is_owner("SCHEMA", full_name, app_sp):
                quoted_schema = f"{_instruction_identifier(catalog)}.{_instruction_identifier(schema)}"
                principal = _instruction_identifier(app_sp)
                return SetupStep(
                    id=SetupStepId.SCHEMAS,
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
        try:
            return self._workspace.grants.get_effective(securable_type, full_name, principal=app_sp)
        except Exception:
            return None

    def _is_owner(
        self,
        securable_type: str,
        full_name: str,
        app_sp: str,
        reader_ws: WorkspaceClient | None = None,
    ) -> bool:
        workspace = reader_ws or self._workspace
        try:
            if securable_type == "VOLUME":
                securable = workspace.volumes.read(full_name)
            elif securable_type == "CATALOG":
                securable = workspace.catalogs.get(full_name)
            elif securable_type == "SCHEMA":
                securable = workspace.schemas.get(full_name)
            elif securable_type == "TABLE":
                securable = workspace.tables.get(full_name)
            else:
                return False
        except Exception:
            return False
        owner = getattr(securable, "owner", None)
        return isinstance(owner, str) and owner.casefold() == app_sp.casefold()

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


def _missing_privileges(response: EffectivePermissionsList, required: frozenset[str]) -> frozenset[str]:
    privileges = _privileges(response)
    if "ALL_PRIVILEGES" in privileges:
        return frozenset()
    return required - privileges


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


def _pg_boolean(value: str | None) -> bool:
    return isinstance(value, str) and value.casefold() in {"true", "t", "1"}
