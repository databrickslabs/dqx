"""Deployment-agnostic capability checks for DQX Studio setup resources."""

import logging
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.grants import GrantInspector, has_privilege
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources
from databricks_labs_dqx_app.backend.setup.verification_memo import (
    VerificationMemo,
    VerifiedRequirement,
    requirement_fingerprint,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import validate_identifier

_VOLUME_PRIVILEGES = frozenset({"READ_VOLUME", "WRITE_VOLUME"})
_CATALOG_PRIVILEGES = frozenset({"USE_CATALOG", "CREATE_SCHEMA"})
_CATALOG_PRIVILEGE_ORDER = ("USE_CATALOG", "CREATE_SCHEMA")
_SCHEMA_PRIVILEGES = frozenset({"USE_SCHEMA", "CREATE_TABLE"})
_SCHEMA_PRIVILEGE_ORDER = ("USE_SCHEMA", "CREATE_TABLE")
_VOLUME_PRIVILEGE_ORDER = ("READ_VOLUME", "WRITE_VOLUME")
_RUNNER_OUTPUTS_SCOPE = "task_runner_outputs"
_ADMIN_VERIFIED_SUFFIX = " was confirmed by an administrator earlier. Click Verify again to re-check it."
_OVERRIDABLE = (SetupActionId.VERIFY_AGAIN, SetupActionId.OVERRIDE)
RUN_GRANTS_AS_OWNER = "Ask the owner of these objects to run the following in a SQL editor, then click Verify again:"
logger = logging.getLogger(__name__)


class ResourceCheckers:
    """Check and reconcile capabilities of already-resolved setup resources.

    Args:
        resources: Resolved Studio resources to check.
        workspace: App service principal workspace client.
        sql: App service principal SQL executor.
        compute: Warehouse access inspection service.
        app_sp_id: Resolved app service principal name; an empty string means the
            identity is unresolved and identity-dependent checks require action.
        verification_memo: Store of the last fully verified catalog-level requirement
            sets. Unattended checks (no administrator readers) reuse it for grants the
            app service principal cannot inspect; without it they fail closed.
    """

    def __init__(
        self,
        *,
        resources: ActiveResources,
        workspace: WorkspaceClient,
        sql: SqlExecutor,
        compute: ComputeService,
        app_sp_id: str,
        verification_memo: VerificationMemo | None = None,
        configured_warehouse_id: Callable[[], str | None] | None = None,
    ) -> None:
        self._resources = resources
        self._configured_warehouse_id = configured_warehouse_id
        self._workspace = workspace
        self._sql = sql
        self._compute = compute
        self._inspector = GrantInspector(workspace)
        self._app_sp = "" if _has_control_characters(app_sp_id) else app_sp_id.strip()
        self._memo = verification_memo

    def check_unity_catalog(
        self,
        reader_sql: SqlExecutor | None = None,
        *,
        reader_ws: WorkspaceClient | None = None,
    ) -> SetupStep:
        """Verify catalog access for the app service principal and every audience principal.

        Audience principals are first granted USE CATALOG on a best-effort basis; the
        result is then verified from effective privileges, whatever the grant outcome.
        A full verification is remembered; an unattended check (no readers) whose only
        gap is uninspectable grants passes when the requirement set is unchanged since
        then. Any privilege observed missing always blocks.

        Args:
            reader_sql: Administrator SQL executor enabling the SHOW GRANTS fallback
                when effective permissions cannot be read.
            reader_ws: Administrator client; its presence marks an attended check that
                never reuses a previous verification.
        """
        app_sp = self._app_sp
        if not app_sp:
            return _identity_required(SetupStepId.UNITY_CATALOG)

        catalog = self._resources.volume.catalog
        self._grant_catalog_usage_to_audience()
        inspector = GrantInspector(self._workspace, reader_sql)
        requirements = [(app_sp, _CATALOG_PRIVILEGES)]
        requirements.extend(
            (principal, frozenset({"USE_CATALOG"})) for principal in self._resources.audience.uc_principals
        )
        fingerprint = requirement_fingerprint(
            SetupStepId.UNITY_CATALOG.value,
            catalog,
            (
                VerifiedRequirement(principal, "CATALOG", catalog, privilege)
                for principal, required in requirements
                for privilege in required
            ),
        )
        missing_by_principal: list[tuple[str, frozenset[str]]] = []
        uninspectable = False
        for principal, required in requirements:
            privileges = inspector.privileges("CATALOG", catalog, principal, required=required)
            if privileges is None:
                uninspectable = True
                continue
            missing = frozenset(privilege for privilege in required if not has_privilege(privileges, privilege))
            if missing and principal == app_sp:
                owner = inspector.owner("CATALOG", catalog)
                if owner is not None and owner.casefold() == app_sp.casefold():
                    missing = frozenset()
            if missing:
                missing_by_principal.append((principal, missing))
        if uninspectable:
            if not missing_by_principal and self._reuses_verification(
                SetupStepId.UNITY_CATALOG.value, fingerprint, reader_sql, reader_ws
            ):
                return _passed(SetupStepId.UNITY_CATALOG, "Catalog access" + _ADMIN_VERIFIED_SUFFIX)
            return SetupStep(
                id=SetupStepId.UNITY_CATALOG,
                state=StepState.ACTION_REQUIRED,
                code="catalog_permission_check_failed",
                summary="Studio couldn't read the permissions on its catalog.",
                instructions=(
                    _read_permissions_instruction("catalog", instruction_identifier(catalog)),
                    *_catalog_grant_instructions(catalog, missing_by_principal),
                ),
                actions=_OVERRIDABLE,
            )
        if missing_by_principal:
            return SetupStep(
                id=SetupStepId.UNITY_CATALOG,
                state=StepState.ACTION_REQUIRED,
                code="catalog_permissions_missing",
                summary="Studio or its users don't have the permissions they need on the catalog.",
                instructions=(RUN_GRANTS_AS_OWNER, *_catalog_grant_instructions(catalog, missing_by_principal)),
                actions=_OVERRIDABLE,
            )
        self._record_verification(SetupStepId.UNITY_CATALOG.value, fingerprint)
        return _passed(SetupStepId.UNITY_CATALOG, "Studio and its users can use the catalog.")

    def check_runner_access(
        self,
        job_id: int,
        reader_ws: WorkspaceClient | None = None,
        *,
        reader_sql: SqlExecutor | None = None,
        include_outputs: bool = False,
    ) -> SetupStep:
        """Grant, then verify, wheel access, schema usage, temporary-schema SELECT, and main-schema data access.

        Least-privilege grants on Studio-managed objects are applied best effort
        before inspection; anything still missing is reported for an administrator
        to apply. The runner never receives ALL PRIVILEGES, catalog privileges, or
        Genie or demo schema access. A full verification is remembered; an unattended
        check (no readers) whose only gap is uninspectable grants passes when the
        requirement set is unchanged since then. Any privilege observed missing blocks.

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
        app_sp = self._app_sp
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
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_identity_unresolved",
                summary="Studio couldn't find the service principal that runs its jobs.",
                instructions=(
                    "Set the task-runner job to run as a service principal other than the app's own "
                    "(Jobs > the Studio task runner > Run as), then click Verify again.",
                ),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        self._grant_runner_access(principal, include_outputs=include_outputs)
        volume = self._resources.volume
        catalog = instruction_identifier(volume.catalog)
        schema = f"{catalog}.{instruction_identifier(volume.schema)}"
        quoted_volume = f"{schema}.{instruction_identifier(volume.volume)}"
        quoted_principal = instruction_identifier(principal)
        requirements = [
            ("CATALOG", volume.catalog, "USE_CATALOG", "USE_CATALOG", catalog),
            ("SCHEMA", self._main_schema_full_name(), "USE_SCHEMA", "USE_SCHEMA", schema),
            *(
                (
                    "SCHEMA",
                    f"{volume.catalog}.{self._resources.tmp_schema}",
                    privilege,
                    grant,
                    f"{catalog}.{instruction_identifier(self._resources.tmp_schema)}",
                )
                for privilege, grant in (("USE_SCHEMA", "USE_SCHEMA"), ("SELECT", "SELECT"))
            ),
            ("VOLUME", self._volume_full_name(), "READ_VOLUME", "READ_VOLUME", quoted_volume),
        ]
        if include_outputs:
            requirements.extend(
                ("SCHEMA", self._main_schema_full_name(), privilege, privilege, schema)
                for privilege in ("SELECT", "MODIFY")
            )
        instructions: list[str] = []
        unknown: list[str] = []
        unknown_kinds: set[str] = set()
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
                unknown_kinds.add(kind)
                guidance = _read_permissions_instruction(kind.lower(), quoted_name)
                if guidance not in unknown:
                    unknown.append(guidance)
                continue
            instructions.append(f"GRANT {grant} ON {kind} {quoted_name} TO {quoted_principal};")
        scope = _RUNNER_OUTPUTS_SCOPE if include_outputs else SetupStepId.TASK_RUNNER.value
        fingerprint = requirement_fingerprint(
            scope,
            volume.catalog,
            (
                VerifiedRequirement(principal, kind, full_name, privilege)
                for kind, full_name, privilege, *_ in requirements
            ),
        )
        if unknown:
            # Studio-managed schemas and the wheels volume must stay inspectable by the app;
            # only the user-selected catalog may rely on a previous verification.
            if (
                not instructions
                and unknown_kinds == {"CATALOG"}
                and self._reuses_verification(scope, fingerprint, reader_sql, reader_ws)
            ):
                return _passed(SetupStepId.TASK_RUNNER, "Task-runner access" + _ADMIN_VERIFIED_SUFFIX)
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_permission_check_failed",
                summary="Studio couldn't read the task runner's permissions.",
                instructions=(*unknown, *instructions),
                actions=_OVERRIDABLE,
            )
        if instructions:
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_permissions_missing",
                summary="The task runner doesn't have all the permissions it needs to run checks.",
                instructions=(RUN_GRANTS_AS_OWNER, *instructions),
                actions=_OVERRIDABLE,
            )
        self._record_verification(scope, fingerprint)
        return _passed(SetupStepId.TASK_RUNNER, "The task runner has the access it needs.")

    def _reuses_verification(
        self,
        scope: str,
        fingerprint: str,
        reader_sql: SqlExecutor | None,
        reader_ws: WorkspaceClient | None,
    ) -> bool:
        """Whether an unattended check may reuse the last full verification of *scope*."""
        if reader_sql is not None or reader_ws is not None or self._memo is None:
            return False
        return self._memo.is_verified(scope, fingerprint)

    def _record_verification(self, scope: str, fingerprint: str) -> None:
        if self._memo is not None:
            self._memo.record(scope, fingerprint)

    def ensure_storage(self, *, provision: bool) -> SetupStep:
        """Provision or verify the prefix-derived schemas and the wheels volume.

        Existing schemas are only accepted when the app service principal owns or can
        manage them, so Studio never adopts a schema it does not manage.

        Args:
            provision: Create missing storage (Marketplace). When false (bundle
                deployments) missing storage is reported instead of created.

        Returns:
            A passed storage step, or the first blocking step.
        """
        app_sp = self._app_sp
        if not app_sp:
            return _identity_required(SetupStepId.STORAGE)
        volume = self._resources.volume
        schemas = (
            volume.schema,
            self._resources.tmp_schema,
            self._resources.genie_schema,
            self._resources.demo_schema,
        )
        missing_schemas: list[str] = []
        collisions: list[str] = []
        incomplete: list[tuple[str, frozenset[str]]] = []
        for schema in schemas:
            status, missing = self._schema_status(schema, app_sp)
            if status == "failed":
                return _storage_check_failed()
            if status == "missing":
                missing_schemas.append(schema)
            elif status == "collision":
                collisions.append(schema)
            elif status == "incomplete":
                incomplete.append((schema, missing))
        if collisions:
            return self._collision_step(collisions)
        if incomplete:
            return self._schema_permissions_step(incomplete, app_sp)

        try:
            volume_exists = self._volume_exists()
        except Exception:
            return _storage_check_failed()
        if missing_schemas or not volume_exists:
            if not provision:
                return SetupStep(
                    id=SetupStepId.STORAGE,
                    state=StepState.ACTION_REQUIRED,
                    code="storage_missing",
                    summary="Studio's schemas and wheels volume haven't been created yet.",
                    instructions=("Run make app-deploy again to create them, then click Verify again.",),
                    actions=(SetupActionId.VERIFY_AGAIN,),
                )
            created = self._create_storage(missing_schemas, volume_exists)
            if created is not None:
                return created
            # Creation is idempotent, so a pre-existing object may have been adopted by mistake.
            recheck_collisions: list[str] = []
            for schema in missing_schemas:
                status, _ = self._schema_status(schema, app_sp)
                if status == "failed":
                    return _storage_check_failed()
                if status == "missing":
                    return _creation_failed()
                if status != "ok":
                    recheck_collisions.append(schema)
            if recheck_collisions:
                return self._collision_step(recheck_collisions)
        return self._check_volume_access(app_sp)

    def _schema_status(self, schema: str, app_sp: str) -> tuple[str, frozenset[str]]:
        """Classify a storage schema as missing, ok, collision, incomplete or failed.

        A schema is Studio-managed when the app service principal owns it, or holds
        MANAGE together with USE SCHEMA and CREATE TABLE.
        """
        full_name = f"{self._resources.volume.catalog}.{schema}"
        try:
            existing = self._workspace.schemas.get(full_name)
        except NotFound:
            return "missing", frozenset()
        except Exception:
            return "failed", frozenset()
        owner = getattr(existing, "owner", None)
        if isinstance(owner, str) and owner.casefold() == app_sp.casefold():
            return "ok", frozenset()
        privileges = self._inspector.privileges("SCHEMA", full_name, app_sp, required=_SCHEMA_PRIVILEGES | {"MANAGE"})
        if privileges is None:
            return "failed", frozenset()
        if not has_privilege(privileges, "MANAGE"):
            return "collision", frozenset()
        missing = frozenset(name for name in _SCHEMA_PRIVILEGES if not has_privilege(privileges, name))
        return ("incomplete", missing) if missing else ("ok", frozenset())

    def _collision_step(self, schemas: list[str]) -> SetupStep:
        catalog = instruction_identifier(self._resources.volume.catalog)
        return SetupStep(
            id=SetupStepId.STORAGE,
            state=StepState.ACTION_REQUIRED,
            code="storage_collision",
            summary="A schema with the name Studio wants to use already exists, and Studio doesn't manage it.",
            instructions=tuple(
                f"Schema {catalog}.{instruction_identifier(schema)} already exists. Choose a different storage "
                "prefix, or rename or drop that schema, then click Verify again."
                for schema in schemas
            ),
            actions=(SetupActionId.VERIFY_AGAIN,),
        )

    def _schema_permissions_step(self, incomplete: list[tuple[str, frozenset[str]]], app_sp: str) -> SetupStep:
        catalog = instruction_identifier(self._resources.volume.catalog)
        principal = instruction_identifier(app_sp)
        return SetupStep(
            id=SetupStepId.STORAGE,
            state=StepState.ACTION_REQUIRED,
            code="storage_permissions_missing",
            summary="Studio needs more permissions on one of its schemas.",
            instructions=(
                RUN_GRANTS_AS_OWNER,
                *(
                    "GRANT "
                    + ", ".join(name for name in _SCHEMA_PRIVILEGE_ORDER if name in missing)
                    + f" ON SCHEMA {catalog}.{instruction_identifier(schema)} TO {principal};"
                    for schema, missing in incomplete
                ),
            ),
            actions=(SetupActionId.VERIFY_AGAIN,),
        )

    def _volume_exists(self) -> bool:
        try:
            self._workspace.volumes.read(self._volume_full_name())
        except NotFound:
            return False
        return True

    def _create_storage(self, missing_schemas: list[str], volume_exists: bool) -> SetupStep | None:
        """Create missing storage; return a failure step, or None on success."""
        try:
            catalog = self._sql.q(_validated_identifier(self._resources.volume.catalog))
            for schema in missing_schemas:
                self._sql.execute_no_schema(
                    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{self._sql.q(_validated_identifier(schema))}"
                )
            if not volume_exists:
                volume = self._resources.volume
                self._sql.execute_no_schema(
                    "CREATE VOLUME IF NOT EXISTS "
                    f"{catalog}.{self._sql.q(_validated_identifier(volume.schema))}."
                    f"{self._sql.q(_validated_identifier(volume.volume))}"
                )
        except Exception:
            return _creation_failed()
        return None

    def _check_volume_access(self, app_sp: str) -> SetupStep:
        full_name = self._volume_full_name()
        privileges = self._inspector.privileges("VOLUME", full_name, app_sp, required=_VOLUME_PRIVILEGES)
        if privileges is None:
            return _storage_check_failed()
        missing = [name for name in _VOLUME_PRIVILEGE_ORDER if not has_privilege(privileges, name)]
        if missing and not self._is_owner("VOLUME", full_name, app_sp):
            volume = self._resources.volume
            quoted_volume = ".".join(
                instruction_identifier(part) for part in (volume.catalog, volume.schema, volume.volume)
            )
            return SetupStep(
                id=SetupStepId.STORAGE,
                state=StepState.ACTION_REQUIRED,
                code="volume_permissions_missing",
                summary="Studio needs permission to read and write its wheels volume.",
                instructions=(
                    RUN_GRANTS_AS_OWNER,
                    f"GRANT {', '.join(p for p in missing)} ON VOLUME {quoted_volume} "
                    f"TO {instruction_identifier(app_sp)};",
                ),
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return _passed(SetupStepId.STORAGE, "Studio's schemas and wheels volume are ready.")

    def _grant_runner_access(self, principal: str, *, include_outputs: bool) -> None:
        """Best-effort least-privilege grants for the task runner on Studio-managed objects.

        Grants USE SCHEMA and SELECT on the temporary-view schema (the runner reads OBO
        temporary views through that schema-level SELECT, not per-view grants). Never grants
        ALL PRIVILEGES, catalog privileges, or access to the Genie or demo schemas.
        """
        volume = self._resources.volume
        try:
            catalog = self._sql.q(_validated_identifier(volume.catalog))
            main = f"{catalog}.{self._sql.q(_validated_identifier(volume.schema))}"
            tmp = f"{catalog}.{self._sql.q(_validated_identifier(self._resources.tmp_schema))}"
            wheels = f"{main}.{self._sql.q(_validated_identifier(volume.volume))}"
            grantee = self._sql.q(principal)
        except Exception:
            logger.warning("Could not grant the task runner access to Studio resources; verifying its access instead.")
            return
        statements = [f"GRANT USE_SCHEMA ON SCHEMA {main} TO {grantee}"]
        if include_outputs:
            statements.append(f"GRANT SELECT, MODIFY ON SCHEMA {main} TO {grantee}")
        statements.append(f"GRANT USE_SCHEMA, SELECT ON SCHEMA {tmp} TO {grantee}")
        statements.append(f"GRANT READ_VOLUME ON VOLUME {wheels} TO {grantee}")
        for statement in statements:
            try:
                self._sql.execute_no_schema(statement)
            except Exception:
                logger.warning("Could not grant the task runner a Studio privilege; verifying its access instead.")

    def _grant_catalog_usage_to_audience(self) -> None:
        try:
            catalog = self._sql.q(_validated_identifier(self._resources.volume.catalog))
        except Exception:
            logger.warning("Could not grant audience principals USE CATALOG; verifying their access instead.")
            return
        for principal in self._resources.audience.uc_principals:
            try:
                self._sql.execute_no_schema(f"GRANT USE_CATALOG ON CATALOG {catalog} TO {self._sql.q(principal)}")
            except Exception:
                logger.warning("Could not grant an audience principal USE CATALOG; verifying its access instead.")

    def check_warehouse(
        self,
        warehouse_id: str | None = None,
        reader_ws: WorkspaceClient | None = None,
        *,
        include_audience: bool = True,
    ) -> SetupStep:
        """Verify the app SP holds CAN_MANAGE on the SQL warehouse and the audience CAN_USE.

        Args:
            warehouse_id: Candidate warehouse ID, or the warehouse Studio runs SQL on when
                omitted (the administrator's override, else the bound warehouse).
            reader_ws: Client permitted to inspect the candidate's access controls,
                or the app service principal client when omitted.
            include_audience: Grant the audience CAN_USE, then verify it. When false only the
                app service principal's access is read, so probing a candidate warehouse
                never changes its permissions.
        """
        effective_warehouse_id = (warehouse_id or self.effective_warehouse_id()).strip()
        effective_reader_ws = reader_ws or self._workspace
        warehouse = instruction_identifier(effective_warehouse_id)
        try:
            status = self._compute.warehouse_access_status(effective_warehouse_id, reader_ws=effective_reader_ws)
            if status == "missing":
                return SetupStep(
                    id=SetupStepId.WAREHOUSE,
                    state=StepState.ACTION_REQUIRED,
                    code="warehouse_permissions_missing",
                    summary="Studio needs the Can manage permission on its SQL warehouse.",
                    instructions=(
                        f"Give the app's service principal Can manage on SQL warehouse {warehouse} "
                        "(SQL Warehouses > the warehouse > Permissions), then click Verify again.",
                    ),
                    actions=_OVERRIDABLE,
                )
            if status != "granted":
                return SetupStep(
                    id=SetupStepId.WAREHOUSE,
                    state=StepState.ACTION_REQUIRED,
                    code="warehouse_permission_unknown",
                    summary="Studio couldn't read the permissions on its SQL warehouse.",
                    instructions=(
                        f"Make sure the app's service principal has Can manage on SQL warehouse {warehouse}, "
                        "then click Verify again while signed in as someone who can manage the warehouse.",
                    ),
                    actions=_OVERRIDABLE,
                )
            if not include_audience:
                return _passed(SetupStepId.WAREHOUSE, "Studio can use the SQL warehouse.")
            status = self._compute.reconcile_warehouse_audience(
                effective_warehouse_id, self._resources.audience.workspace_principals
            )
        except Exception:
            return SetupStep(
                id=SetupStepId.WAREHOUSE,
                state=StepState.ACTION_REQUIRED,
                code="warehouse_permission_check_failed",
                summary="Studio couldn't check the permissions on its SQL warehouse.",
                instructions=(f"Check that SQL warehouse {warehouse} still exists, then click Verify again.",),
                actions=_OVERRIDABLE,
            )
        if status == "granted":
            return _passed(SetupStepId.WAREHOUSE, "Studio and its users can use the SQL warehouse.")
        audience_instructions = tuple(
            f"Give group {instruction_identifier(group)} Can use on SQL warehouse {warehouse} "
            "(SQL Warehouses > the warehouse > Permissions)."
            for group in self._resources.audience.workspace_principals
        )
        if status == "missing":
            return SetupStep(
                id=SetupStepId.WAREHOUSE,
                state=StepState.ACTION_REQUIRED,
                code="warehouse_audience_missing",
                summary="Studio users can't use the SQL warehouse yet.",
                instructions=audience_instructions,
                actions=_OVERRIDABLE,
            )
        return SetupStep(
            id=SetupStepId.WAREHOUSE,
            state=StepState.ACTION_REQUIRED,
            code="warehouse_audience_unverified",
            summary="Studio can use the SQL warehouse, but couldn't confirm that Studio users can.",
            instructions=audience_instructions,
            actions=_OVERRIDABLE,
        )

    def effective_warehouse_id(self) -> str:
        """Return the warehouse Studio runs SQL on: the administrator's choice, else the bound warehouse."""
        if self._configured_warehouse_id is not None:
            try:
                configured = self._configured_warehouse_id()
            except Exception:
                logger.warning("Could not read the configured SQL warehouse; checking the bound warehouse instead.")
                configured = None
            if configured and configured.strip():
                return configured
        return self._resources.warehouse_id

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


def _catalog_grant_instructions(catalog: str, missing: list[tuple[str, frozenset[str]]]) -> tuple[str, ...]:
    quoted_catalog = instruction_identifier(catalog)
    return tuple(
        f"GRANT {', '.join(name for name in _CATALOG_PRIVILEGE_ORDER if name in privileges)} "
        f"ON CATALOG {quoted_catalog} TO {instruction_identifier(principal)};"
        for principal, privileges in missing
    )


def _read_permissions_instruction(kind: str, quoted_name: str) -> str:
    return (
        f"Sign in as the owner of {kind} {quoted_name} or a metastore admin, then click Verify again "
        "so Studio can read its permissions. You also need access to Studio's SQL warehouse."
    )


def _creation_failed() -> SetupStep:
    return _action_required(
        SetupStepId.STORAGE,
        "storage_creation_failed",
        "Studio couldn't create its schemas and wheels volume.",
        action=SetupActionId.RECONCILE,
    )


def _storage_check_failed() -> SetupStep:
    return _action_required(
        SetupStepId.STORAGE,
        "storage_permission_check_failed",
        "Studio couldn't check who owns its schemas and wheels volume.",
    )


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
        "Studio couldn't work out which service principal it runs as.",
    )


def _validated_identifier(value: str) -> str:
    return validate_identifier(value)


def instruction_identifier(value: str) -> str:
    """Quote *value* for display in administrator instructions, replacing control characters.

    Args:
        value: Identifier to display.

    Returns:
        The backtick-quoted, sanitized identifier.
    """
    sanitized = replace_control_characters(value)
    return "`" + sanitized.replace("`", "``") + "`"


def _has_control_characters(value: str) -> bool:
    return replace_control_characters(value) != value
