"""Setup checks that run before any Unity Catalog storage is configured."""

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.pg_executor import PgExecutor
from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.sql_utils import validate_identifier


class BootstrapCheckers:
    """Verify the app identity and Lakebase, which Studio needs before storage is chosen.

    Args:
        workspace: App service principal workspace client.
        pg: Lakebase executor bound to the app service principal.
        lakebase_schema: Lakebase schema that holds Studio application tables.
    """

    def __init__(self, *, workspace: WorkspaceClient, pg: PgExecutor, lakebase_schema: str) -> None:
        self._workspace = workspace
        self._pg = pg
        self._lakebase_schema = lakebase_schema
        self._app_sp: str = ""

    def check_app_identity(self) -> SetupStep:
        """Verify that the app service principal identity can be resolved."""
        if self.app_sp_id():
            return SetupStep(
                id=SetupStepId.IDENTITY,
                state=StepState.PASSED,
                summary="The app service principal identity is available.",
            )
        return SetupStep(
            id=SetupStepId.IDENTITY,
            state=StepState.ACTION_REQUIRED,
            code="app_identity_unresolved",
            summary="Could not resolve the app service principal identity.",
            instructions=("Verify the Databricks App service principal binding.",),
            actions=(SetupActionId.VERIFY_AGAIN,),
        )

    def app_sp_id(self) -> str:
        """Return the resolved app service principal name, or an empty string when unresolved.

        A successful resolution is cached for the instance; a failed or invalid lookup is
        retried on the next call.
        """
        if self._app_sp:
            return self._app_sp
        try:
            identity = self._workspace.current_user.me()
            candidate = (identity.user_name or identity.id or "").strip()
        except Exception:
            return ""
        if replace_control_characters(candidate) != candidate:
            return ""
        self._app_sp = candidate
        return candidate

    def check_lakebase(self) -> SetupStep:
        """Verify non-mutating connectivity to the configured Lakebase database."""
        try:
            self._pg.query("SELECT 1")
        except Exception:
            return SetupStep(
                id=SetupStepId.LAKEBASE,
                state=StepState.ACTION_REQUIRED,
                code="lakebase_connectivity_failed",
                summary="Could not connect to the configured Lakebase database.",
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return SetupStep(
            id=SetupStepId.LAKEBASE,
            state=StepState.PASSED,
            summary="Lakebase connectivity is available.",
        )

    def ensure_lakebase_schema(self) -> SetupStep:
        """Create the validated Lakebase schema if it is absent before migrations run."""
        try:
            schema = validate_identifier(self._lakebase_schema)
            self._pg.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS {self._pg.q(schema)}")
        except Exception:
            return SetupStep(
                id=SetupStepId.LAKEBASE,
                state=StepState.ACTION_REQUIRED,
                code="lakebase_schema_creation_failed",
                summary="Could not create the required Lakebase schema.",
                actions=(SetupActionId.RECONCILE,),
            )
        return SetupStep(
            id=SetupStepId.LAKEBASE,
            state=StepState.PASSED,
            summary="The required Lakebase schema is available.",
        )
