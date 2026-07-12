"""Admin-only, destructive maintenance endpoints.

Currently hosts the "Reset database" feature, which clears all DQX
Studio-managed data. The whole router is hard-gated to
:class:`UserRole.ADMIN` so no non-admin can reach any endpoint here via the
API — the UI gate is a convenience, this is the real boundary.
"""

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_database_reset_service,
    get_obo_ws,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import ResetDatabaseIn, ResetDatabaseOut
from databricks_labs_dqx_app.backend.services.database_reset_service import (
    RESET_CONFIRMATION_PHRASE,
    DatabaseResetService,
)

# Router-level ADMIN gate: every route below requires the ADMIN role,
# enforced server-side regardless of any UI gating.
router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])


@router.post("/reset-database", response_model=ResetDatabaseOut, operation_id="resetDatabase")
def reset_database(
    body: ResetDatabaseIn,
    svc: Annotated[DatabaseResetService, Depends(get_database_reset_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> ResetDatabaseOut:
    """Clear ALL DQX Studio-managed data (Admin only). DESTRUCTIVE.

    Guardrails:

    - **Role**: the router requires :class:`UserRole.ADMIN`; a non-admin is
      rejected with 403 before this handler runs.
    - **Confirmation phrase**: the request body must carry the exact
      :data:`RESET_CONFIRMATION_PHRASE`; any mismatch is a 400. This is
      defense-in-depth on top of the role gate — an accidental or replayed
      request without the phrase cannot trigger the wipe.

    Scope: only the app's own ``dq_*`` tables are cleared (rows DELETEd, not
    tables dropped). The schema, the ``dq_migrations`` version tracker, and
    admin role mappings are preserved so the app keeps working and admins
    keep access. Customer/monitored data tables are never touched.
    """
    # Defense-in-depth confirmation check (case-sensitive exact match).
    if body.confirmation_phrase != RESET_CONFIRMATION_PHRASE:
        raise HTTPException(
            status_code=400,
            detail="Confirmation phrase does not match. Type the exact phrase to confirm the reset.",
        )

    try:
        user = obo_ws.current_user.me()
        performed_by = user.user_name or "unknown"
    except Exception:
        # The reset itself does not depend on identity resolution; fall back
        # to a placeholder actor rather than failing the operation.
        logger.warning("Could not resolve acting user for database reset; using 'unknown'", exc_info=True)
        performed_by = "unknown"

    try:
        result = svc.reset_all_data(performed_by=performed_by)
    except Exception as e:
        logger.error(f"Database reset failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database reset failed. See server logs for details.")

    return ResetDatabaseOut(
        status="reset",
        performed_by=result.performed_by,
        performed_at=result.performed_at,
        cleared_tables=result.cleared_tables,
        failed_tables=result.failed_tables,
        preserved_note=result.preserved_note,
    )
