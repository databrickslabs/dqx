"""Admin-only, destructive maintenance endpoints.

Currently hosts the "Reset database" feature, which clears all DQX
Studio-managed data, and the "Deploy demo content" feature, which seeds the
Studio with a governed e-commerce demo on a background daemon thread. The
whole router is hard-gated to :class:`UserRole.ADMIN` so no non-admin can
reach any endpoint here via the API — the UI gate is a convenience, this is
the real boundary.
"""

import threading
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.demo.seed_service import DemoSeedService
from databricks_labs_dqx_app.backend.demo.status import DemoStatus, DemoStatusStore
from databricks_labs_dqx_app.backend.dependencies import (
    get_database_reset_service,
    get_demo_seed_service,
    get_demo_status_store,
    get_obo_ws,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    DemoContentStatusOut,
    DeployDemoContentIn,
    DeployDemoContentOut,
    ResetDatabaseIn,
    ResetDatabaseOut,
)
from databricks_labs_dqx_app.backend.services.database_reset_service import (
    RESET_CONFIRMATION_PHRASE,
    DatabaseResetService,
)

# Router-level ADMIN gate: every route below requires the ADMIN role,
# enforced server-side regardless of any UI gating.
router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])


def _utc_now_str() -> str:
    """Return the current UTC time as a ``YYYY-MM-DD HH:MM:SS`` string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _launch_seed(target: Callable[[], None]) -> None:
    """Run *target* on a named daemon thread and return immediately.

    Factored out as a module-level seam so tests can drive the launch
    synchronously (running the target inline) while production keeps the
    fire-and-forget daemon-thread behaviour.
    """
    threading.Thread(target=target, name="dqx-demo-seed", daemon=True).start()


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
        raise HTTPException(
            status_code=500, detail="Database reset failed. See server logs for details."
        ) from e

    return ResetDatabaseOut(
        status="reset",
        performed_by=result.performed_by,
        performed_at=result.performed_at,
        cleared_tables=result.cleared_tables,
        failed_tables=result.failed_tables,
        preserved_note=result.preserved_note,
    )


@router.post("/demo/deploy", response_model=DeployDemoContentOut, operation_id="deployDemoContent")
def deploy_demo_content(
    body: DeployDemoContentIn,
    seeder: Annotated[DemoSeedService, Depends(get_demo_seed_service)],
    status_store: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DeployDemoContentOut:
    """Launch the governed demo-content seed on a background thread (Admin only).

    The seed runs for ~30min, so this endpoint fires it on a named daemon thread
    and returns immediately with the initial ``running`` state. Progress is
    polled via ``GET /demo/status``. A 409 is returned when a seed is already
    in progress so two concurrent deploys can't race.

    The seed service owns its terminal status: it writes ``succeeded`` or
    ``failed`` to the status store itself. The thread target only logs on an
    escaped failure — it does not overwrite the status.
    """
    if status_store.is_running():
        raise HTTPException(status_code=409, detail="A demo deployment is already in progress.")

    try:
        performed_by = obo_ws.current_user.me().user_name or "unknown"
    except Exception:
        logger.warning("Could not resolve acting user for demo deploy; using 'unknown'", exc_info=True)
        performed_by = "unknown"

    started_at = _utc_now_str()
    status_store.set(
        DemoStatus(
            state="running",
            phase="starting",
            message="Demo deployment queued.",
            started_at=started_at,
            updated_at=started_at,
        ),
        user_email=performed_by,
    )

    def _run() -> None:
        try:
            seeder.run(user_email=performed_by, wipe_first=body.wipe_first)
        except Exception:
            # The seed service already wrote a terminal 'failed' status; just log.
            logger.error("Demo content deployment failed", exc_info=True)

    _launch_seed(_run)
    return DeployDemoContentOut(status="running", started_at=started_at)


@router.get("/demo/status", response_model=DemoContentStatusOut, operation_id="demoContentStatus")
def demo_content_status(
    status_store: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
) -> DemoContentStatusOut:
    """Return the current state of the long-running demo-content seed (Admin only)."""
    status = status_store.get()
    return DemoContentStatusOut(
        state=status.state,
        phase=status.phase,
        message=status.message,
        started_at=status.started_at,
        updated_at=status.updated_at,
    )
