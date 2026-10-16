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
    get_obo_sql_executor,
    get_obo_ws,
    get_reset_status_store,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    DemoContentStatusOut,
    DeployDemoContentIn,
    DeployDemoContentOut,
    ResetDatabaseIn,
    ResetDatabaseOut,
    ResetStatusOut,
)
from databricks_labs_dqx_app.backend.services.database_reset_service import (
    RESET_CONFIRMATION_PHRASE,
    DatabaseResetService,
)
from databricks_labs_dqx_app.backend.services.reset_status import ResetStatus, ResetStatusStore
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _sanitize(text: str) -> str:
    """Strip newlines/carriage returns to prevent log/status injection (CWE-117)."""
    return text.replace("\n", " ").replace("\r", " ").strip()


# Router-level ADMIN gate: every route below requires the ADMIN role,
# enforced server-side regardless of any UI gating.
router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])

# Serializes the reset/demo "acquire running" check-and-set so it is atomic
# within a process. Both the reset and demo handlers each read is_running() on
# BOTH stores and then persist a `running` status — a non-atomic sequence where
# two near-simultaneous requests could both observe "not running" and both
# launch a destructive job. This ONE lock is shared by both handlers so it also
# enforces reset⇄demo mutual exclusion (they share the SP warehouse + Lakebase
# and must not race). It guards ONLY the check-and-set, and is released before
# the long-running work / daemon-thread launch. NOTE: this is single-process
# atomicity only; under multiple uvicorn workers a true guard would need a
# DB-level lock (e.g. a conditional status upsert). Single-instance is the
# current deployment target, so the in-process lock is the right scope now.
_job_lock = threading.Lock()


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


def _launch_reset(target: Callable[[], None]) -> None:
    """Run *target* on a named daemon thread and return immediately.

    The reset seam, mirroring :func:`_launch_seed`: tests monkeypatch this to
    run the target inline while production keeps the fire-and-forget daemon
    thread. Kept separate from the seed seam so each launch is independently
    controllable in tests.
    """
    threading.Thread(target=target, name="dqx-db-reset", daemon=True).start()


@router.post("/reset-database", response_model=ResetDatabaseOut, operation_id="resetDatabase")
def reset_database(
    body: ResetDatabaseIn,
    svc: Annotated[DatabaseResetService, Depends(get_database_reset_service)],
    status_store: Annotated[ResetStatusStore, Depends(get_reset_status_store)],
    demo_status_store: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> ResetDatabaseOut:
    """Clear ALL DQX Studio-managed data on a background thread (Admin only). DESTRUCTIVE.

    The reset runs 32 cross-backend DELETEs and a full Ask-Genie space
    reprovision, which can outlive the Databricks Apps gateway idle timeout — so
    this endpoint fires the work on a named daemon thread and returns immediately
    with the initial ``running`` state. Progress and the terminal ``succeeded`` /
    ``failed`` outcome (with counts) are polled via ``GET /admin/reset-status``.

    Guardrails:

    - **Role**: the router requires :class:`UserRole.ADMIN`; a non-admin is
      rejected with 403 before this handler runs.
    - **Confirmation phrase**: the request body must carry the exact
      :data:`RESET_CONFIRMATION_PHRASE`; any mismatch is a 400. This is
      defense-in-depth on top of the role gate — an accidental or replayed
      request without the phrase cannot trigger the wipe.
    - **Mutual exclusion**: a 409 is returned when a reset is already running,
      or when a demo deployment is in progress — the two share the SP warehouse
      + Lakebase and must not race.

    Scope: only the app's own ``dq_*`` tables are cleared (rows DELETEd, not
    tables dropped). The schema, the ``dq_migrations`` version tracker, and
    admin role mappings are preserved so the app keeps working and admins
    keep access. Customer/monitored data tables are never touched.

    The thread owns the terminal status: it writes ``succeeded`` (with counts)
    or ``failed`` (with the error message) to the reset status store, wrapping
    its body so an exception is always recorded rather than lost.
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

    started_at = _utc_now_str()

    # Mutual exclusion: reset and demo deploy share the SP warehouse + Lakebase,
    # so never let them run concurrently. The is_running() checks and the
    # set(running) below are a check-then-set that must be ATOMIC — otherwise two
    # near-simultaneous requests could both observe "not running" and both launch
    # a destructive reset (TOCTOU). ``_job_lock`` (shared with the demo handler)
    # makes acquiring "running" atomic within the process; it is released before
    # the long reset runs on the daemon thread below.
    with _job_lock:
        if status_store.is_running():
            raise HTTPException(status_code=409, detail="A database reset is already in progress.")
        if demo_status_store.is_running():
            raise HTTPException(
                status_code=409,
                detail="A demo deployment is in progress. Wait for it to finish before resetting.",
            )
        status_store.set(
            ResetStatus(
                state="running",
                message="Database reset queued.",
                started_at=started_at,
                updated_at=started_at,
            ),
            user_email=performed_by,
        )

    def _run() -> None:
        try:
            result = svc.reset_all_data(performed_by=performed_by)
            status_store.set(
                ResetStatus(
                    state="succeeded",
                    message=result.preserved_note,
                    started_at=started_at,
                    updated_at=_utc_now_str(),
                    cleared_count=len(result.cleared_tables),
                    failed_count=len(result.failed_tables),
                ),
                user_email=performed_by,
            )
        except Exception as exc:
            # Never lose a failure: record a terminal 'failed' status (the
            # audit log line lives inside reset_all_data; this is the escaped
            # error). The message is newline-stripped to prevent log/status
            # forging (CWE-117).
            logger.error("Database reset failed", exc_info=True)
            status_store.set(
                ResetStatus(
                    state="failed",
                    message=_sanitize(str(exc)) or "Database reset failed. See server logs for details.",
                    started_at=started_at,
                    updated_at=_utc_now_str(),
                ),
                user_email=performed_by,
            )

    _launch_reset(_run)
    return ResetDatabaseOut(state="running", started_at=started_at)


@router.post("/demo/deploy", response_model=DeployDemoContentOut, operation_id="deployDemoContent")
def deploy_demo_content(
    body: DeployDemoContentIn,
    seeder: Annotated[DemoSeedService, Depends(get_demo_seed_service)],
    status_store: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
    reset_status_store: Annotated[ResetStatusStore, Depends(get_reset_status_store)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    obo_sql: Annotated[SqlExecutor, Depends(get_obo_sql_executor)],
) -> DeployDemoContentOut:
    """Launch the governed demo-content seed on a background thread (Admin only).

    The seed runs for ~30min, so this endpoint fires it on a named daemon thread
    and returns immediately with the initial ``running`` state. Progress is
    polled via ``GET /demo/status``. A 409 is returned when a seed is already
    in progress, or when a database reset is running — the two share the SP
    warehouse + Lakebase and must not race.

    The seed service owns its terminal status: it writes ``succeeded`` or
    ``failed`` to the status store itself. The thread target only logs on an
    escaped failure — it does not overwrite the status.
    """
    try:
        performed_by = obo_ws.current_user.me().user_name or "unknown"
    except Exception:
        logger.warning("Could not resolve acting user for demo deploy; using 'unknown'", exc_info=True)
        performed_by = "unknown"

    started_at = _utc_now_str()

    # Mutual exclusion, atomic check-and-set — see the matching block in
    # ``reset_database``. The is_running() checks (demo + reset) and the
    # set(running) below must not interleave with a concurrent request, or two
    # deploys (or a deploy racing a reset) could both launch. ``_job_lock`` is
    # the SAME lock the reset handler uses, so it enforces demo⇄reset exclusion
    # too; it is released before the ~30min seed runs on the daemon thread.
    with _job_lock:
        if status_store.is_running():
            raise HTTPException(status_code=409, detail="A demo deployment is already in progress.")
        if reset_status_store.is_running():
            raise HTTPException(
                status_code=409,
                detail="A database reset is in progress. Wait for it to finish before deploying demo content.",
            )
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

    # Governed class.* column tags need ASSIGN on the tag policy — the app SP
    # usually lacks it, but the admin triggering this deploy usually holds it.
    # Hand the seeder the caller's OBO SqlExecutor so the SET TAG DDL runs as
    # them (falling back to the SP). SET TAG needs only the `sql` warehouse
    # scope — no Unity Catalog OBO API scope. Tagging is the seed's first phase,
    # so the OBO token is still fresh when the background thread reaches it.
    seeder.set_tagging_sql(obo_sql)

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


@router.get("/reset-status", response_model=ResetStatusOut, operation_id="resetStatus")
def reset_status(
    status_store: Annotated[ResetStatusStore, Depends(get_reset_status_store)],
) -> ResetStatusOut:
    """Return the current state of the long-running database-reset job (Admin only)."""
    status = status_store.get()
    return ResetStatusOut(
        state=status.state,
        message=status.message,
        started_at=status.started_at,
        updated_at=status.updated_at,
        cleared_count=status.cleared_count,
        failed_count=status.failed_count,
    )
