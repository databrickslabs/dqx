from __future__ import annotations

import json
from typing import Annotated
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentUserRole,
    get_conf,
    get_job_service,
    get_obo_ws,
    get_sp_sql_executor,
    get_view_service,
    require_role,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    BatchProfileRunIn,
    BatchProfileRunOut,
    ProfileResultsOut,
    ProfileRunIn,
    ProfileRunOut,
    ProfileRunSummaryOut,
    RunStatusOut,
)
from databricks_labs_dqx_app.backend.run_status_manager import get_run_metadata, has_terminal_result, update_run_status
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.view_service import ViewService

router = APIRouter()

_PROFILER_TABLE = "dq_profiling_results"

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.get(
    "/runs",
    response_model=list[ProfileRunSummaryOut],
    operation_id="listProfileRuns",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_profile_runs(
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> list[ProfileRunSummaryOut]:
    """Return profiling run history, newest first."""
    try:
        table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_profiling_results"
        rows = job_svc.list_run_rows(table)
        return [
            ProfileRunSummaryOut(
                run_id=row.get("run_id") or "",
                source_table_fqn=row.get("source_table_fqn") or "",
                status=row.get("status"),
                rows_profiled=int(v) if (v := row.get("rows_profiled")) else None,
                columns_profiled=int(v) if (v := row.get("columns_profiled")) else None,
                duration_seconds=float(v) if (v := row.get("duration_seconds")) else None,
                requesting_user=row.get("requesting_user"),
                canceled_by=row.get("canceled_by"),
                updated_at=row.get("updated_at"),
                created_at=row.get("created_at"),
            )
            for row in rows
        ]
    except Exception as e:
        logger.error("Failed to list profile runs: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list profile runs: {e}")


@router.post(
    "/run",
    response_model=ProfileRunOut,
    operation_id="submitProfileRun",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def submit_profile_run(
    body: ProfileRunIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> ProfileRunOut:
    """Create a temporary view (OBO) and submit a profiler job (SP)."""
    try:
        run_id = uuid4().hex[:16]

        # Get requesting user email
        user = obo_ws.current_user.me()
        requesting_user = user.user_name or "unknown"

        # Create view using OBO token — inherits user's table permissions
        view_fqn = view_svc.create_view(body.table_fqn, sample_limit=body.sample_limit)

        # Submit job using SP credentials
        config = {
            "sample_limit": body.sample_limit,
            "source_table_fqn": body.table_fqn,
            "columns": body.columns,
            "profile_options": body.profile_options,
        }
        job_run_id = job_svc.submit_run(
            task_type="profile",
            view_fqn=view_fqn,
            config=config,
            run_id=run_id,
            requesting_user=requesting_user,
        )

        # Write a RUNNING placeholder row immediately so the job appears in history
        results_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_profiling_results"
        job_svc.record_run_started(
            table=results_table,
            run_id=run_id,
            requesting_user=requesting_user,
            source_table_fqn=body.table_fqn,
            view_fqn=view_fqn,
            sample_limit=body.sample_limit,
            job_run_id=job_run_id,
        )

        return ProfileRunOut(run_id=run_id, job_run_id=job_run_id, view_fqn=view_fqn)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to submit profile run for %s: %s", body.table_fqn, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit profile run: {e}")


@router.post(
    "/batch-run",
    response_model=BatchProfileRunOut,
    operation_id="submitBatchProfileRun",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def submit_batch_profile_run(
    body: BatchProfileRunIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> BatchProfileRunOut:
    """Create temporary views and submit profiler jobs for multiple tables in parallel."""
    if not body.table_fqns:
        raise HTTPException(status_code=400, detail="table_fqns cannot be empty")

    try:
        user = obo_ws.current_user.me()
        requesting_user = user.user_name or "unknown"

        runs: list[ProfileRunOut] = []
        errors: list[str] = []

        for table_fqn in body.table_fqns:
            try:
                run_id = uuid4().hex[:16]

                view_fqn = view_svc.create_view(table_fqn, sample_limit=body.sample_limit)

                config = {
                    "sample_limit": body.sample_limit,
                    "source_table_fqn": table_fqn,
                    "columns": None,
                    "profile_options": body.profile_options,
                }
                job_run_id = job_svc.submit_run(
                    task_type="profile",
                    view_fqn=view_fqn,
                    config=config,
                    run_id=run_id,
                    requesting_user=requesting_user,
                )

                runs.append(ProfileRunOut(run_id=run_id, job_run_id=job_run_id, view_fqn=view_fqn))
                logger.info("Submitted batch profile run for %s (run_id=%s)", table_fqn, run_id)

                # Write RUNNING placeholder so job appears in history immediately
                results_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_profiling_results"
                job_svc.record_run_started(
                    table=results_table,
                    run_id=run_id,
                    requesting_user=requesting_user,
                    source_table_fqn=table_fqn,
                    view_fqn=view_fqn,
                    sample_limit=body.sample_limit,
                    job_run_id=job_run_id,
                )
            except Exception as table_err:
                logger.error("Failed to submit profile run for %s: %s", table_fqn, table_err, exc_info=True)
                errors.append(f"{table_fqn}: {table_err}")

        if not runs and errors:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to submit any profile runs: {'; '.join(errors)}",
            )

        if errors:
            logger.warning("Some tables failed in batch profile: %s", errors)

        return BatchProfileRunOut(runs=runs)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to submit batch profile run: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit batch profile run: {e}")


@router.get(
    "/runs/{run_id}/status",
    response_model=RunStatusOut,
    operation_id="getProfileRunStatus",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_profile_run_status(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> RunStatusOut:
    """Poll the status of a profiler job run. Cleans up the view when job terminates."""
    try:
        meta = get_run_metadata(sql, app_conf, _PROFILER_TABLE, run_id)
        if meta.job_run_id is None:
            terminal = has_terminal_result(sql, app_conf, _PROFILER_TABLE, run_id)
            if terminal:
                if meta.view_fqn and "tmp_view_" in meta.view_fqn:
                    try:
                        view_svc.drop_view(meta.view_fqn)
                    except Exception:
                        pass
                return RunStatusOut(
                    run_id=run_id,
                    state="TERMINATED",
                    result_state="SUCCESS" if terminal == "SUCCESS" else "FAILED",
                    message=None if terminal == "SUCCESS" else f"Run finished with status: {terminal}",
                    view_cleaned_up=True,
                )
            return RunStatusOut(
                run_id=run_id,
                state="TERMINATED",
                result_state="FAILED",
                message="Run metadata is missing job_run_id. The run may have been created before tracking was enabled.",
            )

        status = job_svc.get_run_status(meta.job_run_id)
        view_cleaned_up = False

        is_terminal = status.state in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED")

        if is_terminal and meta.view_fqn:
            try:
                view_svc.drop_view(meta.view_fqn)
                view_cleaned_up = True
                logger.info("Cleaned up temporary view: %s", meta.view_fqn)
            except Exception as cleanup_err:
                logger.warning("Failed to clean up view %s: %s", meta.view_fqn, cleanup_err)

        if is_terminal and status.state != "TERMINATED":
            update_run_status(
                sql,
                app_conf,
                _PROFILER_TABLE,
                run_id,
                status=status.state,
                error_message=status.message,
            )
        elif is_terminal and status.result_state and status.result_state == "CANCELED":
            update_run_status(
                sql,
                app_conf,
                _PROFILER_TABLE,
                run_id,
                status="CANCELED",
                error_message=status.message or "Canceled externally",
            )
        elif is_terminal and status.result_state and status.result_state != "SUCCESS":
            update_run_status(
                sql,
                app_conf,
                _PROFILER_TABLE,
                run_id,
                status="FAILED",
                error_message=status.message,
            )

        return RunStatusOut(
            run_id=run_id,
            state=status.state,
            result_state=status.result_state,
            message=status.message,
            view_cleaned_up=view_cleaned_up,
        )
    except Exception as e:
        logger.error("Failed to get profile run status (run_id=%s): %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get run status: {e}")


@router.post(
    "/runs/{run_id}/cancel",
    operation_id="cancelProfileRun",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def cancel_profile_run(
    run_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    user_role: CurrentUserRole,
) -> dict[str, str]:
    """Cancel a running profiler job."""
    try:
        canceling_user = obo_ws.current_user.me().user_name or "unknown"

        meta = get_run_metadata(sql, app_conf, _PROFILER_TABLE, run_id)
        is_owner = not meta.requesting_user or meta.requesting_user == canceling_user
        can_cancel_others = user_role in (UserRole.ADMIN, UserRole.RULE_APPROVER)
        if not is_owner and not can_cancel_others:
            raise HTTPException(status_code=403, detail="You can only cancel your own runs")
        if meta.job_run_id is None:
            update_run_status(
                sql,
                app_conf,
                _PROFILER_TABLE,
                run_id,
                status="FAILED",
                error_message="Run metadata missing job_run_id; marked as failed.",
            )
            return {"status": "canceled", "run_id": run_id}

        job_svc.cancel_run(meta.job_run_id)
        update_run_status(
            sql,
            app_conf,
            _PROFILER_TABLE,
            run_id,
            status="CANCELED",
            error_message=f"Canceled by {canceling_user}",
            canceled_by=canceling_user,
        )
        if meta.view_fqn:
            try:
                view_svc.drop_view(meta.view_fqn)
                logger.info("Cleaned up temporary view after cancel: %s", meta.view_fqn)
            except Exception as cleanup_err:
                logger.warning("Failed to clean up view %s after cancel: %s", meta.view_fqn, cleanup_err)
        return {"status": "canceled", "run_id": run_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to cancel profile run (run_id=%s): %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel run: {e}")


@router.get(
    "/runs/{run_id}/results",
    response_model=ProfileResultsOut,
    operation_id="getProfileRunResults",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_profile_run_results(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> ProfileResultsOut:
    """Read profiler results from the Delta table."""
    try:
        table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_profiling_results"
        row = job_svc.get_run_result_row(table, run_id)

        if row is None:
            raise HTTPException(status_code=404, detail=f"No results found for run_id={run_id}")

        if row.get("status") == "FAILED":
            raise HTTPException(
                status_code=500, detail=f"Profile run failed: {row.get('error_message', 'Unknown error')}"
            )

        summary_json = row.get("summary_json") or "{}"
        rules_json = row.get("generated_rules_json") or "[]"

        return ProfileResultsOut(
            run_id=run_id,
            source_table_fqn=row.get("source_table_fqn") or "",
            rows_profiled=int(v) if (v := row.get("rows_profiled")) else None,
            columns_profiled=int(v) if (v := row.get("columns_profiled")) else None,
            duration_seconds=float(v) if (v := row.get("duration_seconds")) else None,
            generated_rules=json.loads(rules_json),
            summary=json.loads(summary_json),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get profile results (run_id=%s): %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get profile results: {e}")
