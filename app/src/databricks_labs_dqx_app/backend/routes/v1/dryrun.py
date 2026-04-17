from __future__ import annotations

import json
from collections.abc import Callable
from typing import Annotated, Any
from uuid import uuid4

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_check_validator,
    get_conf,
    get_job_service,
    get_obo_ws,
    get_rules_catalog_service,
    get_view_service,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    BatchRunFromCatalogIn,
    BatchRunFromCatalogOut,
    DryRunIn,
    DryRunResultsOut,
    DryRunSubmitOut,
    RunStatusOut,
    ValidationRunSummaryOut,
)
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.run_status_manager import get_run_metadata, update_run_status
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
from databricks_labs_dqx_app.backend.services.view_service import ViewService

router = APIRouter()

_DRYRUN_TABLE = "dq_validation_runs"
_SQL_CHECK_PREFIX = "__sql_check__/"


def _extract_sql_query(checks: list[dict[str, Any]]) -> str | None:
    """Return the SQL query from the first sql_query check, or None."""
    for check in checks:
        fn = (check.get("check") or {}).get("function", "")
        if fn == "sql_query":
            return (check.get("check") or {}).get("arguments", {}).get("query")
    return None


@router.get("/runs", response_model=list[ValidationRunSummaryOut], operation_id="listValidationRuns")
def list_validation_runs(
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> list[ValidationRunSummaryOut]:
    """Return validation (dry-run) history, newest first."""
    try:
        table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"
        rows = job_svc.list_dryrun_rows(table)
        return [
            ValidationRunSummaryOut(
                run_id=row.get("run_id") or "",
                source_table_fqn=row.get("source_table_fqn") or "",
                status=row.get("status"),
                requesting_user=row.get("requesting_user"),
                canceled_by=row.get("canceled_by"),
                updated_at=row.get("updated_at"),
                sample_size=int(v) if (v := row.get("sample_size")) else None,
                total_rows=int(v) if (v := row.get("total_rows")) else None,
                valid_rows=int(v) if (v := row.get("valid_rows")) else None,
                invalid_rows=int(v) if (v := row.get("invalid_rows")) else None,
                created_at=row.get("created_at"),
                run_type=row.get("run_type"),
            )
            for row in rows
        ]
    except Exception as e:
        logger.error("Failed to list validation runs: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list validation runs: {e}")


@router.post(
    "/batch-from-catalog",
    response_model=BatchRunFromCatalogOut,
    operation_id="batchRunFromCatalog",
)
def batch_run_from_catalog(
    body: BatchRunFromCatalogIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    rules_svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> BatchRunFromCatalogOut:
    """Read approved checks from the rules catalog for the given tables and submit dry-run jobs."""
    submitted: list[DryRunSubmitOut] = []
    errors: list[str] = []

    user = obo_ws.current_user.me()
    requesting_user = user.user_name or "unknown"
    runs_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"

    for table_fqn in body.table_fqns:
        try:
            approved_checks = rules_svc.get_approved_checks_for_table(table_fqn)
            if not approved_checks:
                errors.append(f"{table_fqn}: no approved rules found in catalog")
                continue

            run_id = uuid4().hex[:16]
            is_sql_check = table_fqn.startswith(_SQL_CHECK_PREFIX)

            if is_sql_check:
                sql_query = _extract_sql_query(approved_checks)
                if not sql_query:
                    errors.append(f"{table_fqn}: SQL check has no query")
                    continue
                view_fqn = view_svc.create_view_from_sql(sql_query)
            else:
                view_fqn = view_svc.create_view(table_fqn)

            config = {
                "checks": approved_checks,
                "sample_size": body.sample_size,
                "source_table_fqn": table_fqn,
                "is_sql_check": is_sql_check,
            }
            job_run_id = job_svc.submit_run(
                task_type="dryrun",
                view_fqn=view_fqn,
                config=config,
                run_id=run_id,
                requesting_user=requesting_user,
            )
            submitted.append(DryRunSubmitOut(run_id=run_id, job_run_id=job_run_id, view_fqn=view_fqn))

            job_svc.record_dryrun_started(
                table=runs_table,
                run_id=run_id,
                requesting_user=requesting_user,
                source_table_fqn=table_fqn,
                view_fqn=view_fqn,
                sample_size=body.sample_size,
                job_run_id=job_run_id,
            )
        except Exception as e:
            logger.error("Failed to submit run for %s: %s", table_fqn, e, exc_info=True)
            errors.append(f"{table_fqn}: {e}")

    return BatchRunFromCatalogOut(submitted=submitted, errors=errors)


@router.post("", response_model=DryRunSubmitOut, operation_id="submitDryRun")
def submit_dry_run(
    body: DryRunIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    validate_checks_fn: Annotated[Callable[[list[Any]], ChecksValidationStatus], Depends(get_check_validator)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> DryRunSubmitOut:
    """Validate checks, create a temporary view (OBO), and submit a dry-run job (SP)."""
    try:
        # Validate checks first
        validation = validate_checks_fn(body.checks)
        if validation.has_errors:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid checks: {validation.errors}",
            )

        run_id = uuid4().hex[:16]

        # Get requesting user email
        user = obo_ws.current_user.me()
        requesting_user = user.user_name or "unknown"

        # Create view using OBO token — inherits user's table permissions
        is_sql_check = body.table_fqn.startswith(_SQL_CHECK_PREFIX)
        if is_sql_check:
            sql_query = _extract_sql_query(body.checks)
            if not sql_query:
                raise HTTPException(status_code=400, detail="SQL check has no query")
            view_fqn = view_svc.create_view_from_sql(sql_query)
        else:
            view_fqn = view_svc.create_view(body.table_fqn)

        # Submit job using SP credentials
        config = {
            "checks": body.checks,
            "sample_size": body.sample_size,
            "source_table_fqn": body.table_fqn,
            "is_sql_check": is_sql_check,
        }
        job_run_id = job_svc.submit_run(
            task_type="dryrun",
            view_fqn=view_fqn,
            config=config,
            run_id=run_id,
            requesting_user=requesting_user,
        )

        runs_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"
        job_svc.record_dryrun_started(
            table=runs_table,
            run_id=run_id,
            requesting_user=requesting_user,
            source_table_fqn=body.table_fqn,
            view_fqn=view_fqn,
            sample_size=body.sample_size,
            job_run_id=job_run_id,
        )

        return DryRunSubmitOut(run_id=run_id, job_run_id=job_run_id, view_fqn=view_fqn)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to submit dry run for %s: %s", body.table_fqn, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit dry run: {e}")


@router.get("/runs/{run_id}/status", response_model=RunStatusOut, operation_id="getDryRunStatus")
def get_dry_run_status(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> RunStatusOut:
    """Poll the status of a dry-run job. Cleans up the view when job terminates."""
    try:
        meta = get_run_metadata(job_svc, app_conf, _DRYRUN_TABLE, run_id)
        if meta.job_run_id is None:
            raise HTTPException(status_code=404, detail=f"No job_run_id found for run_id={run_id}")

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
                job_svc,
                app_conf,
                _DRYRUN_TABLE,
                run_id,
                status=status.state,
                error_message=status.message,
            )
        elif is_terminal and status.result_state and status.result_state != "SUCCESS":
            update_run_status(
                job_svc,
                app_conf,
                _DRYRUN_TABLE,
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
        logger.error("Failed to get dry run status (run_id=%s): %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get run status: {e}")


@router.post("/runs/{run_id}/cancel", operation_id="cancelDryRun")
def cancel_dry_run(
    run_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> dict[str, str]:
    """Cancel a running dry-run job."""
    try:
        canceling_user = obo_ws.current_user.me().user_name or "unknown"

        meta = get_run_metadata(job_svc, app_conf, _DRYRUN_TABLE, run_id)
        if meta.requesting_user and meta.requesting_user != canceling_user:
            raise HTTPException(status_code=403, detail="You can only cancel your own runs")
        if meta.job_run_id is None:
            raise HTTPException(status_code=404, detail=f"No job_run_id found for run_id={run_id}")

        job_svc.cancel_run(meta.job_run_id)
        update_run_status(
            job_svc,
            app_conf,
            _DRYRUN_TABLE,
            run_id,
            status="CANCELED",
            error_message=f"Canceled by {canceling_user}",
            canceled_by=canceling_user,
        )
        return {"status": "canceled", "run_id": run_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to cancel dry run (run_id=%s): %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel run: {e}")


@router.get("/runs/{run_id}/results", response_model=DryRunResultsOut, operation_id="getDryRunResults")
def get_dry_run_results(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> DryRunResultsOut:
    """Read dry-run results from the Delta table."""
    try:
        table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"
        row = job_svc.get_run_result_row(table, run_id)

        if row is None:
            raise HTTPException(status_code=404, detail=f"No results found for run_id={run_id}")

        if row.get("status") == "FAILED":
            raise HTTPException(status_code=500, detail=f"Dry run failed: {row.get('error_message', 'Unknown error')}")

        error_summary_json = row.get("error_summary_json") or "[]"
        sample_invalid_json = row.get("sample_invalid_json") or "[]"

        return DryRunResultsOut(
            run_id=run_id,
            source_table_fqn=row.get("source_table_fqn") or "",
            total_rows=int(v) if (v := row.get("total_rows")) else None,
            valid_rows=int(v) if (v := row.get("valid_rows")) else None,
            invalid_rows=int(v) if (v := row.get("invalid_rows")) else None,
            error_summary=json.loads(error_summary_json),
            sample_invalid=json.loads(sample_invalid_json),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get dry run results (run_id=%s): %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get dry run results: {e}")
