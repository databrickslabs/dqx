from __future__ import annotations

import json
from collections.abc import Callable
from typing import Annotated, Any
from uuid import uuid4

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentUserRole,
    get_app_settings_service,
    get_check_validator,
    get_conf,
    get_job_service,
    get_obo_ws,
    get_rules_catalog_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    get_view_service,
    require_role,
    require_runner,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
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
from databricks_labs_dqx_app.backend.run_status_manager import get_run_metadata, has_terminal_result, update_run_status
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
from databricks_labs_dqx_app.backend.services.view_service import ViewService

router = APIRouter()

_DRYRUN_TABLE = "dq_validation_runs"
_SQL_CHECK_PREFIX = "__sql_check__/"
_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_NON_VIEWERS = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


def _extract_sql_query(checks: list[dict[str, Any]]) -> str | None:
    """Return the SQL query from the first sql_query check, or None."""
    for check in checks:
        fn = (check.get("check") or {}).get("function", "")
        if fn == "sql_query":
            return (check.get("check") or {}).get("arguments", {}).get("query")
    return None


def _catalog_of(fqn: str) -> str:
    """Extract the catalog part from a fully qualified table name."""
    if fqn.startswith(_SQL_CHECK_PREFIX):
        fqn = fqn[len(_SQL_CHECK_PREFIX) :]
    parts = fqn.split(".", 1)
    return parts[0] if parts else ""


@router.get(
    "/runs",
    response_model=list[ValidationRunSummaryOut],
    operation_id="listValidationRuns",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def list_validation_runs(
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> list[ValidationRunSummaryOut]:
    """Return validation (dry-run) history filtered to user-accessible catalogs."""
    try:
        table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"
        rows = job_svc.list_dryrun_rows(table)
        results: list[ValidationRunSummaryOut] = []
        for row in rows:
            fqn = row.get("source_table_fqn") or ""
            if not fqn.startswith(_SQL_CHECK_PREFIX) and _catalog_of(fqn) not in user_catalogs:
                continue
            checks: list[dict[str, Any]] = []
            raw = row.get("checks_json")
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        checks = parsed
                except (json.JSONDecodeError, TypeError):
                    pass
            results.append(
                ValidationRunSummaryOut(
                    run_id=row.get("run_id") or "",
                    source_table_fqn=fqn,
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
                    error_message=row.get("error_message"),
                    checks=checks,
                )
            )
        return results
    except Exception as e:
        logger.error("Failed to list validation runs: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list validation runs: {e}")


@router.post(
    "/batch-from-catalog",
    response_model=BatchRunFromCatalogOut,
    operation_id="batchRunFromCatalog",
    # Executing approved rules from the Run Rules page is gated on the
    # orthogonal runner role (admins are implicit runners). Authors and
    # approvers without an explicit RUNNER mapping cannot trigger batch
    # runs even though they would otherwise pass the _NON_VIEWERS check.
    dependencies=[require_runner()],
)
def batch_run_from_catalog(
    body: BatchRunFromCatalogIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    rules_svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    settings_svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> BatchRunFromCatalogOut:
    """Read approved checks from the rules catalog for the given tables and submit dry-run jobs."""
    submitted: list[DryRunSubmitOut] = []
    errors: list[str] = []

    user = obo_ws.current_user.me()
    requesting_user = user.user_name or "unknown"
    runs_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"

    # Custom metrics apply globally — fetch once for the whole batch.
    custom_metrics = settings_svc.get_custom_metrics()

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

            config: dict[str, Any] = {
                "checks": approved_checks,
                "sample_size": body.sample_size,
                "source_table_fqn": table_fqn,
                "is_sql_check": is_sql_check,
            }
            if custom_metrics:
                config["custom_metrics"] = custom_metrics
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


@router.post(
    "",
    response_model=DryRunSubmitOut,
    operation_id="submitDryRun",
    dependencies=[require_role(*_NON_VIEWERS)],
)
def submit_dry_run(
    body: DryRunIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    validate_checks_fn: Annotated[Callable[[list[Any]], ChecksValidationStatus], Depends(get_check_validator)],
    settings_svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
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
        config: dict[str, Any] = {
            "checks": body.checks,
            "sample_size": body.sample_size,
            "source_table_fqn": body.table_fqn,
            "is_sql_check": is_sql_check,
        }
        if body.skip_history:
            config["skip_history"] = True
        # Skip custom metrics for preview runs — they're scoped to small
        # samples and have no downstream dashboard consumers, so the
        # extra observer columns just add noise.
        if not body.skip_history:
            custom_metrics = settings_svc.get_custom_metrics()
            if custom_metrics:
                config["custom_metrics"] = custom_metrics
        job_run_id = job_svc.submit_run(
            task_type="dryrun",
            view_fqn=view_fqn,
            config=config,
            run_id=run_id,
            requesting_user=requesting_user,
        )

        if not body.skip_history:
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


@router.get(
    "/runs/{run_id}/status",
    response_model=RunStatusOut,
    operation_id="getDryRunStatus",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_dry_run_status(
    run_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    view_svc: Annotated[ViewService, Depends(get_view_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    job_run_id_param: Annotated[int | None, Query(alias="job_run_id")] = None,
    view_fqn_param: Annotated[str | None, Query(alias="view_fqn")] = None,
) -> RunStatusOut:
    """Poll the status of a dry-run job. Cleans up the view when job terminates.

    When *job_run_id* and optionally *view_fqn* are supplied as query
    parameters the endpoint skips the database lookup, which is required
    for validation dry runs that are not recorded in the history table.
    Ownership of the *job_run_id* is verified against the OBO caller via the
    Databricks Jobs API so a client cannot use a guessed *view_fqn* to drop
    another user's temporary view.
    """
    try:
        if job_run_id_param is not None:
            requesting_user = obo_ws.current_user.me().user_name or "unknown"
            run_owner = job_svc.get_run_creator(job_run_id_param)
            if run_owner and run_owner != requesting_user:
                raise HTTPException(status_code=403, detail="You can only check status of your own runs")
            resolved_job_run_id = job_run_id_param
            resolved_view_fqn = view_fqn_param
            has_history_row = False
        else:
            meta = get_run_metadata(sql, app_conf, _DRYRUN_TABLE, run_id)
            if meta.job_run_id is None:
                terminal = has_terminal_result(sql, app_conf, _DRYRUN_TABLE, run_id)
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
            resolved_job_run_id = meta.job_run_id
            resolved_view_fqn = meta.view_fqn
            has_history_row = True

        status = job_svc.get_run_status(resolved_job_run_id)
        view_cleaned_up = False

        is_terminal = status.state in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED")

        if is_terminal and resolved_view_fqn:
            try:
                view_svc.drop_view(resolved_view_fqn)
                view_cleaned_up = True
                logger.info("Cleaned up temporary view: %s", resolved_view_fqn)
            except Exception as cleanup_err:
                logger.warning("Failed to clean up view %s: %s", resolved_view_fqn, cleanup_err)

        if has_history_row and is_terminal and status.state != "TERMINATED":
            update_run_status(
                sql,
                app_conf,
                _DRYRUN_TABLE,
                run_id,
                status=status.state,
                error_message=status.message,
            )
        elif has_history_row and is_terminal and status.result_state and status.result_state == "CANCELED":
            update_run_status(
                sql,
                app_conf,
                _DRYRUN_TABLE,
                run_id,
                status="CANCELED",
                error_message=status.message or "Canceled externally",
            )
        elif has_history_row and is_terminal and status.result_state and status.result_state != "SUCCESS":
            update_run_status(
                sql,
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get dry run status (run_id=%s): %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get run status: {e}")


@router.post(
    "/runs/{run_id}/cancel",
    operation_id="cancelDryRun",
    dependencies=[require_role(*_NON_VIEWERS)],
)
def cancel_dry_run(
    run_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    user_role: CurrentUserRole,
    job_run_id_param: Annotated[int | None, Query(alias="job_run_id")] = None,
) -> dict[str, str]:
    """Cancel a running dry-run job.

    When *job_run_id* is supplied as a query parameter the endpoint
    skips the database lookup (needed for validation dry runs that were
    not recorded in the history table). Ownership is still enforced via
    the Databricks Jobs API — only admins/approvers may cancel others' runs.

    Note: a non-owner caller can confirm whether a given *job_run_id* exists
    (and belongs to someone else) by observing the 403 response. This is
    accepted — Databricks job IDs are large random integers and the response
    leaks no identifying information beyond existence.
    """
    try:
        canceling_user = obo_ws.current_user.me().user_name or "unknown"
        can_cancel_others = user_role in (UserRole.ADMIN, UserRole.RULE_APPROVER)

        if job_run_id_param is not None:
            run_owner = job_svc.get_run_creator(job_run_id_param)
            if run_owner and run_owner != canceling_user and not can_cancel_others:
                raise HTTPException(status_code=403, detail="You can only cancel your own runs")
            job_svc.cancel_run(job_run_id_param)
            return {"status": "canceled", "run_id": run_id}

        meta = get_run_metadata(sql, app_conf, _DRYRUN_TABLE, run_id)
        is_owner = not meta.requesting_user or meta.requesting_user == canceling_user
        if not is_owner and not can_cancel_others:
            raise HTTPException(status_code=403, detail="You can only cancel your own runs")
        if meta.job_run_id is None:
            update_run_status(
                sql,
                app_conf,
                _DRYRUN_TABLE,
                run_id,
                status="FAILED",
                error_message="Run metadata missing job_run_id; marked as failed.",
            )
            return {"status": "canceled", "run_id": run_id}

        job_svc.cancel_run(meta.job_run_id)
        update_run_status(
            sql,
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


@router.get(
    "/runs/{run_id}/results",
    response_model=DryRunResultsOut,
    operation_id="getDryRunResults",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_dry_run_results(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DryRunResultsOut:
    """Read dry-run results from the Delta table."""
    try:
        table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"
        row = job_svc.get_run_result_row(table, run_id)

        if row is None:
            raise HTTPException(status_code=404, detail=f"No results found for run_id={run_id}")

        fqn = row.get("source_table_fqn") or ""
        if not fqn.startswith(_SQL_CHECK_PREFIX) and _catalog_of(fqn) not in user_catalogs:
            raise HTTPException(status_code=403, detail="You do not have access to this run's catalog")

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
