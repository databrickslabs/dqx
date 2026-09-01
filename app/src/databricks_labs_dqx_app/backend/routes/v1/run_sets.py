"""Run-set query routes (Data Products Task 3).

Read-only surface over :class:`~databricks_labs_dqx_app.backend.services.run_sets.RunSetService` —
run sets are minted by the run-submission services (``BindingRunService``
today; ``DataProductService`` in Task 4), never by a route directly.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_run_set_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import RunSetDetailOut, RunSetMemberDetailOut, RunSetSummaryOut
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService

router = APIRouter()

# View-only surface (design spec §5: "View products/runs: VIEWER+").
_VIEWERS_PLUS = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


@router.get(
    "",
    response_model=list[RunSetSummaryOut],
    operation_id="listRunSets",
    dependencies=[require_role(*_VIEWERS_PLUS)],
)
def list_run_sets(
    run_set_svc: Annotated[RunSetService, Depends(get_run_set_service)],
    product_id: Annotated[str, Query(description="Data product to list run sets for")],
    limit: Annotated[int, Query(le=200)] = 50,
) -> list[RunSetSummaryOut]:
    """List the run sets triggered for a data product, newest first."""
    try:
        summaries = run_set_svc.list_for_product(product_id, limit=limit)
        return [
            RunSetSummaryOut(
                run_set_id=s.run_set_id,
                product_id=s.product_id,
                product_version=s.product_version,
                source=s.source,
                trigger=s.trigger,
                created_by=s.created_by,
                created_at=s.created_at.isoformat() if s.created_at else None,
                member_count=s.member_count,
                status=s.status,
            )
            for s in summaries
        ]
    except Exception as e:
        logger.error(f"Failed to list run sets for product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list run sets: {e}")


@router.get(
    "/{run_set_id}",
    response_model=RunSetDetailOut,
    operation_id="getRunSet",
    dependencies=[require_role(*_VIEWERS_PLUS)],
)
def get_run_set(
    run_set_id: str,
    run_set_svc: Annotated[RunSetService, Depends(get_run_set_service)],
) -> RunSetDetailOut:
    """Get a run set and its resolved members (table, version, status, counts)."""
    try:
        detail = run_set_svc.get(run_set_id)
        return RunSetDetailOut(
            run_set_id=detail.run_set_id,
            product_id=detail.product_id,
            product_version=detail.product_version,
            source=detail.source,
            trigger=detail.trigger,
            created_by=detail.created_by,
            created_at=detail.created_at.isoformat() if detail.created_at else None,
            status=detail.status,
            members=[
                RunSetMemberDetailOut(
                    run_id=m.run_id,
                    binding_id=m.binding_id,
                    table_fqn=m.table_fqn,
                    binding_version=m.binding_version,
                    status=m.status,
                    total_rows=m.total_rows,
                    valid_rows=m.valid_rows,
                    invalid_rows=m.invalid_rows,
                    error_rows=m.error_rows,
                    warning_rows=m.warning_rows,
                )
                for m in detail.members
            ],
        )
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get run set {run_set_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get run set: {e}")
