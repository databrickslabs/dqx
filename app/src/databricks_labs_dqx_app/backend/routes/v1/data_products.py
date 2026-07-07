"""Data Products routes (Data Products Task 4).

CRUD + publish + member management + run fan-out over
:class:`~databricks_labs_dqx_app.backend.services.data_product_service.DataProductService`.
RBAC (design spec §5): view VIEWER+; create/update/delete/members/publish
RULE_AUTHOR+ (no approver gate — members are already approval-gated via
their own binding lifecycle); run uses the same orthogonal RUNNER gate as
``runMonitoredTable`` / ``batch_run_from_catalog``.
"""

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_data_product_service, get_obo_ws, require_role, require_runner
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    AddDataProductMemberIn,
    CreateDataProductIn,
    DataProductOut,
    RunDataProductIn,
    RunDataProductOut,
    UpdateDataProductIn,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductService,
    DuplicateDataProductNameError,
    NoRunnableMembersError,
)

router = APIRouter()

_VIEWERS_PLUS = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


def _current_user_email(obo_ws: WorkspaceClient) -> str:
    user = obo_ws.current_user.me()
    return user.user_name or "unknown"


# ------------------------------------------------------------------
# List / Get
# ------------------------------------------------------------------


@router.get(
    "",
    response_model=list[DataProductOut],
    operation_id="listDataProducts",
    dependencies=[require_role(*_VIEWERS_PLUS)],
)
def list_data_products(
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
) -> list[DataProductOut]:
    """List every data product with resolved members and list-view counters."""
    try:
        return [DataProductOut.from_domain(d) for d in svc.list_products()]
    except Exception as e:
        logger.error(f"Failed to list data products: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list data products: {e}")


@router.get(
    "/{product_id}",
    response_model=DataProductOut,
    operation_id="getDataProduct",
    dependencies=[require_role(*_VIEWERS_PLUS)],
)
def get_data_product(
    product_id: str,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
) -> DataProductOut:
    """Get a single data product with its resolved members."""
    try:
        detail = svc.get(product_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Data product not found: {product_id}")
        return DataProductOut.from_domain(detail)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get data product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get data product: {e}")


# ------------------------------------------------------------------
# Create / Update / Delete
# ------------------------------------------------------------------


@router.post(
    "",
    response_model=DataProductOut,
    operation_id="createDataProduct",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def create_data_product(
    body: CreateDataProductIn,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DataProductOut:
    """Create a new data product (status ``draft``, no approver gate)."""
    try:
        user_email = _current_user_email(obo_ws)
        product = svc.create(body.name, body.description, body.steward, user_email)
        detail = svc.get(product.product_id)
        assert detail is not None  # just created
        return DataProductOut.from_domain(detail)
    except DuplicateDataProductNameError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create data product: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create data product: {e}")


@router.patch(
    "/{product_id}",
    response_model=DataProductOut,
    operation_id="updateDataProduct",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def update_data_product(
    product_id: str,
    body: UpdateDataProductIn,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DataProductOut:
    """Apply a partial update. Any successful update flips ``published`` -> ``draft``."""
    try:
        user_email = _current_user_email(obo_ws)
        updates = body.model_dump(exclude_unset=True)
        svc.update(product_id, updates, user_email)
        detail = svc.get(product_id)
        assert detail is not None  # just updated
        return DataProductOut.from_domain(detail)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except DuplicateDataProductNameError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update data product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update data product: {e}")


@router.delete(
    "/{product_id}",
    operation_id="deleteDataProduct",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def delete_data_product(
    product_id: str,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
) -> dict[str, str]:
    """Delete a data product and its members."""
    try:
        svc.delete(product_id)
        return {"status": "deleted", "product_id": product_id}
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete data product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete data product: {e}")


# ------------------------------------------------------------------
# Members
# ------------------------------------------------------------------


@router.post(
    "/{product_id}/members",
    response_model=DataProductOut,
    operation_id="addDataProductMember",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def add_data_product_member(
    product_id: str,
    body: AddDataProductMemberIn,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DataProductOut:
    """Add (or update the pin of) a member. Upserts by ``binding_id``."""
    try:
        user_email = _current_user_email(obo_ws)
        svc.add_member(product_id, body.binding_id, body.pinned_version, user_email)
        detail = svc.get(product_id)
        assert detail is not None  # just added a member to it
        return DataProductOut.from_domain(detail)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to add member to data product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add data product member: {e}")


@router.delete(
    "/{product_id}/members/{member_id}",
    response_model=DataProductOut,
    operation_id="removeDataProductMember",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def remove_data_product_member(
    product_id: str,
    member_id: str,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DataProductOut:
    """Remove a member from a data product."""
    try:
        user_email = _current_user_email(obo_ws)
        svc.remove_member(product_id, member_id, user_email)
        detail = svc.get(product_id)
        assert detail is not None  # just removed a member from it
        return DataProductOut.from_domain(detail)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to remove member {member_id} from data product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to remove data product member: {e}")


# ------------------------------------------------------------------
# Publish
# ------------------------------------------------------------------


@router.post(
    "/{product_id}/publish",
    response_model=DataProductOut,
    operation_id="publishDataProduct",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def publish_data_product(
    product_id: str,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DataProductOut:
    """Publish a data product — bumps ``version`` by 1 and sets ``status='published'``."""
    try:
        user_email = _current_user_email(obo_ws)
        svc.publish(product_id, user_email)
        detail = svc.get(product_id)
        assert detail is not None  # just published it
        return DataProductOut.from_domain(detail)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to publish data product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to publish data product: {e}")


# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------


@router.post(
    "/{product_id}/run",
    response_model=RunDataProductOut,
    operation_id="runDataProduct",
    # Same orthogonal RUNNER gate as ``runMonitoredTable`` / batch_run_from_catalog.
    dependencies=[require_runner()],
)
def run_data_product(
    product_id: str,
    body: RunDataProductIn,
    svc: Annotated[DataProductService, Depends(get_data_product_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RunDataProductOut:
    """Run every runnable member of a data product through a shared run set."""
    try:
        user_email = _current_user_email(obo_ws)
        result = svc.run(product_id, source=body.source, user_email=user_email, trigger="manual")
        return RunDataProductOut.from_domain(result)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except NoRunnableMembersError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to run data product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to run data product: {e}")
