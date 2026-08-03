"""Rules Marketplace routes — admin-only pack catalogue.

The whole router is hard-gated to :class:`UserRole.ADMIN`; the UI sidebar gate
and route redirect are conveniences, this is the real boundary. Packs are
bundled YAML, loaded + validated + cached by the marketplace loader.
"""

from collections.abc import Callable
from typing import Annotated, Any

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from fastapi import APIRouter, Depends

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_check_validator, require_role
from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.marketplace.models import MarketplacePacksOut

router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])


@router.get("/packs", response_model=MarketplacePacksOut, operation_id="listMarketplacePacks")
def list_marketplace_packs(
    validate_fn: Annotated[Callable[[list[dict[str, Any]]], ChecksValidationStatus], Depends(get_check_validator)],
) -> MarketplacePacksOut:
    """Return the full marketplace pack catalogue (admin only)."""
    return MarketplacePacksOut(packs=loader.load_packs(validate_fn))
