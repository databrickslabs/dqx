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
from databricks_labs_dqx_app.backend.dependencies import get_check_validator, get_registry_service, require_role
from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.marketplace.models import MarketplacePacksOut
from databricks_labs_dqx_app.backend.registry_models import get_rule_name
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService

router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])


@router.get("/packs", response_model=MarketplacePacksOut, operation_id="listMarketplacePacks")
def list_marketplace_packs(
    validate_fn: Annotated[Callable[[list[dict[str, Any]]], ChecksValidationStatus], Depends(get_check_validator)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
) -> MarketplacePacksOut:
    """Return the full marketplace pack catalogue (admin only).

    Each rule is flagged ``imported`` when a rule of the same name already
    exists in the registry (any active status), so the UI can disable adding it
    again. Name-match, not fingerprint: a pack rule the user has since edited
    still reads as already-added, which is the intent for the disable state.
    """
    packs = loader.load_packs(validate_fn)
    existing_names = {
        name for r in registry.list_rules() if (name := get_rule_name(r.user_metadata)) is not None
    }
    # Copy (never mutate the cached packs) with the per-rule imported flag set.
    packs_out = [
        pack.model_copy(
            update={
                "rules": [rule.model_copy(update={"imported": rule.name in existing_names}) for rule in pack.rules]
            }
        )
        for pack in packs
    ]
    return MarketplacePacksOut(packs=packs_out)
