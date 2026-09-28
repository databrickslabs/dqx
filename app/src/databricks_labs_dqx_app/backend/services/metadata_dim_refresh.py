"""Cached refresh coordination for Genie metadata dimensions."""

import asyncio
import logging

from databricks_labs_dqx_app.backend.cache import app_cache
from databricks_labs_dqx_app.backend.services.metadata_dim_service import MetadataDimService
from databricks_labs_dqx_app.backend.services.resource_tagging_service import ResourceTaggingService, TagTarget

_METADATA_DIM_REFRESH_TTL_SECONDS = 60 * 60
logger = logging.getLogger(__name__)


@app_cache.cached("genie:metadata-dims", ttl=_METADATA_DIM_REFRESH_TTL_SECONDS, reliable=True)
async def refresh_metadata_dims(
    metadata_dims: MetadataDimService,
    resource_tagger: ResourceTaggingService | None = None,
    tag_targets: tuple[TagTarget, ...] = (),
) -> None:
    """Refresh Genie metadata dimensions and restore their tags at most once per TTL window."""
    await asyncio.to_thread(metadata_dims.refresh)
    if resource_tagger is not None:
        try:
            await asyncio.to_thread(resource_tagger.reconcile, tag_targets)
        except Exception:
            logger.warning("Could not restore DQX Studio ownership tags on Genie metadata dimensions")
