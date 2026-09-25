"""Cached refresh coordination for Genie metadata dimensions."""

import asyncio

from databricks_labs_dqx_app.backend.cache import app_cache
from databricks_labs_dqx_app.backend.services.metadata_dim_service import MetadataDimService

_METADATA_DIM_REFRESH_TTL_SECONDS = 60 * 60


@app_cache.cached("genie:metadata-dims", ttl=_METADATA_DIM_REFRESH_TTL_SECONDS, reliable=True)
async def refresh_metadata_dims(metadata_dims: MetadataDimService) -> None:
    """Refresh Genie metadata dimensions at most once per TTL window."""
    await asyncio.to_thread(metadata_dims.refresh)
