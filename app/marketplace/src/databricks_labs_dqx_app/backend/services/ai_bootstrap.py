"""AI bootstrap — embeddings backfill.

Rule suggestions use in-app cosine retrieval over the OLTP
``dq_rule_embeddings`` corpus (:class:`~databricks_labs_dqx_app.backend.services.rule_retriever.CosineRuleRetriever`),
not Databricks Vector Search. When AI is enabled this helper re-embeds every
currently-published rule so pre-existing rules (including built-ins) become
searchable via cosine retrieval.

Access to the AI models is not granted here: model services are Unity Catalog
securables, so the setup AI step (``setup/ai_access.py``) checks the grants and
tells an administrator which ones are missing.

**Never raises.** Any failure is logged and swallowed — the caller (app
startup, or the admin "enable AI" flow) must not crash or block on this.
"""

import asyncio
import logging

from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService

logger = logging.getLogger(__name__)


class AiBootstrap:
    """Best-effort embeddings backfill for AI rule suggestions."""

    def __init__(self, embeddings: RuleEmbeddingsService, registry: RegistryService) -> None:
        self._embeddings = embeddings
        self._registry = registry

    async def ensure_ai_ready(self) -> None:
        """Backfill embeddings for published rules. Never raises."""
        await asyncio.to_thread(self._backfill_published_rules)

    def _backfill_published_rules(self) -> None:
        """Re-embed every currently-published rule (best-effort).

        So pre-existing published rules (including built-ins) are searchable
        via cosine retrieval as soon as AI is enabled, without requiring an
        admin to separately trigger ``POST /backfill-embeddings``. Never raises.
        """
        try:
            published = self._registry.list_rules(status="approved")
            embedded = self._embeddings.backfill(published)
            logger.info(
                "AI bootstrap embeddings backfill embedded %d/%d published rule(s)",
                embedded,
                len(published),
            )
        except Exception:
            logger.warning("AI bootstrap embeddings backfill failed (non-fatal)", exc_info=True)
