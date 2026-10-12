"""AI bootstrap — serving-endpoint grants + embeddings backfill.

Rule suggestions use in-app cosine retrieval over the OLTP
``dq_rule_embeddings`` corpus (:class:`~databricks_labs_dqx_app.backend.services.rule_retriever.CosineRuleRetriever`),
not Databricks Vector Search. This helper still runs two best-effort steps
when AI is enabled so that path works out of the box:

1. Grants ``CAN_QUERY`` on the configured AI/embedding serving endpoints so
   OBO AI calls work without a manual per-user grant.
2. Re-embeds every currently-published rule so pre-existing rules (including
   built-ins) become searchable via cosine retrieval.

**Never raises.** Any Databricks SDK failure is logged and swallowed — the
caller (app startup, or the admin "enable AI" flow) must not crash or block
on this.
"""

import asyncio
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel

from databricks_labs_dqx_app.backend.config import conf
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService

logger = logging.getLogger(__name__)

# Least-privilege grant for OBO AI calls (see ``AIGateway`` module docstring):
# query-only, never CAN_MANAGE.
_GRANT_PERMISSION_LEVEL = ServingEndpointPermissionLevel.CAN_QUERY

# Fallback grant target when no admin group is configured (``DQX_ADMIN_GROUP``)
# — the built-in account-wide group, so OBO AI calls work out of the box
# without requiring a manual per-user grant on every workspace.
_FALLBACK_GRANT_GROUP = "account users"


class AiBootstrap:
    """Best-effort grants + embeddings backfill for AI rule suggestions."""

    def __init__(
        self,
        sp_ws: WorkspaceClient,
        app_settings: AppSettingsService,
        embeddings: RuleEmbeddingsService,
        registry: RegistryService,
    ) -> None:
        self._sp_ws = sp_ws
        self._app_settings = app_settings
        self._embeddings = embeddings
        self._registry = registry

    async def ensure_ai_ready(self) -> None:
        """Grant serving-endpoint access and backfill embeddings. Never raises."""
        await asyncio.to_thread(self._grant_serving_endpoint_access)
        await asyncio.to_thread(self._backfill_published_rules)

    def _grant_serving_endpoint_access(self) -> None:
        """Best-effort grant of ``CAN_QUERY`` on the AI + embedding serving endpoints.

        Targets ``DQX_ADMIN_GROUP`` (``conf.admin_group``) if configured,
        otherwise the built-in account-wide ``account users`` group. Never
        raises: system/foundation-model endpoints (e.g. ``databricks-gpt-5-5``)
        reject permission changes, and that failure — like any other SDK
        error — is logged and swallowed so it can never block the AI-enable
        save.
        """
        group = (conf.admin_group or "").strip() or _FALLBACK_GRANT_GROUP
        endpoint_names = {
            self._app_settings.get_ai_endpoint_name(),
            self._app_settings.get_embedding_endpoint_name(),
        }
        for endpoint_name in endpoint_names:
            if not endpoint_name:
                continue
            self._grant_endpoint_can_query(endpoint_name, group)

    def _grant_endpoint_can_query(self, endpoint_name: str, group: str) -> None:
        try:
            endpoint = self._sp_ws.serving_endpoints.get(endpoint_name)
            endpoint_id = endpoint.id if endpoint else None
            if not endpoint_id:
                logger.warning("Serving endpoint %s has no id; skipping permission grant", endpoint_name)
                return
            self._sp_ws.serving_endpoints.update_permissions(
                serving_endpoint_id=endpoint_id,
                access_control_list=[
                    ServingEndpointAccessControlRequest(
                        group_name=group,
                        permission_level=_GRANT_PERMISSION_LEVEL,
                    )
                ],
            )
            logger.info("Granted CAN_QUERY on serving endpoint %s to group %s", endpoint_name, group)
        except DatabricksError as e:
            # Not every endpoint is grantable — e.g. system/foundation-model
            # endpoints reject permission changes. Non-fatal.
            logger.warning("Could not grant CAN_QUERY on serving endpoint %s to group %s: %s", endpoint_name, group, e)
        except Exception:
            logger.warning(
                "Unexpected error granting CAN_QUERY on serving endpoint %s (non-fatal)", endpoint_name, exc_info=True
            )

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
