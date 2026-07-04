"""Vector Search auto-provisioning — Rules Registry Phase 7F.

:class:`VectorStoreProvisioner` provides an idempotent, best-effort,
non-blocking ``ensure_vector_store()`` that creates the Vector Search
endpoint + index backing the rule-mapping suggester
(:class:`~databricks_labs_dqx_app.backend.services.rule_retriever.VectorSearchRetriever`)
when an admin has configured ``embedding_endpoint_name`` /
``vs_endpoint_name`` / ``vs_index_name`` (see
:class:`~databricks_labs_dqx_app.backend.services.app_settings_service.AppSettingsService`),
if they don't already exist.

**Never raises.** Any Databricks SDK failure (permission denied, transient
outage, quota) is logged and swallowed — the caller (app startup, or the
admin "enable AI" flow) must not crash or block on this. Endpoint/index
creation is asynchronous on the Databricks side and can take minutes to
reach ``ONLINE``; this method only *submits* the creation request and
returns immediately. The suggester keeps reporting ``available=False``
(via :meth:`VectorSearchRetriever.is_available`) until the index comes
online — no additional polling is done here.

**Index type — Direct Access, not Delta Sync.** Rule embeddings are stored
in ``dq_rule_embeddings``, an OLTP table routed through
``OltpExecutorProtocol`` (Lakebase Postgres, or the Delta-OLTP-fallback
table used when Lakebase is disabled) rather than a dedicated UC-Delta
corpus a Delta-Sync index could read directly. A Direct Access index is
therefore the only index type that fits without provisioning new Delta
infrastructure, and the app is responsible for keeping it in sync: see
:meth:`~databricks_labs_dqx_app.backend.services.rule_embeddings.RuleEmbeddingsService.embed_and_store`,
which upserts into both the OLTP corpus table and (best-effort) this index.

No Vector Search resource is declared in ``app/databricks.yml`` — this
stays entirely config-driven so a deploy with no Vector Search
infrastructure provisioned (and AI left off) never attempts a call.
"""

from __future__ import annotations

import asyncio
import json
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.errors.platform import NotFound
from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel
from databricks.sdk.service.vectorsearch import (
    DirectAccessVectorIndexSpec,
    EmbeddingVectorColumn,
    EndpointType,
    VectorIndexType,
)

from databricks_labs_dqx_app.backend.config import conf
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService

logger = logging.getLogger(__name__)

# Schema of the Direct Access index — mirrors the ``dq_rule_embeddings``
# OLTP corpus table (see ``rule_embeddings.RuleEmbeddingsService``).
PRIMARY_KEY_COLUMN = "rule_id"
TEXT_COLUMN = "embed_text"
EMBEDDING_COLUMN = "embedding"

# Short, static probe text used only to discover the embedding endpoint's
# output dimension when creating the index — never logged or stored.
_DIMENSION_PROBE_TEXT = "dqx rule embedding dimension probe"

# Least-privilege grant for OBO AI calls (see ``AIGateway`` module docstring):
# query-only, never CAN_MANAGE.
_GRANT_PERMISSION_LEVEL = ServingEndpointPermissionLevel.CAN_QUERY

# Fallback grant target when no admin group is configured (``DQX_ADMIN_GROUP``)
# — the built-in account-wide group, so OBO AI calls work out of the box
# without requiring a manual per-user grant on every workspace.
_FALLBACK_GRANT_GROUP = "account users"


class VectorStoreProvisioner:
    """Best-effort, idempotent creator of the Vector Search endpoint + Direct Access index."""

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

    async def ensure_vector_store(self) -> None:
        """Create the configured Vector Search endpoint + index if missing.

        Also runs two independent, best-effort steps on every call — neither
        gated on the Vector Search index config, and neither able to fail
        this method or each other:

        1. Grants ``CAN_QUERY`` on the configured AI/embedding serving
           endpoints (see :meth:`_grant_serving_endpoint_access`) so OBO AI
           calls work without a manual per-user grant.
        2. Re-embeds every currently-published rule once the endpoint/index
           are confirmed present (see :meth:`_backfill_published_rules`),
           so pre-existing rules (including the built-ins) become
           searchable as soon as the index exists.

        The endpoint/index creation itself is a no-op when
        ``embedding_endpoint_name`` / ``vs_endpoint_name`` / ``vs_index_name``
        aren't all configured. Never raises.
        """
        await asyncio.to_thread(self._grant_serving_endpoint_access)

        embedding_endpoint = self._app_settings.get_embedding_endpoint_name()
        vs_endpoint = self._app_settings.get_vs_endpoint_name()
        vs_index = self._app_settings.get_vs_index_name()
        if not embedding_endpoint or not vs_endpoint or not vs_index:
            logger.debug("Vector Search settings incomplete; skipping ensure_vector_store")
            return

        try:
            await asyncio.to_thread(self._ensure_endpoint, vs_endpoint)
            await asyncio.to_thread(self._ensure_index, vs_endpoint, vs_index)
        except Exception:
            logger.warning("ensure_vector_store failed (non-fatal)", exc_info=True)

        await asyncio.to_thread(self._backfill_published_rules)

    # ------------------------------------------------------------------
    # Serving-endpoint permissions — least-privilege CAN_QUERY grant so OBO
    # AI calls (AIGateway, embedding calls) work without a manual per-user
    # grant on every workspace.
    # ------------------------------------------------------------------

    def _grant_serving_endpoint_access(self) -> None:
        """Best-effort grant of ``CAN_QUERY`` on the AI + embedding serving endpoints.

        Targets ``DQX_ADMIN_GROUP`` (``conf.admin_group``) if configured,
        otherwise the built-in account-wide ``account users`` group. Never
        raises: system/foundation-model endpoints (e.g. ``databricks-gpt-5-5``)
        reject permission changes, and that failure — like any other SDK
        error — is logged and swallowed so it can never block the AI-enable
        save or Vector Search provisioning.
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
            logger.warning(
                "Could not grant CAN_QUERY on serving endpoint %s to group %s: %s", endpoint_name, group, e
            )
        except Exception:
            logger.warning(
                "Unexpected error granting CAN_QUERY on serving endpoint %s (non-fatal)", endpoint_name, exc_info=True
            )

    # ------------------------------------------------------------------
    # Embeddings backfill — best-effort catch-up for pre-existing rules.
    # ------------------------------------------------------------------

    def _backfill_published_rules(self) -> None:
        """Re-embed every currently-published rule after provisioning (best-effort).

        Runs after the Vector Search endpoint/index are confirmed present,
        so pre-existing published rules (including the 75 built-ins) are
        searchable as soon as the index exists, without requiring an admin
        to separately trigger ``POST /backfill-embeddings``. Never raises.
        """
        try:
            published = self._registry.list_rules(status="approved")
            embedded = self._embeddings.backfill(published)
            logger.info(
                "Post-provisioning embeddings backfill embedded %d/%d published rule(s)",
                embedded,
                len(published),
            )
        except Exception:
            logger.warning("Post-provisioning embeddings backfill failed (non-fatal)", exc_info=True)

    # ------------------------------------------------------------------
    # Endpoint
    # ------------------------------------------------------------------

    def _ensure_endpoint(self, endpoint_name: str) -> None:
        try:
            self._sp_ws.vector_search_endpoints.get_endpoint(endpoint_name)
            logger.debug("Vector Search endpoint %s already exists", endpoint_name)
            return
        except NotFound:
            pass

        logger.info("Creating Vector Search endpoint %s", endpoint_name)
        self._sp_ws.vector_search_endpoints.create_endpoint(
            name=endpoint_name,
            endpoint_type=EndpointType.STANDARD,
        )

    # ------------------------------------------------------------------
    # Index
    # ------------------------------------------------------------------

    def _ensure_index(self, endpoint_name: str, index_name: str) -> None:
        try:
            self._sp_ws.vector_search_indexes.get_index(index_name)
            logger.debug("Vector Search index %s already exists", index_name)
            return
        except NotFound:
            pass

        dimension = self._probe_embedding_dimension()
        if dimension is None:
            logger.warning(
                "Could not determine embedding dimension (embedding endpoint returned no vector); "
                "skipping Vector Search index creation for %s",
                index_name,
            )
            return

        logger.info("Creating Vector Search Direct Access index %s on endpoint %s", index_name, endpoint_name)
        self._sp_ws.vector_search_indexes.create_index(
            name=index_name,
            endpoint_name=endpoint_name,
            primary_key=PRIMARY_KEY_COLUMN,
            index_type=VectorIndexType.DIRECT_ACCESS,
            direct_access_index_spec=DirectAccessVectorIndexSpec(
                embedding_vector_columns=[
                    EmbeddingVectorColumn(name=EMBEDDING_COLUMN, embedding_dimension=dimension)
                ],
                schema_json=json.dumps(
                    {
                        PRIMARY_KEY_COLUMN: "string",
                        TEXT_COLUMN: "string",
                        EMBEDDING_COLUMN: "array<float>",
                    }
                ),
            ),
        )

    def _probe_embedding_dimension(self) -> int | None:
        """Embed a short static probe string to learn the endpoint's output dimension."""
        vector = self._embeddings.embed_text(_DIMENSION_PROBE_TEXT)
        return len(vector) if vector else None
