"""Rule mapping suggester retrieval seam — Rules Registry Phase 4B/4C (design spec §8).

``RuleRetriever`` is the swappable seam behind the mapping suggester
(:mod:`databricks_labs_dqx_app.backend.services.rule_suggester`): any
implementation that can turn a free-text query into a ranked list of
candidate published rule ids satisfies it.

:class:`VectorSearchRetriever` is the production implementation, backed by
a Databricks Vector Search index over the ``dq_rule_embeddings`` corpus
(see ``services.rule_embeddings``). It requires an admin to configure
``embedding_endpoint_name``, ``vs_endpoint_name``, and ``vs_index_name``
(see ``AppSettingsService``) — with any of the three unset,
:meth:`VectorSearchRetriever.is_available` reports ``False`` with a
specific reason, and the suggester surfaces that reason as
``available=False`` rather than attempting a call. No Vector Search
infrastructure is required for the app to build, deploy, or serve any
other feature.

Since Phase 8B, ``embedding_endpoint_name`` / ``vs_endpoint_name`` /
``vs_index_name`` always auto-derive to a non-empty default (see
``AppSettingsService``), so the "is a setting missing" check above is no
longer sufficient on its own to detect an unprovisioned deployment —
:meth:`VectorSearchRetriever.is_available` additionally calls
``get_index()`` to confirm the index actually exists and has finished
building (``status.ready``) before reporting ``available=True``. Without
this live check, ``is_available`` would report ``True`` the instant AI is
enabled even though ``VectorStoreProvisioner`` only just *submitted* the
async creation request (index creation can take minutes) — the suggester
would then attempt a real query against a not-yet-existing index and
surface an opaque "Rule retrieval failed" instead of a clear, actionable
"not provisioned yet" message.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.errors.platform import NotFound

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService

logger = logging.getLogger(__name__)


class RuleRetrievalUnavailableError(Exception):
    """Raised by a :class:`RuleRetriever` when retrieval cannot be performed."""


@dataclass
class RetrievedRule:
    """One candidate rule returned by a :class:`RuleRetriever`."""

    rule_id: str
    score: float = 0.0


class RuleRetriever(Protocol):
    """Swappable retrieval seam for the rule-mapping suggester (design spec §8)."""

    def is_available(self) -> tuple[bool, str]:
        """Return ``(available, reason)`` — *reason* is populated only when unavailable."""
        ...

    def retrieve(self, query_text: str, top_k: int) -> list[RetrievedRule]:
        """Return up to *top_k* candidate rules ranked by relevance to *query_text*.

        Raises:
            RuleRetrievalUnavailableError: retrieval could not be performed
                (e.g. infra unconfigured, embedding call failed).
        """
        ...


class VectorSearchRetriever:
    """Databricks Vector Search-backed :class:`RuleRetriever`.

    Embeds *query_text* via the same :class:`RuleEmbeddingsService` used to
    populate the corpus, then queries the configured Vector Search index
    for nearest neighbours by ``rule_id``.
    """

    def __init__(
        self,
        sp_ws: WorkspaceClient,
        app_settings: AppSettingsService,
        embeddings: RuleEmbeddingsService,
    ) -> None:
        self._sp_ws = sp_ws
        self._app_settings = app_settings
        self._embeddings = embeddings

    def is_available(self) -> tuple[bool, str]:
        """Return ``(True, "")`` iff Vector Search is configured **and** the index is online.

        Checks, in order:

        1. All three settings resolve to a non-empty value (they always do
           once auto-derived, so this mainly guards against an admin having
           explicitly saved an empty override).
        2. The Vector Search index actually exists — it may not yet if AI
           was just enabled and :class:`~...vector_store.VectorStoreProvisioner`
           hasn't finished (or failed) creating it.
        3. The index reports ``status.ready`` — index creation is
           asynchronous and can take minutes to come online.

        Every negative case returns a specific, human-readable *reason* so
        the caller never has to guess why suggestions aren't available.
        """
        missing = [
            label
            for label, value in (
                ("embedding_endpoint_name", self._app_settings.get_embedding_endpoint_name()),
                ("vs_endpoint_name", self._app_settings.get_vs_endpoint_name()),
                ("vs_index_name", self._app_settings.get_vs_index_name()),
            )
            if not value
        ]
        if missing:
            return False, f"Vector Search is not configured (missing: {', '.join(missing)})."

        index_name = self._app_settings.get_vs_index_name()
        try:
            index = self._sp_ws.vector_search_indexes.get_index(index_name)
        except NotFound:
            return False, (
                f"Vector Search index '{index_name}' has not been provisioned yet. "
                "Ask an admin to enable AI in Settings — provisioning is automatic but can "
                "take a few minutes to complete."
            )
        except DatabricksError as e:
            return False, f"Vector Search is unavailable: {e}"

        status = getattr(index, "status", None)
        if getattr(status, "ready", None) is False:
            return False, (
                f"Vector Search index '{index_name}' is still being built and isn't ready for "
                "queries yet. Try again in a few minutes."
            )
        return True, ""

    def retrieve(self, query_text: str, top_k: int) -> list[RetrievedRule]:
        available, reason = self.is_available()
        if not available:
            raise RuleRetrievalUnavailableError(reason)

        query_vector = self._embeddings.embed_text(query_text)
        if query_vector is None:
            raise RuleRetrievalUnavailableError("Embedding endpoint returned no vector for the query text.")

        index_name = self._app_settings.get_vs_index_name()
        response = self._sp_ws.vector_search_indexes.query_index(
            index_name=index_name,
            columns=["rule_id"],
            query_vector=query_vector,
            num_results=top_k,
        )
        result = getattr(response, "result", None)
        rows = getattr(result, "data_array", None) or []
        candidates: list[RetrievedRule] = []
        for row in rows:
            if not row:
                continue
            rule_id = str(row[0])
            score = 0.0
            if len(row) > 1:
                try:
                    score = float(row[-1])
                except (TypeError, ValueError):
                    score = 0.0
            candidates.append(RetrievedRule(rule_id=rule_id, score=score))
        return candidates[:top_k]
