"""Rule mapping suggester retrieval seam — Rules Registry Phase 4B/4C (design spec §8).

``RuleRetriever`` is the swappable seam behind the mapping suggester
(:mod:`databricks_labs_dqx_app.backend.services.rule_suggester`): any
implementation that can turn a free-text query into a ranked list of
candidate published rule ids satisfies it.

:class:`CosineRuleRetriever` is the **production default** (design spec §8):
a pure-Python cosine scan over the ``dq_rule_embeddings`` OLTP corpus (see
``services.rule_embeddings``), mirroring dqlake's retriever. It has no
Vector Search index or endpoint dependency — as soon as the embedding
endpoint is configured and rules are embedded (best-effort on publish +
startup backfill), suggestions work. The rule corpus is small enough that a
full in-app scan is inexpensive.
"""

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService

logger = logging.getLogger(__name__)


def cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity of two equal-length vectors, in pure Python.

    Returns ``0.0`` for mismatched lengths or a zero-magnitude vector
    (rather than raising) so a single malformed stored embedding can never
    crash a retrieval.
    """
    if len(a) != len(b) or not a:
        return 0.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for x, y in zip(a, b):
        dot += x * y
        norm_a += x * x
        norm_b += y * y
    if norm_a <= 0.0 or norm_b <= 0.0:
        return 0.0
    return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))


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

    def retrieve_many(self, query_texts: Sequence[str], top_k: int) -> list[list[RetrievedRule]]:
        """Return one ranked candidate list per *query_texts* entry.

        Implementations should batch embedding work and load the corpus once
        when possible. Raises the same errors as :meth:`retrieve`.
        """
        ...


class CosineRuleRetriever:
    """In-app cosine :class:`RuleRetriever` over the OLTP embeddings corpus.

    This is the **production default** (design spec §8 — the ``RuleRetriever``
    seam). It mirrors dqlake's ``CosineRuleRetriever``: embed *query_text* via
    the same :class:`RuleEmbeddingsService` used to populate the corpus, then
    rank the stored ``dq_rule_embeddings`` rows by cosine similarity in pure
    Python.

    Availability requires only that an embedding endpoint is configured (the
    query text must be embeddable). An empty corpus is *not* an availability
    failure — it surfaces downstream as the suggester's "no published rules"
    reason, exactly like dqlake.
    """

    def __init__(self, embeddings: RuleEmbeddingsService) -> None:
        self._embeddings = embeddings

    def is_available(self) -> tuple[bool, str]:
        """Return ``(True, "")`` iff an embedding endpoint is configured."""
        if not self._embeddings.is_configured():
            return False, (
                "AI rule suggestions aren't available: no embedding endpoint is configured. "
                "Ask an admin to enable AI in Settings."
            )
        return True, ""

    def retrieve(self, query_text: str, top_k: int) -> list[RetrievedRule]:
        return self.retrieve_many([query_text], top_k)[0]

    def retrieve_many(self, query_texts: Sequence[str], top_k: int) -> list[list[RetrievedRule]]:
        """Batch-embed *query_texts*, score once against a single corpus load.

        One ``iter_embeddings`` read and chunked ``embed_texts`` calls replace
        N independent retrieve round trips (dominant cost on wide tables).
        """
        available, reason = self.is_available()
        if not available:
            raise RuleRetrievalUnavailableError(reason)
        if not query_texts:
            return []

        query_vectors = self._embeddings.embed_texts(list(query_texts))
        if any(vector is None for vector in query_vectors):
            raise RuleRetrievalUnavailableError("Embedding endpoint returned no vector for the query text.")

        corpus = self._embeddings.iter_embeddings()
        results: list[list[RetrievedRule]] = []
        for query_vector in query_vectors:
            assert query_vector is not None  # guarded above
            scored = [
                RetrievedRule(rule_id=rule_id, score=cosine_similarity(query_vector, vector))
                for rule_id, vector in corpus
            ]
            scored.sort(key=lambda candidate: candidate.score, reverse=True)
            results.append(scored[:top_k] if top_k > 0 else scored)
        return results
