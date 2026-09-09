"""Tests for ``services/rule_retriever.py`` — Rules Registry Phase 4B/4C.

``CosineRuleRetriever`` is the production ``RuleRetriever``: pure-Python
cosine over the OLTP embeddings corpus. Scenarios where the embedding
endpoint is unconfigured must report a specific reason via ``is_available``
and never let ``retrieve`` attempt a call.
"""

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService
from databricks_labs_dqx_app.backend.services.rule_retriever import (
    CosineRuleRetriever,
    RuleRetrievalUnavailableError,
    cosine_similarity,
)


class TestCosineSimilarity:
    def test_identical_vectors_score_one(self):
        assert cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)

    def test_orthogonal_vectors_score_zero(self):
        assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)

    def test_mismatched_lengths_return_zero(self):
        assert cosine_similarity([1.0, 2.0], [1.0]) == 0.0

    def test_zero_magnitude_returns_zero(self):
        assert cosine_similarity([0.0, 0.0], [1.0, 1.0]) == 0.0


class TestCosineRuleRetriever:
    """The production retriever: pure-Python cosine over the OLTP corpus."""

    @pytest.fixture
    def cosine_embeddings(self):
        embeddings = create_autospec(RuleEmbeddingsService, instance=True)
        embeddings.is_configured.return_value = True
        return embeddings

    @pytest.fixture
    def cosine_retriever(self, cosine_embeddings):
        return CosineRuleRetriever(embeddings=cosine_embeddings)

    def test_available_when_embedding_endpoint_configured(self, cosine_retriever):
        available, reason = cosine_retriever.is_available()
        assert available is True
        assert reason == ""

    def test_unavailable_when_no_embedding_endpoint(self, cosine_retriever, cosine_embeddings):
        cosine_embeddings.is_configured.return_value = False
        available, reason = cosine_retriever.is_available()
        assert available is False
        assert "embedding endpoint" in reason

    def test_ranks_by_cosine_and_truncates(self, cosine_retriever, cosine_embeddings):
        cosine_embeddings.embed_texts.return_value = [[1.0, 0.0]]
        cosine_embeddings.iter_embeddings.return_value = [
            ("far", [0.0, 1.0]),
            ("near", [1.0, 0.0]),
            ("mid", [1.0, 1.0]),
        ]

        results = cosine_retriever.retrieve("query", top_k=2)

        assert [r.rule_id for r in results] == ["near", "mid"]
        assert results[0].score == pytest.approx(1.0)
        cosine_embeddings.embed_texts.assert_called_once_with(["query"])

    def test_retrieve_many_batches_and_loads_corpus_once(self, cosine_retriever, cosine_embeddings):
        cosine_embeddings.embed_texts.return_value = [[1.0, 0.0], [0.0, 1.0]]
        cosine_embeddings.iter_embeddings.return_value = [
            ("near_a", [1.0, 0.0]),
            ("near_b", [0.0, 1.0]),
        ]

        results = cosine_retriever.retrieve_many(["qa", "qb"], top_k=1)

        assert [r[0].rule_id for r in results] == ["near_a", "near_b"]
        cosine_embeddings.embed_texts.assert_called_once_with(["qa", "qb"])
        cosine_embeddings.iter_embeddings.assert_called_once()

    def test_empty_corpus_returns_empty(self, cosine_retriever, cosine_embeddings):
        cosine_embeddings.embed_texts.return_value = [[1.0, 0.0]]
        cosine_embeddings.iter_embeddings.return_value = []

        assert cosine_retriever.retrieve("query", top_k=8) == []

    def test_raises_when_unavailable(self, cosine_retriever, cosine_embeddings):
        cosine_embeddings.is_configured.return_value = False

        with pytest.raises(RuleRetrievalUnavailableError):
            cosine_retriever.retrieve("query", top_k=8)

        cosine_embeddings.iter_embeddings.assert_not_called()

    def test_raises_when_query_embedding_fails(self, cosine_retriever, cosine_embeddings):
        cosine_embeddings.embed_texts.return_value = [None]

        with pytest.raises(RuleRetrievalUnavailableError):
            cosine_retriever.retrieve("query", top_k=8)
