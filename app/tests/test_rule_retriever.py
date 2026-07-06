"""Tests for ``services/rule_retriever.py`` — Rules Registry Phase 4B/4C.

``VectorSearchRetriever`` is the swappable production ``RuleRetriever``
seam (design spec §8): every scenario where infra is unconfigured must
report a specific reason via ``is_available`` and never let ``retrieve``
attempt a call.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService
from databricks_labs_dqx_app.backend.services.rule_retriever import (
    RuleRetrievalUnavailableError,
    VectorSearchRetriever,
)


@pytest.fixture
def app_settings():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_embedding_endpoint_name.return_value = "embed-endpoint"
    settings.get_vs_endpoint_name.return_value = "vs-endpoint"
    settings.get_vs_index_name.return_value = "catalog.schema.vs_index"
    return settings


@pytest.fixture
def sp_ws():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.vector_search_indexes.get_index.return_value = SimpleNamespace(status=SimpleNamespace(ready=True))
    return ws


@pytest.fixture
def embeddings():
    return create_autospec(RuleEmbeddingsService, instance=True)


@pytest.fixture
def retriever(sp_ws, app_settings, embeddings):
    return VectorSearchRetriever(sp_ws=sp_ws, app_settings=app_settings, embeddings=embeddings)


class TestIsAvailable:
    def test_available_when_fully_configured(self, retriever):
        available, reason = retriever.is_available()
        assert available is True
        assert reason == ""

    @pytest.mark.parametrize(
        "missing_setting",
        ["get_embedding_endpoint_name", "get_vs_endpoint_name", "get_vs_index_name"],
    )
    def test_unavailable_when_any_setting_missing(self, retriever, app_settings, missing_setting):
        getattr(app_settings, missing_setting).return_value = ""

        available, reason = retriever.is_available()

        assert available is False
        assert reason != ""

    def test_reason_lists_every_missing_setting(self, retriever, app_settings):
        app_settings.get_embedding_endpoint_name.return_value = ""
        app_settings.get_vs_endpoint_name.return_value = ""

        _, reason = retriever.is_available()

        assert "embedding_endpoint_name" in reason
        assert "vs_endpoint_name" in reason
        assert "vs_index_name" not in reason

    def test_unavailable_when_index_not_yet_provisioned(self, retriever, sp_ws):
        """Settings are configured (always true once auto-derived) but the index doesn't exist yet."""
        sp_ws.vector_search_indexes.get_index.side_effect = NotFound("index not found")

        available, reason = retriever.is_available()

        assert available is False
        assert "not been provisioned yet" in reason

    def test_unavailable_when_index_not_ready(self, retriever, sp_ws):
        sp_ws.vector_search_indexes.get_index.return_value = SimpleNamespace(status=SimpleNamespace(ready=False))

        available, reason = retriever.is_available()

        assert available is False
        assert "still being built" in reason

    def test_does_not_check_index_when_settings_missing(self, retriever, app_settings, sp_ws):
        app_settings.get_vs_index_name.return_value = ""

        retriever.is_available()

        sp_ws.vector_search_indexes.get_index.assert_not_called()


class TestRetrieve:
    def test_raises_when_unavailable(self, retriever, app_settings, sp_ws):
        app_settings.get_vs_index_name.return_value = ""

        with pytest.raises(RuleRetrievalUnavailableError):
            retriever.retrieve("some query", top_k=8)

        sp_ws.vector_search_indexes.query_index.assert_not_called()

    def test_raises_when_embedding_fails(self, retriever, embeddings, sp_ws):
        embeddings.embed_text.return_value = None

        with pytest.raises(RuleRetrievalUnavailableError):
            retriever.retrieve("some query", top_k=8)

        sp_ws.vector_search_indexes.query_index.assert_not_called()

    def test_happy_path_parses_rows(self, retriever, embeddings, sp_ws, app_settings):
        embeddings.embed_text.return_value = [0.1, 0.2, 0.3]
        sp_ws.vector_search_indexes.query_index.return_value = SimpleNamespace(
            result=SimpleNamespace(data_array=[["rule-a", 0.95], ["rule-b", 0.42]])
        )

        results = retriever.retrieve("some query", top_k=8)

        assert [r.rule_id for r in results] == ["rule-a", "rule-b"]
        assert results[0].score == pytest.approx(0.95)
        _, kwargs = sp_ws.vector_search_indexes.query_index.call_args
        assert kwargs["index_name"] == "catalog.schema.vs_index"
        assert kwargs["query_vector"] == [0.1, 0.2, 0.3]
        assert kwargs["num_results"] == 8

    def test_truncates_to_top_k(self, retriever, embeddings, sp_ws):
        embeddings.embed_text.return_value = [0.1]
        sp_ws.vector_search_indexes.query_index.return_value = SimpleNamespace(
            result=SimpleNamespace(data_array=[["a", 0.9], ["b", 0.8], ["c", 0.7]])
        )

        results = retriever.retrieve("q", top_k=2)

        assert len(results) == 2

    def test_handles_empty_result(self, retriever, embeddings, sp_ws):
        embeddings.embed_text.return_value = [0.1]
        sp_ws.vector_search_indexes.query_index.return_value = SimpleNamespace(result=SimpleNamespace(data_array=[]))

        assert retriever.retrieve("q", top_k=8) == []
