"""Tests for ``services/vector_store.py`` — Rules Registry Phase 7F.

``ensure_vector_store`` must be idempotent (no-op when the endpoint/index
already exist), a no-op when Vector Search settings are incomplete, and
never raise — any Databricks SDK failure is logged and swallowed so a
transient outage or missing permission can never crash app startup or the
admin "enable AI" flow.
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.errors.platform import NotFound
from databricks.sdk.service.vectorsearch import VectorIndexType

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService
from databricks_labs_dqx_app.backend.services.vector_store import VectorStoreProvisioner


@pytest.fixture
def app_settings():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_embedding_endpoint_name.return_value = "embed-endpoint"
    settings.get_vs_endpoint_name.return_value = "vs-endpoint"
    settings.get_vs_index_name.return_value = "catalog.schema.vs_index"
    return settings


@pytest.fixture
def sp_ws():
    return create_autospec(WorkspaceClient, instance=True)


@pytest.fixture
def embeddings():
    svc = create_autospec(RuleEmbeddingsService, instance=True)
    svc.embed_text.return_value = [0.1, 0.2, 0.3, 0.4]
    return svc


@pytest.fixture
def provisioner(sp_ws, app_settings, embeddings):
    return VectorStoreProvisioner(sp_ws=sp_ws, app_settings=app_settings, embeddings=embeddings)


class TestEnsureVectorStoreSkipsWhenUnconfigured:
    @pytest.mark.parametrize(
        "missing_setting",
        ["get_embedding_endpoint_name", "get_vs_endpoint_name", "get_vs_index_name"],
    )
    async def test_no_sdk_calls_when_any_setting_missing(self, provisioner, app_settings, sp_ws, missing_setting):
        getattr(app_settings, missing_setting).return_value = ""

        await provisioner.ensure_vector_store()

        sp_ws.vector_search_endpoints.get_endpoint.assert_not_called()
        sp_ws.vector_search_endpoints.create_endpoint.assert_not_called()
        sp_ws.vector_search_indexes.get_index.assert_not_called()
        sp_ws.vector_search_indexes.create_index.assert_not_called()


class TestEnsureVectorStoreEndpoint:
    async def test_creates_endpoint_when_missing(self, provisioner, sp_ws):
        sp_ws.vector_search_endpoints.get_endpoint.side_effect = NotFound("no such endpoint")
        sp_ws.vector_search_indexes.get_index.side_effect = NotFound("no such index")

        await provisioner.ensure_vector_store()

        sp_ws.vector_search_endpoints.create_endpoint.assert_called_once()
        _, kwargs = sp_ws.vector_search_endpoints.create_endpoint.call_args
        assert kwargs["name"] == "vs-endpoint"

    async def test_does_not_recreate_existing_endpoint(self, provisioner, sp_ws):
        sp_ws.vector_search_endpoints.get_endpoint.return_value = object()
        sp_ws.vector_search_indexes.get_index.side_effect = NotFound("no such index")

        await provisioner.ensure_vector_store()

        sp_ws.vector_search_endpoints.create_endpoint.assert_not_called()


class TestEnsureVectorStoreIndex:
    async def test_creates_direct_access_index_when_missing(self, provisioner, sp_ws, embeddings):
        sp_ws.vector_search_endpoints.get_endpoint.return_value = object()
        sp_ws.vector_search_indexes.get_index.side_effect = NotFound("no such index")

        await provisioner.ensure_vector_store()

        sp_ws.vector_search_indexes.create_index.assert_called_once()
        _, kwargs = sp_ws.vector_search_indexes.create_index.call_args
        assert kwargs["name"] == "catalog.schema.vs_index"
        assert kwargs["endpoint_name"] == "vs-endpoint"
        assert kwargs["index_type"] == VectorIndexType.DIRECT_ACCESS
        assert kwargs["direct_access_index_spec"] is not None

    async def test_does_not_recreate_existing_index(self, provisioner, sp_ws):
        sp_ws.vector_search_endpoints.get_endpoint.return_value = object()
        sp_ws.vector_search_indexes.get_index.return_value = object()

        await provisioner.ensure_vector_store()

        sp_ws.vector_search_indexes.create_index.assert_not_called()

    async def test_skips_index_creation_when_embedding_probe_fails(self, provisioner, sp_ws, embeddings):
        sp_ws.vector_search_endpoints.get_endpoint.return_value = object()
        sp_ws.vector_search_indexes.get_index.side_effect = NotFound("no such index")
        embeddings.embed_text.return_value = None

        await provisioner.ensure_vector_store()

        sp_ws.vector_search_indexes.create_index.assert_not_called()


class TestEnsureVectorStoreIsIdempotentAndNonFatal:
    async def test_second_call_is_a_pure_no_op(self, provisioner, sp_ws):
        sp_ws.vector_search_endpoints.get_endpoint.return_value = object()
        sp_ws.vector_search_indexes.get_index.return_value = object()

        await provisioner.ensure_vector_store()
        await provisioner.ensure_vector_store()

        sp_ws.vector_search_endpoints.create_endpoint.assert_not_called()
        sp_ws.vector_search_indexes.create_index.assert_not_called()

    async def test_swallows_endpoint_creation_errors(self, provisioner, sp_ws):
        sp_ws.vector_search_endpoints.get_endpoint.side_effect = NotFound("no such endpoint")
        sp_ws.vector_search_endpoints.create_endpoint.side_effect = DatabricksError("permission denied")

        await provisioner.ensure_vector_store()  # must not raise

    async def test_swallows_index_creation_errors(self, provisioner, sp_ws):
        sp_ws.vector_search_endpoints.get_endpoint.return_value = object()
        sp_ws.vector_search_indexes.get_index.side_effect = NotFound("no such index")
        sp_ws.vector_search_indexes.create_index.side_effect = DatabricksError("permission denied")

        await provisioner.ensure_vector_store()  # must not raise

    async def test_swallows_unexpected_get_endpoint_errors(self, provisioner, sp_ws):
        sp_ws.vector_search_endpoints.get_endpoint.side_effect = DatabricksError("transient outage")

        await provisioner.ensure_vector_store()  # must not raise
