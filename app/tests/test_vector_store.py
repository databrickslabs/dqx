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
from databricks.sdk.service.serving import ServingEndpointPermissionLevel
from databricks.sdk.service.vectorsearch import VectorIndexType

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService
from databricks_labs_dqx_app.backend.services.vector_store import VectorStoreProvisioner


@pytest.fixture
def app_settings():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_embedding_endpoint_name.return_value = "embed-endpoint"
    settings.get_vs_endpoint_name.return_value = "vs-endpoint"
    settings.get_vs_index_name.return_value = "catalog.schema.vs_index"
    settings.get_ai_endpoint_name.return_value = "ai-endpoint"
    return settings


@pytest.fixture
def sp_ws():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.serving_endpoints.get.return_value.id = "endpoint-id-123"
    return ws


@pytest.fixture
def embeddings():
    svc = create_autospec(RuleEmbeddingsService, instance=True)
    svc.embed_text.return_value = [0.1, 0.2, 0.3, 0.4]
    svc.backfill.return_value = 0
    return svc


@pytest.fixture
def registry():
    svc = create_autospec(RegistryService, instance=True)
    svc.list_rules.return_value = []
    return svc


@pytest.fixture
def provisioner(sp_ws, app_settings, embeddings, registry):
    return VectorStoreProvisioner(sp_ws=sp_ws, app_settings=app_settings, embeddings=embeddings, registry=registry)


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


class TestGrantServingEndpointAccess:
    """CAN_QUERY auto-grant on the AI/embedding serving endpoints — least-privilege OBO enablement."""

    async def test_grants_can_query_on_ai_and_embedding_endpoints(self, provisioner, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = "ai-endpoint"
        app_settings.get_embedding_endpoint_name.return_value = "embed-endpoint"

        await provisioner.ensure_vector_store()

        granted_endpoint_names = {call.args[0] for call in sp_ws.serving_endpoints.get.call_args_list}
        assert granted_endpoint_names == {"ai-endpoint", "embed-endpoint"}
        assert sp_ws.serving_endpoints.update_permissions.call_count == 2
        for _, kwargs in sp_ws.serving_endpoints.update_permissions.call_args_list:
            assert kwargs["serving_endpoint_id"] == "endpoint-id-123"
            acl = kwargs["access_control_list"]
            assert len(acl) == 1
            assert acl[0].permission_level == ServingEndpointPermissionLevel.CAN_QUERY
            # Least-privilege: never CAN_MANAGE.
            assert acl[0].permission_level != ServingEndpointPermissionLevel.CAN_MANAGE

    async def test_dedupes_when_ai_and_embedding_endpoint_are_the_same(self, provisioner, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = "shared-endpoint"
        app_settings.get_embedding_endpoint_name.return_value = "shared-endpoint"

        await provisioner.ensure_vector_store()

        sp_ws.serving_endpoints.get.assert_called_once_with("shared-endpoint")

    async def test_skips_endpoints_that_are_unset(self, provisioner, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = ""
        app_settings.get_embedding_endpoint_name.return_value = ""

        await provisioner.ensure_vector_store()

        sp_ws.serving_endpoints.get.assert_not_called()
        sp_ws.serving_endpoints.update_permissions.assert_not_called()

    async def test_grants_to_configured_admin_group(self, provisioner, sp_ws, app_settings, monkeypatch):
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.services.vector_store.conf.admin_group", "dqx-admins"
        )

        await provisioner.ensure_vector_store()

        _, kwargs = sp_ws.serving_endpoints.update_permissions.call_args
        assert kwargs["access_control_list"][0].group_name == "dqx-admins"

    async def test_falls_back_to_account_users_when_no_admin_group_configured(
        self, provisioner, sp_ws, app_settings, monkeypatch
    ):
        monkeypatch.setattr("databricks_labs_dqx_app.backend.services.vector_store.conf.admin_group", None)

        await provisioner.ensure_vector_store()

        _, kwargs = sp_ws.serving_endpoints.update_permissions.call_args
        assert kwargs["access_control_list"][0].group_name == "account users"

    async def test_swallows_not_grantable_error_and_still_completes(self, provisioner, sp_ws):
        """System/foundation-model endpoints (e.g. databricks-gpt-5-5) reject permission changes."""
        sp_ws.serving_endpoints.update_permissions.side_effect = DatabricksError("This endpoint is not grantable.")
        sp_ws.vector_search_endpoints.get_endpoint.return_value = object()
        sp_ws.vector_search_indexes.get_index.side_effect = NotFound("no such index")

        await provisioner.ensure_vector_store()  # must not raise

        # The rest of provisioning still ran despite the grant failure.
        sp_ws.vector_search_indexes.create_index.assert_called()

    async def test_swallows_get_endpoint_error(self, provisioner, sp_ws):
        sp_ws.serving_endpoints.get.side_effect = DatabricksError("permission denied")

        await provisioner.ensure_vector_store()  # must not raise

    async def test_grant_failure_does_not_prevent_second_endpoint_grant(self, provisioner, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = "endpoint-a"
        app_settings.get_embedding_endpoint_name.return_value = "endpoint-b"
        sp_ws.serving_endpoints.update_permissions.side_effect = [
            DatabricksError("not grantable"),
            None,
        ]

        await provisioner.ensure_vector_store()  # must not raise

        assert sp_ws.serving_endpoints.update_permissions.call_count == 2


class TestBackfillPublishedRulesAfterProvisioning:
    """Post-provisioning best-effort re-embed of already-published rules (incl. built-ins)."""

    async def test_backfills_published_rules_when_index_ready(self, provisioner, registry, embeddings):
        rule_a, rule_b = object(), object()
        registry.list_rules.return_value = [rule_a, rule_b]

        await provisioner.ensure_vector_store()

        registry.list_rules.assert_called_once_with(status="approved")
        embeddings.backfill.assert_called_once_with([rule_a, rule_b])

    async def test_backfill_failure_does_not_raise(self, provisioner, registry):
        registry.list_rules.side_effect = RuntimeError("SQL warehouse unreachable")

        await provisioner.ensure_vector_store()  # must not raise

    async def test_no_backfill_when_vector_search_settings_incomplete(self, provisioner, app_settings, registry):
        app_settings.get_vs_index_name.return_value = ""

        await provisioner.ensure_vector_store()

        registry.list_rules.assert_not_called()
