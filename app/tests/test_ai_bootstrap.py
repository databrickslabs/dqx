"""Tests for ``services/ai_bootstrap.py``.

``ensure_ai_ready`` grants ``CAN_QUERY`` on AI/embedding serving endpoints and
backfills embeddings for published rules. It must never raise — any Databricks
SDK failure is logged and swallowed so a transient outage or missing
permission can never crash app startup or the admin "enable AI" flow.
"""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service.serving import ServingEndpointPermissionLevel

from databricks_labs_dqx_app.backend.services.ai_bootstrap import AiBootstrap
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService


@pytest.fixture
def app_settings():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_embedding_endpoint_name.return_value = "embed-endpoint"
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
    svc.backfill.return_value = 0
    return svc


@pytest.fixture
def registry():
    svc = create_autospec(RegistryService, instance=True)
    svc.list_rules.return_value = []
    return svc


@pytest.fixture
def bootstrap(sp_ws, app_settings, embeddings, registry):
    return AiBootstrap(sp_ws=sp_ws, app_settings=app_settings, embeddings=embeddings, registry=registry)


class TestGrantServingEndpointAccess:
    """CAN_QUERY auto-grant on the AI/embedding serving endpoints — least-privilege OBO enablement."""

    async def test_grants_can_query_on_ai_and_embedding_endpoints(self, bootstrap, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = "ai-endpoint"
        app_settings.get_embedding_endpoint_name.return_value = "embed-endpoint"

        await bootstrap.ensure_ai_ready()

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

    async def test_dedupes_when_ai_and_embedding_endpoint_are_the_same(self, bootstrap, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = "shared-endpoint"
        app_settings.get_embedding_endpoint_name.return_value = "shared-endpoint"

        await bootstrap.ensure_ai_ready()

        sp_ws.serving_endpoints.get.assert_called_once_with("shared-endpoint")

    async def test_skips_endpoints_that_are_unset(self, bootstrap, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = ""
        app_settings.get_embedding_endpoint_name.return_value = ""

        await bootstrap.ensure_ai_ready()

        sp_ws.serving_endpoints.get.assert_not_called()
        sp_ws.serving_endpoints.update_permissions.assert_not_called()

    async def test_grants_to_configured_admin_group(self, bootstrap, sp_ws, app_settings, monkeypatch):
        monkeypatch.setattr("databricks_labs_dqx_app.backend.services.ai_bootstrap.conf.admin_group", "dqx-admins")

        await bootstrap.ensure_ai_ready()

        _, kwargs = sp_ws.serving_endpoints.update_permissions.call_args
        assert kwargs["access_control_list"][0].group_name == "dqx-admins"

    async def test_falls_back_to_account_users_when_no_admin_group_configured(
        self, bootstrap, sp_ws, app_settings, monkeypatch
    ):
        monkeypatch.setattr("databricks_labs_dqx_app.backend.services.ai_bootstrap.conf.admin_group", None)

        await bootstrap.ensure_ai_ready()

        _, kwargs = sp_ws.serving_endpoints.update_permissions.call_args
        assert kwargs["access_control_list"][0].group_name == "account users"

    async def test_swallows_not_grantable_error_and_still_backfills(self, bootstrap, sp_ws, registry, embeddings):
        """System/foundation-model endpoints (e.g. databricks-gpt-5-5) reject permission changes."""
        sp_ws.serving_endpoints.update_permissions.side_effect = DatabricksError("This endpoint is not grantable.")
        rule_a = object()
        registry.list_rules.return_value = [rule_a]

        await bootstrap.ensure_ai_ready()  # must not raise

        # Backfill still ran despite the grant failure.
        embeddings.backfill.assert_called_once_with([rule_a])

    async def test_swallows_get_endpoint_error(self, bootstrap, sp_ws):
        sp_ws.serving_endpoints.get.side_effect = DatabricksError("permission denied")

        await bootstrap.ensure_ai_ready()  # must not raise

    async def test_grant_failure_does_not_prevent_second_endpoint_grant(self, bootstrap, sp_ws, app_settings):
        app_settings.get_ai_endpoint_name.return_value = "endpoint-a"
        app_settings.get_embedding_endpoint_name.return_value = "endpoint-b"
        sp_ws.serving_endpoints.update_permissions.side_effect = [
            DatabricksError("not grantable"),
            None,
        ]

        await bootstrap.ensure_ai_ready()  # must not raise

        assert sp_ws.serving_endpoints.update_permissions.call_count == 2


class TestBackfillPublishedRules:
    """Best-effort re-embed of already-published rules (incl. built-ins)."""

    async def test_backfills_published_rules(self, bootstrap, registry, embeddings):
        rule_a, rule_b = object(), object()
        registry.list_rules.return_value = [rule_a, rule_b]

        await bootstrap.ensure_ai_ready()

        registry.list_rules.assert_called_once_with(status="approved")
        embeddings.backfill.assert_called_once_with([rule_a, rule_b])

    async def test_backfill_failure_does_not_raise(self, bootstrap, registry):
        registry.list_rules.side_effect = RuntimeError("SQL warehouse unreachable")

        await bootstrap.ensure_ai_ready()  # must not raise
