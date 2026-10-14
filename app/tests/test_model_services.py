"""Tests for ``services/model_services.py`` — the Unity Gateway model-service transport."""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

from databricks_labs_dqx_app.backend.services.model_services import (
    CHAT_COMPLETIONS_API,
    EMBEDDINGS_API,
    ModelServices,
    model_service_name,
)


@pytest.fixture
def ws():
    return create_autospec(WorkspaceClient, instance=True)


class TestModelServiceName:
    @pytest.mark.parametrize(
        ("configured", "expected"),
        [
            ("databricks-claude-sonnet-4-5", "system.ai.claude-sonnet-4-5"),
            ("databricks-gte-large-en", "system.ai.gte-large-en"),
            ("  databricks-gpt-5-4-nano ", "system.ai.gpt-5-4-nano"),
            ("system.ai.claude-sonnet-4-5", "system.ai.claude-sonnet-4-5"),
            ("main.models.databricks-custom", "main.models.databricks-custom"),
            ("my-provisioned-endpoint", "my-provisioned-endpoint"),
            ("", ""),
        ],
    )
    def test_maps_legacy_endpoint_names_to_system_ai(self, configured, expected):
        assert model_service_name(configured) == expected


class TestChat:
    def test_posts_an_openai_compatible_request(self, ws):
        ws.api_client.do.return_value = {"choices": []}

        ModelServices(ws).chat(
            model="system.ai.claude-sonnet-4-5",
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=10,
            temperature=0,
        )

        ws.api_client.do.assert_called_once_with(
            "POST",
            "/ai-gateway/mlflow/v1/chat/completions",
            body={
                "model": "system.ai.claude-sonnet-4-5",
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 10,
                "temperature": 0,
            },
        )

    def test_omits_temperature_when_unset(self, ws):
        ws.api_client.do.return_value = {}

        ModelServices(ws).chat(model="system.ai.x", messages=[], max_tokens=5)

        assert "temperature" not in ws.api_client.do.call_args.kwargs["body"]


class TestEmbed:
    def test_posts_inputs_to_the_embeddings_route(self, ws):
        ws.api_client.do.return_value = {"data": [{"index": 0, "embedding": [0.1]}]}

        response = ModelServices(ws).embed(model="system.ai.gte-large-en", inputs=["a"])

        assert response == {"data": [{"index": 0, "embedding": [0.1]}]}
        ws.api_client.do.assert_called_once_with(
            "POST", "/ai-gateway/mlflow/v1/embeddings", body={"model": "system.ai.gte-large-en", "input": ["a"]}
        )


class TestExists:
    def test_true_when_the_model_service_resolves(self, ws):
        ws.api_client.do.return_value = {"name": "model-services/system.ai.x"}

        assert ModelServices(ws).exists("system.ai.x") is True
        ws.api_client.do.assert_called_once_with("GET", "/api/2.1/unity-catalog/model-services/system.ai.x")

    def test_encodes_each_name_part_into_the_path(self, ws):
        ws.api_client.do.return_value = {}

        ModelServices(ws).exists("a.b.c/../../../permissions/catalog/x?y#z")

        path = ws.api_client.do.call_args.args[1]
        assert path == "/api/2.1/unity-catalog/model-services/a.b.c%2F..%2F..%2F..%2Fpermissions%2Fcatalog%2Fx%3Fy%23z"

    def test_false_when_not_found(self, ws):
        ws.api_client.do.side_effect = NotFound("Resource not found")

        assert ModelServices(ws).exists("system.ai.x") is False

    def test_other_errors_propagate(self, ws):
        ws.api_client.do.side_effect = PermissionDenied("no")

        with pytest.raises(PermissionDenied):
            ModelServices(ws).exists("system.ai.x")


class TestListNames:
    def test_filters_by_api_type_and_follows_pages(self, ws):
        ws.api_client.do.side_effect = [
            {
                "model_services": [
                    {"name": "model-services/system.ai.b", "supported_api_types": [CHAT_COMPLETIONS_API]},
                    {"name": "model-services/system.ai.e", "supported_api_types": [EMBEDDINGS_API]},
                ],
                "next_page_token": "t1",
            },
            {"model_services": [{"name": "model-services/system.ai.a", "supported_api_types": [CHAT_COMPLETIONS_API]}]},
        ]

        names = ModelServices(ws).list_names(api_type=CHAT_COMPLETIONS_API)

        assert names == ["system.ai.a", "system.ai.b"]
        assert ws.api_client.do.call_args_list[1].kwargs["query"] == {
            "parent": "schemas/system.ai",
            "page_token": "t1",
        }
