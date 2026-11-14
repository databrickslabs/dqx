"""Tests for the ``GET /config/model-services`` admin route.

Backs the AI settings model dropdown: returns the chat model services in ``system.ai``,
degrading to an empty list (never a 500) when the lookup fails.
"""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from databricks_labs_dqx_app.backend.routes.v1.config import list_model_services


@pytest.fixture
def sp_ws():
    return create_autospec(WorkspaceClient, instance=True)


class TestListModelServices:
    @pytest.mark.asyncio
    async def test_returns_chat_model_names(self, sp_ws):
        sp_ws.api_client.do.return_value = {
            "model_services": [
                {"name": "model-services/system.ai.gpt-5-5", "supported_api_types": ["mlflow/v1/chat/completions"]},
                {"name": "model-services/system.ai.gte-large-en", "supported_api_types": ["mlflow/v1/embeddings"]},
                {
                    "name": "model-services/system.ai.claude-sonnet-4-5",
                    "supported_api_types": ["anthropic/v1/messages", "mlflow/v1/chat/completions"],
                },
            ]
        }

        result = await list_model_services(sp_ws)

        assert result.names == ["system.ai.claude-sonnet-4-5", "system.ai.gpt-5-5"]

    @pytest.mark.asyncio
    async def test_degrades_to_empty_list_on_sdk_error(self, sp_ws):
        sp_ws.api_client.do.side_effect = DatabricksError("boom")

        result = await list_model_services(sp_ws)

        assert result.names == []
