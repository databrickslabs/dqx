"""Tests for the ``GET /config/serving-endpoints`` admin route (Rules Registry Phase 7F).

Backs the AI settings serving-endpoint dropdown: returns the workspace's serving
endpoint names, degrading to an empty list (never a 500) when the SDK call fails.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from databricks_labs_dqx_app.backend.routes.v1.config import list_serving_endpoints


@pytest.fixture
def sp_ws():
    return create_autospec(WorkspaceClient, instance=True)


class TestListServingEndpoints:
    @pytest.mark.asyncio
    async def test_returns_endpoint_names(self, sp_ws):
        sp_ws.serving_endpoints.list.return_value = iter(
            [SimpleNamespace(name="databricks-gpt-5-5"), SimpleNamespace(name="my-custom-endpoint")]
        )

        result = await list_serving_endpoints(sp_ws)

        assert result.names == ["databricks-gpt-5-5", "my-custom-endpoint"]

    @pytest.mark.asyncio
    async def test_sorts_and_dedupes_names(self, sp_ws):
        sp_ws.serving_endpoints.list.return_value = iter(
            [SimpleNamespace(name="zeta"), SimpleNamespace(name="alpha"), SimpleNamespace(name="alpha")]
        )

        result = await list_serving_endpoints(sp_ws)

        assert result.names == ["alpha", "zeta"]

    @pytest.mark.asyncio
    async def test_ignores_unnamed_endpoints(self, sp_ws):
        sp_ws.serving_endpoints.list.return_value = iter([SimpleNamespace(name=None), SimpleNamespace(name="")])

        result = await list_serving_endpoints(sp_ws)

        assert result.names == []

    @pytest.mark.asyncio
    async def test_degrades_to_empty_list_on_sdk_error(self, sp_ws):
        sp_ws.serving_endpoints.list.side_effect = DatabricksError("boom")

        result = await list_serving_endpoints(sp_ws)

        assert result.names == []
