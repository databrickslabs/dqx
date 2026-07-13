"""Tests for the View Data routes (P22-B) — preview + AI query error mapping."""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from databricks.labs.dqx.errors import UnsafeSqlQueryError
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.table_data import (
    TablePreviewIn,
    TableQueryIn,
    get_sample_questions,
    preview_table_data,
    query_table_data,
    router,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import AIUnavailableError
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService, TableColumn
from databricks_labs_dqx_app.backend.services.table_data_service import PreviewResult, TableDataService


@pytest.fixture
def svc():
    return create_autospec(TableDataService, instance=True)


@pytest.fixture
def discovery():
    mock = create_autospec(DiscoveryService, instance=True)
    mock.get_table_columns_async.return_value = [
        TableColumn(name="city", type_name="STRING", comment=None, nullable=True, position=0)
    ]
    return mock


def _result():
    return PreviewResult(columns=["id"], rows=[{"id": "1"}], generated_sql=None, truncated=False)


class TestPreview:
    @pytest.mark.asyncio
    async def test_returns_rows_and_ai_flag(self, svc):
        async def _preview(fqn):
            return _result()

        svc.preview.side_effect = _preview
        svc.ai_available.return_value = True

        result = await preview_table_data(TablePreviewIn(table_fqn="c.s.t"), svc)
        assert result.row_count == 1
        assert result.columns == ["id"]
        assert result.ai_available is True

    @pytest.mark.asyncio
    async def test_invalid_fqn_maps_to_400(self, svc):
        async def _boom(fqn):
            raise ValueError("bad fqn")

        svc.preview.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await preview_table_data(TablePreviewIn(table_fqn="bad"), svc)
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_sql_failure_maps_to_502(self, svc):
        async def _boom(fqn):
            raise RuntimeError("warehouse down")

        svc.preview.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await preview_table_data(TablePreviewIn(table_fqn="c.s.t"), svc)
        assert exc.value.status_code == 502


class TestQuery:
    @pytest.mark.asyncio
    async def test_returns_generated_sql(self, svc):
        async def _query(fqn, question, email):
            return PreviewResult(columns=["id"], rows=[{"id": "6"}], generated_sql="SELECT id FROM c.s.t", truncated=False)

        svc.query.side_effect = _query
        result = await query_table_data(TableQueryIn(table_fqn="c.s.t", question="q"), svc, "user@x")
        assert result.generated_sql == "SELECT id FROM c.s.t"
        assert result.ai_available is True

    @pytest.mark.asyncio
    async def test_ai_unavailable_maps_to_503(self, svc):
        async def _boom(fqn, question, email):
            raise AIUnavailableError("off")

        svc.query.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await query_table_data(TableQueryIn(table_fqn="c.s.t", question="q"), svc, "user@x")
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_unsafe_sql_maps_to_400(self, svc):
        async def _boom(fqn, question, email):
            raise UnsafeSqlQueryError("nope")

        svc.query.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await query_table_data(TableQueryIn(table_fqn="c.s.t", question="q"), svc, "user@x")
        assert exc.value.status_code == 400


_THREE = ["Which city is hottest?", "How does rainfall vary by city?", "What is the average temperature?"]


class TestSampleQuestions:
    @pytest.mark.asyncio
    async def test_happy_path_returns_three_questions(self, svc, discovery):
        svc.ai_available.return_value = True
        svc.sample_questions.return_value = list(_THREE)

        result = await get_sample_questions("c.s.t", svc, discovery, "user@x")

        assert result.questions == _THREE
        discovery.get_table_columns_async.assert_awaited_once_with("c", "s", "t")

    @pytest.mark.asyncio
    async def test_ai_unavailable_returns_empty_without_lookups(self, svc, discovery):
        svc.ai_available.return_value = False

        result = await get_sample_questions("c.s.t", svc, discovery, "user@x")

        assert result.questions == []
        discovery.get_table_columns_async.assert_not_called()
        svc.sample_questions.assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_fqn_returns_empty_without_lookups(self, svc, discovery):
        svc.ai_available.return_value = True

        result = await get_sample_questions("not_an_fqn", svc, discovery, "user@x")

        assert result.questions == []
        discovery.get_table_columns_async.assert_not_called()

    @pytest.mark.asyncio
    async def test_schema_fetch_failure_degrades_to_empty(self, svc, discovery):
        svc.ai_available.return_value = True
        discovery.get_table_columns_async.side_effect = RuntimeError("uc down")

        result = await get_sample_questions("c.s.t", svc, discovery, "user@x")

        assert result.questions == []

    @pytest.mark.asyncio
    async def test_service_failure_degrades_to_empty(self, svc, discovery):
        svc.ai_available.return_value = True
        svc.sample_questions.side_effect = RuntimeError("endpoint down")

        result = await get_sample_questions("c.s.t", svc, discovery, "user@x")

        assert result.questions == []

    def test_rbac_matches_the_ask_endpoint(self):
        """Same gating as /query: no role guard — auth + UC OBO perms are the boundary."""
        by_path = {route.path: route for route in router.routes}
        assert "/sample-questions" in by_path
        assert by_path["/sample-questions"].dependencies == by_path["/query"].dependencies == []
