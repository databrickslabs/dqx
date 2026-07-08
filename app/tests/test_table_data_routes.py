"""Tests for the View Data routes (P22-B) — preview + AI query error mapping."""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from databricks.labs.dqx.errors import UnsafeSqlQueryError
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.table_data import (
    TablePreviewIn,
    TableQueryIn,
    preview_table_data,
    query_table_data,
)
from databricks_labs_dqx_app.backend.services.ai_gateway import AIUnavailableError
from databricks_labs_dqx_app.backend.services.table_data_service import PreviewResult, TableDataService


@pytest.fixture
def svc():
    return create_autospec(TableDataService, instance=True)


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
