"""Tests for the Rule Test routes (P22-E) — run + generate error mapping + native guard."""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from databricks.labs.dqx.errors import UnsafeSqlQueryError
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.rule_test import (
    AdhocRunIn,
    GenerateDataIn,
    RuleTestRunIn,
    SlotIn,
    TableRunIn,
    generate_rule_test_data,
    run_rule_test,
)
from databricks_labs_dqx_app.backend.rule_test_sql import TestRow, TestRunResult
from databricks_labs_dqx_app.backend.services.ai_gateway import AIUnavailableError
from databricks_labs_dqx_app.backend.services.rule_test_service import GeneratedTestData, RuleTestService


@pytest.fixture
def svc():
    return create_autospec(RuleTestService, instance=True)


def _adhoc_body(**kw):
    return RuleTestRunIn(
        mode="sql",
        predicate="{{a}} > 0",
        polarity="pass",
        slots=[SlotIn(name="a", family="numeric")],
        source_kind="adhoc",
        adhoc=AdhocRunIn(columns=["a"], rows=[["5"], ["-3"]]),
        **kw,
    )


class TestRun:
    @pytest.mark.asyncio
    async def test_adhoc_maps_result(self, svc):
        async def _run(**kwargs):
            return TestRunResult(
                columns=["a"],
                rows=[TestRow(cells={"a": "5"}, passed=True, row_idx=0), TestRow(cells={"a": "-3"}, passed=False, row_idx=1)],
                truncated=False,
            )

        svc.run_adhoc.side_effect = _run
        out = await run_rule_test(_adhoc_body(), svc)
        assert out.columns == ["a"]
        assert out.rows[0].passed is True
        assert out.rows[1].passed is False

    @pytest.mark.asyncio
    async def test_native_rejected_without_calling_service(self, svc):
        body = _adhoc_body()
        body.mode = "dqx_native"
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(body, svc)
        assert exc.value.status_code == 400
        svc.run_adhoc.assert_not_called()

    @pytest.mark.asyncio
    async def test_unsafe_predicate_maps_to_400(self, svc):
        async def _boom(**kwargs):
            raise UnsafeSqlQueryError("nope")

        svc.run_adhoc.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(_adhoc_body(), svc)
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_warehouse_failure_maps_to_502(self, svc):
        async def _boom(**kwargs):
            raise RuntimeError("warehouse down: schema secret")

        svc.run_adhoc.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(_adhoc_body(), svc)
        assert exc.value.status_code == 502
        assert "warehouse down" not in exc.value.detail  # sanitized

    @pytest.mark.asyncio
    async def test_table_missing_mapping_maps_to_400(self, svc):
        body = RuleTestRunIn(
            mode="sql",
            predicate="{{a}} > 0",
            slots=[SlotIn(name="a", family="numeric")],
            source_kind="table",
            table=TableRunIn(table_fqn="c.s.t", column_mapping={}),
        )
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(body, svc)
        assert exc.value.status_code == 400
        svc.run_table.assert_not_called()


class TestGenerate:
    @pytest.mark.asyncio
    async def test_returns_generated_rows(self, svc):
        async def _gen(**kwargs):
            return GeneratedTestData(columns=["a"], rows=[["5"], ["-3"]])

        svc.generate_test_data.side_effect = _gen
        out = await generate_rule_test_data(
            GenerateDataIn(predicate="{{a}} > 0", polarity="pass", columns=[SlotIn(name="a", family="numeric")]),
            svc,
            "u@x",
        )
        assert out.columns == ["a"]
        assert out.rows == [["5"], ["-3"]]

    @pytest.mark.asyncio
    async def test_ai_unavailable_maps_to_503(self, svc):
        async def _boom(**kwargs):
            raise AIUnavailableError("off")

        svc.generate_test_data.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await generate_rule_test_data(
                GenerateDataIn(predicate="p", columns=[SlotIn(name="a")]), svc, "u@x"
            )
        assert exc.value.status_code == 503
