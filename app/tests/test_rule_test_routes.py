"""Tests for the Rule Test routes (P22-E) — run + generate error mapping + native guard."""

from unittest.mock import create_autospec

import pytest
from databricks.labs.dqx.errors import UnsafeSqlQueryError
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.rule_test import (
    AdhocGridIn,
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
from databricks_labs_dqx_app.backend.services.rule_test_service import (
    GeneratedGrid,
    GeneratedTestData,
    RuleTestService,
)


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
                rows=[
                    TestRow(cells={"a": "5"}, passed=True, row_idx=0),
                    TestRow(cells={"a": "-3"}, passed=False, row_idx=1),
                ],
                truncated=False,
            )

        svc.run_adhoc.side_effect = _run
        out = await run_rule_test(_adhoc_body(), svc)
        assert out.columns == ["a"]
        assert out.rows[0].passed is True
        assert out.rows[1].passed is False

    @pytest.mark.asyncio
    async def test_native_compiles_and_runs(self, svc):
        async def _run(**kwargs):
            assert kwargs["predicate"] == "({{a}} IS NOT NULL)"
            return TestRunResult(
                columns=["a"],
                rows=[TestRow(cells={"a": "x"}, passed=True, row_idx=0)],
                truncated=False,
            )

        svc.run_adhoc.side_effect = _run
        body = RuleTestRunIn(
            mode="dqx_native",
            function="is_not_null",
            native_arguments={"column": "{{a}}"},
            slots=[SlotIn(name="a", family="text")],
            source_kind="adhoc",
            adhoc=AdhocRunIn(columns=["a"], rows=[["x"]]),
        )
        out = await run_rule_test(body, svc)
        assert out.rows[0].passed is True

    @pytest.mark.asyncio
    async def test_unsupported_native_rejected_without_calling_service(self, svc):
        body = RuleTestRunIn(
            mode="dqx_native",
            function="is_unique",
            native_arguments={"columns": ["{{a}}"]},
            slots=[SlotIn(name="a", family="text")],
            source_kind="adhoc",
            adhoc=AdhocRunIn(columns=["a"], rows=[["x"]]),
        )
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(body, svc)
        assert exc.value.status_code == 400
        svc.run_adhoc.assert_not_called()

    @pytest.mark.asyncio
    async def test_advanced_lowcode_rejected_without_calling_service(self, svc):
        # A low-code rule that folds joins/group-by into a dataset-level query
        # can't be row-tested — the route 400s (belt-and-braces) rather than
        # letting the service run a misleading row-only test.
        body = _adhoc_body()
        body.mode = "lowcode"
        body.lowcode_advanced = True
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(body, svc)
        assert exc.value.status_code == 400
        assert "joins or grouping" in exc.value.detail
        svc.run_adhoc.assert_not_called()

    @pytest.mark.asyncio
    async def test_simple_lowcode_is_testable(self, svc):
        async def _run(**kwargs):
            return TestRunResult(
                columns=["a"], rows=[TestRow(cells={"a": "5"}, passed=True, row_idx=0)], truncated=False
            )

        svc.run_adhoc.side_effect = _run
        body = _adhoc_body()
        body.mode = "lowcode"
        out = await run_rule_test(body, svc)
        assert out.rows[0].passed is True

    @pytest.mark.asyncio
    async def test_unsafe_predicate_maps_to_400(self, svc):
        async def _boom(**kwargs):
            raise UnsafeSqlQueryError("nope")

        svc.run_adhoc.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(_adhoc_body(), svc)
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_warehouse_failure_surfaces_error_in_502(self, svc):
        async def _boom(**kwargs):
            raise RuntimeError("permission denied on table cat.s.t")

        svc.run_adhoc.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await run_rule_test(_adhoc_body(), svc)
        assert exc.value.status_code == 502
        # The user's own OBO query failure is surfaced so a failed test is actionable.
        assert "permission denied on table cat.s.t" in exc.value.detail

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

    @pytest.mark.asyncio
    async def test_a_joined_table_needs_no_binding_in_table_mode(self, svc):
        # A cross-table rule names the table it joins by its own FQN, so table mode
        # resolves it against real data; only the rule's COLUMN slots need mapping.
        captured: dict[str, object] = {}

        async def _run(**kwargs):
            captured.update(kwargs)
            return TestRunResult(columns=["order_id"], rows=[], truncated=False)

        svc.run_table.side_effect = _run
        body = RuleTestRunIn(
            mode="sql",
            predicate=(
                "SELECT {{order_id}}, (c.id IS NULL) AS condition "
                "FROM {{input_view}} JOIN main.sales.customers c ON c.id = {{order_id}}"
            ),
            slots=[SlotIn(name="order_id", family="any")],
            source_kind="table",
            table=TableRunIn(table_fqn="c.s.t", column_mapping={"order_id": "order_id"}),
        )
        await run_rule_test(body, svc)
        source = captured["source"]
        assert source.column_mapping == {"order_id": "order_id"}

    @pytest.mark.asyncio
    async def test_manual_mode_passes_reference_grids_through_keyed_by_table(self, svc):
        captured: dict[str, object] = {}

        async def _run(**kwargs):
            captured.update(kwargs)
            return TestRunResult(columns=["a"], rows=[], truncated=False)

        svc.run_adhoc.side_effect = _run
        body = _adhoc_body()
        body.adhoc = AdhocRunIn(
            columns=["a"],
            rows=[["5"]],
            ref_grids={"main.sales.customers": AdhocGridIn(columns=["id"], rows=[["5"]], families={"id": "numeric"})},
        )
        await run_rule_test(body, svc)
        source = captured["source"]
        assert source.ref_grids["main.sales.customers"].columns == ["id"]
        assert source.ref_grids["main.sales.customers"].rows == [["5"]]

    @pytest.mark.asyncio
    async def test_a_slot_named_input_view_needs_no_table_binding(self, svc):
        # `{{input_view}}` is DQX's reserved token for the data being checked, which
        # the builders resolve themselves — demanding a column for it would block a
        # test that needs none.
        async def _run(**_kwargs):
            return TestRunResult(columns=["a"], rows=[], truncated=False)

        svc.run_table.side_effect = _run
        body = RuleTestRunIn(
            mode="sql",
            predicate="SELECT (x IS NULL) AS condition FROM {{input_view}}",
            slots=[SlotIn(name="input_view", family="any")],
            source_kind="table",
            table=TableRunIn(table_fqn="c.s.t", column_mapping={}),
        )
        await run_rule_test(body, svc)
        svc.run_table.assert_called_once()


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
    async def test_forwards_reference_tables_and_returns_their_grids(self, svc):
        captured: dict[str, object] = {}

        async def _gen(**kwargs):
            captured.update(kwargs)
            return GeneratedTestData(
                columns=["customer_id"],
                rows=[["C-1"], ["C-9"]],
                refs={"main.sales.customers": GeneratedGrid(columns=[("id", "text")], rows=[["C-1"]])},
            )

        svc.generate_test_data.side_effect = _gen
        out = await generate_rule_test_data(
            GenerateDataIn(
                predicate=(
                    "SELECT (c.id IS NULL) AS condition FROM {{input_view}} "
                    "JOIN main.sales.customers c ON c.id = {{customer_id}}"
                ),
                polarity="pass",
                columns=[SlotIn(name="customer_id", family="text")],
                ref_tables=["main.sales.customers"],
            ),
            svc,
            "u@x",
        )
        assert captured["ref_tables"] == ["main.sales.customers"]
        assert out.refs["main.sales.customers"].columns == [SlotIn(name="id", family="text")]
        assert out.refs["main.sales.customers"].rows == [["C-1"]]

    @pytest.mark.asyncio
    async def test_ai_unavailable_maps_to_503(self, svc):
        async def _boom(**kwargs):
            raise AIUnavailableError("off")

        svc.generate_test_data.side_effect = _boom
        with pytest.raises(HTTPException) as exc:
            await generate_rule_test_data(GenerateDataIn(predicate="p", columns=[SlotIn(name="a")]), svc, "u@x")
        assert exc.value.status_code == 503
