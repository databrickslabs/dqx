"""Tests for RuleTestService — rule-test execution + AI test-data generation (P22-E)."""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from databricks.labs.dqx.errors import UnsafeSqlQueryError

from databricks_labs_dqx_app.backend.rule_test_sql import AdhocSource, TableSource
from databricks_labs_dqx_app.backend.services.ai_gateway import AIGateway, AIResponseParseError
from databricks_labs_dqx_app.backend.services.rule_test_service import RuleTestService


@pytest.fixture
def ai_gateway():
    gw = create_autospec(AIGateway, instance=True)
    gw.is_enabled.return_value = True
    gw.endpoint_name.return_value = "databricks-gpt-5-5"
    return gw


@pytest.fixture
def service(sql_executor_mock, ai_gateway):
    return RuleTestService(sql=sql_executor_mock, ai_gateway=ai_gateway)


class TestAiAvailable:
    def test_true_when_enabled_with_endpoint(self, service):
        assert service.ai_available() is True

    def test_false_when_disabled(self, service, ai_gateway):
        ai_gateway.is_enabled.return_value = False
        assert service.ai_available() is False


class TestRunAdhoc:
    @pytest.mark.asyncio
    async def test_runs_and_maps_verdicts(self, service, sql_executor_mock):
        sql_executor_mock.query_dicts.return_value = [
            {"amount": "5", "__row_idx": "0", "__passed": "true"},
            {"amount": "-3", "__row_idx": "1", "__passed": "false"},
        ]
        src = AdhocSource(columns=["amount"], rows=[["5"], ["-3"]], families={"amount": "numeric"}, column_mapping={"amount": "amount"})

        result = await service.run_adhoc(predicate="{{amount}} > 0", polarity="pass", source=src)

        sql = sql_executor_mock.query_dicts.call_args.args[0]
        assert "VALUES (0, '5'), (1, '-3')" in sql
        assert result.columns == ["amount"]
        assert result.rows[0].passed is True
        assert result.rows[1].passed is False
        assert result.rows[0].row_idx == 0

    @pytest.mark.asyncio
    async def test_rejects_unsafe_predicate(self, service, sql_executor_mock):
        src = AdhocSource(columns=["a"], rows=[["1"]], families={}, column_mapping={"a": "a"})
        with pytest.raises(UnsafeSqlQueryError):
            await service.run_adhoc(predicate="1=1; DROP TABLE x", polarity="pass", source=src)
        sql_executor_mock.query_dicts.assert_not_called()

    @pytest.mark.asyncio
    async def test_assembled_query_gate_rejects_injected_keyword(self, service, sql_executor_mock):
        # P22-E: a manual (or AI-generated) cell carrying a forbidden statement is
        # neutralised into a quoted literal by _lit, and the fully-assembled-query
        # is_sql_query_safe gate rejects it as belt-and-braces — nothing runs.
        src = AdhocSource(
            columns=["a", "b"],
            rows=[["foo\\", "'); DROP TABLE t; --"]],
            families={"a": "text", "b": "text"},
            column_mapping={"a": "a", "b": "b"},
        )
        with pytest.raises(UnsafeSqlQueryError):
            await service.run_adhoc(predicate="{{a}} IS NOT NULL", polarity="pass", source=src)
        sql_executor_mock.query_dicts.assert_not_called()

    @pytest.mark.asyncio
    async def test_filter_yields_none_verdict_for_excluded_row(self, service, sql_executor_mock):
        # A row the ROW FILTER excludes comes back with a SQL NULL verdict, which
        # maps to a None verdict (grid leaves it untinted); an in-filter row keeps
        # its pass/fail verdict.
        sql_executor_mock.query_dicts.return_value = [
            {"amount": "5", "region": "US", "__row_idx": "0", "__passed": "true"},
            {"amount": "-3", "region": "US", "__row_idx": "1", "__passed": "false"},
            {"amount": "9", "region": "EU", "__row_idx": "2", "__passed": None},
        ]
        src = AdhocSource(
            columns=["amount", "region"],
            rows=[["5", "US"], ["-3", "US"], ["9", "EU"]],
            families={"amount": "numeric", "region": "text"},
            column_mapping={"amount": "amount", "region": "region"},
        )

        result = await service.run_adhoc(
            predicate="{{amount}} > 0", polarity="pass", source=src, row_filter="{{region}} = 'US'"
        )

        sql = sql_executor_mock.query_dicts.call_args.args[0]
        assert "CASE WHEN NOT (`region` = 'US')" in sql
        assert result.rows[0].passed is True
        assert result.rows[1].passed is False
        assert result.rows[2].passed is None

    @pytest.mark.asyncio
    async def test_rejects_unsafe_filter(self, service, sql_executor_mock):
        # The filter is user SQL and must pass the same is_sql_query_safe gate as
        # the predicate — an unsafe filter is rejected before anything runs.
        src = AdhocSource(columns=["a"], rows=[["1"]], families={}, column_mapping={"a": "a"})
        with pytest.raises(UnsafeSqlQueryError):
            await service.run_adhoc(
                predicate="{{a}} > 0", polarity="pass", source=src, row_filter="1=1; DROP TABLE x"
            )
        sql_executor_mock.query_dicts.assert_not_called()

    @pytest.mark.asyncio
    async def test_injection_data_without_keyword_runs_as_harmless_literal(self, service, sql_executor_mock):
        # A trailing-backslash + break-out attempt with no forbidden keyword passes
        # the safety gate BUT is fully escaped: the payload can only ever appear as
        # an inert quoted literal, never as executable SQL.
        sql_executor_mock.query_dicts.return_value = [{"a": "x", "__row_idx": "0", "__passed": "true"}]
        src = AdhocSource(
            columns=["a", "b"],
            rows=[["foo\\", "') OR 1=1 --"]],
            families={"a": "text", "b": "text"},
            column_mapping={"a": "a", "b": "b"},
        )
        await service.run_adhoc(predicate="{{a}} IS NOT NULL", polarity="pass", source=src)
        sql = sql_executor_mock.query_dicts.call_args.args[0]
        assert r"'foo\\'" in sql  # backslash doubled — literal stays closed
        assert "''') OR 1=1 --'" in sql  # payload is a quoted literal, not raw SQL


class TestRunTable:
    @pytest.mark.asyncio
    async def test_runs_against_table(self, service, sql_executor_mock):
        sql_executor_mock.query_dicts.return_value = [{"amount": "5", "__passed": "true"}]
        src = TableSource(table="c.s.t", column_mapping={"amount": "amount"}, sample_kind="records", sample_value=10)

        result = await service.run_table(predicate="{{amount}} > 0", polarity="pass", source=src)

        sql = sql_executor_mock.query_dicts.call_args.args[0]
        assert "FROM `c`.`s`.`t` ORDER BY rand() LIMIT 10" in sql
        assert result.rows[0].passed is True
        assert result.rows[0].row_idx is None

    @pytest.mark.asyncio
    async def test_rejects_invalid_fqn(self, service):
        src = TableSource(table="bad", column_mapping={"amount": "amount"})
        with pytest.raises(ValueError):
            await service.run_table(predicate="{{amount}} > 0", polarity="pass", source=src)


class TestGenerateTestData:
    @pytest.mark.asyncio
    async def test_parses_and_projects_columns(self, service, ai_gateway):
        ai_gateway.query.return_value = '{"columns": ["amount"], "rows": [["5"], ["-3"]]}'

        result = await service.generate_test_data(
            predicate="{{amount}} > 0", polarity="pass", columns=[("amount", "numeric")], row_count=8, user_email="u@x"
        )

        assert result.columns == ["amount"]
        assert result.rows == [["5"], ["-3"]]

    @pytest.mark.asyncio
    async def test_coerces_scalar_cells_and_pads_short_rows(self, service, ai_gateway):
        ai_gateway.query.return_value = '{"rows": [[5, true], ["only-one"]]}'

        result = await service.generate_test_data(
            predicate="p", polarity="pass", columns=[("a", "numeric"), ("b", "boolean")], row_count=8, user_email="u@x"
        )

        assert result.columns == ["a", "b"]
        assert result.rows == [["5", "true"], ["only-one", None]]

    @pytest.mark.asyncio
    async def test_row_count_clamped_into_prompt(self, service, ai_gateway):
        ai_gateway.query.return_value = '{"rows": []}'
        await service.generate_test_data(predicate="p", polarity="pass", columns=[("a", "text")], row_count=999, user_email="u@x")
        user_msg = ai_gateway.query.call_args.kwargs["messages"][1]["content"]
        assert '"row_count": 20' in user_msg

    @pytest.mark.asyncio
    async def test_malformed_response_raises(self, service, ai_gateway):
        ai_gateway.query.return_value = "not json at all"
        with pytest.raises(AIResponseParseError):
            await service.generate_test_data(predicate="p", polarity="pass", columns=[("a", "text")], row_count=8, user_email="u@x")
