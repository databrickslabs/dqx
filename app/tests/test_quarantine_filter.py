"""Tests for the ``check_name`` filter on the quarantine routes.

We exercise the predicate builder + ``_query_quarantine`` directly with a
fake :class:`SqlExecutor` so we can assert on the SQL that would have been
sent to the warehouse — no warehouse needed.
"""

from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.routes.v1.quarantine import (
    _CHECK_NAME_RE,
    _check_name_predicate,
    _query_quarantine,
)


class _AppConf:
    def __init__(self):
        self.catalog = "main"
        self.schema_name = "dqx"


def _sql(count: int = 0, rows: list | None = None, *, catalog: str = "main", schema: str = "dqx"):
    sql = MagicMock()
    sql.catalog = catalog
    sql.schema = schema
    sql.param.side_effect = lambda name: f":{name}"
    sql.query.return_value = [[count]]
    sql.query_dicts.return_value = rows or []
    return sql


class TestCheckNameRegex:
    @pytest.mark.parametrize(
        "name",
        ["is_not_null", "_internal", "abc123", "Check42", "fare_amount_not_in_range"],
    )
    def test_accepts_valid(self, name: str):
        assert _CHECK_NAME_RE.match(name) is not None

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "1leading_digit",
            "name with space",
            "name'); DROP TABLE x;--",
            "kebab-case",
            "name.with.dots",
            "a" * 129,  # too long
        ],
    )
    def test_rejects_invalid(self, name: str):
        assert _CHECK_NAME_RE.match(name) is None


class TestCheckNamePredicate:
    def test_filters_both_errors_and_warnings(self):
        pred = _check_name_predicate(":check_name")
        # The predicate must cover BOTH the errors column and the warnings
        # column; an OR between the two is the whole point — warning-only
        # rules write to ``warnings`` but never to ``errors``.
        assert "errors" in pred
        assert "warnings" in pred
        assert ":check_name" in pred
        assert " OR " in pred

    def test_uses_typed_from_json_projection(self):
        # We deliberately project the VARIANT column to a typed array so
        # the higher-order ``exists`` can apply ``e -> e.name = ...``.
        # Locking that projection in stops a careless refactor from
        # falling back to a substring match (which would be unsafe).
        pred = _check_name_predicate(":check_name")
        assert "from_json(to_json(errors), 'array<struct<name:string>>')" in pred
        assert "from_json(to_json(warnings), 'array<struct<name:string>>')" in pred
        assert "EXISTS(" in pred


class TestQueryQuarantine:
    def test_omits_check_name_clause_when_none(self):
        sql = _sql(count=42)
        _query_quarantine(sql, _AppConf(), "run-1", offset=0, limit=10)
        count_call = sql.query.call_args.args[0]
        data_call = sql.query_dicts.call_args.args[0]
        assert "exists" not in count_call.lower()
        assert "exists" not in data_call.lower()
        assert "run_id = :run_id" in count_call
        assert "LIMIT :limit OFFSET :offset" in data_call

    def test_appends_check_name_predicate(self):
        sql = _sql(count=3)
        _query_quarantine(sql, _AppConf(), "run-1", offset=0, limit=10, check_name="fare_in_range")
        count_call = sql.query.call_args.args[0]
        data_call = sql.query_dicts.call_args.args[0]
        # Same WHERE on both queries — pagination relies on that
        # invariance, so check explicitly.
        assert "EXISTS(" in count_call
        assert "EXISTS(" in data_call
        assert "e.name = :check_name" in count_call
        assert "w.name = :check_name" in data_call

    def test_rejects_injection_attempt(self):
        sql = _sql()
        with pytest.raises(ValueError, match="Invalid check_name"):
            _query_quarantine(sql, _AppConf(), "run-1", check_name="x'); DROP TABLE quarantine;--")
        # SQL must never reach the warehouse.
        sql.query.assert_not_called()
        sql.query_dicts.assert_not_called()

    def test_run_id_is_bound_even_with_backslash_and_quote(self):
        sql = _sql(count=0)
        run_id = "run\\' OR 1=1 --"
        _query_quarantine(sql, _AppConf(), run_id, offset=0, limit=10)
        count_call = sql.query.call_args.args[0]
        assert run_id not in count_call
        assert sql.query.call_args.kwargs["parameters"] == {"run_id": run_id}
        assert sql.query_dicts.call_args.kwargs["parameters"] == {"run_id": run_id, "limit": 10, "offset": 0}

    def test_pagination_params_propagate_to_data_query(self):
        sql = _sql(count=100)
        _query_quarantine(sql, _AppConf(), "run-1", offset=20, limit=5, check_name="my_check")
        data_call = sql.query_dicts.call_args.args[0]
        assert "LIMIT :limit OFFSET :offset" in data_call
        assert sql.query_dicts.call_args.kwargs["parameters"]["offset"] == 20


class TestHyphenatedAppCatalog:
    """The dq_quarantine_records read FQNs must backtick-quote the
    activated catalog/schema (quote_object_fqn) so a hyphenated app
    catalog stays parseable — same convention as the dq_results reads."""

    QUOTED_TABLE = "`prod-east`.`dqx-studio`.dq_quarantine_records"

    class _HyphenConf:
        def __init__(self):
            self.catalog = "prod-east"
            self.schema_name = "dqx-studio"

    def test_query_quarantine_reads_are_quoted(self):
        sql = _sql(count=0, catalog="prod-east", schema="dqx-studio")
        _query_quarantine(sql, self._HyphenConf(), "run-1", offset=0, limit=10)
        assert self.QUOTED_TABLE in sql.query.call_args.args[0]
        assert self.QUOTED_TABLE in sql.query_dicts.call_args.args[0]

    def test_export_read_is_quoted(self, app_config):
        from unittest.mock import create_autospec

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from databricks_labs_dqx_app.backend.common.authorization import UserRole
        from databricks_labs_dqx_app.backend.dependencies import (
            get_conf,
            get_sp_sql_executor,
            get_user_role,
        )
        from databricks_labs_dqx_app.backend.routes.v1.quarantine import router
        from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

        sql = create_autospec(SqlExecutor, instance=True)
        sql.catalog = "prod-east"
        sql.schema = "dqx-studio"
        sql.param.side_effect = lambda name: f":{name}"
        sql.query_dicts.return_value = []
        app = FastAPI()
        app.include_router(router, prefix="/api/v1/quarantine")
        app.dependency_overrides[get_sp_sql_executor] = lambda: sql
        app.dependency_overrides[get_conf] = lambda: app_config.model_copy(
            update={"catalog": "prod-east", "schema_name": "dqx-studio"}
        )
        app.dependency_overrides[get_user_role] = lambda: UserRole.ADMIN
        client = TestClient(app)

        resp = client.get("/api/v1/quarantine/runs/run-1/export", params={"format": "json"})
        assert resp.status_code == 200
        assert self.QUOTED_TABLE in sql.query_dicts.call_args.args[0]
        assert "run_id = :run_id" in sql.query_dicts.call_args.args[0]
        assert sql.query_dicts.call_args.kwargs["parameters"] == {"run_id": "run-1", "limit": 50_000}
