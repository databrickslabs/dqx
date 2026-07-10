"""Unit tests for ``backend.routes.v1.quarantine_samples``.

Follows ``test_dq_score_route.py``'s convention: a minimal FastAPI app
with just the router under test and ``dependency_overrides`` for the
SQL/auth seams (spec-bound autospecs, no live workspace).

Security invariants under test (design doc §3):

1. A live OBO SELECT self-check on the SOURCE table runs as the caller
   before any row data is returned.
2. Denial yields HTTP 200 with an empty records list — never 403/404,
   which would confirm or deny the table's existence.
3. A row filter or column mask on the source table suppresses the sample
   entirely (``suppressed: true``, no records).
4. The quarantine-table read runs via the SP executor, only after 1–3.
5. Ordering: SELECT self-check first, fine-grained check second, SP
   fetch last — a user without access never even triggers the metadata
   probe.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, ColumnMask, TableInfo, TableRowFilter
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_conf,
    get_obo_ws,
    get_preview_sql_executor,
    get_sp_sql_executor,
    get_user_role,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

FQN = "main.sales.orders"
URL = f"/api/v1/quarantine-samples/{FQN}"

PLAIN_TABLE = TableInfo(row_filter=None, columns=[ColumnInfo(name="id"), ColumnInfo(name="amount")])


@pytest.fixture
def obo_sql_mock() -> MagicMock:
    """OBO preview executor — the SELECT self-check passes by default."""
    mock = create_autospec(SqlExecutor, instance=True)
    mock.query.return_value = []
    return mock


@pytest.fixture
def obo_ws_mock() -> MagicMock:
    """OBO WorkspaceClient — no fine-grained controls by default."""
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.tables.get.return_value = PLAIN_TABLE
    return ws


@pytest.fixture
def sp_sql_mock() -> MagicMock:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def client(obo_sql_mock, obo_ws_mock, sp_sql_mock, app_config) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.quarantine_samples import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/quarantine-samples")
    app.dependency_overrides[get_preview_sql_executor] = lambda: obo_sql_mock
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws_mock
    app.dependency_overrides[get_sp_sql_executor] = lambda: sp_sql_mock
    app.dependency_overrides[get_conf] = lambda: app_config
    app.dependency_overrides[get_user_role] = lambda: UserRole.VIEWER
    return TestClient(app)


def quarantine_row(
    quarantine_id: str = "q1",
    row_data: dict[str, object] | None = None,
    errors: object = None,
    warnings: object = None,
) -> dict[str, str | None]:
    """One dq_quarantine_records row as the SP SELECT returns it.

    ``row_data``/``errors``/``warnings`` are VARIANT columns rendered via
    ``to_json(...)``, so they arrive as JSON strings (mirrors
    ``_query_quarantine`` in routes/v1/quarantine.py).
    """
    return {
        "quarantine_id": quarantine_id,
        "run_id": "r1",
        "row_data": json.dumps(row_data if row_data is not None else {"id": 1, "amount": "12.5"}),
        "errors": json.dumps(errors) if errors is not None else None,
        "warnings": json.dumps(warnings) if warnings is not None else None,
        "created_at": "2026-07-10 00:00:00",
    }


class TestObODenial:
    """Invariants 1, 2, 4, 5 — the cheap-denial path."""

    def test_denied_user_gets_200_with_empty_records(self, client, obo_sql_mock):
        obo_sql_mock.query.side_effect = RuntimeError("PERMISSION_DENIED: no SELECT on table")
        resp = client.get(URL)
        # Empty 200, never 403/404 — must not confirm or deny the table's
        # existence to a caller without access to it.
        assert resp.status_code == 200
        body = resp.json()
        assert body["records"] == []
        assert body["suppressed"] is False
        assert body["source_table_fqn"] == FQN

    def test_denied_user_triggers_no_sp_read_and_no_metadata_probe(
        self, client, obo_sql_mock, obo_ws_mock, sp_sql_mock
    ):
        obo_sql_mock.query.side_effect = RuntimeError("PERMISSION_DENIED")
        client.get(URL)
        # No quarantine data is ever fetched (invariant 4) and the caller
        # does not even learn whether the table has fine-grained controls
        # (invariant 5).
        sp_sql_mock.query_dicts.assert_not_called()
        obo_ws_mock.tables.get.assert_not_called()

    def test_self_check_runs_as_the_user_not_the_sp(self, client, obo_sql_mock, sp_sql_mock):
        client.get(URL)
        # The source-table probe goes through the OBO executor only.
        stmt = obo_sql_mock.query.call_args[0][0]
        assert "`main`.`sales`.`orders`" in stmt
        assert "LIMIT 0" in stmt
        for call in sp_sql_mock.query_dicts.call_args_list:
            assert "`main`.`sales`.`orders`" not in call[0][0]


class TestSuppression:
    """Invariant 3 — fine-grained controls suppress the sample entirely."""

    def test_row_filter_suppresses_sample(self, client, obo_ws_mock, sp_sql_mock):
        obo_ws_mock.tables.get.return_value = TableInfo(
            row_filter=TableRowFilter(function_name="main.sales.f", input_column_names=["region"])
        )
        resp = client.get(URL)
        assert resp.status_code == 200
        body = resp.json()
        assert body["suppressed"] is True
        assert body["records"] == []
        sp_sql_mock.query_dicts.assert_not_called()

    def test_column_mask_suppresses_sample(self, client, obo_ws_mock, sp_sql_mock):
        obo_ws_mock.tables.get.return_value = TableInfo(
            row_filter=None,
            columns=[ColumnInfo(name="ssn", mask=ColumnMask(function_name="main.sales.m"))],
        )
        resp = client.get(URL)
        assert resp.json()["suppressed"] is True
        assert resp.json()["records"] == []
        sp_sql_mock.query_dicts.assert_not_called()

    def test_metadata_failure_fails_closed_to_suppression(self, client, obo_ws_mock, sp_sql_mock):
        obo_ws_mock.tables.get.side_effect = RuntimeError("transient UC failure")
        resp = client.get(URL)
        assert resp.status_code == 200
        assert resp.json()["suppressed"] is True
        assert resp.json()["records"] == []
        sp_sql_mock.query_dicts.assert_not_called()


class TestCheckOrdering:
    """Invariant 5 — SELECT self-check, then fine-grained check, then SP fetch."""

    def test_checks_run_in_order_before_data_fetch(self, client, obo_sql_mock, obo_ws_mock, sp_sql_mock):
        order: list[str] = []
        obo_sql_mock.query.side_effect = lambda *a, **k: order.append("obo_select_check") or []
        obo_ws_mock.tables.get.side_effect = lambda *a, **k: order.append("fine_grained_check") or PLAIN_TABLE
        sp_sql_mock.query_dicts.side_effect = lambda *a, **k: order.append("sp_fetch") or []
        resp = client.get(URL)
        assert resp.status_code == 200
        assert order == ["obo_select_check", "fine_grained_check", "sp_fetch"]


class TestHappyPath:
    def test_returns_rows_with_per_cell_failures(self, client, sp_sql_mock):
        sp_sql_mock.query_dicts.return_value = [
            quarantine_row(
                quarantine_id="q1",
                row_data={"id": 1, "name": None, "amount": "12.5"},
                errors=[{"name": "id_is_null", "message": "Column 'id' value is null", "columns": ["id", "amount"]}],
                warnings=[{"name": "name_empty", "message": "empty", "columns": ["name"]}],
            )
        ]
        resp = client.get(URL)
        assert resp.status_code == 200
        body = resp.json()
        assert body["suppressed"] is False
        assert len(body["records"]) == 1
        record = body["records"][0]
        assert record["record_key"] == "q1"
        assert record["row_values"] == {"id": "1", "name": None, "amount": "12.5"}
        assert record["failed_columns"] == ["amount", "id", "name"]
        assert record["failures"] == [
            {"rule_name": "id_is_null", "message": "Column 'id' value is null", "columns": ["id", "amount"]},
            {"rule_name": "name_empty", "message": "empty", "columns": ["name"]},
        ]

    def test_legacy_dict_shaped_errors_are_coerced(self, client, sp_sql_mock):
        # Legacy SQL-check rows wrote errors as a single {check_name: message}
        # dict (see quarantine.py's _row_to_record) — still displayed, no
        # column attribution.
        sp_sql_mock.query_dicts.return_value = [
            quarantine_row(errors={"orders_sql_check": "row failed the check"})
        ]
        resp = client.get(URL)
        record = resp.json()["records"][0]
        assert record["failures"] == [
            {"rule_name": "orders_sql_check", "message": "row failed the check", "columns": []}
        ]
        assert record["failed_columns"] == []

    def test_malformed_payloads_are_tolerated(self, client, sp_sql_mock):
        # A corrupt VARIANT rendering must not 500 the whole sample.
        row = quarantine_row()
        row["row_data"] = "{not json"
        row["errors"] = "also not json"
        sp_sql_mock.query_dicts.return_value = [row]
        resp = client.get(URL)
        assert resp.status_code == 200
        record = resp.json()["records"][0]
        assert record["row_values"] == {}
        assert record["failures"] == []

    def test_query_targets_quarantine_table_and_escapes_fqn(self, client, sp_sql_mock, app_config):
        client.get(URL)
        stmt = sp_sql_mock.query_dicts.call_args[0][0]
        # Catalog/schema are backtick-quoted (hyphenated-catalog support).
        assert f"`{app_config.catalog}`.`{app_config.schema_name}`.dq_quarantine_records" in stmt
        assert f"source_table_fqn = '{FQN}'" in stmt
        assert "to_json(row_data)" in stmt
        assert "to_json(errors)" in stmt
        assert "to_json(warnings)" in stmt
        assert "LIMIT 50" in stmt

    def test_limit_param_is_honoured(self, client, sp_sql_mock):
        client.get(URL, params={"limit": 7})
        assert "LIMIT 7" in sp_sql_mock.query_dicts.call_args[0][0]

    @pytest.mark.parametrize("limit", [0, 201])
    def test_out_of_range_limit_is_rejected(self, client, sp_sql_mock, limit):
        resp = client.get(URL, params={"limit": limit})
        assert resp.status_code == 422
        sp_sql_mock.query_dicts.assert_not_called()


class TestValidationAndErrors:
    def test_malformed_fqn_returns_400_without_any_backend_call(
        self, client, obo_sql_mock, obo_ws_mock, sp_sql_mock
    ):
        resp = client.get("/api/v1/quarantine-samples/not-a-three-part-name")
        assert resp.status_code == 400
        obo_sql_mock.query.assert_not_called()
        obo_ws_mock.tables.get.assert_not_called()
        sp_sql_mock.query_dicts.assert_not_called()

    def test_sp_query_failure_maps_to_500(self, client, sp_sql_mock):
        # Only reachable AFTER both OBO checks passed, so a 500 here leaks
        # nothing to unauthorized callers.
        sp_sql_mock.query_dicts.side_effect = RuntimeError("warehouse down")
        resp = client.get(URL)
        assert resp.status_code == 500


class TestRbac:
    def test_route_is_gated_for_all_roles(self):
        """The app-level gate is viewer-visible (defense in depth); the real
        row-data boundary is the live OBO self-check."""
        from databricks_labs_dqx_app.backend.routes.v1 import quarantine_samples

        for route in quarantine_samples.router.routes:
            if getattr(route, "operation_id", None) != "getQuarantineSample":
                continue
            for dep in route.dependencies:
                for cell in getattr(dep.dependency, "__closure__", None) or ():
                    val = cell.cell_contents
                    if isinstance(val, tuple) and val and all(isinstance(v, UserRole) for v in val):
                        assert set(val) == {
                            UserRole.ADMIN,
                            UserRole.RULE_APPROVER,
                            UserRole.RULE_AUTHOR,
                            UserRole.VIEWER,
                        }
                        return
        raise AssertionError("No require_role dependency found for getQuarantineSample")


class TestHyphenatedAppCatalog:
    def test_quarantine_read_is_quoted(self, client, sp_sql_mock, app_config):
        # The dq_quarantine_records FQN must backtick-quote the
        # config-sourced catalog/schema (quote_object_fqn) so a hyphenated
        # app catalog stays parseable — dq_results-read convention.
        client.app.dependency_overrides[get_conf] = lambda: app_config.model_copy(
            update={"catalog": "prod-east", "schema_name": "dqx-studio"}
        )
        resp = client.get(URL)
        assert resp.status_code == 200
        stmt = sp_sql_mock.query_dicts.call_args[0][0]
        assert "`prod-east`.`dqx-studio`.dq_quarantine_records" in stmt
