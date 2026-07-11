"""Unit tests for ``backend.routes.v1.dq_results``.

Follows ``test_dq_score_route.py``'s convention: a minimal FastAPI app
with just the router under test and ``dependency_overrides`` for the
SQL/auth seams (spec-bound autospecs, no live workspace).

The aggregation math itself is pinned in ``test_dq_results_service.py``;
these tests pin the SQL statements, the catalog gating per endpoint, the
AS-OF-RUN attribution wiring (severity/dimension/columns/rule id read
off the view rows and the failure structs' own frozen user_metadata —
never a live join to the binding's current rule metadata), and — for the
filtered failed-rows endpoint — the Task 7 security-invariant ORDER
(OBO self-check first, fine-grained suppression second, SP fetch last).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, ColumnMask, TableInfo, TableRowFilter
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_apply_rules_service,
    get_conf,
    get_data_product_service,
    get_entitlement_service,
    get_monitored_table_service,
    get_obo_ws,
    get_preview_sql_executor,
    get_score_cache_service,
    get_sp_sql_executor,
    get_user_catalog_names,
    get_user_role,
)
from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductDetail,
    DataProductMemberDetail,
    DataProductService,
)
from databricks_labs_dqx_app.backend.services.entitlement_service import EntitlementService
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
from databricks_labs_dqx_app.backend.services.score_view_service import (
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

ACCESSIBLE_CATALOGS = frozenset({"main", "dev"})
FQN = "main.sales.orders"
PLAIN_TABLE = TableInfo(row_filter=None, columns=[ColumnInfo(name="id"), ColumnInfo(name="amount")])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sql_mock() -> MagicMock:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def monitored_tables_mock() -> MagicMock:
    mock = create_autospec(MonitoredTableService, instance=True)
    mock.get.return_value = None
    mock.get_by_table_fqn.return_value = None
    mock.get_binding_ids_by_table_fqn.return_value = {}
    return mock


@pytest.fixture
def apply_rules_mock() -> MagicMock:
    mock = create_autospec(ApplyRulesService, instance=True)
    mock.list_bindings_for_rule.return_value = []
    return mock


@pytest.fixture
def data_products_mock() -> MagicMock:
    mock = create_autospec(DataProductService, instance=True)
    mock.get.return_value = None
    return mock


@pytest.fixture
def app_settings_mock() -> MagicMock:
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_label_definitions.return_value = []
    return mock


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
def score_cache_mock() -> MagicMock:
    mock = create_autospec(ScoreCacheService, instance=True)
    mock.refresh_all_for_tables.return_value = (0, 0)
    return mock


@pytest.fixture
def entitlement_mock() -> MagicMock:
    """Entitlement cache (P4.1 piggyback) — the record upsert is best-effort."""
    mock = create_autospec(EntitlementService, instance=True)
    mock.record_entitlement.return_value = True
    return mock


USER_EMAIL = "viewer@example.com"


@pytest.fixture
def client(
    sql_mock,
    monitored_tables_mock,
    apply_rules_mock,
    data_products_mock,
    app_settings_mock,
    obo_sql_mock,
    obo_ws_mock,
    score_cache_mock,
    entitlement_mock,
    app_config,
) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.dq_results import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/dq-results")
    app.dependency_overrides[get_sp_sql_executor] = lambda: sql_mock
    app.dependency_overrides[get_conf] = lambda: app_config
    app.dependency_overrides[get_user_catalog_names] = lambda: ACCESSIBLE_CATALOGS
    app.dependency_overrides[get_user_role] = lambda: UserRole.VIEWER
    app.dependency_overrides[get_user_email] = lambda: USER_EMAIL
    app.dependency_overrides[get_monitored_table_service] = lambda: monitored_tables_mock
    app.dependency_overrides[get_apply_rules_service] = lambda: apply_rules_mock
    app.dependency_overrides[get_data_product_service] = lambda: data_products_mock
    app.dependency_overrides[get_app_settings_service] = lambda: app_settings_mock
    app.dependency_overrides[get_preview_sql_executor] = lambda: obo_sql_mock
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws_mock
    app.dependency_overrides[get_score_cache_service] = lambda: score_cache_mock
    app.dependency_overrides[get_entitlement_service] = lambda: entitlement_mock
    return TestClient(app)


# ---------------------------------------------------------------------------
# Row/detail builders
# ---------------------------------------------------------------------------


def check_row(
    check: str = "c1",
    errors: int | None = 0,
    warnings: int | None = 0,
    total: int | None = 100,
    run_id: str = "r1",
    run_date: str = "2026-07-01 00:00:00",
    fqn: str = FQN,
    severity: str | None = None,
    dimension: str | None = None,
    rule_id: str | None = None,
    columns: list[str] | None = None,
) -> dict[str, str | None]:
    """One v_dq_check_results row, Statement-Execution shaped (all strings).

    The attribution columns (severity/dimension/registry_rule_id/columns)
    are the AS-OF-RUN values the view parsed out of the run's frozen
    ``checks_json`` — NULL for legacy/untagged runs.
    """
    return {
        "input_location": fqn,
        "run_id": run_id,
        "run_date": run_date,
        "check_name": check,
        "error_count": None if errors is None else str(errors),
        "warning_count": None if warnings is None else str(warnings),
        "input_row_count": None if total is None else str(total),
        "severity": severity,
        "dimension": dimension,
        "registry_rule_id": rule_id,
        "columns_json": None if columns is None else json.dumps(columns),
    }


def metrics_row(fqn: str, run_id: str, input_rows: int | None, valid_rows: int | None) -> dict[str, str | None]:
    return {
        "input_location": fqn,
        "run_id": run_id,
        "input_rows": None if input_rows is None else str(input_rows),
        "valid_rows": None if valid_rows is None else str(valid_rows),
    }


def runs_row(
    run_id: str = "r1",
    run_ts: str = "2026-07-01 00:00:00",
    pass_rate: float | None = 0.9,
    failed: int | None = 10,
    total: int | None = 100,
    run_mode: str | None = "published",
) -> dict[str, str | None]:
    return {
        "run_id": run_id,
        "run_ts": run_ts,
        "pass_rate": None if pass_rate is None else str(pass_rate),
        "failed_tests": None if failed is None else str(failed),
        "total_tests": None if total is None else str(total),
        "run_mode": run_mode,
    }


def sql_dispatch(
    sql_mock: MagicMock,
    *,
    check_rows: list[dict[str, str | None]] | None = None,
    metrics_rows: list[dict[str, str | None]] | None = None,
    runs_rows: list[dict[str, str | None]] | None = None,
    quarantine_rows: list[dict[str, str | None]] | None = None,
) -> None:
    """Route each SP query to its canned result by the statement's target."""

    def dispatch(stmt: str, **_kwargs: object) -> list[dict[str, str | None]]:
        # Quarantine first: its published-run subselect references the
        # shaping view inside the same statement.
        if "dq_quarantine_records" in stmt:
            return quarantine_rows or []
        if SHAPING_VIEW_NAME in stmt:
            return check_rows or []
        if METRIC_VIEW_NAME in stmt:
            return runs_rows or []
        if "dq_metrics" in stmt:
            return metrics_rows or []
        raise AssertionError(f"Unexpected statement: {stmt}")

    sql_mock.query_dicts.side_effect = dispatch


def binding_detail(binding_id: str, fqn: str) -> MonitoredTableDetail:
    # Applied-rule summaries are deliberately absent: since P2.1b the
    # dq-results paths never read the binding's current rule metadata —
    # bindings only resolve ids to tables (runs picker, rule scoping).
    return MonitoredTableDetail(table=MonitoredTable(binding_id=binding_id, table_fqn=fqn), applied_rules=[])


def product_detail(product_id: str, members: list[tuple[str, str]]) -> DataProductDetail:
    return DataProductDetail(
        product=MagicMock(name="DataProduct"),
        members=[
            DataProductMemberDetail(
                id=f"m{i}",
                binding_id=binding_id,
                table_fqn=fqn,
                binding_status="approved",
                binding_version=1,
                pinned_version=None,
                rules_count=1,
                checks_count=1,
                runnable=True,
            )
            for i, (binding_id, fqn) in enumerate(members)
        ],
        member_count=len(members),
    )


LABEL_DEFINITIONS = [
    {
        "key": "dimension",
        "values": ["Validity", "Completeness"],
        "value_colors": {"Validity": "#2563EB", "Completeness": "#16A34A"},
    },
    {
        "key": "severity",
        "values": ["Low", "Medium", "High", "Critical"],
        "value_colors": {"Low": "#16A34A", "Medium": "#D97706", "High": "#EA580C", "Critical": "#DC2626"},
    },
]


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------


class TestRegistries:
    def test_severities_carry_rank_from_array_order(self, client, app_settings_mock):
        app_settings_mock.get_label_definitions.return_value = LABEL_DEFINITIONS
        resp = client.get("/api/v1/dq-results/registries/severities")
        assert resp.status_code == 200
        body = resp.json()
        # dqlake convention: ascending rank, Low=1 .. Critical=4.
        assert body == [
            {"name": "Low", "color": "#16A34A", "rank": 1},
            {"name": "Medium", "color": "#D97706", "rank": 2},
            {"name": "High", "color": "#EA580C", "rank": 3},
            {"name": "Critical", "color": "#DC2626", "rank": 4},
        ]

    def test_dimensions_from_label_definition(self, client, app_settings_mock):
        app_settings_mock.get_label_definitions.return_value = LABEL_DEFINITIONS
        body = client.get("/api/v1/dq-results/registries/dimensions").json()
        assert [d["name"] for d in body] == ["Validity", "Completeness"]
        assert body[0]["rank"] == 1

    def test_missing_color_falls_back_to_neutral(self, client, app_settings_mock):
        app_settings_mock.get_label_definitions.return_value = [
            {"key": "severity", "values": ["Low"], "value_colors": {}}
        ]
        body = client.get("/api/v1/dq-results/registries/severities").json()
        assert body[0]["color"] == "#6B7280"

    def test_absent_definition_yields_empty_list(self, client, app_settings_mock):
        app_settings_mock.get_label_definitions.return_value = []
        assert client.get("/api/v1/dq-results/registries/severities").json() == []
        assert client.get("/api/v1/dq-results/registries/dimensions").json() == []

    def test_settings_failure_maps_to_500(self, client, app_settings_mock):
        app_settings_mock.get_label_definitions.side_effect = RuntimeError("lakebase down")
        assert client.get("/api/v1/dq-results/registries/severities").status_code == 500


# ---------------------------------------------------------------------------
# Table results
# ---------------------------------------------------------------------------


class TestTableResults:
    def test_malformed_fqn_returns_400(self, client, sql_mock):
        resp = client.get("/api/v1/dq-results/table/not-a-three-part-name")
        assert resp.status_code == 400
        sql_mock.query_dicts.assert_not_called()

    def test_inaccessible_catalog_returns_403_before_sql(self, client, sql_mock):
        resp = client.get("/api/v1/dq-results/table/secret.hr.salaries")
        assert resp.status_code == 403
        sql_mock.query_dicts.assert_not_called()

    def test_breakdowns_use_as_of_run_attribution_from_the_view_rows(
        self, client, sql_mock, monitored_tables_mock
    ):
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row(
                    "c1",
                    errors=10,
                    warnings=0,
                    total=100,
                    severity="High",
                    dimension="Completeness",
                    rule_id="rule-1",
                    columns=["id"],
                ),
                check_row(
                    "c2",
                    errors=0,
                    warnings=30,
                    total=100,
                    severity="Low",
                    dimension="Validity",
                    rule_id="rule-2",
                    columns=["amount"],
                ),
            ],
        )
        resp = client.get(f"/api/v1/dq-results/table/{FQN}")
        assert resp.status_code == 200
        body = resp.json()
        by_dim = {g["label"]: g for g in body["by_dimension"]}
        assert by_dim["Completeness"]["pass_rate"] == pytest.approx(0.9)
        assert by_dim["Validity"]["failed_tests"] == 30
        by_sev = {g["label"]: g for g in body["by_severity"]}
        assert set(by_sev) == {"High", "Low"}
        assert {g["label"] for g in body["by_column"]} == {"id", "amount"}
        assert {g["label"] for g in body["by_rule"]} == {"c1", "c2"}
        # Table endpoint fills `tables` (dqlake parity), not `by_table`.
        assert [g["label"] for g in body["tables"]] == [FQN]
        assert body["by_table"] == []
        # binding_id is a by_table-only enrichment; the tables axis keeps None.
        assert body["tables"][0]["binding_id"] is None
        # VERSION ACCURACY: the binding's CURRENT rule metadata is never
        # consulted — attribution is frozen into the run's checks_json.
        monitored_tables_mock.get_by_table_fqn.assert_not_called()

    def test_untagged_runs_serve_unattributed_results(self, client, sql_mock):
        # Legacy runs (NULL checks_json) or hand-authored checks arrive
        # with NULL attribution columns and land in the untagged bucket.
        sql_dispatch(sql_mock, check_rows=[check_row("adhoc_check", errors=1, total=10)])
        body = client.get(f"/api/v1/dq-results/table/{FQN}").json()
        assert [g["label"] for g in body["by_dimension"]] == [None]
        assert [g["label"] for g in body["by_rule"]] == ["adhoc_check"]

    def test_query_targets_shaping_view_and_escapes_fqn(self, client, sql_mock, app_config):
        sql_dispatch(sql_mock)
        client.get(f"/api/v1/dq-results/table/{FQN}")
        stmt = sql_mock.query_dicts.call_args_list[0][0][0]
        # Catalog/schema are backtick-quoted (hyphenated-catalog support).
        assert f"`{app_config.catalog}`.`{app_config.schema_name}`.{SHAPING_VIEW_NAME}" in stmt
        assert f"'{FQN}'" in stmt
        # The as-of-run attribution columns ride along on the same query.
        assert "severity, dimension, registry_rule_id" in stmt
        assert "to_json(columns) AS columns_json" in stmt

    def test_run_id_filter_is_pushed_into_sql(self, client, sql_mock):
        sql_dispatch(sql_mock)
        client.get(f"/api/v1/dq-results/table/{FQN}", params={"run_id": "r42"})
        stmt = sql_mock.query_dicts.call_args_list[0][0][0]
        assert "run_id = 'r42'" in stmt

    def test_facets_restrict_breakdowns_but_not_trend_failures(self, client, sql_mock):
        # dqlake parity: the table reader's trend_failures series filters on
        # binding/run only — the drilldown chips never restrict it.
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("c1", errors=10, total=100, dimension="Completeness"),
                check_row("c2", errors=20, total=100, dimension="Validity"),
            ],
        )
        body = client.get(f"/api/v1/dq-results/table/{FQN}", params={"dimension": "Completeness"}).json()
        assert [g["label"] for g in body["by_rule"]] == ["c1"]
        assert body["trend"][0]["pass_rate"] == pytest.approx(0.9)
        assert body["trend_failures"][0]["failed_test_count"] == 30  # unfiltered

    def test_retagged_rule_keeps_historical_attribution(self, client, sql_mock, monitored_tables_mock):
        # THE POINT of P2.1b: the same check carries the severity it RAN
        # with in each run — a later severity edit (which would change the
        # binding's current metadata) cannot rewrite the older run.
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("c1", errors=1, total=10, run_id="r1", run_date="d1", severity="Low"),
                check_row("c1", errors=1, total=10, run_id="r2", run_date="d2", severity="Critical"),
            ],
        )
        body = client.get(f"/api/v1/dq-results/table/{FQN}").json()
        assert {g["label"] for g in body["by_severity"]} == {"Low", "Critical"}
        monitored_tables_mock.get_by_table_fqn.assert_not_called()

    def test_renamed_rule_is_one_by_rule_row_and_rule_id_facet_spans_the_rename(self, client, sql_mock):
        # P5.2: the same registry rule renamed between runs is ONE by_rule
        # row labeled with the newest run's name and carrying the additive
        # rule_id; faceting on that rule_id selects the old-name runs too.
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("old_name", errors=10, total=100, run_id="r1", run_date="d1", rule_id="rule-1"),
                check_row("new_name", errors=5, total=100, run_id="r2", run_date="d2", rule_id="rule-1"),
            ],
        )
        body = client.get(f"/api/v1/dq-results/table/{FQN}").json()
        assert [(g["label"], g["rule_id"]) for g in body["by_rule"]] == [("new_name", "rule-1")]
        assert body["by_rule"][0]["failed_tests"] == 15
        filtered = client.get(f"/api/v1/dq-results/table/{FQN}", params={"rule": "rule-1"}).json()
        assert filtered["by_rule"][0]["failed_tests"] == 15  # both runs matched

    def test_failed_records_derived_from_input_minus_valid(self, client, sql_mock, monitored_tables_mock):
        sql_dispatch(
            sql_mock,
            check_rows=[check_row("c1", errors=10, total=100, run_id="r1")],
            metrics_rows=[metrics_row(FQN, "r1", input_rows=100, valid_rows=93)],
        )
        body = client.get(f"/api/v1/dq-results/table/{FQN}").json()
        assert body["trend_failures"][0]["failed_records"] == 7

    def test_axes_trend_skips_breakdowns(self, client, sql_mock):
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10)])
        body = client.get(f"/api/v1/dq-results/table/{FQN}", params={"axes": "trend"}).json()
        assert body["by_rule"] == [] and body["tables"] == []
        assert body["trend"] != []

    def test_axes_breakdown_skips_trends(self, client, sql_mock):
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10)])
        body = client.get(f"/api/v1/dq-results/table/{FQN}", params={"axes": "breakdown"}).json()
        assert body["trend"] == [] and body["trend_failures"] == []
        assert body["by_rule"] != []

    def test_oltp_outage_cannot_affect_table_results(self, client, sql_mock, monitored_tables_mock):
        # Attribution no longer touches the OLTP store at all — a broken
        # MonitoredTableService is simply never called on this path.
        monitored_tables_mock.get_by_table_fqn.side_effect = RuntimeError("lakebase hiccup")
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10, dimension="Validity")])
        resp = client.get(f"/api/v1/dq-results/table/{FQN}")
        assert resp.status_code == 200
        assert [g["label"] for g in resp.json()["by_dimension"]] == ["Validity"]

    def test_sql_failure_maps_to_500(self, client, sql_mock):
        sql_mock.query_dicts.side_effect = RuntimeError("warehouse down")
        assert client.get(f"/api/v1/dq-results/table/{FQN}").status_code == 500


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


class TestRuns:
    def test_runs_measure_the_metric_view_grouped_by_run(self, client, sql_mock, app_config):
        sql_dispatch(sql_mock, runs_rows=[runs_row("r2", "2026-07-02 00:00:00", 0.8, 20, 100), runs_row()])
        resp = client.get(f"/api/v1/dq-results/runs/{FQN}")
        assert resp.status_code == 200
        rows = resp.json()["rows"]
        assert rows[0] == {
            "run_id": "r2",
            "run_ts": "2026-07-02 00:00:00",
            "pass_rate": pytest.approx(0.8),
            "failed_tests": 20,
            "total_tests": 100,
            "run_mode": "published",
        }
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert f"`{app_config.catalog}`.`{app_config.schema_name}`.{METRIC_VIEW_NAME}" in stmt
        assert "MEASURE(score) AS pass_rate" in stmt
        # run_mode rides along per run (lossless: a run has exactly one mode).
        assert "GROUP BY run_id, run_time, run_mode" in stmt
        assert "ORDER BY run_time DESC" in stmt
        assert f"'{FQN}'" in stmt

    def test_binding_id_resolves_to_its_table(self, client, sql_mock, monitored_tables_mock):
        monitored_tables_mock.get.return_value = binding_detail("b1", FQN)
        sql_dispatch(sql_mock, runs_rows=[runs_row()])
        resp = client.get("/api/v1/dq-results/runs/b1")
        assert resp.status_code == 200
        assert len(resp.json()["rows"]) == 1
        monitored_tables_mock.get.assert_called_once_with("b1")
        assert f"'{FQN}'" in sql_mock.query_dicts.call_args[0][0]

    def test_unknown_identifier_returns_400(self, client, sql_mock, monitored_tables_mock):
        monitored_tables_mock.get.return_value = None
        resp = client.get("/api/v1/dq-results/runs/nope")
        assert resp.status_code == 400
        sql_mock.query_dicts.assert_not_called()

    def test_inaccessible_catalog_returns_403(self, client, sql_mock):
        resp = client.get("/api/v1/dq-results/runs/secret.hr.salaries")
        assert resp.status_code == 403
        sql_mock.query_dicts.assert_not_called()

    def test_binding_resolving_to_inaccessible_catalog_returns_403(
        self, client, sql_mock, monitored_tables_mock
    ):
        monitored_tables_mock.get.return_value = binding_detail("b1", "secret.hr.salaries")
        resp = client.get("/api/v1/dq-results/runs/b1")
        assert resp.status_code == 403
        sql_mock.query_dicts.assert_not_called()

    def test_binding_resolving_to_invalid_fqn_returns_400_before_sql(self, client, sql_mock, monitored_tables_mock):
        # Defense-in-depth: the binding-resolved FQN is app-DB-sourced and
        # must be re-validated before it reaches a SQL string literal.
        monitored_tables_mock.get.return_value = binding_detail("b1", "main.sales.evil\\")
        resp = client.get("/api/v1/dq-results/runs/b1")
        assert resp.status_code == 400
        sql_mock.query_dicts.assert_not_called()

    def test_sql_failure_maps_to_500(self, client, sql_mock):
        sql_mock.query_dicts.side_effect = RuntimeError("warehouse down")
        assert client.get(f"/api/v1/dq-results/runs/{FQN}").status_code == 500


# ---------------------------------------------------------------------------
# Product results
# ---------------------------------------------------------------------------


class TestProductResults:
    def test_unknown_product_returns_404(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = None
        assert client.get("/api/v1/dq-results/product/nope").status_code == 404
        sql_mock.query_dicts.assert_not_called()

    def test_aggregates_members_with_by_table(self, client, sql_mock, data_products_mock, monitored_tables_mock):
        data_products_mock.get.return_value = product_detail(
            "p1", [("b1", "main.sales.orders"), ("b2", "dev.sales.items")]
        )
        monitored_tables_mock.get_by_table_fqn.return_value = None
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("c1", errors=10, total=100, fqn="main.sales.orders"),
                check_row("c2", errors=30, total=100, fqn="dev.sales.items"),
            ],
        )
        body = client.get("/api/v1/dq-results/product/p1").json()
        # Product endpoint fills `by_table` (dqlake parity), not `tables`.
        assert {g["label"] for g in body["by_table"]} == {"main.sales.orders", "dev.sales.items"}
        assert body["tables"] == []
        assert body["trend"][0]["pass_rate"] == pytest.approx(1 - 40 / 200)
        assert {p["series"] for p in body["trend_by_table"]} == {"main.sales.orders", "dev.sales.items"}

    def test_inaccessible_members_are_filtered_not_403(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = product_detail(
            "p1", [("b1", "main.sales.orders"), ("b2", "secret.hr.salaries")]
        )
        sql_dispatch(sql_mock)
        resp = client.get("/api/v1/dq-results/product/p1")
        assert resp.status_code == 200
        for call in sql_mock.query_dicts.call_args_list:
            assert "secret.hr.salaries" not in call[0][0]

    def test_product_runs_roll_up_member_tables(self, client, sql_mock, data_products_mock):
        data_products_mock.get.return_value = product_detail("p1", [("b1", "main.sales.orders")])
        sql_dispatch(sql_mock, runs_rows=[runs_row()])
        body = client.get("/api/v1/dq-results/product/p1/runs").json()
        assert body["rows"][0]["run_id"] == "r1"
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert "'main.sales.orders'" in stmt

    def test_product_with_no_accessible_members_yields_empty_runs(
        self, client, sql_mock, data_products_mock
    ):
        data_products_mock.get.return_value = product_detail("p1", [("b1", "secret.hr.salaries")])
        body = client.get("/api/v1/dq-results/product/p1/runs").json()
        assert body == {"rows": []}
        sql_mock.query_dicts.assert_not_called()

    def test_by_table_rows_carry_member_binding_ids_without_extra_lookup(
        self, client, sql_mock, data_products_mock, monitored_tables_mock
    ):
        data_products_mock.get.return_value = product_detail(
            "p1", [("b1", "main.sales.orders"), ("b2", "dev.sales.items")]
        )
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("c1", errors=10, total=100, fqn="main.sales.orders"),
                check_row("c2", errors=30, total=100, fqn="dev.sales.items"),
            ],
        )
        body = client.get("/api/v1/dq-results/product/p1").json()
        by_table = {g["label"]: g["binding_id"] for g in body["by_table"]}
        assert by_table == {"main.sales.orders": "b1", "dev.sales.items": "b2"}
        # Member binding ids are already loaded — no extra OLTP round-trip.
        monitored_tables_mock.get_binding_ids_by_table_fqn.assert_not_called()

    def test_invalid_member_fqn_is_filtered_before_interpolation(self, client, sql_mock, data_products_mock):
        # Defense-in-depth: app-DB-sourced member FQNs are re-validated on
        # read — a backslash would defeat escape_sql_string's escaping.
        data_products_mock.get.return_value = product_detail(
            "p1", [("b1", "main.sales.orders"), ("b2", "main.sales.evil\\")]
        )
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10)])
        resp = client.get("/api/v1/dq-results/product/p1")
        assert resp.status_code == 200
        for call in sql_mock.query_dicts.call_args_list:
            assert "evil" not in call[0][0]


# ---------------------------------------------------------------------------
# Global results
# ---------------------------------------------------------------------------


class TestGlobalResults:
    def test_filters_rows_to_accessible_catalogs(self, client, sql_mock, monitored_tables_mock):
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("c1", errors=10, total=100, fqn="main.sales.orders"),
                check_row("c2", errors=50, total=100, fqn="secret.hr.salaries"),
            ],
        )
        body = client.get("/api/v1/dq-results/global").json()
        assert [g["label"] for g in body["by_table"]] == ["main.sales.orders"]
        # Attribution rides on the view rows — no OLTP lookups anywhere.
        monitored_tables_mock.get_by_table_fqn.assert_not_called()

    def test_global_query_has_no_table_filter(self, client, sql_mock, app_config):
        sql_dispatch(sql_mock)
        client.get("/api/v1/dq-results/global")
        stmt = sql_mock.query_dicts.call_args_list[0][0][0]
        assert f"`{app_config.catalog}`.`{app_config.schema_name}`.{SHAPING_VIEW_NAME}" in stmt
        assert "input_location IN" not in stmt

    def test_sql_failure_maps_to_500(self, client, sql_mock):
        sql_mock.query_dicts.side_effect = RuntimeError("warehouse down")
        assert client.get("/api/v1/dq-results/global").status_code == 500

    def test_by_table_rows_carry_binding_ids_from_one_batched_lookup(self, client, sql_mock, monitored_tables_mock):
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("c1", errors=10, total=100, fqn="main.sales.orders"),
                check_row("c2", errors=5, total=100, fqn="dev.sales.items"),  # not monitored
            ],
        )
        monitored_tables_mock.get_binding_ids_by_table_fqn.return_value = {"main.sales.orders": "b1"}
        body = client.get("/api/v1/dq-results/global").json()
        by_table = {g["label"]: g for g in body["by_table"]}
        assert by_table["main.sales.orders"]["binding_id"] == "b1"
        assert by_table["dev.sales.items"]["binding_id"] is None
        # ONE batched lookup over the accessible tables — never per-table.
        monitored_tables_mock.get_binding_ids_by_table_fqn.assert_called_once_with(
            ["dev.sales.items", "main.sales.orders"]
        )
        monitored_tables_mock.get_by_table_fqn.assert_not_called()
        monitored_tables_mock.get.assert_not_called()

    def test_binding_id_lookup_failure_degrades_to_unlinked_rows(self, client, sql_mock, monitored_tables_mock):
        # OLTP outage: results still serve, rows just aren't linkable.
        monitored_tables_mock.get_binding_ids_by_table_fqn.side_effect = RuntimeError("lakebase hiccup")
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10)])
        resp = client.get("/api/v1/dq-results/global")
        assert resp.status_code == 200
        assert resp.json()["by_table"][0]["binding_id"] is None


# ---------------------------------------------------------------------------
# Rule results
# ---------------------------------------------------------------------------


class TestRuleResults:
    def test_restricts_rows_by_the_frozen_registry_rule_id(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        # Applications only SCOPE the tables; row membership comes from
        # each run's own frozen registry_rule_id provenance tag — a check
        # renamed since the run still attributes to the rule.
        apply_rules_mock.list_bindings_for_rule.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="rule-1")
        ]
        monitored_tables_mock.get.return_value = binding_detail("b1", FQN)
        sql_dispatch(
            sql_mock,
            check_rows=[
                check_row("c1", errors=10, total=100, rule_id="rule-1", dimension="Completeness"),
                check_row("c2", errors=30, total=100, rule_id="rule-2"),  # another rule's check — excluded
                check_row("adhoc", errors=5, total=100),  # no provenance — excluded
            ],
        )
        body = client.get("/api/v1/dq-results/rule/rule-1").json()
        assert [g["label"] for g in body["by_rule"]] == ["c1"]
        assert body["trend"][0]["pass_rate"] == pytest.approx(0.9)
        assert [g["label"] for g in body["by_table"]] == [FQN]

    def test_rule_with_no_applications_yields_empty_results(self, client, sql_mock, apply_rules_mock):
        apply_rules_mock.list_bindings_for_rule.return_value = []
        body = client.get("/api/v1/dq-results/rule/rule-1").json()
        assert body["by_rule"] == [] and body["trend"] == []
        sql_mock.query_dicts.assert_not_called()

    def test_inaccessible_tables_are_filtered_not_403(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="rule-1"),
            AppliedRule(id="ar2", binding_id="b2", rule_id="rule-1"),
        ]
        details = {
            "b1": binding_detail("b1", FQN),
            "b2": binding_detail("b2", "secret.hr.salaries"),
        }
        monitored_tables_mock.get.side_effect = details.get
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10, rule_id="rule-1")])
        resp = client.get("/api/v1/dq-results/rule/rule-1")
        assert resp.status_code == 200
        for call in sql_mock.query_dicts.call_args_list:
            assert "secret.hr.salaries" not in call[0][0]

    def test_broken_binding_lookup_is_skipped_not_500(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="rule-1"),
            AppliedRule(id="ar2", binding_id="b-broken", rule_id="rule-1"),
        ]

        def get_binding(binding_id: str) -> MonitoredTableDetail:
            if binding_id == "b1":
                return binding_detail("b1", FQN)
            raise RuntimeError("lakebase hiccup")

        monitored_tables_mock.get.side_effect = get_binding
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10, rule_id="rule-1")])
        resp = client.get("/api/v1/dq-results/rule/rule-1")
        assert resp.status_code == 200
        assert [g["label"] for g in resp.json()["by_table"]] == [FQN]

    def test_by_table_rows_carry_the_scoping_binding_ids(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="rule-1")
        ]
        monitored_tables_mock.get.return_value = binding_detail("b1", FQN)
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10, rule_id="rule-1")])
        body = client.get("/api/v1/dq-results/rule/rule-1").json()
        assert body["by_table"][0]["label"] == FQN
        assert body["by_table"][0]["binding_id"] == "b1"
        # The scoping loop already resolved the bindings — no batched lookup.
        monitored_tables_mock.get_binding_ids_by_table_fqn.assert_not_called()

    def test_invalid_binding_fqn_is_skipped_before_interpolation(
        self, client, sql_mock, apply_rules_mock, monitored_tables_mock
    ):
        apply_rules_mock.list_bindings_for_rule.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="rule-1"),
            AppliedRule(id="ar2", binding_id="b-bad", rule_id="rule-1"),
        ]
        details = {
            "b1": binding_detail("b1", FQN),
            "b-bad": binding_detail("b-bad", "main.sales.evil\\"),
        }
        monitored_tables_mock.get.side_effect = details.get
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10, rule_id="rule-1")])
        resp = client.get("/api/v1/dq-results/rule/rule-1")
        assert resp.status_code == 200
        assert [g["label"] for g in resp.json()["by_table"]] == [FQN]
        for call in sql_mock.query_dicts.call_args_list:
            assert "evil" not in call[0][0]


# ---------------------------------------------------------------------------
# Hyphenated app catalog — quoted read FQNs through every query builder
# ---------------------------------------------------------------------------


class TestHyphenatedAppCatalog:
    """The app's own catalog/schema names come from config and can be
    hyphenated (``prod-east``). Every read-path FQN must backtick-quote its
    parts — consistently with the DDL side — or the statements won't parse."""

    QUOTED_PREFIX = "`prod-east`.`dqx-studio`"

    @pytest.fixture(autouse=True)
    def _hyphenated_conf(self, client, app_config):
        client.app.dependency_overrides[get_conf] = lambda: app_config.model_copy(
            update={"catalog": "prod-east", "schema_name": "dqx-studio"}
        )

    def test_shaping_view_and_dq_metrics_reads_are_quoted(self, client, sql_mock):
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10)])
        assert client.get(f"/api/v1/dq-results/table/{FQN}").status_code == 200
        stmts = [call[0][0] for call in sql_mock.query_dicts.call_args_list]
        assert any(f"{self.QUOTED_PREFIX}.{SHAPING_VIEW_NAME}" in stmt for stmt in stmts)
        assert any(f"{self.QUOTED_PREFIX}.dq_metrics" in stmt for stmt in stmts)

    def test_metric_view_read_is_quoted(self, client, sql_mock):
        sql_dispatch(sql_mock, runs_rows=[runs_row()])
        assert client.get(f"/api/v1/dq-results/runs/{FQN}").status_code == 200
        assert f"{self.QUOTED_PREFIX}.{METRIC_VIEW_NAME}" in sql_mock.query_dicts.call_args[0][0]

    def test_quarantine_read_is_quoted(self, client, sql_mock):
        assert client.get(FAILED_ROWS_URL).status_code == 200
        assert f"{self.QUOTED_PREFIX}.dq_quarantine_records" in sql_mock.query_dicts.call_args[0][0]


# ---------------------------------------------------------------------------
# Filtered failed rows — Task 7 security invariants re-asserted
# ---------------------------------------------------------------------------


def quarantine_row(
    quarantine_id: str = "q1",
    row_data: dict[str, object] | None = None,
    errors: object = None,
    warnings: object = None,
    created_at: str = "2026-07-10 00:00:00",
) -> dict[str, str | None]:
    return {
        "quarantine_id": quarantine_id,
        "run_id": "r1",
        "row_data": json.dumps(row_data if row_data is not None else {"id": 1, "amount": "12.5"}),
        "errors": json.dumps(errors) if errors is not None else None,
        "warnings": json.dumps(warnings) if warnings is not None else None,
        "created_at": created_at,
    }


FAILED_ROWS_URL = f"/api/v1/dq-results/failed-rows/{FQN}"


class TestFailedRowsSecurity:
    """The Task 7 invariants apply UNCHANGED to the filtered endpoint."""

    def test_denied_user_gets_200_with_empty_rows(self, client, obo_sql_mock, obo_ws_mock):
        obo_sql_mock.query.side_effect = RuntimeError("PERMISSION_DENIED: no SELECT on table")
        resp = client.get(FAILED_ROWS_URL)
        assert resp.status_code == 200
        assert resp.json() == {"rows": [], "total": 0, "suppressed": False}
        # The caller does not even learn whether the table has
        # fine-grained controls.
        obo_ws_mock.tables.get.assert_not_called()

    def test_denied_user_triggers_no_sp_read(self, client, obo_sql_mock, sql_mock):
        obo_sql_mock.query.side_effect = RuntimeError("PERMISSION_DENIED")
        client.get(FAILED_ROWS_URL)
        sql_mock.query_dicts.assert_not_called()

    def test_row_filter_suppresses_sample(self, client, obo_ws_mock, sql_mock):
        obo_ws_mock.tables.get.return_value = TableInfo(
            row_filter=TableRowFilter(function_name="main.sales.f", input_column_names=["region"])
        )
        resp = client.get(FAILED_ROWS_URL)
        assert resp.json() == {"rows": [], "total": 0, "suppressed": True}
        sql_mock.query_dicts.assert_not_called()

    def test_column_mask_suppresses_sample(self, client, obo_ws_mock, sql_mock):
        obo_ws_mock.tables.get.return_value = TableInfo(
            row_filter=None,
            columns=[ColumnInfo(name="ssn", mask=ColumnMask(function_name="main.sales.m"))],
        )
        assert client.get(FAILED_ROWS_URL).json()["suppressed"] is True
        sql_mock.query_dicts.assert_not_called()

    def test_metadata_failure_fails_closed_to_suppression(self, client, obo_ws_mock, sql_mock):
        obo_ws_mock.tables.get.side_effect = RuntimeError("transient UC failure")
        assert client.get(FAILED_ROWS_URL).json()["suppressed"] is True
        sql_mock.query_dicts.assert_not_called()

    def test_checks_run_in_order_before_data_fetch(self, client, obo_sql_mock, obo_ws_mock, sql_mock):
        order: list[str] = []
        obo_sql_mock.query.side_effect = lambda *a, **k: order.append("obo_select_check") or []
        obo_ws_mock.tables.get.side_effect = lambda *a, **k: order.append("fine_grained_check") or PLAIN_TABLE
        sql_mock.query_dicts.side_effect = lambda *a, **k: order.append("sp_fetch") or []
        resp = client.get(FAILED_ROWS_URL)
        assert resp.status_code == 200
        assert order == ["obo_select_check", "fine_grained_check", "sp_fetch"]

    def test_malformed_fqn_returns_400_without_any_backend_call(
        self, client, obo_sql_mock, obo_ws_mock, sql_mock
    ):
        resp = client.get("/api/v1/dq-results/failed-rows/not-a-three-part-name")
        assert resp.status_code == 400
        obo_sql_mock.query.assert_not_called()
        obo_ws_mock.tables.get.assert_not_called()
        sql_mock.query_dicts.assert_not_called()

    def test_sp_query_failure_maps_to_500(self, client, sql_mock):
        sql_mock.query_dicts.side_effect = RuntimeError("warehouse down")
        assert client.get(FAILED_ROWS_URL).status_code == 500


class TestFailedRowsEntitlementPiggyback:
    """P4.1: a successful failed-rows call caches the caller's entitlement.

    The caller just proved live SELECT on the source table — the exact
    verification the Genie ``v_dq_failing_rows`` gate relies on — so the
    entitlement row is upserted here without a separate verify round-trip.
    """

    def test_success_path_records_the_entitlement(self, client, entitlement_mock):
        assert client.get(FAILED_ROWS_URL).status_code == 200
        entitlement_mock.record_entitlement.assert_called_once_with(USER_EMAIL, FQN)

    def test_obo_denial_records_nothing(self, client, obo_sql_mock, entitlement_mock):
        obo_sql_mock.query.side_effect = RuntimeError("PERMISSION_DENIED")
        client.get(FAILED_ROWS_URL)
        entitlement_mock.record_entitlement.assert_not_called()

    def test_fine_grained_suppression_records_nothing(self, client, obo_ws_mock, entitlement_mock):
        # A fine-grained-controlled table must not open in v_dq_failing_rows
        # when this endpoint itself suppresses the sample.
        obo_ws_mock.tables.get.return_value = TableInfo(
            row_filter=TableRowFilter(function_name="main.sales.f", input_column_names=["region"])
        )
        client.get(FAILED_ROWS_URL)
        entitlement_mock.record_entitlement.assert_not_called()

    def test_malformed_fqn_records_nothing(self, client, entitlement_mock):
        client.get("/api/v1/dq-results/failed-rows/not-a-three-part-name")
        entitlement_mock.record_entitlement.assert_not_called()

    def test_upsert_sits_after_both_gates(
        self, client, obo_sql_mock, obo_ws_mock, sql_mock, entitlement_mock
    ):
        # Placement pin: the entitlement is recorded only once BOTH Task 7
        # gates have passed — never between them — and before the SP fetch.
        order: list[str] = []
        obo_sql_mock.query.side_effect = lambda *a, **k: order.append("obo_select_check") or []
        obo_ws_mock.tables.get.side_effect = lambda *a, **k: order.append("fine_grained_check") or PLAIN_TABLE
        entitlement_mock.record_entitlement.side_effect = (
            lambda *a, **k: order.append("entitlement_upsert") or True
        )
        sql_mock.query_dicts.side_effect = lambda *a, **k: order.append("sp_fetch") or []
        assert client.get(FAILED_ROWS_URL).status_code == 200
        assert order == ["obo_select_check", "fine_grained_check", "entitlement_upsert", "sp_fetch"]


class TestFailedRowsShapeAndFilters:
    def test_rows_carry_struct_enriched_failures_and_run_ts(self, client, sql_mock, monitored_tables_mock):
        # Severity/dimension/rule_id come from the failure struct's OWN
        # frozen user_metadata (stamped by the DQX engine at run time) —
        # never from a live applied-rule lookup.
        sql_mock.query_dicts.return_value = [
            quarantine_row(
                "q1",
                row_data={"id": 1, "amount": None},
                errors=[
                    {
                        "name": "c1",
                        "message": "id is null",
                        "columns": ["id"],
                        "user_metadata": {
                            "severity": "High",
                            "dimension": "Completeness",
                            "registry_rule_id": "rule-1",
                        },
                    }
                ],
            )
        ]
        body = client.get(FAILED_ROWS_URL).json()
        assert body["total"] == 1
        row = body["rows"][0]
        assert row["record_key"] == "q1"
        assert row["row_values"] == {"id": "1", "amount": None}
        assert row["failed_columns"] == ["id"]
        assert row["run_ts"] == "2026-07-10 00:00:00"
        assert row["failures"] == [
            {
                "rule_id": "rule-1",
                "rule_name": "c1",
                "quality_dimension": "Completeness",
                "severity": "High",
                "message": "id is null",
                "columns": ["id"],
            }
        ]
        monitored_tables_mock.get_by_table_fqn.assert_not_called()

    def test_unattributed_failures_have_null_metadata(self, client, sql_mock):
        sql_mock.query_dicts.return_value = [
            quarantine_row("q1", errors=[{"name": "adhoc", "message": "boom", "columns": []}])
        ]
        failure = client.get(FAILED_ROWS_URL).json()["rows"][0]["failures"][0]
        assert failure["rule_id"] is None
        assert failure["quality_dimension"] is None
        assert failure["severity"] is None
        assert failure["rule_name"] == "adhoc"

    def _two_rows(self, sql_mock):
        sql_mock.query_dicts.return_value = [
            quarantine_row(
                "q1",
                errors=[
                    {
                        "name": "c1",
                        "message": "m1",
                        "columns": ["id"],
                        "user_metadata": {"severity": "High", "dimension": "Completeness"},
                    }
                ],
            ),
            quarantine_row(
                "q2",
                errors=[
                    {
                        "name": "c2",
                        "message": "m2",
                        "columns": ["amount"],
                        "user_metadata": {"severity": "Low", "dimension": "Validity"},
                    }
                ],
            ),
        ]

    def test_rule_filter(self, client, sql_mock):
        self._two_rows(sql_mock)
        body = client.get(FAILED_ROWS_URL, params={"rule": "c1"}).json()
        assert [r["record_key"] for r in body["rows"]] == ["q1"]
        assert body["total"] == 1

    def test_severity_filter_via_frozen_struct_metadata(self, client, sql_mock):
        self._two_rows(sql_mock)
        body = client.get(FAILED_ROWS_URL, params={"severity": "Low"}).json()
        assert [r["record_key"] for r in body["rows"]] == ["q2"]

    def test_dimension_filter_via_frozen_struct_metadata(self, client, sql_mock):
        self._two_rows(sql_mock)
        body = client.get(FAILED_ROWS_URL, params={"dimension": "Completeness"}).json()
        assert [r["record_key"] for r in body["rows"]] == ["q1"]

    def test_column_filter_over_failed_columns(self, client, sql_mock):
        self._two_rows(sql_mock)
        body = client.get(FAILED_ROWS_URL, params={"column": "amount"}).json()
        assert [r["record_key"] for r in body["rows"]] == ["q2"]

    def test_repeatable_facets_are_ored(self, client, sql_mock):
        self._two_rows(sql_mock)
        body = client.get(FAILED_ROWS_URL, params={"rule": ["c1", "c2"]}).json()
        assert body["total"] == 2

    def test_unfiltered_scan_uses_limit(self, client, sql_mock):
        client.get(FAILED_ROWS_URL, params={"limit": 7})
        assert "LIMIT 7" in sql_mock.query_dicts.call_args[0][0]

    def test_filtered_scan_widens_the_window(self, client, sql_mock):
        # With active filters the SP scan window is wider than the page so a
        # selective filter can still fill it.
        client.get(FAILED_ROWS_URL, params={"limit": 7, "rule": "c1"})
        assert "LIMIT 1000" in sql_mock.query_dicts.call_args[0][0]

    def test_limit_caps_rows_but_total_counts_matches(self, client, sql_mock):
        self._two_rows(sql_mock)
        body = client.get(FAILED_ROWS_URL, params={"limit": 1, "rule": ["c1", "c2"]}).json()
        assert len(body["rows"]) == 1
        assert body["total"] == 2

    @pytest.mark.parametrize("limit", [0, 100001])
    def test_out_of_range_limit_is_rejected(self, client, sql_mock, limit):
        resp = client.get(FAILED_ROWS_URL, params={"limit": limit})
        assert resp.status_code == 422
        sql_mock.query_dicts.assert_not_called()


# ---------------------------------------------------------------------------
# include_drafts — published-only defaults everywhere (Task P3.3)
# ---------------------------------------------------------------------------


class TestIncludeDrafts:
    """Every dq-results read defaults to published runs only; the
    ``include_drafts=true`` escape hatch drops the run_mode filter. The
    view's ``run_mode`` column already resolves the stamped run-level tag
    (untagged legacy runs classify as published), so the routes filter one
    column — which is exactly what these tests pin."""

    CHECK_ROW_ENDPOINTS = [
        f"/api/v1/dq-results/table/{FQN}",
        "/api/v1/dq-results/global",
        "/api/v1/dq-results/product/p1",
        "/api/v1/dq-results/rule/rule-1",
    ]

    @pytest.fixture(autouse=True)
    def _scoping(self, data_products_mock, apply_rules_mock, monitored_tables_mock):
        data_products_mock.get.return_value = product_detail("p1", [("b1", FQN)])
        apply_rules_mock.list_bindings_for_rule.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="rule-1")
        ]
        monitored_tables_mock.get.return_value = binding_detail("b1", FQN)

    def _shaping_stmts(self, sql_mock) -> list[str]:
        return [
            call[0][0]
            for call in sql_mock.query_dicts.call_args_list
            if SHAPING_VIEW_NAME in call[0][0] and "dq_quarantine_records" not in call[0][0]
        ]

    @pytest.mark.parametrize("url", CHECK_ROW_ENDPOINTS)
    def test_check_row_reads_default_to_published_runs(self, client, sql_mock, url):
        sql_dispatch(sql_mock)
        assert client.get(url).status_code == 200
        stmts = self._shaping_stmts(sql_mock)
        assert stmts, "expected a shaping-view read"
        assert all("run_mode = 'published'" in stmt for stmt in stmts)

    @pytest.mark.parametrize("url", CHECK_ROW_ENDPOINTS)
    def test_include_drafts_drops_the_run_mode_filter(self, client, sql_mock, url):
        sql_dispatch(sql_mock)
        assert client.get(url, params={"include_drafts": "true"}).status_code == 200
        for stmt in self._shaping_stmts(sql_mock):
            assert "run_mode" not in stmt

    def test_legacy_untagged_rows_flow_through_unchanged(self, client, sql_mock):
        # Untagged-run resolution lives in the VIEW (COALESCE over the
        # stamped tag, defaulting to 'published') — a legacy run the view
        # resolved to 'published' arrives as an ordinary row and is served
        # normally.
        sql_dispatch(sql_mock, check_rows=[check_row("legacy_check", errors=2, total=10)])
        body = client.get(f"/api/v1/dq-results/table/{FQN}").json()
        assert [g["label"] for g in body["by_rule"]] == ["legacy_check"]
        assert "run_mode = 'published'" in self._shaping_stmts(sql_mock)[0]

    @pytest.mark.parametrize(
        "url",
        [f"/api/v1/dq-results/runs/{FQN}", "/api/v1/dq-results/product/p1/runs"],
    )
    def test_runs_lists_default_to_published_runs(self, client, sql_mock, url):
        sql_dispatch(sql_mock, runs_rows=[runs_row()])
        assert client.get(url).status_code == 200
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert METRIC_VIEW_NAME in stmt
        assert "run_mode = 'published'" in stmt

    @pytest.mark.parametrize(
        "url",
        [f"/api/v1/dq-results/runs/{FQN}", "/api/v1/dq-results/product/p1/runs"],
    )
    def test_runs_lists_include_drafts_drops_the_filter_and_badges_rows(self, client, sql_mock, url):
        sql_dispatch(
            sql_mock,
            runs_rows=[
                runs_row("r2", "2026-07-02 00:00:00", run_mode="draft"),
                runs_row("r1", run_mode="published"),
            ],
        )
        body = client.get(url, params={"include_drafts": "true"}).json()
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert "run_mode = 'published'" not in stmt
        # Each row still carries its run_mode so the picker can badge drafts.
        assert [r["run_mode"] for r in body["rows"]] == ["draft", "published"]

    def test_failed_rows_default_scopes_to_published_run_ids(self, client, sql_mock, app_config):
        # dq_quarantine_records has no run_mode column — the published-only
        # default filters by run_id against the shaping view instead.
        sql_dispatch(sql_mock, quarantine_rows=[quarantine_row("q1")])
        body = client.get(FAILED_ROWS_URL).json()
        assert body["total"] == 1
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert "dq_quarantine_records" in stmt
        assert (
            f"run_id IN (SELECT run_id FROM "
            f"`{app_config.catalog}`.`{app_config.schema_name}`.{SHAPING_VIEW_NAME} "
            f"WHERE input_location = '{FQN}' AND run_mode = 'published')" in stmt
        )

    def test_failed_rows_include_drafts_reads_all_runs(self, client, sql_mock):
        sql_dispatch(sql_mock, quarantine_rows=[quarantine_row("q1")])
        body = client.get(FAILED_ROWS_URL, params={"include_drafts": "true"}).json()
        assert body["total"] == 1
        stmt = sql_mock.query_dicts.call_args[0][0]
        assert "run_mode" not in stmt
        assert SHAPING_VIEW_NAME not in stmt

    def test_failed_records_lookup_is_not_run_mode_filtered(self, client, sql_mock):
        # _fetch_failed_records_by_run is a lookup map consulted only for
        # the (table, run) keys already present in the filtered check rows —
        # filtering it again would be redundant, so the dq_metrics read
        # stays mode-agnostic by design.
        sql_dispatch(sql_mock, check_rows=[check_row("c1", errors=1, total=10)])
        client.get(f"/api/v1/dq-results/table/{FQN}")
        metrics_stmts = [
            call[0][0]
            for call in sql_mock.query_dicts.call_args_list
            if "dq_metrics" in call[0][0] and SHAPING_VIEW_NAME not in call[0][0]
        ]
        assert metrics_stmts and all("run_mode" not in stmt for stmt in metrics_stmts)


# ---------------------------------------------------------------------------
# run_id charset validation (SQL-literal safety)
# ---------------------------------------------------------------------------


class TestRunIdValidation:
    """run_id is user input interpolated into a SQL string literal, and
    ``escape_sql_string`` deliberately does NOT escape backslashes — so a
    charset allowlist must reject anything that could break out of the
    literal, with 400 BEFORE any SQL is issued (repo rule: no unvalidated
    user input into SQL strings)."""

    ADVERSARIAL_RUN_IDS = [
        "abc\\",  # trailing backslash consumes the closing quote
        "ab'c",  # embedded single quote
        "x' OR '1'='1",  # classic literal break-out
        "run\nid",  # control character
        "run id",  # whitespace
    ]

    ENDPOINTS = [
        f"/api/v1/dq-results/table/{FQN}",
        "/api/v1/dq-results/product/p1",
        "/api/v1/dq-results/rule/rule-1",
        "/api/v1/dq-results/global",
    ]

    @pytest.mark.parametrize("url", ENDPOINTS)
    @pytest.mark.parametrize("run_id", ADVERSARIAL_RUN_IDS)
    def test_unsafe_run_id_is_rejected_with_400_before_any_sql(
        self, client, sql_mock, data_products_mock, apply_rules_mock, url, run_id
    ):
        data_products_mock.get.return_value = product_detail("p1", [("b1", FQN)])
        apply_rules_mock.list_bindings_for_rule.return_value = [
            AppliedRule(id="ar1", binding_id="b1", rule_id="rule-1")
        ]
        resp = client.get(url, params={"run_id": run_id})
        assert resp.status_code == 400
        sql_mock.query_dicts.assert_not_called()

    @pytest.mark.parametrize(
        "run_id",
        [
            "0e4a3c9a-9d5e-4a2e-8f0a-1b2c3d4e5f6a",  # observer uuid4 format
            "run_2026-07-10T12:00:00.123",  # prefixed/timestamped override
        ],
    )
    def test_safe_run_ids_are_accepted(self, client, sql_mock, run_id):
        sql_dispatch(sql_mock)
        resp = client.get(f"/api/v1/dq-results/table/{FQN}", params={"run_id": run_id})
        assert resp.status_code == 200
        stmt = sql_mock.query_dicts.call_args_list[0][0][0]
        assert f"run_id = '{run_id}'" in stmt


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------


class TestRbac:
    @pytest.mark.parametrize(
        "operation_id",
        [
            "listResultSeverities",
            "listResultDimensions",
            "getGlobalResults",
            "getRuleResults",
            "getProductResults",
            "getProductResultsRuns",
            "getDqResultsFailedRows",
            "getDqResultsRuns",
            "getTableResults",
            "refreshDqScores",
        ],
    )
    def test_route_is_gated_for_all_roles(self, operation_id):
        """Results reads are viewer-visible, like the dq-score routes."""
        from databricks_labs_dqx_app.backend.routes.v1 import dq_results

        for route in dq_results.router.routes:
            if getattr(route, "operation_id", None) != operation_id:
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
        raise AssertionError(f"No require_role dependency found for {operation_id}")


# ---------------------------------------------------------------------------
# POST /refresh-scores (P3.4 score-cache refresh trigger)
# ---------------------------------------------------------------------------


class TestRefreshScores:
    def test_refreshes_tables_products_and_global(self, client, score_cache_mock):
        score_cache_mock.refresh_all_for_tables.return_value = (2, 1)
        resp = client.post("/api/v1/dq-results/refresh-scores", json={"table_fqns": [FQN, "dev.s.t2"]})
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"refreshed_tables": 2, "refreshed_products": 1, "global_refreshed": True}
        score_cache_mock.refresh_all_for_tables.assert_called_once_with([FQN, "dev.s.t2"])

    def test_invalid_fqn_is_rejected_before_the_service_runs(self, client, score_cache_mock):
        resp = client.post("/api/v1/dq-results/refresh-scores", json={"table_fqns": ["bad`fqn"]})
        assert resp.status_code == 400
        score_cache_mock.refresh_all_for_tables.assert_not_called()

    def test_empty_list_is_a_422(self, client, score_cache_mock):
        resp = client.post("/api/v1/dq-results/refresh-scores", json={"table_fqns": []})
        assert resp.status_code == 422
        score_cache_mock.refresh_all_for_tables.assert_not_called()

    def test_list_longer_than_the_cap_is_a_422(self, client, score_cache_mock):
        fqns = [f"main.s.t{i}" for i in range(101)]
        resp = client.post("/api/v1/dq-results/refresh-scores", json={"table_fqns": fqns})
        assert resp.status_code == 422
        score_cache_mock.refresh_all_for_tables.assert_not_called()

    def test_exactly_at_the_cap_is_accepted(self, client, score_cache_mock):
        fqns = [f"main.s.t{i}" for i in range(100)]
        score_cache_mock.refresh_all_for_tables.return_value = (100, 0)
        resp = client.post("/api/v1/dq-results/refresh-scores", json={"table_fqns": fqns})
        assert resp.status_code == 200

    def test_service_failure_maps_to_500(self, client, score_cache_mock):
        score_cache_mock.refresh_all_for_tables.side_effect = RuntimeError("warehouse down")
        resp = client.post("/api/v1/dq-results/refresh-scores", json={"table_fqns": [FQN]})
        assert resp.status_code == 500
