"""Tests for :class:`DataProductService` — the Data Products Task 4 service.

Drives the service against a spec-bound ``SqlExecutor`` mock (per-test
``query`` dispatchers, assertions on ``execute`` call SQL) plus mocked
``MonitoredTableService`` / ``RunSetService`` / ``BindingRunService`` — no
Spark or workspace needed. Covers CRUD + name uniqueness, the
published->draft/modified display-status transitions, member upsert-by-
binding_id, publish version bump, and the run fan-out resolution matrix
(draft always runs, approved skips never-approved unless pinned, 409 on
zero-runnable, per-member submission failures collected into ``skipped``).
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import DataProduct, MonitoredTable
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingRunResult,
    BindingRunService,
    NeverApprovedError,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    BindingNotApprovedError,
    DataProductService,
    DuplicateDataProductNameError,
    InvalidStatusTransitionError,
    NoRunnableMembersError,
    display_status,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableDetail,
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.run_sets import RunSetService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

_PRODUCTS = "dqx_test.dqx_app_test.dq_data_products"
_MEMBERS = "dqx_test.dqx_app_test.dq_data_product_members"


def _mock_sql() -> SqlExecutor:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.dialect = "delta"
    mock.fqn.side_effect = lambda t: {"dq_data_products": _PRODUCTS, "dq_data_product_members": _MEMBERS}.get(
        t, f"dqx_test.dqx_app_test.{t}"
    )
    mock.q.side_effect = lambda i: f"`{i}`"
    mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    mock.query.return_value = []
    return mock


def _product_row(
    product_id: str = "p1",
    name: str = "Orders",
    description: str | None = None,
    steward: str | None = "alice@x",
    schedule_cron: str | None = None,
    schedule_tz: str | None = None,
    status: str = "draft",
    version: str = "0",
    schedule_kind: str | None = "profiling_and_dq",
    score: str | None = None,
    failed_tests: str | None = None,
    total_tests: str | None = None,
    score_computed_at: str | None = None,
) -> list[str | None]:
    # ``schedule_kind`` (B2-52) is selected last among the base columns
    # (index 12); the trailing 4 cells are the dq_score_cache LEFT-JOIN
    # columns the list/get read paths select (P3.4) — all None when the
    # product has never been scored. The non-score read paths select only
    # the first 13 columns; the extra cells are ignored by _row_to_product.
    return [
        product_id,
        name,
        description,
        steward,
        schedule_cron,
        schedule_tz,
        status,
        version,
        "alice@x",
        "2026-07-07T00:00:00",
        "alice@x",
        "2026-07-07T00:00:00",
        schedule_kind,
        score,
        failed_tests,
        total_tests,
        score_computed_at,
    ]


def _member_row(member_id: str, binding_id: str, pinned_version: str | None = None) -> list[str | None]:
    return [member_id, binding_id, pinned_version]


def _list_member_row(
    member_id: str, product_id: str, binding_id: str, pinned_version: str | None = None
) -> list[str | None]:
    # The batched list-path members query selects product_id too, so rows can
    # be grouped app-side (one IN (...) query for ALL products' members).
    return [member_id, product_id, binding_id, pinned_version]


def _table_summary(
    binding_id: str = "b1",
    table_fqn: str = "cat.schema.tbl",
    status: str = "approved",
    version: int = 2,
    applied_rule_count: int = 3,
    check_count: int = 5,
    applied_check_count: int | None = None,
    score: float | None = None,
    failed_tests: int | None = None,
    total_tests: int | None = None,
    score_computed_at: str | None = None,
    last_run_at: datetime | None = None,
) -> MonitoredTableSummary:
    return MonitoredTableSummary(
        table=MonitoredTable(
            binding_id=binding_id, table_fqn=table_fqn, status=status, version=version, last_run_at=last_run_at
        ),
        applied_rule_count=applied_rule_count,
        applied_check_count=applied_check_count,
        check_count=check_count,
        score=score,
        failed_tests=failed_tests,
        total_tests=total_tests,
        score_computed_at=score_computed_at,
    )


def _monitored_table_detail(
    binding_id: str = "b1", version: int = 2, status: str = "approved"
) -> MonitoredTableDetail:
    return MonitoredTableDetail(
        table=MonitoredTable(binding_id=binding_id, table_fqn="cat.schema.tbl", status=status, version=version)
    )


@pytest.fixture
def sql():
    return _mock_sql()


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def run_set_service():
    mock = create_autospec(RunSetService, instance=True)
    mock.create.return_value = "rs-new"
    return mock


@pytest.fixture
def binding_run_service():
    return create_autospec(BindingRunService, instance=True)


@pytest.fixture
def version_service():
    mock = create_autospec(MonitoredTableVersionService, instance=True)
    # Default: no frozen snapshot resolves, so members fall back to live counts.
    mock.snapshot_counts_many.return_value = {}
    return mock


@pytest.fixture
def materializer():
    mock = create_autospec(Materializer, instance=True)
    # Default: no live render counts resolve, so unpinned members fall back to
    # the binding's live summary ``check_count`` (existing count assertions).
    mock.render_binding_checks_counts_many.return_value = {}
    return mock


@pytest.fixture
def app_settings():
    mock = create_autospec(AppSettingsService, instance=True)
    # Default matches production default: new members follow latest unless
    # the caller explicitly pins.
    mock.get_default_auto_upgrade.return_value = True
    mock.resolve_pinned_version_for_new_attachment.side_effect = (
        lambda explicit, current: explicit if explicit is not None else (None if mock.get_default_auto_upgrade() else current)
    )
    return mock


@pytest.fixture
def service(sql, monitored_tables, run_set_service, binding_run_service, version_service, app_settings, materializer):
    return DataProductService(
        sql=sql,
        monitored_tables=monitored_tables,
        run_set_service=run_set_service,
        binding_run_service=binding_run_service,
        version_service=version_service,
        app_settings=app_settings,
        materializer=materializer,
    )


class TestDisplayStatus:
    def test_approved(self):
        p = DataProduct(product_id="p1", name="x", status="approved", version=1)
        assert display_status(p) == "approved"

    def test_pending_approval(self):
        p = DataProduct(product_id="p1", name="x", status="pending_approval", version=0)
        assert display_status(p) == "pending_approval"

    def test_rejected(self):
        p = DataProduct(product_id="p1", name="x", status="rejected", version=0)
        assert display_status(p) == "rejected"

    def test_modified_since_approval(self):
        p = DataProduct(product_id="p1", name="x", status="draft", version=1)
        assert display_status(p) == "modified"

    def test_never_approved_is_draft(self):
        p = DataProduct(product_id="p1", name="x", status="draft", version=0)
        assert display_status(p) == "draft"


class TestListAndGet:
    def test_list_products_resolves_members_and_counters(self, service, sql, monitored_tables, run_set_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="draft", version="0")],
            [_list_member_row("m1", "p1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2)
        ]

        result = service.list_products()
        assert len(result) == 1
        detail = result[0]
        assert detail.product.product_id == "p1"
        assert detail.member_count == 1
        assert detail.runnable_count == 1
        assert detail.members[0].binding_status == "approved"
        assert detail.members[0].runnable is True

    def test_list_products_issues_bounded_queries_independent_of_product_count(
        self, service, sql, monitored_tables, run_set_service, version_service
    ):
        """The list path must issue a BOUNDED number of OLTP queries no matter
        how many products exist (regression: it used to fan out per product —
        members + run sets + run-set members + a Delta scan each, plus one
        snapshot query per pinned member)."""
        sql.query.side_effect = [
            [
                _product_row(product_id="p1"),
                _product_row(product_id="p2", name="Payments"),
                _product_row(product_id="p3", name="Refunds"),
            ],
            [
                _list_member_row("m1", "p1", "b1"),
                _list_member_row("m2", "p2", "b2", pinned_version="1"),
                _list_member_row("m3", "p3", "b3"),
            ],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", table_fqn="cat.s.t1", applied_rule_count=3, check_count=5),
            _table_summary(
                binding_id="b2",
                table_fqn="cat.s.t2",
                applied_rule_count=0,
                check_count=0,
                last_run_at=datetime(2026, 7, 9, 12, 0, 0),
            ),
            _table_summary(binding_id="b3", table_fqn="cat.s.t3", applied_rule_count=7, check_count=9),
        ]
        version_service.snapshot_counts_many.return_value = {("b2", 1): (1, 2)}

        result = service.list_products()

        # Exactly 2 direct OLTP queries: products+scores, all-members-in-one.
        assert sql.query.call_count == 2
        members_query = sql.query.call_args_list[1][0][0]
        assert f"FROM {_MEMBERS}" in members_query
        assert "IN ('p1', 'p2', 'p3')" in members_query
        # Per-product last-run is now derived from the members' denormalized
        # last_run_at (B2-15) — no per-product run-set MAX query.
        version_service.snapshot_counts_many.assert_called_once_with([("b2", 1)])
        # The response shape is unchanged: same per-product details as before.
        by_id = {d.product.product_id: d for d in result}
        assert by_id["p1"].members[0].rules_count == 3
        assert by_id["p1"].members[0].checks_count == 5
        assert by_id["p1"].last_run_at is None
        assert by_id["p2"].members[0].pinned_version == 1
        assert by_id["p2"].members[0].rules_count == 1  # frozen snapshot, not live 0
        assert by_id["p2"].members[0].checks_count == 2
        # p2's member (b2) carries the newest run instant -> product last_run.
        assert by_id["p2"].last_run_at == datetime(2026, 7, 9, 12, 0, 0)
        assert by_id["p3"].members[0].rules_count == 7
        assert all(d.member_count == 1 for d in result)

    def test_product_last_run_is_max_over_members_either_surface(
        self, service, sql, monitored_tables, run_set_service
    ):
        """B2-15: a table space's last-run is MAX over its members' last_run_at.

        Each member's ``last_run_at`` is denormalized on completion regardless
        of trigger surface (MT-direct OR this space's fan-out), so a member run
        via EITHER surface bumps the product — the older per-product run-set MAX
        (grouped by product_id) missed MT-surface runs (product_id=None).
        """
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [
                _list_member_row("m1", "p1", "b1"),
                _list_member_row("m2", "p1", "b2"),
            ],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            # b1 last ran via a table-space fan-out; b2 via the MT surface later.
            _table_summary(binding_id="b1", table_fqn="cat.s.t1", last_run_at=datetime(2026, 7, 8, 6, 0, 0)),
            _table_summary(binding_id="b2", table_fqn="cat.s.t2", last_run_at=datetime(2026, 7, 11, 18, 0, 0)),
        ]

        result = service.list_products()

        assert result[0].last_run_at == datetime(2026, 7, 11, 18, 0, 0)

    def test_list_products_empty(self, service, sql):
        sql.query.return_value = []
        assert service.list_products() == []

    def test_list_products_left_joins_score_cache_in_same_round_trip(self, service, sql, monitored_tables):
        """P3.4: the cached score columns ride along the products query —
        no extra round trip and NEVER a warehouse recompute on page load."""
        sql.query.side_effect = [
            [_product_row(product_id="p1", score="0.9876", failed_tests="12", total_tests="1000",
                          score_computed_at="2026-07-10T00:00:00")],
            [],  # members
        ]
        monitored_tables.list_monitored_tables.return_value = []
        result = service.list_products()
        products_query = sql.query.call_args_list[0][0][0]
        assert "LEFT JOIN dqx_test.dqx_app_test.dq_score_cache" in products_query
        assert "sc.scope_type = 'product'" in products_query
        assert "sc.scope_key = p.product_id" in products_query
        detail = result[0]
        assert detail.score == 0.9876
        assert detail.failed_tests == 12
        assert detail.total_tests == 1000
        assert detail.score_computed_at == "2026-07-10T00:00:00"

    def test_list_products_score_fields_none_when_never_scored(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # members
        ]
        monitored_tables.list_monitored_tables.return_value = []
        detail = service.list_products()[0]
        assert detail.score is None
        assert detail.failed_tests is None
        assert detail.total_tests is None
        assert detail.score_computed_at is None

    def test_members_carry_the_binding_cached_score_from_the_same_round_trip(
        self, service, sql, monitored_tables
    ):
        """P5.3: the Tables tab's per-member DQ score column rides the
        monitored-table summaries already fetched for the counters — the
        summary list LEFT JOINs dq_score_cache, so no extra query."""
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(
                binding_id="b1",
                score=0.9876,
                failed_tests=12,
                total_tests=1000,
                score_computed_at="2026-07-10T00:00:00",
            )
        ]
        detail = service.get("p1")
        assert detail is not None
        member = detail.members[0]
        assert member.score == 0.9876
        assert member.failed_tests == 12
        assert member.total_tests == 1000
        assert member.score_computed_at == "2026-07-10T00:00:00"

    def test_member_score_fields_none_when_the_binding_was_never_scored(
        self, service, sql, monitored_tables
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [_table_summary(binding_id="b1")]
        detail = service.get("p1")
        assert detail is not None
        member = detail.members[0]
        assert member.score is None
        assert member.failed_tests is None
        assert member.total_tests is None
        assert member.score_computed_at is None

    def test_get_carries_cached_score(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1", score="0.5", failed_tests="1", total_tests="2",
                          score_computed_at="2026-07-10T00:00:00")],
            [],  # members
        ]
        monitored_tables.list_monitored_tables.return_value = []
        detail = service.get("p1")
        assert detail is not None
        assert detail.score == 0.5
        assert detail.failed_tests == 1
        assert detail.total_tests == 2

    def test_member_referencing_missing_binding_is_skipped(self, service, sql, monitored_tables, run_set_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_list_member_row("m1", "p1", "b-deleted")],
        ]
        monitored_tables.list_monitored_tables.return_value = []
        result = service.list_products()
        assert result[0].member_count == 0

    def test_get_missing_returns_none(self, service, sql):
        sql.query.return_value = []
        assert service.get("missing") is None

    def test_get_not_runnable_when_status_not_approved(self, service, sql, monitored_tables, run_set_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="pending_approval", version=1)
        ]
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].runnable is False

    def test_unpinned_member_reports_live_counts(self, service, sql, monitored_tables, version_service):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version=None)],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=3, check_count=5)
        ]
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].rules_count == 3
        assert detail.members[0].checks_count == 5
        # No pin -> never consults the frozen snapshot.
        version_service.snapshot_counts_many.assert_not_called()

    def test_unpinned_member_checks_count_from_live_render_not_stale_zero(
        self, service, sql, monitored_tables, materializer
    ):
        """P-item 44: a freshly-saved DRAFT space's ``# Checks`` must reflect the
        checks its applied rules expand to, NOT the materialized ``dq_quality_rules``
        row count (0 until approval/run). ``# Rules`` stays the applied-rule count."""
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="draft", version="0")],
            [_member_row("m1", "b1", pinned_version=None)],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            # Live summary.check_count is 0 (nothing materialized yet), but the
            # 2 applied rules expand to 5 checks (e.g. a for-each-column rule).
            _table_summary(binding_id="b1", status="draft", version=0, applied_rule_count=2, check_count=0)
        ]
        materializer.render_binding_checks_counts_many.return_value = {"b1": 5}
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].rules_count == 2
        assert detail.members[0].checks_count == 5  # live render, not the stale 0
        materializer.render_binding_checks_counts_many.assert_called_once_with([("b1", "cat.schema.tbl")])

    def test_unpinned_member_checks_count_falls_back_to_summary_on_render_error(
        self, service, sql, monitored_tables, materializer
    ):
        from databricks_labs_dqx_app.backend.services.materializer import MaterializationError

        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version=None)],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=3, check_count=5)
        ]
        materializer.render_binding_checks_counts_many.side_effect = MaterializationError("boom")
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].checks_count == 5  # graceful fallback to summary count

    def test_pinned_member_skips_live_render(self, service, sql, monitored_tables, version_service, materializer):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version="1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=0, check_count=0)
        ]
        version_service.snapshot_counts_many.return_value = {("b1", 1): (1, 2)}
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].checks_count == 2  # frozen snapshot
        # A resolved pin reports its frozen snapshot -> no live render needed.
        materializer.render_binding_checks_counts_many.assert_not_called()

    def test_pinned_member_reports_pinned_snapshot_counts(self, service, sql, monitored_tables, version_service):
        # Live binding has moved on to v2 and its live counts differ from the
        # pinned v1 snapshot (regression: pinned member used to show the live
        # count — 0 here — instead of the frozen snapshot's real 2 checks).
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version="1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=0, check_count=0)
        ]
        version_service.snapshot_counts_many.return_value = {("b1", 1): (1, 2)}
        detail = service.get("p1")
        assert detail is not None
        version_service.snapshot_counts_many.assert_called_once_with([("b1", 1)])
        assert detail.members[0].rules_count == 1
        assert detail.members[0].checks_count == 2
        # The pin itself is still surfaced unchanged.
        assert detail.members[0].pinned_version == 1

    def test_pinned_member_falls_back_to_live_counts_when_snapshot_missing(
        self, service, sql, monitored_tables, version_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version="1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=2, applied_rule_count=3, check_count=5)
        ]
        version_service.snapshot_counts_many.return_value = {}  # snapshot not found
        detail = service.get("p1")
        assert detail is not None
        assert detail.members[0].rules_count == 3
        assert detail.members[0].checks_count == 5

    def test_list_renders_only_draft_members_uses_snapshot_for_approved(
        self, service, sql, monitored_tables, materializer
    ):
        """perf/C2: approved members read their frozen ``applied_check_count`` from
        the summary (no render); only never-approved drafts (version==0) trigger a
        live render — mirroring the Tables overview's snapshot/live split."""
        sql.query.side_effect = [
            [
                _product_row(product_id="p1"),
            ],
            [
                _list_member_row("ma", "p1", "ba"),  # approved
                _list_member_row("mb", "p1", "bb"),  # draft
            ],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            # Approved member: version > 0, applied_check_count set.
            _table_summary(
                binding_id="ba",
                table_fqn="cat.s.ta",
                status="approved",
                version=2,
                applied_rule_count=3,
                check_count=5,
                applied_check_count=9,
            ),
            # Draft member: version == 0, applied_check_count absent.
            _table_summary(
                binding_id="bb",
                table_fqn="cat.s.tb",
                status="draft",
                version=0,
                applied_rule_count=2,
                check_count=0,
                applied_check_count=None,
            ),
        ]
        # Render returns a count only for the draft binding.
        recorded_args: list[list[tuple[str, str]]] = []

        def _capture(live_bindings: list[tuple[str, str]]) -> dict[str, int]:
            recorded_args.append(list(live_bindings))
            return {"bb": 4}

        materializer.render_binding_checks_counts_many.side_effect = _capture

        result = service.list_products()

        # Render was called with ONLY the draft binding, not the approved one.
        assert len(recorded_args) == 1
        assert recorded_args[0] == [("bb", "cat.s.tb")]

        by_binding = {m.binding_id: m for m in result[0].members}
        # Approved member: checks_count from applied_check_count (9), not rendered.
        assert by_binding["ba"].checks_count == 9
        # Draft member: checks_count from render (4).
        assert by_binding["bb"].checks_count == 4


class TestCreate:
    def test_create_success(self, service, sql):
        sql.query.return_value = []  # name available
        product = service.create("Orders", "desc", None, "alice@x")
        assert product.name == "Orders"
        assert product.status == "draft"
        assert product.version == 0
        assert product.steward == "alice@x"  # defaults to creator
        insert_sql = sql.execute.call_args[0][0]
        assert f"INSERT INTO {_PRODUCTS}" in insert_sql
        assert "'Orders'" in insert_sql
        # schedule_kind (B2-52) defaults to dq_only and is persisted on create.
        assert product.schedule_kind == "dq_only"
        assert "schedule_kind" in insert_sql
        assert "'dq_only'" in insert_sql

    def test_create_duplicate_name_raises(self, service, sql):
        sql.query.return_value = [["p-existing"]]
        with pytest.raises(DuplicateDataProductNameError):
            service.create("Orders", None, None, "alice@x")

    def test_create_empty_name_raises(self, service):
        with pytest.raises(ValueError):
            service.create("   ", None, None, "alice@x")

    def test_create_seeds_default_grants(
        self, sql, monitored_tables, run_set_service, binding_run_service,
        version_service, app_settings, materializer
    ):
        """DataProductService.create seeds default grants via the injected PermissionsService."""
        from unittest.mock import create_autospec
        from databricks_labs_dqx_app.backend.common.permissions import ObjectType
        from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService

        perms = create_autospec(PermissionsService, instance=True)
        svc = DataProductService(
            sql=sql,
            monitored_tables=monitored_tables,
            run_set_service=run_set_service,
            binding_run_service=binding_run_service,
            version_service=version_service,
            app_settings=app_settings,
            materializer=materializer,
            permissions=perms,
        )
        sql.query.return_value = []  # name available
        product = svc.create("Orders", None, None, "alice@x")
        perms.seed_default_grants.assert_called_once_with(
            ObjectType.DATA_PRODUCT.value,
            product.product_id,
            owner_email="alice@x",
            grantor="alice@x",
        )

    def test_create_skips_seeding_when_no_permissions_service(self, service, sql):
        """DataProductService.create is safe with no PermissionsService injected (None default)."""
        sql.query.return_value = []
        product = service.create("Orders", None, None, "alice@x")
        assert product.status == "draft"


class TestUpdate:
    def test_update_flips_published_to_draft_without_bumping_version(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="published", version="3")],
        ]
        updated = service.update("p1", {"description": "new desc"}, "bob@x")
        assert updated.status == "draft"
        assert updated.version == 3  # unchanged
        assert updated.description == "new desc"
        update_sql = sql.execute.call_args[0][0]
        assert "status = 'draft'" in update_sql
        assert "version =" not in update_sql  # PATCH never bumps version

    def test_update_missing_raises_lookup_error(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.update("missing", {"description": "x"}, "bob@x")

    def test_update_rename_to_existing_name_raises_409(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", name="Orders")],
            [["p-other"]],  # name check finds a different product
        ]
        with pytest.raises(DuplicateDataProductNameError):
            service.update("p1", {"name": "Taken"}, "bob@x")

    def test_update_rename_to_same_name_is_a_noop_check(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", name="Orders")],
        ]
        updated = service.update("p1", {"name": "Orders"}, "bob@x")
        assert updated.name == "Orders"

    def test_update_empty_name_raises_value_error(self, service, sql):
        sql.query.side_effect = [[_product_row(product_id="p1")]]
        with pytest.raises(ValueError):
            service.update("p1", {"name": ""}, "bob@x")

    def test_update_clears_schedule_when_explicitly_null(self, service, sql):
        sql.query.side_effect = [[_product_row(product_id="p1", schedule_cron="0 0 * * *")]]
        service.update("p1", {"schedule_cron": None}, "bob@x")
        update_sql = sql.execute.call_args[0][0]
        assert "schedule_cron = NULL" in update_sql

    def test_update_persists_schedule_kind(self, service, sql):
        sql.query.side_effect = [[_product_row(product_id="p1")]]
        updated = service.update("p1", {"schedule_kind": "profiling_only"}, "bob@x")
        assert updated.schedule_kind == "profiling_only"
        assert "schedule_kind = 'profiling_only'" in sql.execute.call_args[0][0]

    def test_update_never_writes_null_schedule_kind(self, service, sql):
        # schedule_kind is NOT NULL on Postgres — an explicit None must be
        # ignored rather than written as NULL.
        sql.query.side_effect = [[_product_row(product_id="p1")]]
        service.update("p1", {"schedule_kind": None}, "bob@x")
        assert "schedule_kind =" not in sql.execute.call_args[0][0]


class TestMemberTableFqns:
    """B2-52: scheduler profiling fan-out enumerates a product's member tables."""

    def test_lists_distinct_real_fqns(self, service, sql, monitored_tables):
        sql.query.side_effect = [[_member_row("m1", "b1"), _member_row("m2", "b2")]]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", table_fqn="c.s.t1"),
            _table_summary(binding_id="b2", table_fqn="c.s.t2"),
        ]
        assert service.member_table_fqns("p1") == ["c.s.t1", "c.s.t2"]

    def test_dedupes_and_skips_synthetic_and_missing(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_member_row("m1", "b1"), _member_row("m2", "b2"), _member_row("m3", "b3"), _member_row("m4", "b-gone")]
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", table_fqn="c.s.t1"),
            _table_summary(binding_id="b2", table_fqn="c.s.t1"),  # duplicate FQN collapses
            _table_summary(binding_id="b3", table_fqn="__sql_check__/x"),  # synthetic — no physical table
        ]
        # b-gone has no summary → skipped.
        assert service.member_table_fqns("p1") == ["c.s.t1"]


class TestDelete:
    def test_delete_success(self, service, sql):
        sql.query.return_value = [["p1"]]
        service.delete("p1")
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"DELETE FROM {_MEMBERS}" in c for c in calls)
        assert any(f"DELETE FROM {_PRODUCTS}" in c for c in calls)

    def test_delete_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.delete("missing")


class TestMembers:
    def test_add_member_inserts_new_and_flips_to_draft(self, service, sql, monitored_tables):
        monitored_tables.get.return_value = _monitored_table_detail()
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="published")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", 2, "bob@x")
        assert member.binding_id == "b1"
        assert member.pinned_version == 2
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"INSERT INTO {_MEMBERS}" in c for c in calls)
        assert any("status = 'draft'" in c for c in calls)

    def test_add_member_updates_pin_in_place_for_existing_binding(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [["m-existing"]],  # already a member
        ]
        member = service.add_member("p1", "b1", 5, "bob@x")
        assert member.id == "m-existing"
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_MEMBERS}" in c and "pinned_version = 5" in c for c in calls)
        assert not any(f"INSERT INTO {_MEMBERS}" in c for c in calls)

    def test_add_member_missing_product_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.add_member("missing", "b1", None, "bob@x")

    def test_add_member_unspecified_pin_follows_latest_when_auto_upgrade_on(
        self, service, sql, monitored_tables, app_settings
    ):
        # default_auto_upgrade ON (default): a brand-new member with no
        # explicit pin stays unpinned (follow latest) — unchanged behaviour.
        app_settings.get_default_auto_upgrade.return_value = True
        monitored_tables.get.return_value = _monitored_table_detail(version=3)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", None, "bob@x")
        assert member.pinned_version is None
        insert_sql = next(c[0][0] for c in sql.execute.call_args_list if f"INSERT INTO {_MEMBERS}" in c[0][0])
        assert "NULL" in insert_sql

    def test_add_member_unspecified_pin_freezes_current_version_when_auto_upgrade_off(
        self, service, sql, monitored_tables, app_settings
    ):
        # default_auto_upgrade OFF: a brand-new member with no explicit pin
        # freezes to the binding's current published version at attach time.
        app_settings.get_default_auto_upgrade.return_value = False
        monitored_tables.get.return_value = _monitored_table_detail(version=3)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", None, "bob@x")
        assert member.pinned_version == 3
        insert_sql = next(c[0][0] for c in sql.execute.call_args_list if f"INSERT INTO {_MEMBERS}" in c[0][0])
        assert ", 3)" in insert_sql

    def test_add_member_existing_binding_explicit_none_unpins_regardless_of_setting(
        self, service, sql, monitored_tables, app_settings
    ):
        # Attach-time-only: default_auto_upgrade must NOT be consulted when
        # updating an EXISTING member — an explicit None here means the
        # steward chose to unpin, not "unspecified".
        app_settings.get_default_auto_upgrade.return_value = False
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [["m-existing"]],  # already a member
        ]
        member = service.add_member("p1", "b1", None, "bob@x")
        assert member.pinned_version is None
        monitored_tables.get.assert_not_called()
        app_settings.resolve_pinned_version_for_new_attachment.assert_not_called()
        update_sql = next(c[0][0] for c in sql.execute.call_args_list if f"UPDATE {_MEMBERS}" in c[0][0])
        assert "pinned_version = NULL" in update_sql

    def test_add_member_invalid_binding_id_raises(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row — triggers the binding validation
        ]
        # Simulate binding_id not found in monitored_tables
        monitored_tables.get.return_value = None
        with pytest.raises(RuntimeError, match="Monitored table not found"):
            service.add_member("p1", "invalid_binding", None, "bob@x")

    def test_add_member_valid_binding_id_succeeds(self, service, sql, monitored_tables, app_settings):
        # Ensure binding_id exists in monitored_tables
        monitored_tables.get.return_value = _monitored_table_detail(binding_id="b1", version=2)
        app_settings.get_default_auto_upgrade.return_value = True
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="draft")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", 2, "bob@x")
        assert member.binding_id == "b1"
        assert member.pinned_version == 2

    @pytest.mark.parametrize(
        ("status", "version"),
        [
            ("draft", 0),  # brand-new, never approved
            ("pending_approval", 0),  # submitted, not yet approved
            ("rejected", 0),  # rejected without any prior approval
            ("pending_approval", 2),  # previously approved, resubmitted — status alone blocks
            ("rejected", 2),  # previously approved, later rejected — status alone blocks
            ("approved", 0),  # rollup edge: status flag approved but never binding-approved
        ],
    )
    def test_add_member_non_approved_binding_raises_400(self, service, sql, monitored_tables, status, version):
        """P3.2: only bindings passing _is_runnable (approved AND version > 0) can JOIN a space."""
        monitored_tables.get.return_value = _monitored_table_detail(status=status, version=version)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row — new-member branch
        ]
        with pytest.raises(BindingNotApprovedError, match="cat.schema.tbl"):
            service.add_member("p1", "b1", None, "bob@x")
        # Nothing was inserted and the space's status was left untouched.
        sql.execute.assert_not_called()

    def test_add_member_never_approved_message_names_table_and_reason(self, service, sql, monitored_tables):
        monitored_tables.get.return_value = _monitored_table_detail(status="draft", version=0)
        sql.query.side_effect = [[_product_row(product_id="p1")], []]
        with pytest.raises(BindingNotApprovedError, match="never been approved"):
            service.add_member("p1", "b1", None, "bob@x")

    def test_add_member_non_approved_status_message_names_status(self, service, sql, monitored_tables):
        monitored_tables.get.return_value = _monitored_table_detail(status="pending_approval", version=2)
        sql.query.side_effect = [[_product_row(product_id="p1")], []]
        with pytest.raises(BindingNotApprovedError, match="'pending_approval'"):
            service.add_member("p1", "b1", None, "bob@x")

    def test_add_member_modified_underneath_binding_stays_eligible(self, service, sql, monitored_tables):
        """A binding shown as "modified" in the UI (approved rules with
        unapproved edits) still has persisted status 'approved' and version > 0
        — it has a frozen approved snapshot, so it remains eligible (P3.2)."""
        monitored_tables.get.return_value = _monitored_table_detail(status="approved", version=3)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],  # no existing member row
        ]
        member = service.add_member("p1", "b1", 3, "bob@x")
        assert member.binding_id == "b1"
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"INSERT INTO {_MEMBERS}" in c for c in calls)

    def test_add_member_pin_change_on_existing_member_skips_approval_check(self, service, sql, monitored_tables):
        """No retroactive eviction (P3.2): a binding that left 'approved' after
        joining can still have its pin changed — the UPDATE branch never
        consults the binding's status (run() already skips it under
        source='approved')."""
        monitored_tables.get.return_value = _monitored_table_detail(status="draft", version=0)
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [["m-existing"]],  # already a member
        ]
        member = service.add_member("p1", "b1", 4, "bob@x")
        assert member.id == "m-existing"
        monitored_tables.get.assert_not_called()
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"UPDATE {_MEMBERS}" in c and "pinned_version = 4" in c for c in calls)

    def test_remove_member_success_flips_to_draft(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1", status="published")],
            [["m1"]],
        ]
        service.remove_member("p1", "m1", "bob@x")
        calls = [c[0][0] for c in sql.execute.call_args_list]
        assert any(f"DELETE FROM {_MEMBERS}" in c for c in calls)
        assert any("status = 'draft'" in c for c in calls)

    def test_remove_member_missing_raises(self, service, sql):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [],
        ]
        with pytest.raises(LookupError):
            service.remove_member("p1", "missing", "bob@x")


class TestSubmit:
    def test_submit_draft_to_pending_without_version_bump(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="draft", version="1")]
        product = service.submit("p1", "bob@x")
        assert product.status == "pending_approval"
        assert product.version == 1  # unchanged — only approve bumps
        update_sql = sql.execute.call_args[0][0]
        assert "status = 'pending_approval'" in update_sql
        assert "version =" not in update_sql

    def test_submit_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.submit("missing", "bob@x")

    def test_submit_rejected_to_pending(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="rejected", version="1")]
        product = service.submit("p1", "bob@x")
        assert product.status == "pending_approval"

    def test_submit_approved_unchanged_raises_409(self, service, sql):
        """Minor fix: an approved, unchanged space submitted via direct API
        must be rejected — mirrors the P20 registry-rule "no changes to
        submit" guard. Every ``update``/``add_member``/``remove_member`` call
        already flips the space to ``draft`` (even a no-op save), so a space
        still at ``approved`` has zero unpublished changes by construction.
        Without this guard, submitting it would move it to
        ``pending_approval`` — which is not itself runnable — silently
        pausing its scheduled runs.
        """
        sql.query.return_value = [_product_row(product_id="p1", status="approved", version="2")]
        with pytest.raises(InvalidStatusTransitionError):
            service.submit("p1", "bob@x")
        sql.execute.assert_not_called()


class TestApprove:
    def test_approve_bumps_version_and_sets_approved(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="pending_approval", version="1")]
        product = service.approve("p1", "bob@x")
        assert product.status == "approved"
        assert product.version == 2
        update_sql = sql.execute.call_args[0][0]
        assert "version = 2" in update_sql
        assert "status = 'approved'" in update_sql

    def test_approve_non_pending_raises_409(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="draft", version="0")]
        with pytest.raises(InvalidStatusTransitionError):
            service.approve("p1", "bob@x")

    def test_approve_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.approve("missing", "bob@x")


class TestReject:
    def test_reject_pending_to_rejected_without_version_bump(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="pending_approval", version="2")]
        product = service.reject("p1", "bob@x")
        assert product.status == "rejected"
        assert product.version == 2
        update_sql = sql.execute.call_args[0][0]
        assert "status = 'rejected'" in update_sql
        assert "version =" not in update_sql

    def test_reject_non_pending_raises_409(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="approved", version="1")]
        with pytest.raises(InvalidStatusTransitionError):
            service.reject("p1", "bob@x")

    def test_reject_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.reject("missing", "bob@x")


class TestRevert:
    def test_revert_pending_to_draft_without_version_bump(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="pending_approval", version="2")]
        product = service.revert("p1", "bob@x")
        assert product.status == "draft"
        assert product.version == 2  # unchanged — revert only withdraws
        update_sql = sql.execute.call_args[0][0]
        assert "status = 'draft'" in update_sql
        assert "version =" not in update_sql

    def test_revert_non_pending_raises_409(self, service, sql):
        sql.query.return_value = [_product_row(product_id="p1", status="approved", version="1")]
        with pytest.raises(InvalidStatusTransitionError):
            service.revert("p1", "bob@x")
        sql.execute.assert_not_called()

    def test_revert_missing_raises(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.revert("missing", "bob@x")


class TestRun:
    def test_draft_source_runs_never_approved_members_instead_of_skipping(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="draft", version=0)
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-1", job_run_id=1, view_fqn="tmp.v1"
        )
        result = service.run("p1", "draft", "bob@x")
        assert result.run_set_id == "rs-new"
        assert len(result.submitted) == 1
        assert result.skipped == []
        # assert_called_once_with is an exact-match: the fan-out passes NO
        # sampling knob (run_binding has none). Sampling resolves per
        # resolved source inside run_binding — approved members always
        # scan the whole table; draft members are capped by the admin
        # ``draft_run_sample_limit`` setting.
        binding_run_service.run_binding.assert_called_once_with(
            "b1", source="draft", version=None, user_email="bob@x", trigger="manual", run_set_id="rs-new"
        )

    def test_approved_source_skips_never_approved_unpinned_member(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1"), _member_row("m2", "b2")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", table_fqn="cat.s.never", status="draft", version=0),
            _table_summary(binding_id="b2", table_fqn="cat.s.ready", status="approved", version=3),
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-2", job_run_id=2, view_fqn="tmp.v2"
        )
        result = service.run("p1", "approved", "bob@x")
        assert len(result.submitted) == 1
        assert result.submitted[0].binding_id == "b2"
        assert len(result.skipped) == 1
        assert "never approved" in result.skipped[0]
        binding_run_service.run_binding.assert_called_once_with(
            "b2", source="approved", version=None, user_email="bob@x", trigger="manual", run_set_id="rs-new"
        )

    def test_pinned_member_attempted_even_when_never_approved(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1", pinned_version="2")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="draft", version=0)
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-3", job_run_id=3, view_fqn="tmp.v3"
        )
        result = service.run("p1", "approved", "bob@x")
        assert len(result.submitted) == 1
        binding_run_service.run_binding.assert_called_once_with(
            "b1", source="approved", version=2, user_email="bob@x", trigger="manual", run_set_id="rs-new"
        )

    def test_zero_runnable_raises(self, service, sql, monitored_tables):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="draft", version=0)
        ]
        with pytest.raises(NoRunnableMembersError):
            service.run("p1", "approved", "bob@x")

    def test_per_member_submission_failure_collected_and_continues(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1"), _member_row("m2", "b2")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", table_fqn="cat.s.fails", status="approved", version=1),
            _table_summary(binding_id="b2", table_fqn="cat.s.ok", status="approved", version=1),
        ]
        binding_run_service.run_binding.side_effect = [
            NeverApprovedError("boom"),
            BindingRunResult(run_set_id="rs-new", run_id="run-4", job_run_id=4, view_fqn="tmp.v4"),
        ]
        result = service.run("p1", "approved", "bob@x")
        assert len(result.submitted) == 1
        assert result.submitted[0].binding_id == "b2"
        assert len(result.skipped) == 1
        assert "cat.s.fails" in result.skipped[0]

    def test_all_members_fail_rolls_back_empty_run_set(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=1)
        ]
        binding_run_service.run_binding.side_effect = NeverApprovedError("boom")
        result = service.run("p1", "approved", "bob@x")
        assert result.submitted == []
        run_set_service.delete_empty.assert_called_once_with("rs-new")

    def test_missing_product_raises_lookup_error(self, service, sql):
        sql.query.return_value = []
        with pytest.raises(LookupError):
            service.run("missing", "approved", "bob@x")

    def test_shared_run_set_created_with_product_id_and_version(
        self, service, sql, monitored_tables, run_set_service, binding_run_service
    ):
        sql.query.side_effect = [
            [_product_row(product_id="p1", version="4")],
            [_member_row("m1", "b1")],
        ]
        monitored_tables.list_monitored_tables.return_value = [
            _table_summary(binding_id="b1", status="approved", version=1)
        ]
        binding_run_service.run_binding.return_value = BindingRunResult(
            run_set_id="rs-new", run_id="run-5", job_run_id=5, view_fqn="tmp.v5"
        )
        service.run("p1", "approved", "bob@x")
        run_set_service.create.assert_called_once_with(
            product_id="p1", product_version=4, source="approved", trigger="manual", created_by="bob@x"
        )
