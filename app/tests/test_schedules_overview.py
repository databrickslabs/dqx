"""Overview rows flag schedules that cannot run until the source is approved."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from databricks_labs_dqx_app.backend.routes.v1.schedules import list_schedule_overview


def test_overview_flags_schedules_without_an_approved_version() -> None:
    def _table(binding_id: str, version: int) -> SimpleNamespace:
        return SimpleNamespace(
            table=SimpleNamespace(
                binding_id=binding_id,
                table_fqn=f"cat.sch.{binding_id}",
                schedule_cron="0 6 * * *",
                schedule_tz="UTC",
                schedule_kind="dq_only",
                schedule_sample_size=None,
                owner="alice@example.com",
                updated_by=None,
                updated_at=None,
                version=version,
            )
        )

    svc = MagicMock()
    svc.list_trackers.return_value = {}
    svc.list_schedules.return_value = []
    tables = MagicMock()
    tables.list_monitored_tables.return_value = [_table("draft", 0), _table("approved", 2)]
    products = MagicMock()
    products.list_products.return_value = [
        SimpleNamespace(
            product=SimpleNamespace(
                product_id="p1",
                name="Collection",
                schedule_cron="0 6 * * *",
                schedule_tz="UTC",
                schedule_kind="dq_only",
                schedule_sample_size=None,
                owner=None,
                updated_by=None,
                updated_at=None,
                version=0,
            ),
            member_count=3,
        )
    ]

    rows = {row.source_id: row for row in list_schedule_overview(svc, tables, products)}

    assert rows["draft"].awaiting_approval is True
    assert rows["approved"].awaiting_approval is False
    assert rows["p1"].awaiting_approval is True
