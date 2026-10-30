"""Live orphan cleanup of *dq_resolved_rules* rows on Lakebase Postgres."""

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.migrations.postgres import PgMigrationRunner
from databricks_labs_dqx_app.backend.pg_executor import PgExecutor
from databricks_labs_dqx_app.backend.registry_models import AppliedRule, MonitoredTable
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.materializer import Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    AppliedRuleSummary,
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService

_TABLE_FQN = "cat.schema.orders"

_RESOLVED_ROWS = [
    ("orphan_same_table", _TABLE_FQN, "ar_gone"),
    ("orphan_other_table", "cat.schema.customers", "ar_gone_customers"),
    ("orphan_tableless", "__sql_check__/gone-count", "ar_sql_gone"),
    ("live_tableless", "__sql_check__/live-count", "ar_sql_live"),
    ("lookalike_tableless", "XYsqlZcheckWW/x", "ar_lookalike_gone"),
]


@pytest.fixture
def materializer_pg(lakebase_pg: PgExecutor) -> PgExecutor:
    """Provide a migrated Lakebase schema seeded with resolved rules and one live applied rule."""
    PgMigrationRunner(lakebase_pg).run_all()
    lakebase_pg.execute(
        f"INSERT INTO {lakebase_pg.fqn('dq_applied_rules')} (id, binding_id, rule_id, mapping_hash) "
        "VALUES ('ar_sql_live', 'b_other', 'r_sql', 'h')"
    )
    for rule_id, table_fqn, applied_rule_id in _RESOLVED_ROWS:
        lakebase_pg.execute(
            f"INSERT INTO {lakebase_pg.fqn('dq_resolved_rules')} "
            '(rule_id, table_fqn, "check", version, status, source, applied_rule_id, rule_fingerprint) '
            f"VALUES ('{rule_id}', '{table_fqn}', '{{}}'::jsonb, 1, 'approved', 'registry', "
            f"'{applied_rule_id}', 'fp_{rule_id}')"
        )
    return lakebase_pg


def _materializer(pg: PgExecutor, applied_rules: list[AppliedRule]) -> Materializer:
    registry = create_autospec(RegistryService, instance=True)
    registry.get_rule.return_value = None
    monitored_tables = create_autospec(MonitoredTableService, instance=True)
    monitored_tables.get.return_value = MonitoredTableDetail(
        table=MonitoredTable(binding_id="b_orders", table_fqn=_TABLE_FQN, status="approved"),
        applied_rules=[AppliedRuleSummary(applied_rule=a) for a in applied_rules],
    )
    app_settings = create_autospec(AppSettingsService, instance=True)
    app_settings.get_auto_upgrade_without_approval.return_value = False
    return Materializer(sql=pg, registry=registry, monitored_tables=monitored_tables, app_settings=app_settings)


@pytest.mark.parametrize(
    "applied_rules",
    [
        [],
        [AppliedRule(id="ar_unrendered", binding_id="b_orders", rule_id="r_missing", mapping_hash="h")],
    ],
    ids=["no_applied_rules", "with_applied_rules"],
)
def test_cleanup_deletes_only_this_tables_and_tableless_orphans(
    materializer_pg: PgExecutor, applied_rules: list[AppliedRule]
) -> None:
    """Orphans for this table and orphaned tableless SQL checks go; everything else stays."""
    _materializer(materializer_pg, applied_rules).materialize_binding("b_orders")

    remaining = materializer_pg.query(
        f"SELECT rule_id FROM {materializer_pg.fqn('dq_resolved_rules')} ORDER BY rule_id"
    )
    assert [row[0] for row in remaining] == ["live_tableless", "lookalike_tableless", "orphan_other_table"]
