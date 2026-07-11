"""Unit tests for ``backend.services.metadata_dim_service``.

The service full-refreshes two SP-owned UC Delta tables (``dim_dq_rules`` /
``dim_dq_monitored_tables``) from the Rules Registry so the Genie space can
answer authoring/ownership questions without reaching Lakebase. Unit tests
cannot touch a warehouse, so they pin the write CONTRACT: the
CREATE-OR-REPLACE-then-INSERT shape, per-part FQN quoting, the exact column
DDL, the zero-rows case (CREATE only, no INSERT), and literal rendering
(string escaping, NULL for None, TRUE/FALSE, ``TIMESTAMP'<iso>'``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import (
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
)
from databricks_labs_dqx_app.backend.services.metadata_dim_service import (
    DIM_MONITORED_TABLES_TABLE_NAME,
    DIM_RULES_TABLE_NAME,
    MetadataDimService,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService

DIM_RULES_FQN = "`dqx_test`.`dqx_app_test`.dim_dq_rules"
DIM_TABLES_FQN = "`dqx_test`.`dqx_app_test`.dim_dq_monitored_tables"

_TS = datetime(2026, 7, 10, 12, 30, 45, tzinfo=timezone.utc)


def _rule(**overrides) -> RegistryRule:
    defaults = dict(
        rule_id="r1",
        mode="dqx_native",
        status="approved",
        version=2,
        definition=RuleDefinition(),
        user_metadata={
            "name": "id_not_null",
            "description": "id must be present",
            "dimension": "Completeness",
            "severity": "High",
        },
        steward="alice@example.com",
        is_builtin=False,
        created_at=_TS,
        updated_at=_TS,
    )
    defaults.update(overrides)
    return RegistryRule(**defaults)


def _summary(**overrides) -> MonitoredTableSummary:
    defaults = dict(
        binding_id="b1",
        table_fqn="cat.sch.orders",
        steward="bob@example.com",
        status="approved",
        version=3,
        schedule_cron="0 0 * * *",
        created_at=_TS,
        updated_at=_TS,
    )
    defaults.update(overrides)
    return MonitoredTableSummary(table=MonitoredTable(**defaults))


@pytest.fixture
def registry() -> object:
    return create_autospec(RegistryService, instance=True)


@pytest.fixture
def monitored_tables() -> object:
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def service(sql_executor_mock, registry, monitored_tables) -> MetadataDimService:
    return MetadataDimService(sp_sql=sql_executor_mock, registry=registry, monitored_tables=monitored_tables)


def _executed(sql_executor_mock) -> list[str]:
    return [call.args[0] for call in sql_executor_mock.execute.call_args_list]


class TestConstants:
    def test_table_name_constants(self) -> None:
        assert DIM_RULES_TABLE_NAME == "dim_dq_rules"
        assert DIM_MONITORED_TABLES_TABLE_NAME == "dim_dq_monitored_tables"


class TestRefreshRules:
    def test_creates_then_inserts_with_quoted_fqn(self, service, sql_executor_mock, registry, monitored_tables) -> None:
        registry.list_rules.return_value = [_rule()]
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        stmts = _executed(sql_executor_mock)
        create = next(s for s in stmts if DIM_RULES_FQN in s and s.startswith("CREATE OR REPLACE TABLE"))
        insert = next(s for s in stmts if DIM_RULES_FQN in s and s.startswith("INSERT INTO"))
        # CREATE precedes INSERT for the same table.
        assert stmts.index(create) < stmts.index(insert)

    def test_create_carries_the_full_column_ddl(self, service, sql_executor_mock, registry, monitored_tables) -> None:
        registry.list_rules.return_value = []
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        create = next(
            s for s in _executed(sql_executor_mock) if DIM_RULES_FQN in s and s.startswith("CREATE OR REPLACE TABLE")
        )
        for col in (
            "rule_id STRING",
            "name STRING",
            "description STRING",
            "dimension STRING",
            "default_severity STRING",
            "mode STRING",
            "status STRING",
            "is_builtin BOOLEAN",
            "steward STRING",
            "version INT",
            "created_at TIMESTAMP",
            "updated_at TIMESTAMP",
        ):
            assert col in create, col

    def test_insert_renders_tags_bool_int_and_timestamp(
        self, service, sql_executor_mock, registry, monitored_tables
    ) -> None:
        registry.list_rules.return_value = [_rule()]
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        insert = next(s for s in _executed(sql_executor_mock) if DIM_RULES_FQN in s and s.startswith("INSERT INTO"))
        expected = (
            "('r1', 'id_not_null', 'id must be present', 'Completeness', 'High', "
            "'dqx_native', 'approved', FALSE, 'alice@example.com', 2, "
            f"TIMESTAMP'{_TS.isoformat()}', TIMESTAMP'{_TS.isoformat()}')"
        )
        assert insert == f"INSERT INTO {DIM_RULES_FQN} VALUES {expected}"

    def test_missing_tags_and_timestamps_become_null(
        self, service, sql_executor_mock, registry, monitored_tables
    ) -> None:
        registry.list_rules.return_value = [
            _rule(user_metadata={}, steward=None, created_at=None, updated_at=None, version=0, is_builtin=True)
        ]
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        insert = next(s for s in _executed(sql_executor_mock) if DIM_RULES_FQN in s and s.startswith("INSERT INTO"))
        # name/description/dimension/severity/steward/timestamps -> NULL;
        # is_builtin -> TRUE; version -> 0.
        assert insert == (
            f"INSERT INTO {DIM_RULES_FQN} VALUES "
            "('r1', NULL, NULL, NULL, NULL, 'dqx_native', 'approved', TRUE, NULL, 0, NULL, NULL)"
        )

    def test_string_values_are_escaped(self, service, sql_executor_mock, registry, monitored_tables) -> None:
        registry.list_rules.return_value = [_rule(user_metadata={"name": "O'Brien's rule"})]
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        insert = next(s for s in _executed(sql_executor_mock) if DIM_RULES_FQN in s and s.startswith("INSERT INTO"))
        assert "'O''Brien''s rule'" in insert

    def test_multiple_rules_join_into_one_insert(self, service, sql_executor_mock, registry, monitored_tables) -> None:
        registry.list_rules.return_value = [_rule(rule_id="r1"), _rule(rule_id="r2")]
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        inserts = [s for s in _executed(sql_executor_mock) if DIM_RULES_FQN in s and s.startswith("INSERT INTO")]
        assert len(inserts) == 1
        assert "'r1'" in inserts[0] and "'r2'" in inserts[0]
        assert "), (" in inserts[0]

    def test_zero_rules_creates_but_does_not_insert(
        self, service, sql_executor_mock, registry, monitored_tables
    ) -> None:
        registry.list_rules.return_value = []
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        stmts = _executed(sql_executor_mock)
        assert any(DIM_RULES_FQN in s and s.startswith("CREATE OR REPLACE TABLE") for s in stmts)
        assert not any(DIM_RULES_FQN in s and s.startswith("INSERT INTO") for s in stmts)


class TestRefreshMonitoredTables:
    def test_create_carries_the_full_column_ddl(self, service, sql_executor_mock, registry, monitored_tables) -> None:
        registry.list_rules.return_value = []
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        create = next(
            s for s in _executed(sql_executor_mock) if DIM_TABLES_FQN in s and s.startswith("CREATE OR REPLACE TABLE")
        )
        for col in (
            "binding_id STRING",
            "table_fqn STRING",
            "steward STRING",
            "status STRING",
            "schedule_cron STRING",
            "version INT",
            "created_at TIMESTAMP",
            "updated_at TIMESTAMP",
        ):
            assert col in create, col

    def test_insert_reads_the_table_attribute_of_each_summary(
        self, service, sql_executor_mock, registry, monitored_tables
    ) -> None:
        registry.list_rules.return_value = []
        monitored_tables.list_monitored_tables.return_value = [_summary()]
        service.refresh()

        insert = next(s for s in _executed(sql_executor_mock) if DIM_TABLES_FQN in s and s.startswith("INSERT INTO"))
        expected = (
            "('b1', 'cat.sch.orders', 'bob@example.com', 'approved', '0 0 * * *', 3, "
            f"TIMESTAMP'{_TS.isoformat()}', TIMESTAMP'{_TS.isoformat()}')"
        )
        assert insert == f"INSERT INTO {DIM_TABLES_FQN} VALUES {expected}"

    def test_null_schedule_and_steward_become_null(
        self, service, sql_executor_mock, registry, monitored_tables
    ) -> None:
        registry.list_rules.return_value = []
        monitored_tables.list_monitored_tables.return_value = [
            _summary(steward=None, schedule_cron=None, created_at=None, updated_at=None, version=0)
        ]
        service.refresh()

        insert = next(s for s in _executed(sql_executor_mock) if DIM_TABLES_FQN in s and s.startswith("INSERT INTO"))
        assert insert == (
            f"INSERT INTO {DIM_TABLES_FQN} VALUES ('b1', 'cat.sch.orders', NULL, 'approved', NULL, 0, NULL, NULL)"
        )

    def test_zero_tables_creates_but_does_not_insert(
        self, service, sql_executor_mock, registry, monitored_tables
    ) -> None:
        registry.list_rules.return_value = []
        monitored_tables.list_monitored_tables.return_value = []
        service.refresh()

        stmts = _executed(sql_executor_mock)
        assert any(DIM_TABLES_FQN in s and s.startswith("CREATE OR REPLACE TABLE") for s in stmts)
        assert not any(DIM_TABLES_FQN in s and s.startswith("INSERT INTO") for s in stmts)


def test_refresh_propagates_execute_failures(service, sql_executor_mock, registry, monitored_tables) -> None:
    # Not best-effort internally — the startup/scheduler callers are.
    registry.list_rules.return_value = [_rule()]
    monitored_tables.list_monitored_tables.return_value = []
    sql_executor_mock.execute.side_effect = RuntimeError("warehouse down")
    with pytest.raises(RuntimeError, match="warehouse down"):
        service.refresh()
