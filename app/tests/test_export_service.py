"""Tests for the Export service — registry / monitored-table / table-space YAML.

Two layers:

* Pure serialization (``registry_rule_to_check_dict``, ``build_odcs_contract``,
  ``dump_dqx_yaml`` / ``dump_odcs_yaml``) — no DB, exercised directly.
* ``ExportService`` orchestration — the registry / monitored-table /
  data-product services and the materializer are mocked; we assert the right
  render path is taken per format and that not-found targets raise
  ``ExportError``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
import yaml

from databricks_labs_dqx_app.backend.registry_models import RegistryRule, RuleDefinition
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductDetail,
    DataProductMemberDetail,
    DataProductService,
)
from databricks_labs_dqx_app.backend.services.export_service import (
    ExportError,
    ExportService,
    build_odcs_contract,
    dump_dqx_yaml,
    registry_rule_to_check_dict,
)
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    MonitoredTableDetail,
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.registry_models import DataProduct, MonitoredTable
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


def _app_settings_stub() -> AppSettingsService:
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_label_definitions.return_value = []
    return mock


def _native_rule(*, name: str = "email_valid", severity: str = "High") -> RegistryRule:
    return RegistryRule(
        rule_id="r1",
        mode="dqx_native",
        status="approved",
        version=1,
        polarity=None,
        definition=RuleDefinition.model_validate(
            {
                "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
                "slots": [{"name": "column", "family": "any", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        ),
        user_metadata={"name": name, "severity": severity, "dimension": "completeness"},
    )


def _sql_rule() -> RegistryRule:
    return RegistryRule(
        rule_id="r2",
        mode="sql",
        status="approved",
        version=1,
        polarity="fail",
        definition=RuleDefinition.model_validate(
            {"body": {"predicate": "amount < 0"}, "slots": [], "parameters": []}
        ),
        user_metadata={"name": "amount_non_negative", "severity": "Low"},
    )


# ---------------------------------------------------------------------------
# Pure serialization
# ---------------------------------------------------------------------------


class TestRegistryRuleToCheckDict:
    def test_native_rule_keeps_slot_placeholder(self):
        check = registry_rule_to_check_dict(_native_rule(), _app_settings_stub())
        assert check["check"] == {"function": "is_not_null", "arguments": {"column": "{{column}}"}}
        # severity "High" -> criticality "error" via the built-in default map.
        assert check["criticality"] == "error"
        assert check["name"] == "email_valid"
        assert check["user_metadata"]["dimension"] == "completeness"

    def test_sql_rule_renders_expression_and_negate(self):
        check = registry_rule_to_check_dict(_sql_rule(), _app_settings_stub())
        assert check["check"]["function"] == "sql_expression"
        assert check["check"]["arguments"]["expression"] == "amount < 0"
        # polarity "fail" -> negate True
        assert check["check"]["arguments"]["negate"] is True

    def test_message_expr_included_when_set(self):
        rule = _native_rule()
        rule.definition.error_message = "must not be null"
        check = registry_rule_to_check_dict(rule, _app_settings_stub())
        assert check["message_expr"] == "must not be null"

    def test_dqx_yaml_round_trips(self):
        check = registry_rule_to_check_dict(_native_rule(), _app_settings_stub())
        text = dump_dqx_yaml([check])
        loaded = yaml.safe_load(text)
        assert isinstance(loaded, list)
        assert loaded[0]["check"]["function"] == "is_not_null"


class TestBuildOdcsContract:
    def test_one_schema_entry_per_table_with_quality(self):
        checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
        contract = build_odcs_contract(name="My space", tables=[("cat.sch.tbl", checks)])
        assert contract["kind"] == "DataContract"
        assert contract["apiVersion"] == "v3.0.2"
        assert contract["id"] == "urn:datacontract:dqx:my-space"
        assert len(contract["schema"]) == 1
        entry = contract["schema"][0]
        assert entry["name"] == "tbl"
        assert entry["physicalName"] == "cat.sch.tbl"
        assert entry["physicalType"] == "table"
        assert entry["quality"][0] == {
            "type": "custom",
            "engine": "dqx",
            "implementation": checks[0],
        }

    def test_table_without_checks_omits_quality(self):
        contract = build_odcs_contract(name="s", tables=[("cat.sch.empty", [])])
        assert "quality" not in contract["schema"][0]

    def test_odcs_yaml_is_parseable(self):
        contract = build_odcs_contract(name="s", tables=[("cat.sch.tbl", [])])
        loaded = yaml.safe_load(yaml.dump(contract))
        assert loaded["schema"][0]["physicalName"] == "cat.sch.tbl"


# ---------------------------------------------------------------------------
# ExportService orchestration
# ---------------------------------------------------------------------------


@pytest.fixture
def services():
    registry = create_autospec(RegistryService, instance=True)
    materializer = create_autospec(Materializer, instance=True)
    monitored_tables = create_autospec(MonitoredTableService, instance=True)
    data_products = create_autospec(DataProductService, instance=True)
    svc = ExportService(
        registry=registry,
        app_settings=_app_settings_stub(),
        materializer=materializer,
        monitored_tables=monitored_tables,
        data_products=data_products,
    )
    return svc, registry, materializer, monitored_tables, data_products


class TestExportRegistry:
    def test_export_all_rules_dqx(self, services):
        svc, registry, *_ = services
        registry.list_rules.return_value = [_native_rule(), _sql_rule()]
        result = svc.export_registry_rules()
        assert result.format == "dqx"
        assert result.filename == "registry_rules.dqx.yaml"
        loaded = yaml.safe_load(result.content)
        assert len(loaded) == 2

    def test_export_forwards_rule_ids_selection(self, services):
        svc, registry, *_ = services
        registry.list_rules.return_value = [_native_rule()]
        svc.export_registry_rules(rule_ids=["r1", "r2"])
        # The overview selection action bar exports exactly the ticked rows —
        # the explicit id set must reach the registry filter.
        assert registry.list_rules.call_args.kwargs["rule_ids"] == ["r1", "r2"]

    def test_export_single_rule_uses_name_in_filename(self, services):
        svc, registry, *_ = services
        registry.get_rule_with_version.return_value = (_native_rule(name="Email Valid"), None)
        result = svc.export_registry_rule("r1")
        assert result.filename == "rule_email-valid.dqx.yaml"
        assert yaml.safe_load(result.content)[0]["name"] == "Email Valid"

    def test_export_missing_rule_raises(self, services):
        svc, registry, *_ = services
        registry.get_rule_with_version.return_value = None
        with pytest.raises(ExportError):
            svc.export_registry_rule("nope")


class TestExportMonitoredTable:
    def _detail(self, table_fqn="cat.sch.tbl"):
        return MonitoredTableDetail(
            table=MonitoredTable(binding_id="b1", table_fqn=table_fqn, version=1), applied_rules=[]
        )

    def test_single_table_dqx(self, services):
        svc, _registry, materializer, monitored_tables, _dp = services
        monitored_tables.get.return_value = self._detail()
        materializer.render_binding_checks.return_value = [
            {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
        ]
        result = svc.export_monitored_table("b1", "dqx")
        assert result.format == "dqx"
        assert result.filename == "tbl.dqx.yaml"
        assert yaml.safe_load(result.content)[0]["check"]["function"] == "is_not_null"

    def test_single_table_odcs(self, services):
        svc, _registry, materializer, monitored_tables, _dp = services
        monitored_tables.get.return_value = self._detail()
        materializer.render_binding_checks.return_value = [
            {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
        ]
        result = svc.export_monitored_table("b1", "odcs")
        assert result.format == "odcs"
        assert result.filename == "tbl.odcs.yaml"
        contract = yaml.safe_load(result.content)
        assert contract["schema"][0]["physicalName"] == "cat.sch.tbl"
        assert contract["schema"][0]["quality"][0]["engine"] == "dqx"

    def test_missing_table_raises(self, services):
        svc, _registry, _materializer, monitored_tables, _dp = services
        monitored_tables.get.return_value = None
        with pytest.raises(ExportError):
            svc.export_monitored_table("nope", "dqx")

    def test_render_failure_yields_empty_checks(self, services):
        svc, _registry, materializer, monitored_tables, _dp = services
        monitored_tables.get.return_value = self._detail()
        materializer.render_binding_checks.side_effect = MaterializationError("boom")
        result = svc.export_monitored_table("b1", "dqx")
        assert yaml.safe_load(result.content) in (None, [])

    def test_export_all_tables_odcs_one_entry_per_table(self, services):
        svc, _registry, materializer, monitored_tables, _dp = services
        monitored_tables.list_monitored_tables.return_value = [
            MonitoredTableSummary(table=MonitoredTable(binding_id="b1", table_fqn="c.s.t1", version=1)),
            MonitoredTableSummary(table=MonitoredTable(binding_id="b2", table_fqn="c.s.t2", version=1)),
        ]
        materializer.render_binding_checks.return_value = []
        result = svc.export_monitored_tables("odcs")
        contract = yaml.safe_load(result.content)
        assert [e["physicalName"] for e in contract["schema"]] == ["c.s.t1", "c.s.t2"]

    def test_export_tables_binding_ids_restricts_selection(self, services):
        # The overview selection action bar exports exactly the ticked rows:
        # binding_ids post-filters the listed summaries to that explicit set.
        svc, _registry, materializer, monitored_tables, _dp = services
        monitored_tables.list_monitored_tables.return_value = [
            MonitoredTableSummary(table=MonitoredTable(binding_id="b1", table_fqn="c.s.t1", version=1)),
            MonitoredTableSummary(table=MonitoredTable(binding_id="b2", table_fqn="c.s.t2", version=1)),
        ]
        materializer.render_binding_checks.return_value = []
        result = svc.export_monitored_tables("odcs", binding_ids=["b2"])
        contract = yaml.safe_load(result.content)
        assert [e["physicalName"] for e in contract["schema"]] == ["c.s.t2"]


class TestExportDataProduct:
    def _detail(self):
        return DataProductDetail(
            product=DataProduct(product_id="p1", name="Sales space", description="Core sales tables"),
            members=[
                DataProductMemberDetail(
                    id="m1",
                    binding_id="b1",
                    table_fqn="c.s.orders",
                    binding_status="approved",
                    binding_version=1,
                    pinned_version=None,
                    rules_count=1,
                    checks_count=1,
                    runnable=True,
                ),
            ],
        )

    def test_single_product_odcs(self, services):
        svc, _registry, materializer, _mt, data_products = services
        data_products.get.return_value = self._detail()
        materializer.render_binding_checks.return_value = [
            {"criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
        ]
        result = svc.export_data_product("p1", "odcs")
        assert result.filename == "sales-space.odcs.yaml"
        contract = yaml.safe_load(result.content)
        assert contract["name"] == "Sales space"
        assert contract["description"]["purpose"] == "Core sales tables"
        assert contract["schema"][0]["physicalName"] == "c.s.orders"

    def test_single_product_dqx_flat_list(self, services):
        svc, _registry, materializer, _mt, data_products = services
        data_products.get.return_value = self._detail()
        materializer.render_binding_checks.return_value = [
            {"criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
        ]
        result = svc.export_data_product("p1", "dqx")
        assert result.format == "dqx"
        assert len(yaml.safe_load(result.content)) == 1

    def test_missing_product_raises(self, services):
        svc, _registry, _materializer, _mt, data_products = services
        data_products.get.return_value = None
        with pytest.raises(ExportError):
            svc.export_data_product("nope", "dqx")

    def _detail_for(self, product_id, name, table_fqn):
        return DataProductDetail(
            product=DataProduct(product_id=product_id, name=name),
            members=[
                DataProductMemberDetail(
                    id=f"m-{product_id}",
                    binding_id=f"b-{product_id}",
                    table_fqn=table_fqn,
                    binding_status="approved",
                    binding_version=1,
                    pinned_version=None,
                    rules_count=1,
                    checks_count=1,
                    runnable=True,
                ),
            ],
        )

    def test_export_products_product_ids_restricts_selection(self, services):
        # The overview selection action bar exports exactly the ticked rows:
        # product_ids post-filters the listed products to that explicit set.
        svc, _registry, materializer, _mt, data_products = services
        data_products.list_products.return_value = [
            self._detail_for("p1", "Sales space", "c.s.orders"),
            self._detail_for("p2", "Ops space", "c.s.events"),
        ]
        materializer.render_binding_checks.return_value = []
        result = svc.export_data_products("odcs", product_ids=["p2"])
        contract = yaml.safe_load(result.content)
        assert [e["physicalName"] for e in contract["schema"]] == ["c.s.events"]
