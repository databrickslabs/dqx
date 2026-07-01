"""Tests for ``services/contract_rules_service.py``.

We mock out the DQX ``DataContractRulesGenerator`` (and the
``datacontract`` SDK class) so these tests run without the
``[datacontract]`` extras and without invoking the underlying ODCS parser.
The covered surface is the per-schema bucketing, metadata extraction
from raw YAML, and the warnings-on-text-rules contract — i.e. the glue
code that lives in this app, not the upstream DQX library.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.services.contract_rules_service import (
    ContractMetadata,
    ContractRulesService,
)


_VALID_CONTRACT = """
kind: DataContract
apiVersion: v3.0.2
id: urn:datacontract:demo:orders
name: Demo Orders
version: 1.2.0
status: active
owner: Demo Team
domain: ecommerce
description: A demo orders contract
schema:
  - name: orders
    physicalType: table
    physicalName: main.demo.orders
    properties:
      - name: order_id
        logicalType: string
        required: true
        unique: true
      - name: order_total
        logicalType: number
  - name: line_items
    physicalType: table
    properties:
      - name: order_id
        logicalType: string
        required: true
"""


@pytest.fixture
def patched_generator(monkeypatch):
    """Install fake DQX + datacontract modules so import succeeds."""
    # Fake ``datacontract.data_contract.DataContract`` — only used as a
    # marker, the generator instance is mocked.
    dc_pkg = type(sys)("datacontract")
    dc_mod = type(sys)("datacontract.data_contract")
    dc_mod.DataContract = lambda **kwargs: ("contract", kwargs)  # type: ignore[attr-defined]
    dc_pkg.data_contract = dc_mod  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "datacontract", dc_pkg)
    monkeypatch.setitem(sys.modules, "datacontract.data_contract", dc_mod)

    # Fake DataContractRulesGenerator — captures init args, exposes
    # generate_rules_from_contract for the test to set per-case.
    gen_instance = MagicMock(name="GeneratorInstance")
    generator_class = MagicMock(name="DataContractRulesGenerator", return_value=gen_instance)

    fake_gen_mod = type(sys)("databricks.labs.dqx.datacontract.contract_rules_generator")
    fake_gen_mod.DataContractRulesGenerator = generator_class  # type: ignore[attr-defined]
    monkeypatch.setitem(
        sys.modules,
        "databricks.labs.dqx.datacontract.contract_rules_generator",
        fake_gen_mod,
    )
    return generator_class, gen_instance


@pytest.fixture
def service():
    return ContractRulesService(sp_ws=MagicMock(name="SpWorkspaceClient"))


# Contract carrying a single property-level ``type: text`` quality
# expectation so ``_extract_text_expectations`` yields work for the LLM leg.
_TEXT_CONTRACT = """
kind: DataContract
apiVersion: v3.0.2
id: urn:datacontract:demo:orders
name: Demo Orders
version: 1.0.0
schema:
  - name: orders
    physicalName: main.demo.orders
    properties:
      - name: order_total
        logicalType: number
        quality:
          - type: text
            description: order_total must always be positive
"""


def _ai_service_returning(rules):
    ai = MagicMock(name="AiRulesService")
    ai.generate_from_schema_info.return_value = rules
    return ai


def _rule(schema: str | None, name: str, **extra: Any) -> dict[str, Any]:
    meta: dict[str, Any] = {"field": "x", "rule_type": "predefined"}
    if schema is not None:
        meta["schema"] = schema
    return {"name": name, "criticality": "error", "user_metadata": meta, **extra}


class TestGenerate:
    def test_extracts_top_level_metadata(self, service, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []

        result = service.generate(contract_text=_VALID_CONTRACT)
        meta = result.metadata
        assert meta.contract_id == "urn:datacontract:demo:orders"
        assert meta.name == "Demo Orders"
        assert meta.version == "1.2.0"
        assert meta.odcs_api_version == "v3.0.2"
        assert meta.status == "active"
        assert meta.owner == "Demo Team"
        assert meta.domain == "ecommerce"

    def test_buckets_rules_by_schema_name(self, service, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = [
            _rule("orders", "orders_id_not_null"),
            _rule("orders", "orders_total_range"),
            _rule("line_items", "li_order_id_not_null"),
        ]

        result = service.generate(contract_text=_VALID_CONTRACT)

        # Both contract schemas are present, in declaration order.
        assert [s.schema_name for s in result.schemas] == ["orders", "line_items"]
        orders = result.schemas[0]
        assert orders.physical_name == "main.demo.orders"
        assert orders.property_count == 2
        assert [r["name"] for r in orders.rules] == [
            "orders_id_not_null",
            "orders_total_range",
        ]
        line_items = result.schemas[1]
        assert line_items.physical_name is None
        assert line_items.property_count == 1
        assert [r["name"] for r in line_items.rules] == ["li_order_id_not_null"]
        assert result.unassigned_rules == []
        assert result.total_rules == 3

    def test_rule_without_schema_metadata_lands_in_unassigned(self, service, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = [
            _rule(None, "orphan_rule"),
        ]
        result = service.generate(contract_text=_VALID_CONTRACT)
        assert [r["name"] for r in result.unassigned_rules] == ["orphan_rule"]
        # Existing schemas are still listed even if they have no rules.
        assert all(s.rules == [] for s in result.schemas)

    def test_unknown_schema_in_rule_metadata_is_surfaced(self, service, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = [
            _rule("ghost_schema", "ghost_rule"),
        ]
        result = service.generate(contract_text=_VALID_CONTRACT)
        names = [s.schema_name for s in result.schemas]
        assert "ghost_schema" in names
        ghost = next(s for s in result.schemas if s.schema_name == "ghost_schema")
        assert ghost.physical_name is None
        assert ghost.property_count == 0
        assert [r["name"] for r in ghost.rules] == ["ghost_rule"]

    def test_passes_options_through_to_generator(self, service, patched_generator):
        klass, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        service.generate(
            contract_text=_VALID_CONTRACT,
            generate_predefined_rules=False,
            generate_schema_validation=False,
            strict_schema_validation=False,
            default_criticality="warn",
        )
        klass.assert_called_once()
        kwargs = gen.generate_rules_from_contract.call_args.kwargs
        assert kwargs["generate_predefined_rules"] is False
        assert kwargs["generate_schema_validation"] is False
        assert kwargs["strict_schema_validation"] is False
        assert kwargs["default_criticality"] == "warn"
        # We always pin text rule processing off — the AI page owns that flow.
        assert kwargs["process_text_rules"] is False

    def test_text_rules_request_emits_warning(self, service, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        result = service.generate(
            contract_text=_VALID_CONTRACT,
            process_text_rules=True,
        )
        assert any("AI-Assisted" in w for w in result.warnings)

    def test_empty_contract_raises(self, service, patched_generator):
        with pytest.raises(ValueError):
            service.generate(contract_text="   \n  ")

    def test_invalid_yaml_raises_value_error(self, service, patched_generator):
        with pytest.raises(ValueError):
            service.generate(contract_text=":\n- bad: [unclosed")

    def test_non_mapping_top_level_raises(self, service, patched_generator):
        with pytest.raises(ValueError):
            service.generate(contract_text="- just\n- a\n- list")


class TestValidationGate:
    def test_valid_rules_have_no_validation_errors(self, service, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = [
            {
                "name": "orders_id_not_null",
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "order_id"}},
                "user_metadata": {"schema": "orders"},
            }
        ]
        result = service.generate(contract_text=_VALID_CONTRACT)
        assert result.validation_errors == []

    def test_malformed_rule_surfaces_validation_error(self, service, patched_generator):
        _, gen = patched_generator
        # Rule missing the ``check`` block — DQEngine.validate_checks flags it
        # but generation still succeeds (non-blocking).
        gen.generate_rules_from_contract.return_value = [_rule("orders", "broken_rule")]
        result = service.generate(contract_text=_VALID_CONTRACT)
        assert result.validation_errors  # at least one error reported
        # Generation is not aborted by validation failures.
        assert result.total_rules == 1


class TestLlmRuleSafetyGate:
    def test_drops_rule_with_unknown_function(self, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        ai = _ai_service_returning(
            [{"check": {"function": "totally_made_up_fn", "arguments": {"column": "order_total"}}}]
        )
        svc = ContractRulesService(sp_ws=MagicMock(), ai_service=ai)

        result = svc.generate(contract_text=_TEXT_CONTRACT, process_text_rules=True)

        assert result.total_rules == 0
        assert any("discarded" in w for w in result.warnings)

    def test_drops_rule_with_unsafe_sql(self, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        ai = _ai_service_returning(
            [
                {
                    "check": {
                        "function": "sql_expression",
                        "arguments": {"expression": "order_total > 0; DROP TABLE orders"},
                    }
                }
            ]
        )
        svc = ContractRulesService(sp_ws=MagicMock(), ai_service=ai)

        result = svc.generate(contract_text=_TEXT_CONTRACT, process_text_rules=True)

        assert result.total_rules == 0
        assert any("discarded" in w for w in result.warnings)

    def test_drops_rule_with_unsafe_sql_in_non_allowlisted_arg(self, patched_generator):
        # Regression: the safety gate must not rely on a fixed argument-name
        # allowlist. A registry-resolved check that smuggles a destructive
        # statement into an argument other than expression/query/row_filter
        # (here ``column``) must still be dropped.
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        ai = _ai_service_returning(
            [
                {
                    "check": {
                        "function": "is_not_null",
                        "arguments": {"column": "order_id`) ; DROP TABLE orders --"},
                    }
                }
            ]
        )
        svc = ContractRulesService(sp_ws=MagicMock(), ai_service=ai)

        result = svc.generate(contract_text=_TEXT_CONTRACT, process_text_rules=True)

        assert result.total_rules == 0
        assert any("discarded" in w for w in result.warnings)

    def test_drops_rule_with_unsafe_sql_in_nested_arg(self, patched_generator):
        # Nested structures must be screened too — an unsafe statement buried
        # in a list/dict argument value must not slip past the gate.
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        ai = _ai_service_returning(
            [
                {
                    "check": {
                        "function": "is_in_list",
                        "arguments": {"column": "status", "allowed": ["ok", "DROP TABLE orders"]},
                    }
                }
            ]
        )
        svc = ContractRulesService(sp_ws=MagicMock(), ai_service=ai)

        result = svc.generate(contract_text=_TEXT_CONTRACT, process_text_rules=True)

        assert result.total_rules == 0
        assert any("discarded" in w for w in result.warnings)

    def test_keeps_safe_llm_rule(self, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        ai = _ai_service_returning(
            [{"check": {"function": "sql_expression", "arguments": {"expression": "order_total > 0"}}}]
        )
        svc = ContractRulesService(sp_ws=MagicMock(), ai_service=ai)

        result = svc.generate(contract_text=_TEXT_CONTRACT, process_text_rules=True)

        assert result.total_rules == 1
        kept = result.schemas[0].rules[0]
        assert kept["user_metadata"]["rule_type"] == "text_llm"

    def test_llm_metadata_cannot_override_trusted_lineage(self, patched_generator):
        # Regression: an LLM rule carrying its own user_metadata must not be
        # able to overwrite the trusted contract-lineage keys. Otherwise a
        # forged ``schema`` could mis-route the rule to an unintended bucket /
        # target table, and ``rule_type``/``contract_id`` lineage would be
        # corrupted.
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        ai = _ai_service_returning(
            [
                {
                    "check": {"function": "sql_expression", "arguments": {"expression": "order_total > 0"}},
                    "user_metadata": {
                        "schema": "evil_other_schema",
                        "rule_type": "predefined",
                        "contract_id": "spoofed",
                        "harmless_extra": "kept",
                    },
                }
            ]
        )
        svc = ContractRulesService(sp_ws=MagicMock(), ai_service=ai)

        result = svc.generate(contract_text=_TEXT_CONTRACT, process_text_rules=True)

        assert result.total_rules == 1
        kept = result.schemas[0].rules[0]
        meta = kept["user_metadata"]
        # Trusted keys win.
        assert meta["schema"] == "orders"
        assert meta["rule_type"] == "text_llm"
        assert meta["contract_id"] == "urn:datacontract:demo:orders"
        # Non-conflicting LLM keys are preserved.
        assert meta["harmless_extra"] == "kept"
        # And the rule landed in the correct (trusted) schema bucket.
        assert result.schemas[0].schema_name == "orders"

    def test_llm_failure_warning_excludes_raw_exception(self, patched_generator):
        _, gen = patched_generator
        gen.generate_rules_from_contract.return_value = []
        ai = MagicMock(name="AiRulesService")
        ai.generate_from_schema_info.side_effect = RuntimeError("secret-internal-endpoint-detail")
        svc = ContractRulesService(sp_ws=MagicMock(), ai_service=ai)

        result = svc.generate(contract_text=_TEXT_CONTRACT, process_text_rules=True)

        assert result.total_rules == 0
        # The raw exception text must not leak into the user-facing warnings.
        assert all("secret-internal-endpoint-detail" not in w for w in result.warnings)
        assert any("Could not generate a rule" in w for w in result.warnings)


class TestMetadataExtraction:
    def test_first_str_picks_first_non_empty_value(self):
        # Direct test of the helper through metadata parsing — confirms
        # the ``info`` fallback path used for older / nested contracts.
        contract = """
kind: DataContract
apiVersion: v3.0.2
info:
  id: urn:from-info
  title: From Info
  version: 9.9.9
schema: []
"""
        svc = ContractRulesService(sp_ws=MagicMock())
        metadata, schemas = svc._parse_metadata_and_schemas(contract)  # type: ignore[attr-defined]
        assert isinstance(metadata, ContractMetadata)
        assert metadata.contract_id == "urn:from-info"
        assert metadata.name == "From Info"
        assert metadata.version == "9.9.9"
        assert schemas == {}
