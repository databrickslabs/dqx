"""Tests for the Rules Registry domain model (Phase 2A / task 2.2).

Covers the Pydantic 2 domain models — ``RegistryRule``, ``RuleVersion``,
``RuleSlot``, ``RuleParameter``, ``RuleDefinition`` — plus the reserved
tag-key helpers. Descriptive metadata (name/description/dimension/severity)
is NOT a column on ``dq_rules``: it lives as reserved keys inside
``user_metadata``, exactly like the Phase 1 ``LabelDefinition`` tags. These
tests exercise the domain model in isolation (no DB, no Spark).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# RuleSlot
# ---------------------------------------------------------------------------


class TestRuleSlot:
    @pytest.fixture
    def RuleSlot(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleSlot

        return RuleSlot

    def test_valid_slot(self, RuleSlot):
        slot = RuleSlot(name="column", family="numeric", position=0, cardinality="one")
        assert slot.name == "column"
        assert slot.family == "numeric"
        assert slot.cardinality == "one"

    def test_defaults(self, RuleSlot):
        slot = RuleSlot(name="column", family="any")
        assert slot.position == 0
        assert slot.cardinality == "one"

    @pytest.mark.parametrize("family", ["numeric", "text", "temporal", "boolean", "any"])
    def test_accepts_all_families(self, RuleSlot, family):
        assert RuleSlot(name="c", family=family).family == family

    def test_rejects_invalid_family(self, RuleSlot):
        with pytest.raises(ValidationError):
            RuleSlot(name="c", family="not-a-family")

    def test_rejects_invalid_cardinality(self, RuleSlot):
        with pytest.raises(ValidationError):
            RuleSlot(name="c", family="any", cardinality="lots")

    @pytest.mark.parametrize("cardinality", ["one", "many"])
    def test_accepts_both_cardinalities(self, RuleSlot, cardinality):
        assert RuleSlot(name="c", family="any", cardinality=cardinality).cardinality == cardinality


# ---------------------------------------------------------------------------
# RuleParameter
# ---------------------------------------------------------------------------


class TestRuleParameter:
    @pytest.fixture
    def RuleParameter(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleParameter

        return RuleParameter

    @pytest.mark.parametrize(
        ("param_type", "value"),
        [
            ("number", 3.14),
            ("string", "hello"),
            ("list", ["a", "b"]),
            ("boolean", True),
            ("regex", r"^\d+$"),
            ("ref_table", "catalog.schema.table"),
            ("ref_column", "id"),
        ],
    )
    def test_accepts_all_types(self, RuleParameter, param_type, value):
        param = RuleParameter(name="threshold", type=param_type, value=value)
        assert param.type == param_type
        assert param.value == value

    def test_value_defaults_to_none(self, RuleParameter):
        param = RuleParameter(name="threshold", type="number")
        assert param.value is None

    def test_rejects_invalid_type(self, RuleParameter):
        with pytest.raises(ValidationError):
            RuleParameter(name="x", type="not-a-type", value="y")


# ---------------------------------------------------------------------------
# RuleDefinition
# ---------------------------------------------------------------------------


class TestRuleDefinition:
    @pytest.fixture
    def module(self):
        from databricks_labs_dqx_app.backend import registry_models

        return registry_models

    def test_holds_body_plus_slots_and_params(self, module):
        definition = module.RuleDefinition(
            body={"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            slots=[module.RuleSlot(name="column", family="any")],
            parameters=[],
        )
        assert definition.body["function"] == "is_not_null"
        assert definition.slots[0].name == "column"

    def test_defaults_are_empty(self, module):
        definition = module.RuleDefinition(body={"predicate": "1=1"})
        assert definition.slots == []
        assert definition.parameters == []


# ---------------------------------------------------------------------------
# RegistryRule
# ---------------------------------------------------------------------------


class TestRegistryRule:
    @pytest.fixture
    def module(self):
        from databricks_labs_dqx_app.backend import registry_models

        return registry_models

    def _make(self, module, **overrides):
        defaults = dict(
            rule_id="r1",
            mode="dqx_native",
            status="draft",
            definition=module.RuleDefinition(body={"function": "is_not_null", "arguments": {}}),
        )
        defaults.update(overrides)
        return module.RegistryRule(**defaults)

    def test_minimal_construction(self, module):
        rule = self._make(module)
        assert rule.rule_id == "r1"
        assert rule.version == 0
        assert rule.is_builtin is False
        assert rule.user_metadata == {}

    @pytest.mark.parametrize("mode", ["dqx_native", "lowcode", "sql"])
    def test_accepts_all_modes(self, module, mode):
        assert self._make(module, mode=mode).mode == mode

    def test_rejects_invalid_mode(self, module):
        with pytest.raises(ValidationError):
            self._make(module, mode="not-a-mode")

    @pytest.mark.parametrize(
        "status", ["draft", "pending_approval", "approved", "rejected", "deprecated"]
    )
    def test_accepts_all_statuses(self, module, status):
        assert self._make(module, status=status).status == status

    def test_rejects_invalid_status(self, module):
        with pytest.raises(ValidationError):
            self._make(module, status="not-a-status")

    @pytest.mark.parametrize("polarity", ["pass", "fail", None])
    def test_accepts_valid_polarity(self, module, polarity):
        assert self._make(module, polarity=polarity).polarity == polarity

    def test_rejects_invalid_polarity(self, module):
        with pytest.raises(ValidationError):
            self._make(module, polarity="maybe")

    def test_name_description_dimension_severity_are_not_fields(self, module):
        """Descriptive metadata is NOT a column — it's a reserved tag."""
        rule = self._make(module)
        assert not hasattr(rule, "name")
        assert not hasattr(rule, "description")
        assert not hasattr(rule, "dimension")
        assert not hasattr(rule, "severity")

    def test_user_metadata_carries_reserved_tags(self, module):
        rule = self._make(
            module,
            user_metadata={
                "name": "Order ID not null",
                "description": "Ensures order_id is populated",
                "dimension": "Completeness",
                "severity": "High",
                "team": "checkout",
            },
        )
        assert module.get_rule_name(rule.user_metadata) == "Order ID not null"
        assert module.get_rule_description(rule.user_metadata) == "Ensures order_id is populated"
        assert module.get_rule_dimension(rule.user_metadata) == "Completeness"
        assert module.get_rule_severity(rule.user_metadata) == "High"


# ---------------------------------------------------------------------------
# RuleVersion (frozen publish snapshot)
# ---------------------------------------------------------------------------


class TestRuleVersion:
    @pytest.fixture
    def module(self):
        from databricks_labs_dqx_app.backend import registry_models

        return registry_models

    def test_minimal_construction(self, module):
        version = module.RuleVersion(
            rule_id="r1",
            version=1,
            definition=module.RuleDefinition(body={"function": "is_not_null", "arguments": {}}),
        )
        assert version.rule_id == "r1"
        assert version.version == 1
        assert version.user_metadata == {}
        assert version.polarity is None

    def test_rejects_invalid_polarity(self, module):
        with pytest.raises(ValidationError):
            module.RuleVersion(
                rule_id="r1",
                version=1,
                definition=module.RuleDefinition(body={}),
                polarity="nope",
            )


# ---------------------------------------------------------------------------
# Reserved tag-key helpers
# ---------------------------------------------------------------------------


class TestReservedTagHelpers:
    @pytest.fixture
    def module(self):
        from databricks_labs_dqx_app.backend import registry_models

        return registry_models

    def test_reserved_keys_constants(self, module):
        assert module.RESERVED_NAME_KEY == "name"
        assert module.RESERVED_DESCRIPTION_KEY == "description"
        assert module.RESERVED_DIMENSION_KEY == "dimension"
        assert module.RESERVED_SEVERITY_KEY == "severity"
        assert module.RESERVED_RULE_METADATA_KEYS == {
            "name",
            "description",
            "dimension",
            "severity",
        }

    def test_getters_return_none_when_absent(self, module):
        assert module.get_rule_name({}) is None
        assert module.get_rule_description({}) is None
        assert module.get_rule_dimension({}) is None
        assert module.get_rule_severity({}) is None

    def test_getters_ignore_non_string_values(self, module):
        assert module.get_rule_name({"name": 123}) is None
        assert module.get_rule_name({"name": ""}) is None

    def test_set_reserved_tag_adds_and_removes(self, module):
        metadata = module.set_reserved_tag({}, module.RESERVED_DIMENSION_KEY, "Validity")
        assert metadata == {"dimension": "Validity"}
        cleared = module.set_reserved_tag(metadata, module.RESERVED_DIMENSION_KEY, None)
        assert cleared == {}

    def test_set_reserved_tag_does_not_mutate_input(self, module):
        original: dict[str, str] = {"team": "checkout"}
        result = module.set_reserved_tag(original, module.RESERVED_SEVERITY_KEY, "High")
        assert original == {"team": "checkout"}
        assert result == {"team": "checkout", "severity": "High"}
