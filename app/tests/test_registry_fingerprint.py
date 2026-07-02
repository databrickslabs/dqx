"""Tests for the Rules Registry fingerprint function (Phase 2A / task 2.2).

The registry fingerprint is a dedup hash computed over the *canonical*
(mode, definition body, slots, parameters, polarity) — deliberately
excluding descriptive tags (name/description/dimension/severity), steward,
status, and timestamps, so the ``RegistryService`` (a later phase) can warn
when two structurally-identical rules are being created under different
names/owners. It must be deterministic and independent of slot/parameter
declaration order.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint
from databricks_labs_dqx_app.backend.registry_models import (
    RegistryRule,
    RuleDefinition,
    RuleParameter,
    RuleSlot,
)


def _native_rule(**overrides) -> RegistryRule:
    defaults: dict = dict(
        rule_id="r1",
        mode="dqx_native",
        status="draft",
        definition=RuleDefinition(
            body={"function": "is_in_range", "arguments": {"column": "{{column}}", "min_limit": 0, "max_limit": 100}},
            slots=[RuleSlot(name="column", family="numeric", position=0)],
            parameters=[
                RuleParameter(name="min_limit", type="number", value=0),
                RuleParameter(name="max_limit", type="number", value=100),
            ],
        ),
    )
    defaults.update(overrides)
    return RegistryRule(**defaults)


class TestDeterminism:
    def test_same_rule_same_fingerprint(self):
        rule_a = _native_rule()
        rule_b = _native_rule()
        assert compute_registry_rule_fingerprint(rule_a) == compute_registry_rule_fingerprint(rule_b)

    def test_fingerprint_is_sha256_hex(self):
        fp = compute_registry_rule_fingerprint(_native_rule())
        assert len(fp) == 64
        int(fp, 16)  # raises ValueError if not valid hex


class TestOrderIndependence:
    def test_slot_order_does_not_matter(self):
        base = RuleDefinition(
            body={"function": "compare_datasets", "arguments": {}},
            slots=[
                RuleSlot(name="column_a", family="any", position=0),
                RuleSlot(name="column_b", family="any", position=1),
            ],
        )
        reordered = RuleDefinition(
            body={"function": "compare_datasets", "arguments": {}},
            slots=[
                RuleSlot(name="column_b", family="any", position=1),
                RuleSlot(name="column_a", family="any", position=0),
            ],
        )
        rule_a = _native_rule(definition=base)
        rule_b = _native_rule(definition=reordered)
        assert compute_registry_rule_fingerprint(rule_a) == compute_registry_rule_fingerprint(rule_b)

    def test_parameter_order_does_not_matter(self):
        base = RuleDefinition(
            body={"function": "is_in_range", "arguments": {}},
            parameters=[
                RuleParameter(name="min_limit", type="number", value=0),
                RuleParameter(name="max_limit", type="number", value=100),
            ],
        )
        reordered = RuleDefinition(
            body={"function": "is_in_range", "arguments": {}},
            parameters=[
                RuleParameter(name="max_limit", type="number", value=100),
                RuleParameter(name="min_limit", type="number", value=0),
            ],
        )
        rule_a = _native_rule(definition=base)
        rule_b = _native_rule(definition=reordered)
        assert compute_registry_rule_fingerprint(rule_a) == compute_registry_rule_fingerprint(rule_b)

    def test_position_field_does_not_affect_fingerprint(self):
        """Reordering display position without changing bindings is a no-op."""
        definition_pos_0_1 = RuleDefinition(
            body={"function": "is_not_null", "arguments": {}},
            slots=[RuleSlot(name="column", family="any", position=0)],
        )
        definition_pos_5 = RuleDefinition(
            body={"function": "is_not_null", "arguments": {}},
            slots=[RuleSlot(name="column", family="any", position=5)],
        )
        rule_a = _native_rule(definition=definition_pos_0_1)
        rule_b = _native_rule(definition=definition_pos_5)
        assert compute_registry_rule_fingerprint(rule_a) == compute_registry_rule_fingerprint(rule_b)


class TestSensitivity:
    def test_different_body_different_fingerprint(self):
        rule_a = _native_rule()
        rule_b = _native_rule(
            definition=RuleDefinition(
                body={"function": "is_in_range", "arguments": {"column": "{{column}}", "min_limit": 0, "max_limit": 1}},
                slots=[RuleSlot(name="column", family="numeric")],
                parameters=[RuleParameter(name="max_limit", type="number", value=1)],
            )
        )
        assert compute_registry_rule_fingerprint(rule_a) != compute_registry_rule_fingerprint(rule_b)

    def test_different_mode_different_fingerprint(self):
        rule_a = _native_rule(mode="dqx_native")
        rule_b = _native_rule(mode="sql")
        assert compute_registry_rule_fingerprint(rule_a) != compute_registry_rule_fingerprint(rule_b)

    def test_different_polarity_different_fingerprint(self):
        rule_a = _native_rule(mode="sql", polarity="pass")
        rule_b = _native_rule(mode="sql", polarity="fail")
        assert compute_registry_rule_fingerprint(rule_a) != compute_registry_rule_fingerprint(rule_b)

    def test_different_slot_family_different_fingerprint(self):
        rule_a = _native_rule(
            definition=RuleDefinition(
                body={"function": "is_not_null", "arguments": {}},
                slots=[RuleSlot(name="column", family="numeric")],
            )
        )
        rule_b = _native_rule(
            definition=RuleDefinition(
                body={"function": "is_not_null", "arguments": {}},
                slots=[RuleSlot(name="column", family="text")],
            )
        )
        assert compute_registry_rule_fingerprint(rule_a) != compute_registry_rule_fingerprint(rule_b)


class TestIgnoresDescriptiveMetadataAndLifecycle:
    def test_tags_do_not_affect_fingerprint(self):
        rule_a = _native_rule(user_metadata={"name": "Foo", "dimension": "Validity"})
        rule_b = _native_rule(user_metadata={"name": "Bar", "dimension": "Completeness", "team": "x"})
        assert compute_registry_rule_fingerprint(rule_a) == compute_registry_rule_fingerprint(rule_b)

    def test_status_and_steward_do_not_affect_fingerprint(self):
        rule_a = _native_rule(status="draft", steward="alice@x.com")
        rule_b = _native_rule(status="approved", steward="bob@x.com", version=3, is_builtin=True)
        assert compute_registry_rule_fingerprint(rule_a) == compute_registry_rule_fingerprint(rule_b)
