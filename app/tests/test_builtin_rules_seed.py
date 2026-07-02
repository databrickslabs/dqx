"""Tests for the Phase 2C built-in rules seeder (plan §PHASE 2 bullet 2.4).

Exercises the pure definition/metadata builders against the real
``_introspect_check_functions()`` output (same convention as
``test_registry_seed_map.py``) plus the seeding orchestration against a
``create_autospec(RegistryService)`` mock, so we assert both "every
introspected function gets tagged/sloted correctly" and "seeding is
idempotent / never clobbers an admin edit" without a live SQL backend.
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.builtin_rules_seed import (
    build_builtin_definition,
    build_builtin_metadata,
    humanize_function_name,
    resolve_dimension,
    resolve_severity,
    seed_builtin_rules_if_absent,
)
from databricks_labs_dqx_app.backend.registry_models import (
    RESERVED_DESCRIPTION_KEY,
    RESERVED_DIMENSION_KEY,
    RESERVED_NAME_KEY,
    RESERVED_SEVERITY_KEY,
    RegistryRule,
    get_rule_description,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
)
from databricks_labs_dqx_app.backend.routes.v1.check_functions import _introspect_check_functions
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


def _real_check_function(name: str):
    for fn in _introspect_check_functions():
        if fn.name == name:
            return fn
    raise AssertionError(f"{name!r} not found in _introspect_check_functions()")


# ---------------------------------------------------------------------------
# humanize_function_name
# ---------------------------------------------------------------------------


class TestHumanizeFunctionName:
    def test_is_not_null(self):
        assert humanize_function_name("is_not_null") == "Is not null"

    def test_single_word(self):
        assert humanize_function_name("is_unique") == "Is unique"

    def test_empty_string(self):
        assert humanize_function_name("") == ""


# ---------------------------------------------------------------------------
# resolve_dimension
# ---------------------------------------------------------------------------


class TestResolveDimension:
    def test_completeness_family(self):
        assert resolve_dimension("is_not_null") == "Completeness"
        assert resolve_dimension("is_not_null_and_not_empty") == "Completeness"

    def test_uniqueness(self):
        assert resolve_dimension("is_unique") == "Uniqueness"

    def test_consistency(self):
        assert resolve_dimension("foreign_key") == "Consistency"

    def test_timeliness(self):
        assert resolve_dimension("is_data_fresh") == "Timeliness"
        assert resolve_dimension("is_not_in_future") == "Timeliness"

    def test_validity_examples(self):
        assert resolve_dimension("regex_match") == "Validity"
        assert resolve_dimension("is_valid_email") == "Validity"
        assert resolve_dimension("is_in_range") == "Validity"

    def test_default_fallback_is_validity(self):
        assert resolve_dimension("some_totally_unknown_future_check") == "Validity"


# ---------------------------------------------------------------------------
# resolve_severity
# ---------------------------------------------------------------------------


class TestResolveSeverity:
    def test_high_for_integrity_and_uniqueness_checks(self):
        assert resolve_severity("is_unique") == "High"
        assert resolve_severity("foreign_key") == "High"
        assert resolve_severity("has_valid_schema") == "High"

    def test_medium_default_for_completeness_and_validity_checks(self):
        assert resolve_severity("is_not_null") == "Medium"
        assert resolve_severity("regex_match") == "Medium"
        assert resolve_severity("is_data_fresh") == "Medium"

    def test_low_for_geo_geometry_shape_checks(self):
        assert resolve_severity("is_point") == "Low"
        assert resolve_severity("is_polygon") == "Low"

    def test_unmapped_function_defaults_to_medium(self):
        assert resolve_severity("some_totally_unknown_future_check") == "Medium"


# ---------------------------------------------------------------------------
# build_builtin_definition / build_builtin_metadata — real introspection
# ---------------------------------------------------------------------------


class TestBuildBuiltinDefinition:
    def test_is_not_null_has_column_slot_placeholder(self):
        fn = _real_check_function("is_not_null")
        definition = build_builtin_definition(fn)
        assert definition.body["function"] == "is_not_null"
        assert definition.body["arguments"]["column"] == "{{column}}"
        assert [s.name for s in definition.slots] == ["column"]

    def test_is_in_range_leaves_non_column_params_unfrozen(self):
        fn = _real_check_function("is_in_range")
        definition = build_builtin_definition(fn)
        param_names = {p.name for p in definition.parameters}
        assert param_names  # is_in_range has min_limit/max_limit-style params
        # Non-column arguments must NOT be baked into the frozen body —
        # they're left for apply time.
        for name in param_names:
            assert name not in definition.body["arguments"]

    def test_is_unique_many_cardinality_slot(self):
        fn = _real_check_function("is_unique")
        definition = build_builtin_definition(fn)
        assert any(s.cardinality == "many" for s in definition.slots)


class TestBuildBuiltinMetadata:
    def test_is_not_null_tags(self):
        fn = _real_check_function("is_not_null")
        metadata = build_builtin_metadata(fn)
        assert get_rule_name(metadata) == "Is not null"
        assert get_rule_dimension(metadata) == "Completeness"
        assert get_rule_severity(metadata) == "Medium"
        assert get_rule_description(metadata)  # first docstring line, non-empty

    def test_is_unique_high_severity_tag(self):
        fn = _real_check_function("is_unique")
        metadata = build_builtin_metadata(fn)
        assert get_rule_severity(metadata) == "High"

    def test_reserved_keys_only(self):
        fn = _real_check_function("is_unique")
        metadata = build_builtin_metadata(fn)
        assert set(metadata) <= {
            RESERVED_NAME_KEY,
            RESERVED_DESCRIPTION_KEY,
            RESERVED_DIMENSION_KEY,
            RESERVED_SEVERITY_KEY,
        }


# ---------------------------------------------------------------------------
# seed_builtin_rules_if_absent — orchestration (mocked RegistryService)
# ---------------------------------------------------------------------------


@pytest.fixture
def registry_mock():
    return create_autospec(RegistryService, instance=True)


class TestSeedBuiltinRulesIfAbsent:
    def test_seeds_one_rule_per_introspected_function_when_registry_empty(self, registry_mock):
        registry_mock.get_rule_by_fingerprint.return_value = None
        expected = len(list(_introspect_check_functions()))
        created = seed_builtin_rules_if_absent(registry_mock)
        assert created == expected
        assert registry_mock.seed_builtin_rule.call_count == expected

    def test_idempotent_when_all_already_seeded(self, registry_mock):
        existing = RegistryRule(
            rule_id="existing",
            mode="dqx_native",
            status="approved",
            version=1,
            definition=build_builtin_definition(_real_check_function("is_not_null")),
        )
        registry_mock.get_rule_by_fingerprint.return_value = existing
        created = seed_builtin_rules_if_absent(registry_mock)
        assert created == 0
        registry_mock.seed_builtin_rule.assert_not_called()

    def test_does_not_clobber_admin_edited_builtin_rule(self, registry_mock):
        # Simulate an admin having retagged a built-in rule (different name
        # tag) — its definition/fingerprint is unchanged, so seeding must
        # recognize it as already-seeded and never touch it.
        admin_edited = RegistryRule(
            rule_id="existing",
            mode="dqx_native",
            status="approved",
            version=1,
            definition=build_builtin_definition(_real_check_function("is_not_null")),
            user_metadata={"name": "Custom renamed rule", "dimension": "Accuracy"},
        )
        registry_mock.get_rule_by_fingerprint.return_value = admin_edited
        seed_builtin_rules_if_absent(registry_mock)
        registry_mock.seed_builtin_rule.assert_not_called()
        registry_mock.update_draft.assert_not_called()
