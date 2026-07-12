"""Tests for ``profiling_rule_builder`` — profiler check -> registry-rule candidate.

Pure logic over the real DQX check-function introspection (no Databricks, no
workspace): a profiler-generated check in DQX metadata form is turned into a
table-agnostic registry-rule template + slot->column mapping, or skipped.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.profiling_rule_builder import build_profiling_rule


def _check(function: str, arguments: dict, *, name: str = "generated", criticality: str = "warn") -> dict:
    return {"name": name, "criticality": criticality, "check": {"function": function, "arguments": arguments}}


class TestBuildProfilingRule:
    def test_maps_simple_column_check(self):
        candidate = build_profiling_rule(_check("is_not_null", {"column": "amount"}))

        assert candidate is not None
        assert candidate.function == "is_not_null"
        assert candidate.mapping == {"column": "amount"}
        # Column arg becomes a {{slot}} placeholder in the table-agnostic body.
        assert candidate.definition.body == {"function": "is_not_null", "arguments": {"column": "{{column}}"}}
        assert [s.name for s in candidate.definition.slots] == ["column"]
        assert candidate.definition.parameters == []
        # Reserved tags are seeded from the built-in metadata mapping.
        assert candidate.metadata.get("name")
        assert candidate.metadata.get("dimension") == "Completeness"

    def test_freezes_non_column_parameter_values(self):
        candidate = build_profiling_rule(_check("is_in_range", {"column": "score", "min_limit": 0, "max_limit": 100}))

        assert candidate is not None
        assert candidate.mapping == {"column": "score"}
        frozen = {p.name: p.value for p in candidate.definition.parameters}
        assert frozen == {"min_limit": 0, "max_limit": 100}
        # The column slot is still a placeholder; frozen params are NOT in the body.
        assert candidate.definition.body["arguments"] == {"column": "{{column}}"}

    def test_parameter_free_check_matches_builtin_shape(self):
        # No frozen params -> the definition is identical to what the built-in
        # seeder produces, so such suggestions dedupe onto a seeded built-in.
        from databricks_labs_dqx_app.backend.builtin_rules_seed import build_builtin_definition
        from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule
        from databricks_labs_dqx_app.backend.routes.v1.check_functions import _introspect_check_functions

        candidate = build_profiling_rule(_check("is_not_null", {"column": "amount"}))
        cfd = next(f for f in _introspect_check_functions() if f.name == "is_not_null")
        builtin_def = build_builtin_definition(cfd)

        def _fp(definition):
            return compute_registry_rule_fingerprint(
                RegistryRule(rule_id="x", mode="dqx_native", status="draft", version=0, definition=definition)
            )

        assert _fp(candidate.definition) == _fp(builtin_def)

    def test_differs_from_builtin_when_params_frozen(self):
        from databricks_labs_dqx_app.backend.builtin_rules_seed import build_builtin_definition
        from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule
        from databricks_labs_dqx_app.backend.routes.v1.check_functions import _introspect_check_functions

        candidate = build_profiling_rule(_check("is_in_range", {"column": "score", "min_limit": 0, "max_limit": 100}))
        cfd = next(f for f in _introspect_check_functions() if f.name == "is_in_range")
        builtin_def = build_builtin_definition(cfd)

        def _fp(definition):
            return compute_registry_rule_fingerprint(
                RegistryRule(rule_id="x", mode="dqx_native", status="draft", version=0, definition=definition)
            )

        # Frozen concrete bounds make this a distinct rule from the generic template.
        assert _fp(candidate.definition) != _fp(builtin_def)

    def test_identical_profiler_checks_fingerprint_identically(self):
        from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        a = build_profiling_rule(_check("is_in_range", {"column": "score", "min_limit": 0, "max_limit": 100}))
        b = build_profiling_rule(_check("is_in_range", {"column": "other", "min_limit": 0, "max_limit": 100}))

        def _fp(definition):
            return compute_registry_rule_fingerprint(
                RegistryRule(rule_id="x", mode="dqx_native", status="draft", version=0, definition=definition)
            )

        # Same function + same frozen params -> same fingerprint even though the
        # bound column differs (the column is a slot, not part of the template).
        assert _fp(a.definition) == _fp(b.definition)

    def test_accepts_bare_inner_check_shape(self):
        candidate = build_profiling_rule({"function": "is_not_null", "arguments": {"column": "amount"}})
        assert candidate is not None
        assert candidate.mapping == {"column": "amount"}

    def test_unknown_function_is_skipped(self):
        assert build_profiling_rule(_check("not_a_real_check_function", {"column": "x"})) is None

    def test_missing_column_argument_is_skipped(self):
        assert build_profiling_rule(_check("is_not_null", {})) is None

    def test_non_string_column_argument_is_skipped(self):
        assert build_profiling_rule(_check("is_not_null", {"column": 123})) is None

    def test_malformed_check_is_skipped(self):
        assert build_profiling_rule({"check": {"arguments": {"column": "x"}}}) is None
        assert build_profiling_rule({"check": "nonsense"}) is None

    def test_unsafe_sql_query_is_skipped(self):
        # A SQL-bearing check whose query fails the safety scan is dropped, never
        # turned into a persisted rule.
        assert build_profiling_rule(_check("sql_query", {"query": "DROP TABLE users", "input_column": "x"})) is None
