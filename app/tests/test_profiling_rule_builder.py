"""Tests for ``profiling_rule_builder`` — profiler check -> registry-rule candidate.

Pure logic over the real DQX check-function introspection (no Databricks, no
workspace): a profiler-generated check in DQX metadata form is turned into a
table-agnostic registry-rule template + slot->column mapping, or skipped.
"""

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

    # --- Column-qualified name tests ---

    def test_name_contains_humanized_function_and_column(self):
        # The rule name must include both the humanized function label and the column.
        candidate = build_profiling_rule(_check("is_not_null", {"column": "country"}))
        assert candidate is not None
        name = candidate.metadata.get("name", "")
        assert "Is not null" in name
        assert "country" in name

    def test_same_function_different_columns_produce_different_names(self):
        # The primary purpose of column-qualified names: two suggestions for the
        # same function on different columns must be distinguishable.
        a = build_profiling_rule(_check("is_in_list", {"column": "payment_method", "allowed": ["cash", "card"]}))
        b = build_profiling_rule(_check("is_in_list", {"column": "status", "allowed": ["active"]}))
        assert a is not None and b is not None
        assert a.metadata.get("name") != b.metadata.get("name")
        assert "payment_method" in a.metadata.get("name", "")
        assert "status" in b.metadata.get("name", "")

    def test_name_with_params_still_includes_column(self):
        # Frozen parameters don't affect the name — column is still the primary token.
        candidate = build_profiling_rule(
            _check("is_in_range", {"column": "order_amount", "min_limit": 0.0, "max_limit": 1000.0})
        )
        assert candidate is not None
        name = candidate.metadata.get("name", "")
        assert "Is in range" in name
        assert "order_amount" in name

    def test_name_format_uses_colon_separator(self):
        # Format must be "Humanized label: column" so display is clean and scannable.
        candidate = build_profiling_rule(_check("is_not_null", {"column": "user_id"}))
        assert candidate is not None
        assert candidate.metadata.get("name") == "Is not null: user_id"

    def test_name_does_not_affect_fingerprint(self):
        # The rule name (a reserved metadata tag) must NOT be part of the structural
        # fingerprint — two candidates for the same function + same params on different
        # columns have different names but identical fingerprints (the column is a slot).
        from databricks_labs_dqx_app.backend.registry_fingerprint import compute_registry_rule_fingerprint
        from databricks_labs_dqx_app.backend.registry_models import RegistryRule

        a = build_profiling_rule(_check("is_not_null", {"column": "col_a"}))
        b = build_profiling_rule(_check("is_not_null", {"column": "col_b"}))
        assert a is not None and b is not None
        # Names must differ (column-qualified).
        assert a.metadata.get("name") != b.metadata.get("name")

        def _fp(candidate):
            return compute_registry_rule_fingerprint(
                RegistryRule(rule_id="x", mode="dqx_native", status="draft", version=0, definition=candidate.definition)
            )

        # But fingerprints must be equal — name is display-only, not fingerprinted.
        assert _fp(a) == _fp(b)
