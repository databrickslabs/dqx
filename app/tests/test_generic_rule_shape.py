"""Tests for slot-name-independent rule identity used by contract imports."""

from databricks_labs_dqx_app.backend.registry_models import RuleDefinition
from databricks_labs_dqx_app.backend.services.generic_rule_shape import (
    canonical_slot_names,
    generalize,
    generic_name,
    has_generalized_slots,
    shape_key,
    slot_renames_between,
)


def _native(function: str, slots: list[str], params: list[dict] | None = None) -> RuleDefinition:
    args: dict[str, object] = {}
    if len(slots) == 1:
        args["column"] = "{{" + slots[0] + "}}"
    elif slots:
        args["columns"] = ["{{" + s + "}}" for s in slots]
    return RuleDefinition.model_validate(
        {
            "body": {"function": function, "arguments": args},
            "slots": [{"name": s, "family": "any", "position": i, "cardinality": "one"} for i, s in enumerate(slots)],
            "parameters": params or [],
        }
    )


class TestShapeKey:
    def test_slot_names_do_not_change_the_shape(self):
        a = shape_key("dqx_native", _native("is_not_null", ["customer_id"]), None)
        b = shape_key("dqx_native", _native("is_not_null", ["email"]), None)
        c = shape_key("dqx_native", _native("is_not_null", ["value"]), None)
        assert a == b == c

    def test_function_parameters_and_polarity_change_the_shape(self):
        base = shape_key("dqx_native", _native("is_not_null", ["x"]), None)
        assert base != shape_key("dqx_native", _native("is_unique", ["x"]), None)
        assert base != shape_key("dqx_native", _native("is_not_null", ["x"]), "must_fail")
        limit0 = [{"name": "limit", "type": "number", "value": 0}]
        limit1 = [{"name": "limit", "type": "number", "value": 1}]
        assert shape_key("dqx_native", _native("is_not_less_than", ["x"], limit0), None) != shape_key(
            "dqx_native", _native("is_not_less_than", ["x"], limit1), None
        )

    def test_slot_family_is_ignored(self):
        numeric = _native("is_not_null", ["x"])
        numeric.slots[0].family = "numeric"
        assert shape_key("dqx_native", numeric, None) == shape_key("dqx_native", _native("is_not_null", ["y"]), None)

    def test_slot_order_is_positional(self):
        assert shape_key("dqx_native", _native("is_unique", ["a", "b"]), None) == shape_key(
            "dqx_native", _native("is_unique", ["order_id", "line_id"]), None
        )


class TestGeneralize:
    def test_single_slot_becomes_column(self):
        definition, renames = generalize("dqx_native", _native("is_not_null", ["customer_id"]))
        assert renames == {"customer_id": "column"}
        assert definition.body["arguments"] == {"column": "{{column}}"}
        assert [s.name for s in definition.slots] == ["column"]

    def test_multiple_slots_become_numbered(self):
        definition, renames = generalize("dqx_native", _native("is_unique", ["order_id", "line_id"]))
        assert renames == {"order_id": "column_1", "line_id": "column_2"}
        assert definition.body["arguments"] == {"columns": ["{{column_1}}", "{{column_2}}"]}

    def test_swapped_names_do_not_clobber(self):
        definition, _ = generalize("dqx_native", _native("is_unique", ["column_2", "column_1"]))
        assert definition.body["arguments"] == {"columns": ["{{column_1}}", "{{column_2}}"]}

    def test_generalizing_preserves_the_shape(self):
        original = _native("is_not_null", ["email"])
        definition, _ = generalize("dqx_native", original)
        assert shape_key("dqx_native", definition, None) == shape_key("dqx_native", original, None)

    def test_low_code_is_left_alone(self):
        original = _native("is_not_null", ["email"])
        definition, renames = generalize("low_code", original)
        assert renames == {}
        assert definition is original

    def test_three_or_more_slots_generalize_and_are_recognized(self):
        # Regression: generalize numbers every slot (column_1…column_N), and the
        # recognizer must accept all of them — a 3+ slot rule used to be excluded
        # from cross-import reuse, minting a duplicate per import.
        definition, renames = generalize("sql", _native("custom", ["a", "b", "c"]))
        assert renames == {"a": "column_1", "b": "column_2", "c": "column_3"}
        assert has_generalized_slots(definition)


class TestHasGeneralizedSlots:
    def test_canonical_slot_names_scheme(self):
        assert canonical_slot_names(1) == ["column"]
        assert canonical_slot_names(2) == ["column_1", "column_2"]
        assert canonical_slot_names(3) == ["column_1", "column_2", "column_3"]

    def test_accepts_the_canonical_forms(self):
        assert has_generalized_slots(_native("is_not_null", ["column"]))
        assert has_generalized_slots(_native("is_unique", ["column_1", "column_2"]))
        assert has_generalized_slots(_native("custom", ["column_1", "column_2", "column_3"]))

    def test_rejects_column_specific_or_dropped_alias_names(self):
        assert not has_generalized_slots(_native("is_not_null", ["customer_id"]))
        # Legacy neutral-sounding aliases are no longer special-cased: a slot is
        # neutral only when it is exactly what generalize() would assign.
        assert not has_generalized_slots(_native("is_in_list", ["value"]))
        assert not has_generalized_slots(_native("is_not_null", ["col"]))

    def test_whatever_generalize_emits_is_recognized(self):
        # The core invariant: generalize's output always round-trips to True,
        # for any slot count — producer and recognizer share canonical_slot_names.
        for slots in (["a"], ["a", "b"], ["a", "b", "c"], ["a", "b", "c", "d", "e"]):
            definition, _ = generalize("sql", _native("custom", slots))
            assert has_generalized_slots(definition), slots


class TestNamesAndRenames:
    def test_generic_name(self):
        assert generic_name("dqx_native", _native("is_not_null", ["x"])) == "Is not null"
        limit = [{"name": "limit", "type": "number", "value": 0}]
        assert generic_name("dqx_native", _native("is_not_less_than", ["x"], limit)) == "Is not less than (limit: 0)"
        assert generic_name("sql", _native("is_not_null", ["x"])) is None

    def test_slot_renames_between(self):
        assert slot_renames_between(
            _native("is_unique", ["order_id", "line_id"]), _native("is_unique", ["column_1", "column_2"])
        ) == {"order_id": "column_1", "line_id": "column_2"}
