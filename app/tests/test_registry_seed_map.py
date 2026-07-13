"""Tests for slot family / parameter type derivation (Phase 2A / task 2.2-4).

Exercises ``derive_slots_and_parameters`` and ``resolve_slot_family`` against
real ``CheckFunctionDef`` entries produced by
``routes.v1.check_functions._introspect_check_functions`` (the same
introspection the ``listCheckFunctions`` route serves), so this is a
representative end-to-end check of the seed-map refinement over DQX's own
check-function signatures — not just hand-rolled fixtures.
"""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.models import CheckFunctionDef, CheckFunctionParam
from databricks_labs_dqx_app.backend.registry_seed_map import (
    derive_slots_and_parameters,
    resolve_slot_family,
)
from databricks_labs_dqx_app.backend.routes.v1.check_functions import _introspect_check_functions


def _real_check_function(name: str) -> CheckFunctionDef:
    for fn in _introspect_check_functions():
        if fn.name == name:
            return fn
    raise AssertionError(f"{name!r} not found in _introspect_check_functions()")


# ---------------------------------------------------------------------------
# resolve_slot_family — seed-map refinement
# ---------------------------------------------------------------------------


class TestResolveSlotFamily:
    def test_is_valid_email_is_text(self):
        assert resolve_slot_family("is_valid_email") == "text"

    def test_is_in_range_is_any(self):
        # `is_in_range`'s bounds can be numeric OR temporal (date/datetime),
        # so its column slot genuinely spans both families — restricting to
        # "numeric" would wrongly exclude date-range checks at apply time.
        # See `routes.v1.check_functions._COLUMN_FAMILIES`.
        assert resolve_slot_family("is_in_range") == "any"

    def test_is_valid_date_is_text(self):
        # `is_valid_date` parses a STRING column against a format
        # (`F.try_to_timestamp`) — the column under validation is text, not
        # an already-typed date/timestamp (which would validate trivially).
        # See `routes.v1.check_functions._COLUMN_FAMILIES`.
        assert resolve_slot_family("is_valid_date") == "text"

    def test_is_not_null_defaults_to_any(self):
        assert resolve_slot_family("is_not_null") == "any"

    def test_unknown_function_defaults_to_any(self):
        assert resolve_slot_family("some_future_check_nobody_has_seeded_yet") == "any"


# ---------------------------------------------------------------------------
# derive_slots_and_parameters — representative real check functions
# ---------------------------------------------------------------------------


class TestDeriveSlotsAndParametersOnRealFunctions:
    def test_is_not_null_single_any_slot_no_parameters(self):
        slots, parameters = derive_slots_and_parameters(_real_check_function("is_not_null"))
        assert [s.name for s in slots] == ["column"]
        assert slots[0].family == "any"
        assert slots[0].cardinality == "one"
        assert slots[0].position == 0
        assert parameters == []

    def test_is_valid_email_slot_is_text(self):
        slots, _parameters = derive_slots_and_parameters(_real_check_function("is_valid_email"))
        assert [s.name for s in slots] == ["column"]
        assert slots[0].family == "text"

    def test_is_in_range_any_slot_plus_number_parameters(self):
        slots, parameters = derive_slots_and_parameters(_real_check_function("is_in_range"))
        assert [s.name for s in slots] == ["column"]
        # Spans numeric and temporal bounds — stays unconstrained ("any").
        assert slots[0].family == "any"
        param_by_name = {p.name: p for p in parameters}
        assert param_by_name["min_limit"].type == "number"
        assert param_by_name["max_limit"].type == "number"

    def test_is_unique_many_cardinality_columns_slot(self):
        slots, parameters = derive_slots_and_parameters(_real_check_function("is_unique"))
        assert [s.name for s in slots] == ["columns"]
        assert slots[0].cardinality == "many"
        assert slots[0].family == "any"
        param_by_name = {p.name: p for p in parameters}
        assert param_by_name["nulls_distinct"].type == "boolean"

    def test_is_in_list_list_parameter_and_boolean_parameter(self):
        slots, parameters = derive_slots_and_parameters(_real_check_function("is_in_list"))
        assert [s.name for s in slots] == ["column"]
        param_by_name = {p.name: p for p in parameters}
        assert param_by_name["allowed"].type == "list"
        assert param_by_name["case_sensitive"].type == "boolean"

    def test_foreign_key_ref_table_and_ref_column_parameters(self):
        slots, parameters = derive_slots_and_parameters(_real_check_function("foreign_key"))
        # Both `columns` (target table) and `ref_columns` (reference table)
        # are column-*kind* per _classify_param_kind's dedicated "columns"
        # rule, but only the un-prefixed "columns" param refers to the
        # table under test; ref_columns is a reference-table identity and
        # is classified as its own "ref_columns" kind, not "columns" —
        # confirm it lands as a ref_column PARAMETER, not a slot.
        assert [s.name for s in slots] == ["columns"]
        assert slots[0].cardinality == "many"
        param_by_name = {p.name: p for p in parameters}
        assert param_by_name["ref_table"].type == "ref_table"
        assert param_by_name["ref_columns"].type == "ref_column"
        assert param_by_name["negate"].type == "boolean"
        assert param_by_name["null_safe"].type == "boolean"
        # Engine-injected / stateless-app-incompatible params are dropped
        # upstream by ``_HIDDEN_PARAMS`` before we ever see them.
        assert "row_filter" not in param_by_name
        assert "ref_df_name" not in param_by_name


# ---------------------------------------------------------------------------
# derive_slots_and_parameters — synthetic fixtures for edge shapes
# ---------------------------------------------------------------------------


class TestDeriveSlotsAndParametersSynthetic:
    def test_unknown_kind_falls_back_to_string_parameter(self):
        check_function = CheckFunctionDef(
            name="hypothetical_check",
            rule_type="row",
            category="Other",
            doc="",
            params=[CheckFunctionParam(name="weird", kind="some_future_kind", required=False)],
        )
        _slots, parameters = derive_slots_and_parameters(check_function)
        assert parameters[0].type == "string"

    def test_preserves_parameter_order(self):
        check_function = CheckFunctionDef(
            name="hypothetical_check",
            rule_type="row",
            category="Other",
            doc="",
            params=[
                CheckFunctionParam(name="column", kind="column", required=True),
                CheckFunctionParam(name="a", kind="number", required=False),
                CheckFunctionParam(name="b", kind="string", required=False),
            ],
        )
        slots, parameters = derive_slots_and_parameters(check_function)
        assert [s.name for s in slots] == ["column"]
        assert [p.name for p in parameters] == ["a", "b"]

    def test_no_column_params_yields_no_slots(self):
        check_function = CheckFunctionDef(
            name="hypothetical_check",
            rule_type="dataset",
            category="Other",
            doc="",
            params=[CheckFunctionParam(name="sql_query", kind="string", required=True)],
        )
        slots, parameters = derive_slots_and_parameters(check_function)
        assert slots == []
        assert len(parameters) == 1


@pytest.mark.parametrize(
    "name",
    ["is_not_null", "is_valid_email", "is_in_range", "is_unique", "foreign_key", "is_in_list"],
)
def test_slot_positions_are_dense_and_zero_based(name: str) -> None:
    slots, _parameters = derive_slots_and_parameters(_real_check_function(name))
    assert [s.position for s in slots] == list(range(len(slots)))
