"""Unit tests for ``backend.routes.v1.check_functions``.

The route is a thin wrapper over a pure-Python introspection function,
so the bulk of the suite exercises the helpers directly:

1. ``_classify_param_kind`` — every UI-input kind on its own, plus the
   tricky union types (``int | float | Decimal | str | Column | None``)
   and the multi-column ``columns: list[str | Column]`` case.
2. ``_introspect_check_functions`` — checks that we surface a known
   subset of DQX checks (``is_not_null``, ``is_in_range``, ``is_unique``,
   ``sql_expression``), categorize them correctly, hide cross-table
   ones (``foreign_key``, ``compare_datasets``, ``sql_query``), and
   render parameter defaults as the UI expects (``True`` → ``"true"``).

We also smoke-test the FastAPI endpoint via ``TestClient`` to make sure
the route wiring + Pydantic serialization round-trip is intact.
"""

from __future__ import annotations

import inspect
from decimal import Decimal

import pytest

from databricks_labs_dqx_app.backend.models import (
    CheckFunctionDef,
    CheckFunctionsOut,
)
from databricks_labs_dqx_app.backend.routes.v1.check_functions import (
    _HIDDEN_FROM_SINGLE_TABLE,
    _category_for,
    _classify_param_kind,
    _first_doc_line,
    _introspect_check_functions,
    _serialize_default,
)


# ---------------------------------------------------------------------------
# _classify_param_kind — collapse Python type hints to UI input kinds
# ---------------------------------------------------------------------------


class TestClassifyParamKind:
    """Every ``ParamKind`` should be reachable from a representative annotation."""

    def test_first_column_param_yields_column(self) -> None:
        from pyspark.sql import Column

        kind = _classify_param_kind("column", str | Column)
        assert kind == "column"

    def test_column_one_alias_also_yields_column(self) -> None:
        """``is_older_than_col2_for_n_days`` uses ``column1`` / ``column2`` —
        both should still render as a column picker, not free-form text."""
        from pyspark.sql import Column

        assert _classify_param_kind("column1", str | Column) == "column"
        assert _classify_param_kind("column2", str | Column) == "column"

    def test_columns_list_yields_columns(self) -> None:
        """``is_unique(columns: list[str | Column])`` is the multi-column case."""
        from pyspark.sql import Column

        kind = _classify_param_kind("columns", list[str | Column])
        assert kind == "columns"

    def test_bool_yields_boolean(self) -> None:
        assert _classify_param_kind("trim_strings", bool) == "boolean"

    def test_optional_bool_still_yields_boolean(self) -> None:
        assert _classify_param_kind("nulls_distinct", bool | None) == "boolean"

    def test_plain_int_yields_number(self) -> None:
        assert _classify_param_kind("days", int) == "number"

    def test_float_yields_number(self) -> None:
        assert _classify_param_kind("limit", float) == "number"

    def test_messy_numeric_union_yields_number(self) -> None:
        """The classic ``is_in_range`` annotation: numbers + dates + columns + None.
        We pick the most user-meaningful widget (``number``)."""
        from datetime import date, datetime

        from pyspark.sql import Column

        annotation = int | float | Decimal | date | datetime | str | Column | None
        # ``str`` is in the union but the input is fundamentally numeric;
        # ``Column`` is only there for the rare expression case. Picking
        # ``number`` is the right UX call.
        assert _classify_param_kind("min_limit", annotation) == "number"

    def test_plain_str_yields_string(self) -> None:
        assert _classify_param_kind("regex", str) == "string"

    def test_optional_str_yields_string(self) -> None:
        assert _classify_param_kind("date_format", str | None) == "string"

    def test_list_without_column_yields_list(self) -> None:
        """``is_in_list(allowed: list)`` — generic list of values, not columns."""
        assert _classify_param_kind("allowed", list) == "list"

    def test_unannotated_param_falls_back_to_string(self) -> None:
        assert _classify_param_kind("opaque", inspect.Parameter.empty) == "string"


# ---------------------------------------------------------------------------
# _serialize_default — UI gets stable string-like defaults
# ---------------------------------------------------------------------------


class TestSerializeDefault:
    def test_empty_means_required(self) -> None:
        assert _serialize_default(inspect.Parameter.empty) is None

    def test_none_means_no_default(self) -> None:
        assert _serialize_default(None) is None

    def test_true_renders_as_lowercase_string(self) -> None:
        """The UI's ``boolean`` widget compares against ``"true"`` /
        ``"false"`` (string), not Python booleans."""
        assert _serialize_default(True) == "true"
        assert _serialize_default(False) == "false"

    def test_int_renders_as_decimal_string(self) -> None:
        assert _serialize_default(0) == "0"
        assert _serialize_default(42) == "42"

    def test_string_default_passes_through(self) -> None:
        assert _serialize_default("yyyy-MM-dd") == "yyyy-MM-dd"


# ---------------------------------------------------------------------------
# _category_for — UX bucket assignment is deterministic
# ---------------------------------------------------------------------------


class TestCategoryFor:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("is_not_null", "Null & Empty"),
            ("is_null", "Null & Empty"),
            ("is_in_list", "Allowed Values"),
            ("is_in_range", "Numeric & Comparable"),
            ("is_not_less_than", "Numeric & Comparable"),
            ("regex_match", "Patterns & Regex"),
            ("is_valid_ipv4_address", "IP Addresses"),
            ("is_valid_json", "JSON"),
            ("is_aggr_not_greater_than", "Aggregates"),
            ("has_no_outliers", "Outliers"),
            ("is_unique", "Uniqueness"),
            ("has_valid_schema", "Schema"),
            ("sql_expression", "Custom SQL"),
            ("is_valid_date", "Dates & Times"),
            ("is_data_fresh", "Dates & Times"),
            ("is_older_than_col2_for_n_days", "Dates & Times"),
        ],
    )
    def test_known_function_lands_in_expected_bucket(self, name: str, expected: str) -> None:
        assert _category_for(name) == expected

    def test_geo_function_lands_in_geospatial_bucket(self) -> None:
        """Geo checks share a single bucket via the prefix-based fallback."""
        assert _category_for("is_geometry") == "Geospatial"
        assert _category_for("is_polygon") == "Geospatial"
        assert _category_for("is_latitude") == "Geospatial"
        assert _category_for("has_x_coordinate_between") == "Geospatial"

    def test_unknown_function_falls_back_to_other(self) -> None:
        """A future DQX check we haven't categorized yet shouldn't break the UI."""
        assert _category_for("some_brand_new_check_we_havent_seen") == "Other"


# ---------------------------------------------------------------------------
# _first_doc_line — picks the first non-blank line, no surprises
# ---------------------------------------------------------------------------


class TestFirstDocLine:
    def test_picks_first_non_blank_line(self) -> None:
        doc = """
        Checks whether the values in the input column are not null.

        Args:
            column: the column to check
        """
        # Note: ``inspect.getdoc`` would normally dedent this for us; the
        # helper still works on raw indentation because it strips per-line.
        assert _first_doc_line(doc) == "Checks whether the values in the input column are not null."

    def test_empty_doc_returns_empty_string(self) -> None:
        assert _first_doc_line(None) == ""
        assert _first_doc_line("") == ""
        assert _first_doc_line("   \n  \n   ") == ""


# ---------------------------------------------------------------------------
# _introspect_check_functions — end-to-end wiring of the registry
# ---------------------------------------------------------------------------


class TestIntrospectCheckFunctions:
    """The introspection function is the load-bearing piece of the route."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self) -> None:
        # The function uses ``lru_cache``; clear it between tests so we
        # always see the live registry state.
        _introspect_check_functions.cache_clear()

    def _by_name(self, name: str) -> CheckFunctionDef:
        for fn in _introspect_check_functions():
            if fn.name == name:
                return fn
        pytest.fail(f"Check function {name!r} not found in introspected registry")

    def test_returns_well_known_row_level_checks(self) -> None:
        """A representative slice of the row-level checks should be present."""
        names = {fn.name for fn in _introspect_check_functions()}
        assert {
            "is_not_null",
            "is_not_empty",
            "is_in_list",
            "is_in_range",
            "is_not_less_than",
            "is_not_greater_than",
            "is_valid_date",
            "regex_match",
            "sql_expression",
        }.issubset(names)

    def test_returns_well_known_dataset_level_checks(self) -> None:
        """``is_unique`` and aggregate/outlier checks belong in the editor."""
        names = {fn.name for fn in _introspect_check_functions()}
        assert "is_unique" in names
        assert "has_no_outliers" in names
        assert "is_aggr_not_greater_than" in names

    def test_hides_cross_table_dataset_checks(self) -> None:
        """``foreign_key``/``compare_datasets``/``sql_query`` are
        intentionally excluded (handled in the cross-table editor)."""
        names = {fn.name for fn in _introspect_check_functions()}
        for forbidden in _HIDDEN_FROM_SINGLE_TABLE:
            assert forbidden not in names, f"{forbidden} must not appear in single-table function list"

    def test_is_not_null_has_single_required_column_param(self) -> None:
        fn = self._by_name("is_not_null")
        assert fn.rule_type == "row"
        assert fn.category == "Null & Empty"
        assert len(fn.params) == 1
        param = fn.params[0]
        assert param.name == "column"
        assert param.kind == "column"
        assert param.required is True
        assert param.default is None

    def test_is_in_range_marks_both_limits_optional(self) -> None:
        """Both ``min_limit`` and ``max_limit`` default to ``None`` — the UI
        relies on ``required=False`` to render them as optional fields and
        layers a cross-arg validator on top to demand at least one is set."""
        fn = self._by_name("is_in_range")
        params_by_name = {p.name: p for p in fn.params}
        assert params_by_name["min_limit"].required is False
        assert params_by_name["min_limit"].default is None
        assert params_by_name["min_limit"].kind == "number"
        assert params_by_name["max_limit"].required is False
        assert params_by_name["max_limit"].kind == "number"

    def test_is_unique_uses_columns_kind_for_multi_column_input(self) -> None:
        fn = self._by_name("is_unique")
        assert fn.rule_type == "dataset"
        assert fn.category == "Uniqueness"
        params_by_name = {p.name: p for p in fn.params}
        assert "columns" in params_by_name
        assert params_by_name["columns"].kind == "columns"
        assert params_by_name["columns"].required is True
        # ``nulls_distinct`` defaults to ``True``; it should be a boolean
        # widget with the string ``"true"`` as its default.
        assert params_by_name["nulls_distinct"].kind == "boolean"
        assert params_by_name["nulls_distinct"].default == "true"

    def test_internal_row_filter_param_is_hidden(self) -> None:
        """``row_filter`` is auto-injected by the engine — surfacing it in
        the UI would just confuse rule authors."""
        fn = self._by_name("is_unique")
        assert all(p.name != "row_filter" for p in fn.params)

    def test_sql_expression_has_string_expression_param(self) -> None:
        fn = self._by_name("sql_expression")
        params_by_name = {p.name: p for p in fn.params}
        assert params_by_name["expression"].kind == "string"
        assert params_by_name["expression"].required is True

    def test_is_in_list_allowed_param_is_a_list(self) -> None:
        fn = self._by_name("is_in_list")
        params_by_name = {p.name: p for p in fn.params}
        assert params_by_name["allowed"].kind == "list"
        assert params_by_name["allowed"].required is True
        # ``case_sensitive`` defaults to ``True`` — that drives the UI's
        # checkbox initial state.
        assert params_by_name["case_sensitive"].default == "true"

    def test_regex_match_replaces_ui_only_alias(self) -> None:
        """The previous static UI list shipped a fake ``is_valid_regex``;
        the canonical DQX name is ``regex_match`` and that's what the
        registry surfaces."""
        names = {fn.name for fn in _introspect_check_functions()}
        assert "regex_match" in names
        assert "is_valid_regex" not in names

    def test_results_are_sorted_by_category_then_name(self) -> None:
        results = list(_introspect_check_functions())
        sorted_results = sorted(results, key=lambda f: (f.category, f.name))
        assert [f.name for f in results] == [f.name for f in sorted_results]

    def test_response_is_cached(self) -> None:
        """The introspection result shouldn't change shape across calls."""
        first = _introspect_check_functions()
        second = _introspect_check_functions()
        # Same tuple object thanks to lru_cache.
        assert first is second


# ---------------------------------------------------------------------------
# CheckFunctionsOut — Pydantic round-trip
# ---------------------------------------------------------------------------


class TestCheckFunctionsOutShape:
    def test_default_functions_is_empty_list(self) -> None:
        out = CheckFunctionsOut()
        assert out.functions == []

    def test_round_trips_via_json(self) -> None:
        defs = list(_introspect_check_functions())
        out = CheckFunctionsOut(functions=defs)
        as_json = out.model_dump_json()
        rebuilt = CheckFunctionsOut.model_validate_json(as_json)
        assert len(rebuilt.functions) == len(defs)
        assert {f.name for f in rebuilt.functions} == {f.name for f in defs}


# ---------------------------------------------------------------------------
# Route wiring — TestClient smoke test
# ---------------------------------------------------------------------------


class TestRouteWiring:
    """A focused end-to-end check that the FastAPI router is mounted at the
    right prefix, returns 200, and serializes ``CheckFunctionsOut`` faithfully.

    The full ``backend.app`` lifespan touches a Databricks workspace and is
    too heavy for a unit test — we instead stand up a minimal app with just
    the router under test, which is enough to validate every layer (routing,
    Pydantic encoding, response model).
    """

    @pytest.fixture
    def client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from databricks_labs_dqx_app.backend.routes.v1.check_functions import router

        # Clear cache so this fixture sees the live registry rather than a
        # stale tuple from a sibling test.
        _introspect_check_functions.cache_clear()

        app = FastAPI()
        app.include_router(router, prefix="/api/v1/check-functions")
        return TestClient(app)

    def test_returns_200_with_well_known_functions(self, client) -> None:
        resp = client.get("/api/v1/check-functions")
        assert resp.status_code == 200
        body = resp.json()
        assert "functions" in body
        names = {f["name"] for f in body["functions"]}
        # A tight subset that proves the registry was actually walked.
        assert {"is_not_null", "is_in_range", "is_unique", "sql_expression"}.issubset(names)

    def test_omits_cross_table_dataset_checks(self, client) -> None:
        resp = client.get("/api/v1/check-functions")
        names = {f["name"] for f in resp.json()["functions"]}
        assert "foreign_key" not in names
        assert "compare_datasets" not in names
        assert "sql_query" not in names

    def test_response_includes_param_kind_and_required_flags(self, client) -> None:
        """The UI relies on ``kind`` to pick the right input widget and on
        ``required`` to drive the "missing arg" validation message."""
        resp = client.get("/api/v1/check-functions")
        is_in_range = next(f for f in resp.json()["functions"] if f["name"] == "is_in_range")
        params = {p["name"]: p for p in is_in_range["params"]}
        assert params["column"]["kind"] == "column"
        assert params["column"]["required"] is True
        assert params["min_limit"]["kind"] == "number"
        assert params["min_limit"]["required"] is False
