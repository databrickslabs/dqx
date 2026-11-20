"""Reference adaptation preserves Core validation and never samples reference data."""

import copy
import traceback

import pytest
from server.reference_views import build_reference_views, collect_reference_tables, rewrite_to_ref_df_name

REFERENCE_FUNCTIONS = ("foreign_key", "compare_datasets", "has_valid_schema", "aggr_matches_dataset")
REFERENCE = "cat.sch.parent"
VIEW = "cat.tmp.v_1234567890_abcdef123456"


def make_check(
    reference: str = REFERENCE,
    function: str = "foreign_key",
    *,
    for_each_column: list[object] | None = None,
    **extra_arguments: object,
) -> dict[str, object]:
    arguments: dict[str, object] = {"ref_table": reference}
    if function in ("foreign_key", "compare_datasets"):
        arguments.update({"columns": ["id"], "ref_columns": ["id"]})
    if function == "aggr_matches_dataset":
        arguments["column"] = "id"
    arguments.update(extra_arguments)
    inner: dict[str, object] = {"function": function, "arguments": arguments}
    if for_each_column is not None:
        arguments.pop("column", None)
        arguments.pop("columns", None)
        inner["for_each_column"] = for_each_column
    return {"name": "reference_check", "criticality": "error", "check": inner}


def arguments_of(check: dict[str, object]) -> dict[str, object]:
    inner = check["check"]
    assert isinstance(inner, dict)
    arguments = inner["arguments"]
    assert isinstance(arguments, dict)
    return arguments


@pytest.mark.parametrize("function", REFERENCE_FUNCTIONS)
def test_build_handles_each_supported_check_without_mutating_input(function: str) -> None:
    checks = [make_check(function=function)]
    original = copy.deepcopy(checks)
    requested: list[str] = []

    def create_view(reference: str) -> str:
        requested.append(reference)
        return VIEW

    result = build_reference_views(checks, create_view)

    assert requested == [REFERENCE]
    assert result.ref_views == {REFERENCE: VIEW}
    assert result.created_view_fqns == [VIEW]
    expected_arguments = dict(arguments_of(checks[0]))
    del expected_arguments["ref_table"]
    expected_arguments["ref_df_name"] = REFERENCE
    assert arguments_of(result.checks[0]) == expected_arguments
    assert checks == original


@pytest.mark.parametrize("function", REFERENCE_FUNCTIONS)
def test_for_each_column_shape_is_preserved(function: str) -> None:
    columns: list[object] = ["id", "other_id"] if function == "aggr_matches_dataset" else [["id"], ["other_id"]]
    checks = [make_check(function=function, for_each_column=columns)]
    expected = copy.deepcopy(checks)
    expected_arguments = arguments_of(expected[0])
    del expected_arguments["ref_table"]
    expected_arguments["ref_df_name"] = REFERENCE

    result = build_reference_views(checks, lambda reference: VIEW)

    assert result.checks == expected
    assert result.ref_views == {REFERENCE: VIEW}


def test_collect_dedupes_and_preserves_first_occurrence_order() -> None:
    checks = [make_check(), make_check("cat.sch.other"), make_check(function="compare_datasets")]
    assert collect_reference_tables(checks) == [REFERENCE, "cat.sch.other"]


@pytest.mark.parametrize("function", REFERENCE_FUNCTIONS)
def test_duplicate_tables_share_one_full_view(function: str) -> None:
    requested: list[str] = []

    def create_view(reference: str) -> str:
        requested.append(reference)
        return VIEW

    result = build_reference_views([make_check(function=function), make_check()], create_view)

    assert requested == [REFERENCE]
    assert result.created_view_fqns == [VIEW]
    assert all(arguments_of(check)["ref_df_name"] == REFERENCE for check in result.checks)


@pytest.mark.parametrize("function", REFERENCE_FUNCTIONS)
def test_rewritten_checks_do_not_depend_on_generated_view_names(function: str) -> None:
    checks = [make_check(function=function)]
    first = build_reference_views(checks, lambda reference: VIEW)
    second = build_reference_views(checks, lambda reference: "cat.tmp.v_1234567891_012345abcdef")
    assert first.ref_views != second.ref_views
    assert first.checks == second.checks


@pytest.mark.parametrize("function", REFERENCE_FUNCTIONS)
def test_existing_reference_dataframe_is_not_overwritten(function: str) -> None:
    checks = [make_check(function=function, ref_df_name="user_reference")]
    requested: list[str] = []

    def create_view(reference: str) -> str:
        requested.append(reference)
        return VIEW

    result = build_reference_views(checks, create_view)

    assert requested == []
    assert result.checks == checks
    assert result.ref_views == {}
    assert rewrite_to_ref_df_name(checks, {REFERENCE}) == checks


@pytest.mark.parametrize("function", ("foreign_key", "compare_datasets", "has_valid_schema"))
def test_empty_reference_dataframe_name_is_still_a_conflict(function: str) -> None:
    checks = [make_check(function=function, ref_df_name="")]
    assert collect_reference_tables(checks) == []
    assert rewrite_to_ref_df_name(checks, {REFERENCE}) == checks


@pytest.mark.parametrize("ref_df_name", (None, ""))
def test_aggregate_preserves_core_truthiness_semantics(ref_df_name: str | None) -> None:
    checks = [make_check(function="aggr_matches_dataset", ref_df_name=ref_df_name)]
    result = build_reference_views(checks, lambda reference: VIEW)
    assert result.ref_views == {REFERENCE: VIEW}
    assert arguments_of(result.checks[0])["ref_df_name"] == REFERENCE
    assert "ref_table" not in arguments_of(result.checks[0])


@pytest.mark.parametrize("ref_df_name", (False, 0, [], {}))
def test_aggregate_does_not_repair_invalid_reference_dataframe_types(ref_df_name: object) -> None:
    checks = [make_check(function="aggr_matches_dataset", ref_df_name=ref_df_name)]
    assert collect_reference_tables(checks) == []
    assert rewrite_to_ref_df_name(checks, {REFERENCE}) == checks


@pytest.mark.parametrize("expected_schema", ("id INT", ""))
def test_schema_reference_with_expected_schema_remains_invalid(expected_schema: str) -> None:
    checks = [make_check(function="has_valid_schema", expected_schema=expected_schema)]
    assert collect_reference_tables(checks) == []
    assert rewrite_to_ref_df_name(checks, {REFERENCE}) == checks


@pytest.mark.parametrize("function", REFERENCE_FUNCTIONS)
def test_explicit_none_reference_dataframe_remains_eligible(function: str) -> None:
    checks = [make_check(function=function, ref_df_name=None)]
    assert collect_reference_tables(checks) == [REFERENCE]
    assert arguments_of(rewrite_to_ref_df_name(checks, {REFERENCE})[0])["ref_df_name"] == REFERENCE


def test_schema_none_expected_schema_does_not_conflict() -> None:
    checks = [make_check(function="has_valid_schema", expected_schema=None)]
    assert collect_reference_tables(checks) == [REFERENCE]


@pytest.mark.parametrize("function", ("unknown_function", "is_not_null", "sql_query"))
def test_other_functions_are_not_adapted(function: str) -> None:
    checks = [make_check(function=function)]
    assert collect_reference_tables(checks) == []
    assert rewrite_to_ref_df_name(checks, {REFERENCE}) == checks


def test_mixed_checks_sharing_a_table_do_not_repair_invalid_checks() -> None:
    checks = [
        make_check(),
        make_check(ref_df_name="different_reference"),
        make_check(ref_df_name=""),
        make_check(function="has_valid_schema", expected_schema="id INT"),
        make_check(function="unknown_function"),
    ]
    original = copy.deepcopy(checks)
    result = build_reference_views(checks, lambda reference: VIEW)
    assert result.created_view_fqns == [VIEW]
    assert arguments_of(result.checks[0])["ref_df_name"] == REFERENCE
    assert result.checks[1:] == original[1:]
    assert checks == original


def test_unhandled_references_are_unchanged() -> None:
    checks = [make_check(), make_check("cat.sch.other")]
    result = rewrite_to_ref_df_name(checks, {REFERENCE})
    assert arguments_of(result[0])["ref_df_name"] == REFERENCE
    assert result[1] == checks[1]


def test_no_references_is_a_noop() -> None:
    checks: list[dict[str, object]] = [
        {},
        {"check": "malformed"},
        {"check": {"function": "foreign_key", "arguments": None}},
        make_check("  "),
        {"check": {"function": "compare_datasets", "arguments": {"ref_df_name": "existing"}}},
        {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
    ]

    def create_view(reference: str) -> str:
        raise AssertionError("A check without an eligible reference must not create a view")

    result = build_reference_views(checks, create_view)
    assert result.checks is checks
    assert result.ref_views == {}
    assert result.created_view_fqns == []


def test_self_reference_reuses_the_monitored_full_view() -> None:
    def create_view(reference: str) -> str:
        raise AssertionError("The full monitored view should be reused")

    result = build_reference_views([make_check()], create_view, monitored_table_fqn=REFERENCE, monitored_view_fqn=VIEW)
    assert result.created_view_fqns == []
    assert result.ref_views == {REFERENCE: VIEW}
    assert arguments_of(result.checks[0])["ref_df_name"] == REFERENCE


@pytest.mark.parametrize(
    "reference", ("sch.parent", "cat.sch.`parent`", "cat.sch.parent-table", "cat.sch.bad\nname", "cat.sch.bad\x1bname")
)
def test_invalid_names_fail_without_creating_a_view_or_exposing_control_characters(reference: str) -> None:
    def create_view(table: str) -> str:
        raise AssertionError("An invalid reference must fail before view creation")

    with pytest.raises(ValueError, match="Invalid reference table name") as error:
        build_reference_views([make_check(reference)], create_view)

    rendered = "".join(traceback.format_exception(error.value))
    assert "Only alphanumeric" not in rendered  # the underlying validator's raw error is suppressed
    assert "SELECT" not in str(error.value)
    assert "\n" not in str(error.value)
    assert "\x1b" not in rendered


def test_invalid_later_name_still_cleans_up_created_views() -> None:
    dropped: list[str] = []
    with pytest.raises(ValueError, match="Invalid reference table name"):
        build_reference_views(
            [make_check(), make_check("sch.invalid")], lambda reference: VIEW, drop_view=dropped.append
        )
    assert dropped == [VIEW]


def test_creation_error_names_the_table_and_keeps_the_original_cause() -> None:
    original = RuntimeError("warehouse unavailable")

    def create_view(reference: str) -> str:
        raise original

    with pytest.raises(RuntimeError, match="cat.sch.parent") as error:
        build_reference_views([make_check()], create_view)
    assert "warehouse availability" in str(error.value)
    assert "warehouse unavailable" in "".join(traceback.format_exception(error.value))


def test_partial_creation_failure_cleans_up_every_created_view_even_if_one_drop_fails() -> None:
    dropped: list[str] = []

    def create_view(reference: str) -> str:
        if reference.endswith("third"):
            raise RuntimeError("no access")
        return reference.replace("cat.sch.", "cat.tmp.")

    def drop_view(view: str) -> None:
        dropped.append(view)
        if view.endswith("parent"):
            raise RuntimeError("cleanup unavailable")

    with pytest.raises(RuntimeError, match="cat.sch.third"):
        build_reference_views(
            [make_check(), make_check("cat.sch.second"), make_check("cat.sch.third")],
            create_view,
            drop_view=drop_view,
        )
    assert dropped == ["cat.tmp.parent", "cat.tmp.second"]
