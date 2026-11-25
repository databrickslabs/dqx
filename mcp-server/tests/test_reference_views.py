"""Reference tables are exposed through full views, and checks reach DQX unchanged."""

import copy
import traceback

import pytest
from server.reference_views import build_reference_views

REFERENCE_FUNCTIONS = ("foreign_key", "compare_datasets", "has_valid_schema", "aggr_matches_dataset")
REFERENCE = "cat.sch.parent"
VIEW = "cat.tmp.v_1234567890_abcdef123456"


def make_check(reference: object = REFERENCE, function: str = "foreign_key", **extra_arguments: object) -> dict:
    arguments = {"ref_table": reference, **extra_arguments}
    return {"name": "reference_check", "criticality": "error", "check": {"function": function, "arguments": arguments}}


class RecordingViews:
    """Records the sources views are created over, returning a distinct view per call."""

    def __init__(self) -> None:
        self.sources: list[str] = []

    def __call__(self, source: str) -> str:
        self.sources.append(source)
        return f"cat.tmp.v_1234567890_{len(self.sources):012d}"


@pytest.mark.parametrize("function", REFERENCE_FUNCTIONS)
def test_each_reference_table_gets_a_full_view_and_checks_are_unchanged(function: str) -> None:
    checks = [make_check(function=function)]
    original = copy.deepcopy(checks)
    views = RecordingViews()

    ref_views = build_reference_views(checks, views)

    assert views.sources == [REFERENCE]
    assert ref_views == {REFERENCE: "cat.tmp.v_1234567890_000000000001"}
    assert checks == original


def test_distinct_tables_get_one_view_each_in_first_occurrence_order() -> None:
    views = RecordingViews()
    ref_views = build_reference_views([make_check(), make_check("cat.sch.other"), make_check()], views)
    assert views.sources == [REFERENCE, "cat.sch.other"]
    assert list(ref_views) == [REFERENCE, "cat.sch.other"]


def test_names_differing_only_by_case_share_one_view_under_each_spelling() -> None:
    views = RecordingViews()
    ref_views = build_reference_views([make_check(), make_check("CAT.Sch.Parent")], views)
    shared = "cat.tmp.v_1234567890_000000000001"
    assert views.sources == [REFERENCE]
    assert ref_views == {REFERENCE: shared, "CAT.Sch.Parent": shared}


@pytest.mark.parametrize("reference", (REFERENCE, "Cat.Sch.Parent"))
def test_self_reference_reuses_the_monitored_full_view(reference: str) -> None:
    def create_view(table: str) -> str:
        raise AssertionError("The full monitored view should be reused")

    ref_views = build_reference_views(
        [make_check(reference)], create_view, monitored_table_fqn=REFERENCE, monitored_view_fqn=VIEW
    )
    assert ref_views == {reference: VIEW}


@pytest.mark.parametrize(
    "check",
    [
        {},
        {"check": "malformed"},
        {"check": {"function": "foreign_key", "arguments": None}},
        {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
        make_check(""),
        make_check("  "),
        make_check(None),
        make_check(42),
    ],
)
def test_checks_without_a_reference_table_create_no_view(check: dict) -> None:
    def create_view(table: str) -> str:
        raise AssertionError("A check without a reference table must not create a view")

    assert not build_reference_views([check], create_view)


def test_conflicting_reference_arguments_reach_dqx_unchanged() -> None:
    """DQX rejects ref_table with ref_df_name. The server exposes the table and leaves the check to DQX."""
    checks = [make_check(ref_df_name="user_reference")]
    original = copy.deepcopy(checks)
    views = RecordingViews()

    assert build_reference_views(checks, views) == {REFERENCE: "cat.tmp.v_1234567890_000000000001"}
    assert checks == original


@pytest.mark.parametrize(
    "reference", ("sch.parent", "cat.sch.`parent`", "cat.sch.parent-table", "cat.sch.bad\nname", "cat.sch.bad\x1bname")
)
def test_invalid_names_fail_without_creating_a_view_or_echoing_the_name(reference: str) -> None:
    def create_view(table: str) -> str:
        raise AssertionError("An invalid reference must fail before view creation")

    with pytest.raises(ValueError, match="Invalid reference table name in check at index 1") as error:
        build_reference_views([{"check": {}}, make_check(reference)], create_view)

    rendered = "".join(traceback.format_exception(error.value))
    assert "Only alphanumeric" not in rendered  # The underlying validator's raw error is suppressed.
    assert reference not in rendered  # The untrusted name is never echoed, so no sanitizer is relied on.


def test_creation_error_names_the_table_and_keeps_the_original_cause() -> None:
    original = RuntimeError("warehouse unavailable")

    def create_view(reference: str) -> str:
        raise original

    with pytest.raises(RuntimeError, match="cat.sch.parent") as error:
        build_reference_views([make_check()], create_view)
    assert "warehouse availability" in str(error.value)
    assert "warehouse unavailable" in "".join(traceback.format_exception(error.value))
