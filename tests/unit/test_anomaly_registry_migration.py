"""Unit pins for the registry migration's decision points (no Spark).

The migration itself needs a real Delta table and is covered in `tests/integration_anomaly/`. Two pieces of
it are pure and were left untested when it was written, both on paths that only run when something has
already gone wrong, which is exactly when nobody wants a second surprise:

- the schema normaliser, which decides what a row's grouping is across three table shapes
- the permission-message classifier, which decides whether a failed rewrite becomes advice or is re-raised

Getting the second one wrong in either direction is bad: too eager and a genuine bug is reported to the user
as "ask your admin for a grant", too shy and a missing grant surfaces as an unexplained write failure.
"""

import pytest

from databricks.labs.dqx.anomaly.model_registry import (
    LEGACY_GROUPING_COLUMN,
    _is_permission_error,
    normalise_grouping,
)


def test_a_current_row_is_returned_unchanged():
    values = {"grouping": {"baseline_by": ["region"], "sklearn_version": "1.5.0", "config_hash": "abc"}}

    assert normalise_grouping(values) == values["grouping"]


def test_a_pre_release_row_maps_segment_by_onto_baseline_by():
    """The mapping the migration and the read path share."""
    values = {
        LEGACY_GROUPING_COLUMN: {
            "segment_by": ["region", "product"],
            "segment_values": {"region": "eu"},
            "is_global_model": False,
            "sklearn_version": "1.4.2",
            "config_hash": "old-hash",
        }
    }

    grouping = normalise_grouping(values)

    assert grouping["baseline_by"] == ["region", "product"]
    assert grouping["sklearn_version"] == "1.4.2"
    assert grouping["config_hash"] == "old-hash"
    # Dropped deliberately: there is one pooled model now, so neither has a meaning to preserve.
    assert "segment_values" not in grouping
    assert "is_global_model" not in grouping


def test_a_part_migrated_row_prefers_the_legacy_column_when_grouping_is_null():
    """The shape a `mergeSchema` append leaves behind: both columns present, one null per row.

    Worth pinning even though the migration removes the legacy column, because a table written by an earlier
    build of this release is already in this state on someone's workspace.
    """
    values = {
        "grouping": None,
        LEGACY_GROUPING_COLUMN: {
            "segment_by": ["country"],
            "segment_values": None,
            "is_global_model": True,
            "sklearn_version": "1.4.0",
            "config_hash": "h",
        },
    }

    assert normalise_grouping(values)["baseline_by"] == ["country"]


def test_a_row_with_neither_column_yields_an_empty_grouping_rather_than_raising():
    """Defensive: a hand-written or truncated row must not turn a read into a KeyError.

    Reading is how a caller reaches the message telling them to retrain. That message is worth more than
    strictness here.
    """
    grouping = normalise_grouping({})

    assert grouping["baseline_by"] == []
    assert grouping["config_hash"] is None


@pytest.mark.parametrize(
    "message",
    [
        "[PERMISSION_DENIED] User does not have MODIFY on table cat.sch.reg",
        "INSUFFICIENT_PERMISSIONS: cannot write to cat.sch.reg",
        "User bob@example.com does not have permission to modify this table",
    ],
)
def test_a_missing_grant_is_recognised(message: str):
    """Recognised so the caller gets the grant to ask for, not a raw write failure."""
    assert _is_permission_error(Exception(message))


@pytest.mark.parametrize(
    "message",
    [
        "[TABLE_OR_VIEW_NOT_FOUND] cat.sch.reg",
        "Column 'grouping' does not exist",
        "java.lang.OutOfMemoryError: Java heap space",
        "",
    ],
)
def test_a_genuine_failure_is_not_mistaken_for_a_missing_grant(message: str):
    """The direction that matters more.

    Reporting a real bug as "ask an owner for MODIFY" sends someone to their platform team over a defect in
    DQX, and they have no way to discover that from the message.
    """
    assert not _is_permission_error(Exception(message))
