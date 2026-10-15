"""Group keys preserve tuple boundaries and distinguish missing values from literal data."""

from collections.abc import Mapping
import datetime
import itertools
from unittest.mock import create_autospec

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.segment_utils import (
    allocate_baseline_key_column,
    build_baseline_key,
    with_baseline_key,
)
from databricks.labs.dqx.errors import InvalidParameterError


@pytest.mark.parametrize(
    "values, expected",
    [
        ({"country": "DE"}, "2:DE"),
        ({"country": "DE", "product": "casino"}, "2:DE6:casino"),
        ({"country": None, "product": "casino"}, "-1:6:casino"),
        ({"country": ""}, "0:"),
        ({"store_id": 42}, "2:42"),
        ({"store_id": -7}, "2:-7"),
        ({"is_vip": True}, "4:true"),
        ({"is_vip": False}, "5:false"),
        ({"day": datetime.date(2026, 8, 25)}, "10:2026-08-25"),
        ({"country": "日本"}, "6:日本"),
        ({"country": "🌍"}, "4:🌍"),
        (None, ""),
        ({}, ""),
    ],
)
def test_group_key_encoding(values: Mapping[str, object] | None, expected: str) -> None:
    assert build_baseline_key(values) == expected


def test_key_is_independent_of_declaration_order() -> None:
    assert build_baseline_key({"country": "DE", "product": "casino"}) == build_baseline_key(
        {"product": "casino", "country": "DE"}
    )


@pytest.mark.parametrize("punctuation", ["_", "-", ":", "|", "=", ",", " ", "\x1f", "\x00", "\n"])
def test_value_boundaries_do_not_merge_groups(punctuation: str) -> None:
    left = build_baseline_key({"a": f"x{punctuation}y", "b": "z"})
    right = build_baseline_key({"a": "x", "b": f"y{punctuation}z"})
    assert left != right


@pytest.mark.parametrize("literal", ["", "MISSING", "null", "None", "\x00", "-1:"])
def test_missing_dimension_is_distinct_from_literal_values(literal: str) -> None:
    missing = build_baseline_key({"region": None, "product": "casino"})
    present = build_baseline_key({"region": literal, "product": "casino"})
    assert missing != present


def test_adversarial_group_values_have_unique_tuple_keys() -> None:
    values = [None, "", "a", "0:", "-1:", "1:a", "\x1f", "\x00", "a\x1fb", "日本", "🌍", "\\", '"']
    tuples = list(itertools.product(values, repeat=2))
    keys = {build_baseline_key({"a": a, "b": b}) for a, b in tuples}
    assert len(keys) == len(tuples)
    assert build_baseline_key({}) not in keys


@pytest.mark.parametrize(
    "columns",
    [
        [],
        ["amount", "region"],
        ["__dqx_baseline_key"],
        ["__DQX_BASELINE_KEY"],
        ["__dqx_baseline_key", "__dqx_baseline_key_0", "__DQX_BASELINE_KEY_1"],
    ],
)
def test_allocated_baseline_key_preserves_source_names(columns: list[str]) -> None:
    allocated = allocate_baseline_key_column(columns)
    assert allocated.startswith("__dqx_baseline_key")
    assert allocated.casefold() not in {name.casefold() for name in columns}
    if "__dqx_baseline_key" not in {name.casefold() for name in columns}:
        assert allocated == "__dqx_baseline_key"


def test_no_grouping_does_not_modify_the_frame() -> None:
    frame = create_autospec(DataFrame, instance=True)
    assert with_baseline_key(frame, [], key_column="owned_key") is frame


def test_explicit_key_target_must_not_replace_a_source_column() -> None:
    frame = create_autospec(DataFrame, instance=True)
    frame.columns = ["region", "OWNED_KEY"]
    with pytest.raises(InvalidParameterError, match="conflicts with an input column"):
        with_baseline_key(frame, ["region"], key_column="owned_key")
