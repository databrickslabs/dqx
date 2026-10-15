"""Group keys preserve tuple boundaries and distinguish missing values from literal data."""

from collections.abc import Mapping
import datetime
import itertools

import pytest

from databricks.labs.dqx.anomaly.segment_utils import build_baseline_key


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
