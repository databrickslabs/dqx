"""Unit tests for the *ScalarNode* validation and deep-copy helpers in *utils.py*.

Covers the primitive-tree contract used by action *extras* transport (see plan Step 1).
"""

import datetime
from decimal import Decimal

import pytest

from databricks.labs.dqx.errors import InvalidActionError
from databricks.labs.dqx.utils import (
    _SCALAR_VARIABLE_TYPES,
    VariableValue,
    deep_copy_scalar_node,
    validate_scalar_node,
)


# ---------------------------------------------------------------------------
# validate_scalar_node — accept
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        "leaf",
        42,
        3.14,
        True,
        False,
        [],
        (),
        {},
        set(),
        frozenset(),
        [1, "two", 3.0, False],
        (1, "two", 3.0, False),
        {"key": "value", "n": 1, "b": True},
        {"nested": {"deeper": [1, 2, ("x", "y")]}},
        {"mixed": [{"k": 1}, ["a", "b"], (True, 2.5)]},
        {"set": {1, 2, 3}, "frozen": frozenset(["a", "b"])},
    ],
)
def test_validate_scalar_node_accepts_valid_shape(payload: object) -> None:
    """validate_scalar_node returns silently on any legal ScalarNode shape."""
    validate_scalar_node(payload)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_scalar_node — reject
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload,path_hint",
    [
        (None, "<root>"),
        (b"bytes", "<root>"),
        (object(), "<root>"),
        ({"foo": None}, "foo"),
        ({"foo": b"bytes"}, "foo"),
        ({1: "one"}, "<root>"),  # non-str dict key
        ({"foo": [1, None]}, "foo[1]"),
        ({"foo": {"bar": object()}}, "foo.bar"),
        ({"foo": [1, {"bad": None}]}, "foo[1].bad"),
    ],
)
def test_validate_scalar_node_rejects_with_path(payload: object, path_hint: str) -> None:
    """validate_scalar_node raises InvalidActionError with the offending key path in the message."""
    with pytest.raises(InvalidActionError) as exc:
        validate_scalar_node(payload)  # type: ignore[arg-type]
    assert path_hint in str(exc.value)


def test_validate_scalar_node_rejects_set_of_container() -> None:
    """set may only carry leaves; a frozenset-of-frozenset is illegal."""
    with pytest.raises(InvalidActionError):
        validate_scalar_node(frozenset([frozenset(["a"])]))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# deep_copy_scalar_node
# ---------------------------------------------------------------------------


def test_deep_copy_scalar_node_produces_equal_but_not_identical_copy() -> None:
    payload = {
        "a": [1, 2, 3],
        "b": {"c": ("x", "y"), "d": {1, 2}},
    }
    copy = deep_copy_scalar_node(payload)  # type: ignore[arg-type]

    assert copy == payload
    assert copy is not payload
    assert copy["a"] is not payload["a"]
    assert copy["b"] is not payload["b"]
    assert copy["b"]["d"] is not payload["b"]["d"]


def test_deep_copy_scalar_node_isolates_mutation() -> None:
    payload = {"list": [1, 2, 3], "nested": {"inner": ["a"]}}
    copy = deep_copy_scalar_node(payload)  # type: ignore[arg-type]

    # Mutating the copy at every container level must not affect the original.
    copy["list"].append(999)  # type: ignore[union-attr]
    copy["nested"]["inner"].append("z")  # type: ignore[index,union-attr]
    copy["extra"] = "added"  # type: ignore[index]

    assert payload == {"list": [1, 2, 3], "nested": {"inner": ["a"]}}


# ---------------------------------------------------------------------------
# VariableValue regression guard — refactor to compose from ScalarLeaf must not
# change the accepted set at runtime.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "s",
        1,
        1.5,
        True,
        Decimal("1.5"),
        datetime.date(2024, 1, 1),
        datetime.datetime(2024, 1, 1, 12, 0, 0),
        datetime.time(12, 0, 0),
    ],
)
def test_variable_value_types_still_accepted(value: VariableValue) -> None:
    """Every historical VariableValue member is still recognised by _SCALAR_VARIABLE_TYPES."""
    assert isinstance(value, _SCALAR_VARIABLE_TYPES)
