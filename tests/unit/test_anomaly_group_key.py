"""Unit tests for the Python half of the group key.

The group key is the riskiest invariant in group conditioning: training persists baselines under
keys built by :func:`build_baseline_key` and scoring looks them up with keys built by
:func:`baseline_key_column` in Spark. A disagreement does not raise — every lookup simply misses and
falls back to the global baseline, giving a model that trains and scores cleanly while
conditioning on nothing at all.

These tests pin the Python side. Parity with the Spark side needs a session and so lives in
``tests/integration_anomaly/test_anomaly_group_relative_features.py``.
"""

from databricks.labs.dqx.anomaly.segment_utils import (
    BASELINE_KEY_NULL,
    BASELINE_KEY_SEPARATOR,
    build_baseline_key,
)


def test_single_group_column():
    assert build_baseline_key({"country": "DE"}) == "DE"


def test_composite_key_joins_with_the_separator():
    key = build_baseline_key({"country": "DE", "product": "casino"})
    assert key == f"DE{BASELINE_KEY_SEPARATOR}casino"


def test_key_is_independent_of_declaration_order():
    """Ordering by column name means the caller's argument order cannot change the key."""
    assert build_baseline_key({"country": "DE", "product": "casino"}) == build_baseline_key(
        {"product": "casino", "country": "DE"}
    )


def test_nulls_become_a_real_key_rather_than_propagating():
    """A missing dimension must still land in a group, or its rows lose their baseline."""
    key = build_baseline_key({"country": None, "product": "casino"})
    assert key == f"{BASELINE_KEY_NULL}{BASELINE_KEY_SEPARATOR}casino"


def test_integral_values_are_stringified():
    """Integral baseline columns are allowed, so their rendering must be defined."""
    assert build_baseline_key({"store_id": 42}) == "42"
    assert build_baseline_key({"store_id": -7}) == "-7"


def test_booleans_render_the_way_spark_does_not_the_way_python_does():
    """Regression: this assertion used to read ``== "True"`` and was wrong.

    Spark's ``cast("string")`` renders booleans lowercase; Python's ``str(True)`` capitalises. The
    Spark side is the source of truth because it is what runs at scoring time, so the Python half
    must match it. Found by the integration test that pins the two halves against a real session —
    before the fix, training persisted ``True\\x1f42`` and scoring looked up ``true\\x1f42``, missing
    every baseline and silently falling back to the global one.
    """
    assert build_baseline_key({"is_vip": True}) == "true"
    assert build_baseline_key({"is_vip": False}) == "false"


def test_empty_grouping_is_the_empty_key():
    assert build_baseline_key(None) == ""
    assert build_baseline_key({}) == ""


def test_printable_punctuation_in_values_does_not_collide():
    """The ambiguity a printable separator would have: ("x_y", "z") versus ("x", "y_z").

    These collide under a "_" separator and must not collide here. This is what choosing a
    control character buys — real categorical data contains underscores, dashes and colons.
    """
    for punctuation in ("_", "-", ":", "|", "=", ",", " "):
        left = build_baseline_key({"a": f"x{punctuation}y", "b": "z"})
        right = build_baseline_key({"a": "x", "b": f"y{punctuation}z"})
        assert left != right, f"collision on {punctuation!r}"


def test_separator_inside_a_value_is_a_documented_collision():
    """Pins the known limitation rather than pretending it does not exist.

    ``BASELINE_KEY_SEPARATOR`` is a separator, not an escaping scheme, so data that genuinely
    contains the control character can collide. Accepted deliberately: escaping would widen the
    Python/Spark parity contract, which is the invariant most expensive to get wrong. If this
    ever needs fixing, this test is the one that should start failing.
    """
    left = build_baseline_key({"a": f"x{BASELINE_KEY_SEPARATOR}y", "b": "z"})
    right = build_baseline_key({"a": "x", "b": f"y{BASELINE_KEY_SEPARATOR}z"})
    assert left == right


def test_a_null_and_a_literal_missing_are_different_groups():
    """The regression this exists for: the two used to share one key, and therefore one baseline.

    ``BASELINE_KEY_NULL`` was the string ``"MISSING"``, which a categorical dimension can genuinely
    contain -- a region named MISSING in an upstream feed, a status column with an explicit MISSING
    level. Those rows and the rows with a NULL region were persisted under one key and conditioned
    against one median, silently, with nothing to indicate the two populations had been merged.
    """
    with_null = build_baseline_key({"region": None, "product": "casino"})
    with_literal = build_baseline_key({"region": "MISSING", "product": "casino"})

    assert with_null != with_literal
    assert "MISSING" in with_literal
