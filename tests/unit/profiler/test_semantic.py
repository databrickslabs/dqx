from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from databricks.labs.dqx.profiler.semantic import (
    DEFAULT_ENUM_DETECTOR,
    DEFAULT_KEY_DETECTOR,
    DEFAULT_MEASUREMENT_DETECTOR,
    DEFAULT_TEXT_DETECTOR,
    DQProfileContext,
    DQSemanticType,
    DQSemanticTypeDetector,
    SemanticRegistry,
    default_semantic_detectors,
)


# ---------------------------------------------------------------------------
# DQSemanticType immutability (Pydantic v2 guarantees)
# ---------------------------------------------------------------------------


def test_dq_semantic_type_tuple_property_round_trips():
    sem = DQSemanticType(name="enum", properties={"values": (1, 2, 3)})
    assert sem.properties["values"] == (1, 2, 3)
    assert isinstance(sem.properties["values"], tuple)


def test_dq_semantic_type_frozen_blocks_field_reassignment():
    sem = DQSemanticType(name="enum")
    with pytest.raises(ValidationError):
        sem.name = "x"  # type: ignore[misc]


def test_dq_semantic_type_rejects_unsupported_property_value():
    with pytest.raises(ValidationError):
        DQSemanticType(name="enum", properties={"values": {"nested": "dict"}})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# default_semantic_detectors chain shape
# ---------------------------------------------------------------------------


def test_default_semantic_detectors_order():
    chain = default_semantic_detectors()
    assert isinstance(chain, tuple)
    assert [d.name for d in chain] == ["enum", "key", "measurement", "text"]


# ---------------------------------------------------------------------------
# SemanticRegistry immutability + name uniqueness
# ---------------------------------------------------------------------------


def test_semantic_registry_default_matches_helper():
    registry = SemanticRegistry.default()
    assert registry.detectors == default_semantic_detectors()


def test_semantic_registry_empty_constructor_has_empty_detectors():
    assert SemanticRegistry().detectors == ()


def test_semantic_registry_prepend_returns_new_instance():
    original = SemanticRegistry.default()
    snapshot = original.detectors
    extra = DQSemanticTypeDetector(name="custom", detect=lambda _ctx: None)
    new = original.prepend(extra)
    assert new is not original
    assert new.detectors[0] is extra
    assert original.detectors == snapshot  # original unchanged


def test_semantic_registry_replace_returns_new_instance():
    original = SemanticRegistry.default()
    replacement = (DQSemanticTypeDetector(name="only", detect=lambda _ctx: None),)
    new = original.replace(replacement)
    assert new is not original
    assert new.detectors == replacement
    assert original.detectors == default_semantic_detectors()


def test_semantic_registry_assignment_raises():
    registry = SemanticRegistry.default()
    with pytest.raises(ValidationError):
        registry.detectors = ()  # type: ignore[misc]


def test_semantic_registry_prepend_duplicate_name_raises():
    dup = DQSemanticTypeDetector(name="enum", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry.default().prepend(dup)


def test_semantic_registry_replace_duplicate_names_raises():
    dup1 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    dup2 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry().replace([dup1, dup2])


def test_semantic_registry_direct_constructor_duplicate_names_raises():
    dup1 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    dup2 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry(detectors=(dup1, dup2))


# ---------------------------------------------------------------------------
# Chain semantics via a hand-rolled 3-detector chain
# ---------------------------------------------------------------------------


def _fake_ctx(column_name="c", column_type=None, metrics=None, options=None):
    df = create_autospec(DataFrame)
    df.columns = [column_name]
    return DQProfileContext(
        df=df,
        column_name=column_name,
        column_type=column_type or T.IntegerType(),
        metrics=metrics or {},
        options=options or {},
    )


def test_chain_first_match_wins_and_stops():
    calls: list[str] = []

    def make_detector(name: str, match: bool):
        def _detect(ctx):
            calls.append(name)
            return DQSemanticType(name=name) if match else None

        return DQSemanticTypeDetector(name=name, detect=_detect)

    registry = SemanticRegistry(
        detectors=(
            make_detector("a", False),
            make_detector("b", True),
            make_detector("c", True),  # should not be invoked
        )
    )
    ctx = _fake_ctx()
    picked = None
    for detector in registry.detectors:
        result = detector.detect(ctx)
        if result is not None:
            picked = result
            break
    assert picked is not None
    assert picked.name == "b"
    assert calls == ["a", "b"]


def test_empty_chain_leaves_ctx_semantic_type_none():
    registry = SemanticRegistry()
    ctx = _fake_ctx()
    match = None
    for detector in registry.detectors:
        match = detector.detect(ctx)
        if match is not None:
            break
    assert match is None


# ---------------------------------------------------------------------------
# Individual detector guards (metric-only paths)
# ---------------------------------------------------------------------------


def test_measurement_detector_positive_numeric_column():
    ctx = _fake_ctx(
        column_type=T.IntegerType(),
        metrics={"count_non_null": 100, "min": 0, "max": 100, "mean": 50.0, "stddev": 15.0},
    )
    result = DEFAULT_MEASUREMENT_DETECTOR.detect(ctx)
    assert result is not None
    assert result.name == "measurement"
    assert "distribution" in result.properties


def test_measurement_detector_rejects_string_column():
    ctx = _fake_ctx(column_type=T.StringType(), metrics={"count_non_null": 5})
    assert DEFAULT_MEASUREMENT_DETECTOR.detect(ctx) is None


def test_text_detector_positive_string_column():
    ctx = _fake_ctx(column_type=T.StringType(), metrics={"count_non_null": 5})
    result = DEFAULT_TEXT_DETECTOR.detect(ctx)
    assert result is not None
    assert result.name == "text"


def test_text_detector_rejects_numeric_column():
    ctx = _fake_ctx(column_type=T.IntegerType(), metrics={"count_non_null": 5})
    assert DEFAULT_TEXT_DETECTOR.detect(ctx) is None


def test_key_detector_numeric_dense_positive():
    """user_id-like: distinctness=1.0, density=1.0."""
    ctx = _fake_ctx(
        column_type=T.LongType(),
        metrics={"count_non_null": 100, "count_distinct": 100, "min": 1, "max": 100},
    )
    result = DEFAULT_KEY_DETECTOR.detect(ctx)
    assert result is not None
    assert result.name == "key"
    assert result.properties.get("signal") == "density"


def test_key_detector_numeric_sparse_negative_falls_through():
    """cargo_weight-like: distinctness high but density << 0.99."""
    ctx = _fake_ctx(
        column_type=T.DoubleType(),
        metrics={"count_non_null": 100, "count_distinct": 100, "min": 0, "max": 1_000_000},
    )
    assert DEFAULT_KEY_DETECTOR.detect(ctx) is None


def test_key_detector_low_distinctness_returns_none():
    ctx = _fake_ctx(
        column_type=T.IntegerType(),
        metrics={"count_non_null": 100, "count_distinct": 50, "min": 1, "max": 50},
    )
    assert DEFAULT_KEY_DETECTOR.detect(ctx) is None


# ---------------------------------------------------------------------------
# Enum applicability guard
# ---------------------------------------------------------------------------


def _mock_df_with_distinct(column_name: str, distinct_values: list) -> DataFrame:
    distinct_df = create_autospec(DataFrame)
    distinct_df.collect.return_value = [[v] for v in distinct_values]
    distinct_df.distinct.return_value = distinct_df

    df = create_autospec(DataFrame)
    df.columns = [column_name]
    df.select.return_value = distinct_df
    return df


def test_enum_detector_rejects_all_distinct_integer_column():
    """A 20-row all-distinct integer column must not be classified as enum."""
    ctx = DQProfileContext(
        df=_mock_df_with_distinct("c", list(range(20))),
        column_name="c",
        column_type=T.IntegerType(),
        metrics={"count_non_null": 20, "count_distinct": 20},
        options={"max_in_count": 10},
    )
    assert DEFAULT_ENUM_DETECTOR.detect(ctx) is None


def test_enum_detector_positive_low_cardinality():
    ctx = DQProfileContext(
        df=_mock_df_with_distinct("c", ["car", "truck", "van"]),
        column_name="c",
        column_type=T.StringType(),
        metrics={"count_non_null": 300, "count_distinct": 3},
        options={"max_in_count": 10},
    )
    result = DEFAULT_ENUM_DETECTOR.detect(ctx)
    assert result is not None
    assert result.name == "enum"
    assert isinstance(result.properties["values"], tuple)
    assert set(result.properties["values"]) == {"car", "truck", "van"}


# ---------------------------------------------------------------------------
# String-key length stability guard (uses Spark aggregate via autospec)
# ---------------------------------------------------------------------------


def _string_key_ctx(column_name, count_non_null, count_distinct, min_len, max_len):
    agg_row = {"min_len": min_len, "max_len": max_len}
    first_row = create_autospec(dict)
    first_row.__getitem__.side_effect = agg_row.__getitem__

    projected = create_autospec(DataFrame)
    projected.first.return_value = first_row

    df = create_autospec(DataFrame)
    df.columns = [column_name]
    df.select.return_value = projected

    return DQProfileContext(
        df=df,
        column_name=column_name,
        column_type=T.StringType(),
        metrics={"count_non_null": count_non_null, "count_distinct": count_distinct},
        options={},
    )


def test_key_detector_string_uniform_length_positive():
    """UUID-like: every value 36 chars → stability = 1.0 passes."""
    ctx = _string_key_ctx("order_id", count_non_null=50, count_distinct=50, min_len=36, max_len=36)
    result = DEFAULT_KEY_DETECTOR.detect(ctx)
    assert result is not None
    assert result.name == "key"
    assert result.properties.get("signal") == "length_stability"


def test_key_detector_string_variable_length_negative():
    """Free-form names, min 4 / max 40 → stability=0.1 well below threshold."""
    ctx = _string_key_ctx("user_name", count_non_null=50, count_distinct=50, min_len=4, max_len=40)
    assert DEFAULT_KEY_DETECTOR.detect(ctx) is None


def test_key_detector_string_empty_only_returns_none_without_error():
    """max_length == 0 must not trigger ZeroDivisionError; column is not classified as key."""
    ctx = _string_key_ctx("empty_col", count_non_null=5, count_distinct=5, min_len=0, max_len=0)
    assert DEFAULT_KEY_DETECTOR.detect(ctx) is None
