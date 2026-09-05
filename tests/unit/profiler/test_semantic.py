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
    EnumProperties,
    MeasurementProperties,
    SemanticRegistry,
    default_semantic_detectors,
)


# ---------------------------------------------------------------------------
# DQSemanticType immutability (Pydantic v2 guarantees)
# ---------------------------------------------------------------------------


def test_dq_semantic_type_set_property_round_trips():
    sem = DQSemanticType(name="enum", properties=EnumProperties(values={1, 2, 3}))
    typed = sem.get_typed_properties(EnumProperties)
    assert typed is not None
    assert typed.values == {1, 2, 3}
    assert isinstance(typed.values, set)


def test_dq_semantic_type_get_typed_properties_wrong_type_returns_none():
    sem = DQSemanticType(name="enum", properties=EnumProperties(values={1, 2, 3}))
    assert sem.get_typed_properties(MeasurementProperties) is None


def test_dq_semantic_type_frozen_blocks_field_reassignment():
    sem = DQSemanticType(name="enum", properties=EnumProperties(values={1}))
    with pytest.raises(ValidationError):
        sem.name = "x"  # type: ignore[misc]


def test_dq_semantic_type_rejects_unsupported_property_value():
    with pytest.raises(ValidationError):
        EnumProperties(values={"nested": "dict"})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# DQProfileContext.with_metrics
# ---------------------------------------------------------------------------


def test_dq_profile_context_with_metrics_returns_new_instance_with_snapshot():
    original_metrics = {"count": 10, "count_non_null": 8}
    df_mock = create_autospec(DataFrame)
    ctx = DQProfileContext(
        df=df_mock,
        column_name="c",
        column_type=T.IntegerType(),
        metrics=original_metrics,
        options={"max_in_count": 10},
        semantic_type=DQSemanticType(name="measurement"),
    )

    new_metrics = {"count": 10, "count_non_null": 8, "min": 1, "max": 100}
    refreshed = ctx.with_metrics(new_metrics)

    assert refreshed is not ctx
    assert dict(refreshed.metrics) == new_metrics
    assert dict(ctx.metrics) == original_metrics  # original untouched
    assert refreshed.column_name == ctx.column_name
    assert refreshed.column_type == ctx.column_type
    assert refreshed.options == ctx.options
    assert refreshed.semantic_type == ctx.semantic_type
    assert refreshed.df is ctx.df


def test_dq_profile_context_with_metrics_snapshots_supplied_mapping():
    """Later mutations to the source dict must not be reflected in the returned context."""
    source: dict = {"count": 10}
    df_mock = create_autospec(DataFrame)
    ctx = DQProfileContext(
        df=df_mock,
        column_name="c",
        column_type=T.IntegerType(),
    )
    refreshed = ctx.with_metrics(source)
    source["count"] = 999
    assert refreshed.metrics["count"] == 10


def test_dq_profile_context_frozen_blocks_field_reassignment():
    df_mock = create_autospec(DataFrame)
    ctx = DQProfileContext(
        df=df_mock,
        column_name="c",
        column_type=T.IntegerType(),
    )
    with pytest.raises(ValidationError):
        setattr(ctx, "column_name", "x")


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
    assert not SemanticRegistry().detectors


def test_semantic_registry_prepend_returns_new_instance():
    original = SemanticRegistry.default()
    snapshot = original.detectors
    extra = DQSemanticTypeDetector(name="custom", detect=lambda _ctx: None)
    new = original.prepend(extra)
    assert new is not original
    assert new.detectors[0] is extra
    assert original.detectors == snapshot  # original unchanged


def test_semantic_registry_of_returns_new_instance_with_ordered_chain():
    detector_a = DQSemanticTypeDetector(name="a", detect=lambda _ctx: None)
    detector_b = DQSemanticTypeDetector(name="b", detect=lambda _ctx: None)
    detector_c = DQSemanticTypeDetector(name="c", detect=lambda _ctx: None)
    registry = SemanticRegistry.of(detector_a, detector_b, detector_c)
    assert [d.name for d in registry.detectors] == ["a", "b", "c"]


def test_semantic_registry_of_duplicate_names_raises():
    dup1 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    dup2 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry.of(dup1, dup2)


def test_semantic_registry_assignment_raises():
    registry = SemanticRegistry.default()
    with pytest.raises(ValidationError):
        registry.detectors = ()  # type: ignore[misc]


def test_semantic_registry_prepend_duplicate_name_raises():
    dup = DQSemanticTypeDetector(name="enum", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry.default().prepend(dup)


def test_semantic_registry_direct_constructor_duplicate_names_raises():
    dup1 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    dup2 = DQSemanticTypeDetector(name="foo", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry(detectors=(dup1, dup2))


# ---------------------------------------------------------------------------
# SemanticRegistry mutation helpers (append, insert, replace, remove)
# ---------------------------------------------------------------------------


def test_semantic_registry_append_lands_last_and_leaves_original_unchanged():
    original = SemanticRegistry.default()
    snapshot = original.detectors
    extra = DQSemanticTypeDetector(name="custom", detect=lambda _ctx: None)
    new = original.append(extra)
    assert new is not original
    assert new.detectors[-1] is extra
    assert new.detectors[:-1] == snapshot
    assert original.detectors == snapshot


def test_semantic_registry_append_duplicate_name_raises():
    dup = DQSemanticTypeDetector(name="enum", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry.default().append(dup)


def test_semantic_registry_insert_places_detector_after_named_entry():
    original = SemanticRegistry.default()
    extra = DQSemanticTypeDetector(name="custom", detect=lambda _ctx: None)
    new = original.insert("enum", extra)
    names = [d.name for d in new.detectors]
    assert names == ["enum", "custom", "key", "measurement", "text"]
    assert original.detectors == default_semantic_detectors()


def test_semantic_registry_insert_unknown_name_raises_value_error():
    extra = DQSemanticTypeDetector(name="custom", detect=lambda _ctx: None)
    with pytest.raises(ValueError, match="'missing'"):
        SemanticRegistry.default().insert("missing", extra)


def test_semantic_registry_insert_duplicate_name_raises():
    dup = DQSemanticTypeDetector(name="key", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry.default().insert("enum", dup)


def test_semantic_registry_replace_swaps_named_entry_preserving_position():
    original = SemanticRegistry.default()
    replacement = DQSemanticTypeDetector(name="enum", detect=lambda _ctx: None)
    new = original.replace("enum", replacement)
    assert new.detectors[0] is replacement
    assert [d.name for d in new.detectors] == ["enum", "key", "measurement", "text"]
    assert original.detectors == default_semantic_detectors()


def test_semantic_registry_replace_allows_renaming_the_target_entry():
    original = SemanticRegistry.default()
    replacement = DQSemanticTypeDetector(name="renamed_enum", detect=lambda _ctx: None)
    new = original.replace("enum", replacement)
    assert [d.name for d in new.detectors] == ["renamed_enum", "key", "measurement", "text"]


def test_semantic_registry_replace_unknown_name_raises_value_error():
    replacement = DQSemanticTypeDetector(name="x", detect=lambda _ctx: None)
    with pytest.raises(ValueError, match="'missing'"):
        SemanticRegistry.default().replace("missing", replacement)


def test_semantic_registry_replace_name_clash_with_other_entry_raises():
    # Renaming `enum` → `key` would collide with the existing `key` entry.
    replacement = DQSemanticTypeDetector(name="key", detect=lambda _ctx: None)
    with pytest.raises(ValidationError):
        SemanticRegistry.default().replace("enum", replacement)


def test_semantic_registry_remove_drops_named_entry():
    original = SemanticRegistry.default()
    new = original.remove("text")
    assert [d.name for d in new.detectors] == ["enum", "key", "measurement"]
    assert original.detectors == default_semantic_detectors()


def test_semantic_registry_remove_unknown_name_raises_value_error():
    with pytest.raises(ValueError, match="'missing'"):
        SemanticRegistry.default().remove("missing")


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
        def _detect(_ctx):
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
    assert isinstance(result.properties, MeasurementProperties)
    assert result.properties.distribution in {"constant", "uniform", "normal", "exponential", "unknown"}


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
    assert result.properties is None


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
    typed = result.get_typed_properties(EnumProperties)
    assert typed is not None
    assert typed.values == {"car", "truck", "van"}


def test_enum_detector_respects_distinct_ratio_option():
    """When `distinct_ratio` option is set below the observed ratio, semantic-enum is suppressed.

    Mirrors reviewer's 12-rows / 9-distinct example: legacy `is_in` deliberately suppresses
    at low repetition, so semantic profiling must too.
    """
    ctx = DQProfileContext(
        df=_mock_df_with_distinct("c", list("abcdefghi")),
        column_name="c",
        column_type=T.StringType(),
        metrics={"count_non_null": 12, "count_distinct": 9},
        options={"max_in_count": 10, "distinct_ratio": 0.05},
    )
    assert DEFAULT_ENUM_DETECTOR.detect(ctx) is None


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
    assert result.properties is None


def test_key_detector_string_variable_length_negative():
    """Free-form names, min 4 / max 40 → stability=0.1 well below threshold."""
    ctx = _string_key_ctx("user_name", count_non_null=50, count_distinct=50, min_len=4, max_len=40)
    assert DEFAULT_KEY_DETECTOR.detect(ctx) is None


def test_key_detector_string_empty_only_returns_none_without_error():
    """max_length == 0 must not trigger ZeroDivisionError; column is not classified as key."""
    ctx = _string_key_ctx("empty_col", count_non_null=5, count_distinct=5, min_len=0, max_len=0)
    assert DEFAULT_KEY_DETECTOR.detect(ctx) is None
