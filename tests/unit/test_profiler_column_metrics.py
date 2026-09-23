import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.profiler.profiler_column_metrics import (
    PROFILE_COLUMN_METRIC_REGISTRY,
    RESERVED_PROFILE_COLUMN_METRIC_KEYS,
    build_registered_metric_aggregations,
    deregister_profile_column_metric,
    register_profile_column_metric,
)


@pytest.fixture
def restore_profile_column_metric_registry():
    """
    Snapshot PROFILE_COLUMN_METRIC_REGISTRY before the test and restore it after,
    so tests can freely add/overwrite entries without leaking into other tests.
    """
    original_registry = dict(PROFILE_COLUMN_METRIC_REGISTRY)
    try:
        yield
    finally:
        PROFILE_COLUMN_METRIC_REGISTRY.clear()
        PROFILE_COLUMN_METRIC_REGISTRY.update(original_registry)


def test_register_profile_column_metric_registers_under_explicit_type(restore_profile_column_metric_registry):
    @register_profile_column_metric("custom_metric_key")
    def _test_metric(_field, _column_label):
        return None

    assert "custom_metric_key" in PROFILE_COLUMN_METRIC_REGISTRY
    assert PROFILE_COLUMN_METRIC_REGISTRY["custom_metric_key"] is _test_metric


def test_register_profile_column_metric_overwrites_and_warns(caplog, restore_profile_column_metric_registry):
    # Re-registering a non-reserved key replaces the previous function (last-value-wins) and logs a
    # warning so an accidental shadowing is visible rather than silent.
    @register_profile_column_metric("custom_metric_key")
    def _first(_field, _column_label):
        return None

    with caplog.at_level("WARNING"):

        @register_profile_column_metric("custom_metric_key")
        def _second(_field, _column_label):
            return None

    assert PROFILE_COLUMN_METRIC_REGISTRY["custom_metric_key"] is _second
    assert "custom_metric_key" in caplog.text


def test_deregister_profile_column_metric_removes_registered_metric(restore_profile_column_metric_registry):
    @register_profile_column_metric("custom_metric_key")
    def _test_metric(_field, _column_label):
        return None

    assert "custom_metric_key" in PROFILE_COLUMN_METRIC_REGISTRY
    deregister_profile_column_metric("custom_metric_key")
    assert "custom_metric_key" not in PROFILE_COLUMN_METRIC_REGISTRY


def test_deregister_profile_column_metric_missing_key_is_noop(restore_profile_column_metric_registry):
    # Deregistering an unregistered key must not raise, so callers can use it unconditionally in cleanup.
    deregister_profile_column_metric("never_registered_key")
    assert "never_registered_key" not in PROFILE_COLUMN_METRIC_REGISTRY


@pytest.mark.parametrize("reserved_key", sorted(RESERVED_PROFILE_COLUMN_METRIC_KEYS))
def test_register_profile_column_metric_rejects_reserved_key(reserved_key, restore_profile_column_metric_registry):
    snapshot = dict(PROFILE_COLUMN_METRIC_REGISTRY)

    with pytest.raises(InvalidParameterError):

        @register_profile_column_metric(reserved_key)
        def _shadow_metric(_field, _column_label):
            return None

    # Registry must be left unmodified.
    assert PROFILE_COLUMN_METRIC_REGISTRY == snapshot


@pytest.mark.parametrize("reserved_key", sorted(RESERVED_PROFILE_COLUMN_METRIC_KEYS))
def test_deregister_profile_column_metric_rejects_reserved_key(reserved_key, restore_profile_column_metric_registry):
    snapshot = dict(PROFILE_COLUMN_METRIC_REGISTRY)

    with pytest.raises(InvalidParameterError):
        deregister_profile_column_metric(reserved_key)

    assert PROFILE_COLUMN_METRIC_REGISTRY == snapshot


@pytest.mark.parametrize("reserved_key", sorted(RESERVED_PROFILE_COLUMN_METRIC_KEYS))
def test_build_registered_metric_aggregations_skips_reserved_key_collision(
    reserved_key, restore_profile_column_metric_registry
):
    # Even when a colliding entry is injected directly into the registry (bypassing the
    # register_profile_column_metric guard), the reserved key must be excluded from the
    # aggregation list so it cannot shadow the inline count_non_null alias via Row.asDict().
    PROFILE_COLUMN_METRIC_REGISTRY.clear()

    def _shadow(_field, _column_label):
        return F.lit(None).cast(T.LongType())

    PROFILE_COLUMN_METRIC_REGISTRY[reserved_key] = _shadow

    aggregations = build_registered_metric_aggregations(T.StructField("amount", T.IntegerType()), "amount")
    assert not aggregations


def test_build_registered_metric_aggregations_skips_metrics_returning_none(restore_profile_column_metric_registry):
    # Metric functions may return None to opt out for a given field type; those entries must
    # not appear in the aggregation list.
    PROFILE_COLUMN_METRIC_REGISTRY.clear()

    @register_profile_column_metric("always_none")
    def _always_none(_field, _column_label):
        return None

    aggregations = build_registered_metric_aggregations(T.StructField("amount", T.IntegerType()), "amount")
    assert not aggregations
