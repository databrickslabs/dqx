import pytest

from databricks.labs.dqx.profiler.profiler_column_metrics import (
    PROFILE_COLUMN_METRIC_REGISTRY,
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
