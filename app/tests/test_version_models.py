"""Unit tests for VersionOut.from_metadata() — no Spark, no workspace."""

from unittest.mock import patch

from databricks_labs_dqx_app.backend.models import VersionOut, _cached_core_version
from databricks_labs_dqx_app import __version__


def test_from_metadata_returns_version_out() -> None:
    """from_metadata() returns a VersionOut with the app __version__ and a non-empty core_version."""
    _cached_core_version.cache_clear()
    result = VersionOut.from_metadata()
    assert isinstance(result, VersionOut)
    assert result.version == __version__
    assert result.core_version  # non-empty


def test_core_version_cached() -> None:
    """The core version is resolved at most once regardless of how many times from_metadata() is called.

    We verify via lru_cache.cache_info: after clearing the cache and calling
    from_metadata() twice the miss count must be exactly 1 and the hit count
    must be exactly 1.
    """
    _cached_core_version.cache_clear()
    _ = VersionOut.from_metadata()
    _ = VersionOut.from_metadata()

    info = _cached_core_version.cache_info()
    assert info.misses == 1, f"Expected 1 miss, got {info.misses}"
    assert info.hits == 1, f"Expected 1 hit, got {info.hits}"


def test_core_version_fallback() -> None:
    """When the package is not installed, core_version falls back to 'unknown'."""
    _cached_core_version.cache_clear()
    try:
        import importlib.metadata as _im
        with patch.object(_im, "version", side_effect=Exception("not found")):
            result = VersionOut.from_metadata()
        assert result.core_version == "unknown"
    finally:
        _cached_core_version.cache_clear()
