"""
Compatibility patches for test dependencies.

This module applies patches on import - simply importing this module will apply
all necessary compatibility fixes. Import this module BEFORE any imports that
might trigger numba (e.g., shap, sklearn with numba optimizations).

Usage in conftest.py:
    import tests.compat  # noqa: F401  # Apply compatibility patches
"""

from typing import Any
from collections.abc import Callable


def _apply_coverage_patches() -> None:
    """Apply numba/coverage compatibility patches if coverage is installed."""
    try:
        import coverage.types as coverage_types  # type: ignore[import-untyped]
    except ImportError:
        return  # coverage not installed, no patching needed

    _patch_tracer(coverage_types)
    _patch_type_aliases(coverage_types)


def _patch_tracer(coverage_types: Any) -> None:
    """Add missing Tracer class (was renamed to TracerCore in coverage 7.4+)."""
    if not hasattr(coverage_types, 'Tracer') and hasattr(coverage_types, 'TracerCore'):
        coverage_types.Tracer = coverage_types.TracerCore  # type: ignore[attr-defined]


def _patch_type_aliases(coverage_types: Any) -> None:
    """Add missing type aliases that were removed in coverage 7.4."""
    # These are type aliases for coverage.py internal types - values are for runtime compatibility only
    aliases: list[tuple[str, Any]] = [
        ('TShouldTraceFn', Callable[[Any, Any], Any]),  # Can return Any or None
        ('TShouldStartContextFn', Callable[[Any], str | None]),
        ('TFileDisposition', Any),
        ('TWarnFn', Callable[[str, str, int], None]),
    ]
    for name, value in aliases:
        if not hasattr(coverage_types, name):
            setattr(coverage_types, name, value)  # type: ignore[misc,assignment]


# Apply patches immediately on module import
_apply_coverage_patches()
