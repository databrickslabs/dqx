"""Compatibility patches for test dependencies."""

from typing import Any
from collections.abc import Callable


def patch_numba_coverage_compat() -> None:
    """
    Apply compatibility patch for numba 0.63.x and coverage 7.4+.

    Numba 0.63.x expects attributes that were changed or removed in coverage 7.4+.
    This patch adds missing attributes to maintain compatibility between versions.
    """
    try:
        import coverage.types  # type: ignore[import-untyped]
    except ImportError:
        return  # Coverage not installed, no patching needed

    # Add missing Tracer class (was renamed to TracerCore)
    _patch_tracer_class(coverage.types)

    # Add missing type aliases that were removed in coverage 7.4
    _patch_type_alias(coverage.types, 'TShouldTraceFn', Callable[[Any, Any], Any | None])
    _patch_type_alias(coverage.types, 'TShouldStartContextFn', Callable[[Any], str | None])
    _patch_type_alias(coverage.types, 'TFileDisposition', Any)
    _patch_type_alias(coverage.types, 'TWarnFn', Callable[[str, str, int], None])


def _patch_tracer_class(coverage_types: Any) -> None:
    """Patch missing Tracer class by aliasing to TracerCore."""
    if not hasattr(coverage_types, 'Tracer'):
        try:
            coverage_types.Tracer = coverage_types.TracerCore  # type: ignore[attr-defined]
        except AttributeError:
            pass  # TracerCore doesn't exist either


def _patch_type_alias(coverage_types: Any, name: str, value: Any) -> None:
    """
    Helper to patch a single type alias.

    Args:
        coverage_types: The coverage.types module
        name: Name of the type alias to patch
        value: Value to assign to the type alias
    """
    if not hasattr(coverage_types, name):
        try:
            setattr(coverage_types, name, value)  # type: ignore[misc,assignment]
        except (ImportError, AttributeError):
            pass  # Couldn't set attribute, continue gracefully

