"""
LLM-assisted features for DQX.

This module provides LLM-based functionality for data quality analysis,
including primary key detection and other AI-assisted features.

Optional Dependencies:
    - dspy-ai: For LLM programming framework
    - databricks_langchain: For Databricks model serving integration

Install with: pip install dqx[llm] or pip install dspy-ai databricks_langchain
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Track dependency availability
_HAS_LLM_DEPS = None
_IMPORT_ERROR = None


def check_llm_dependencies() -> tuple[bool, Optional[str]]:
    """
    Check if LLM dependencies are available.

    Returns:
        A tuple of (has_dependencies, error_message)
    """
    global _HAS_LLM_DEPS, _IMPORT_ERROR

    if _HAS_LLM_DEPS is not None:
        return _HAS_LLM_DEPS, _IMPORT_ERROR

    try:
        import dspy  # noqa: F401 # type: ignore
        import databricks_langchain  # noqa: F401 # type: ignore

        _HAS_LLM_DEPS = True
        _IMPORT_ERROR = None
        logger.debug("LLM dependencies are available")
        return True, None
    except ImportError as e:
        _HAS_LLM_DEPS = False
        _IMPORT_ERROR = str(e)
        logger.debug(f"LLM dependencies not available: {e}")
        return False, str(e)


def require_llm_dependencies() -> None:
    """
    Raise ImportError if LLM dependencies are not available.

    Raises:
        ImportError: If LLM dependencies are not installed
    """
    has_deps, error = check_llm_dependencies()
    if not has_deps:
        raise ImportError(
            f"LLM-based features require additional dependencies. "
            f"Install with: pip install dqx[llm] or pip install dspy-ai databricks_langchain. "
            f"Error: {error}"
        )


def get_primary_key_detector():
    """
    Get the DatabricksPrimaryKeyDetector class if dependencies are available.

    Returns:
        The DatabricksPrimaryKeyDetector class

    Raises:
        ImportError: If LLM dependencies are not installed
    """
    require_llm_dependencies()
    from .pk_identifier import DatabricksPrimaryKeyDetector

    return DatabricksPrimaryKeyDetector


def is_llm_available() -> bool:
    """
    Check if LLM features are available.

    Returns:
        True if LLM dependencies are installed, False otherwise
    """
    has_deps, _ = check_llm_dependencies()
    return has_deps


def get_llm_unavailable_message() -> str:
    """
    Get a user-friendly message about LLM unavailability.

    Returns:
        A message explaining how to install LLM dependencies
    """
    _, error = check_llm_dependencies()
    return (
        f"LLM-based features require additional dependencies. "
        f"Install with: pip install dqx[llm] or pip install dspy-ai databricks_langchain. "
        f"Error: {error}"
    )


# Export the main classes and functions
__all__ = [
    'check_llm_dependencies',
    'require_llm_dependencies',
    'get_primary_key_detector',
    'is_llm_available',
    'get_llm_unavailable_message',
]
