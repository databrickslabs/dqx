import re
import importlib
from pathlib import Path

import pytest
from databricks.labs import dqx
from databricks.labs.dqx.__version__ import __version__

# Subpackages whose modules import optional dependencies at import time (see AGENTS.md, Critical
# Rule 2): pii -> presidio + spaCy, llm -> dspy, anomaly -> mlflow + scikit-learn + pandas/numpy.
# A module in one of these trees is skipped only when the *missing* import is a third-party package;
# a missing internal dqx module still fails. In CI (`make dev` installs all extras) every module
# imports and should be fully exercised.
_OPTIONAL_SUBPACKAGE_PREFIXES = (
    "databricks.labs.dqx.pii",
    "databricks.labs.dqx.llm",
    "databricks.labs.dqx.anomaly",
)


def _all_module_names() -> list[str]:
    """Discover module names from the filesystem, without importing anything."""
    root = Path(dqx.__file__).parent
    names: list[str] = []
    for path in root.rglob("*.py"):
        parts = list(path.relative_to(root).with_suffix("").parts)
        if parts[-1] == "__main__":  # CLI entry point; excluded from coverage anyway
            continue
        if parts[-1] == "__init__":
            parts.pop()
        names.append(".".join([dqx.__name__, *parts]))
    return sorted(set(names))


def _is_optional_subpackage(module_name: str) -> bool:
    return any(
        module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in _OPTIONAL_SUBPACKAGE_PREFIXES
    )


@pytest.mark.parametrize("module_name", _all_module_names())
def test_module_imports_cleanly(module_name: str) -> None:
    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        # A missing third-party dependency of an optional subpackage is expected in a minimal
        # environment and is skipped. A missing internal module should fail.
        missing = str(exc.name) or ""
        if _is_optional_subpackage(module_name) and not missing.startswith(dqx.__name__):
            pytest.skip(f"optional dependency not installed: {missing}")
        raise


def test_version_import():
    """Verify that __version__ can be imported directly and is a non-empty valid semver string."""
    assert isinstance(__version__, str)
    assert len(__version__) > 0
    # Verify semantic versioning format: X.Y.Z (with optional suffix)
    assert re.match(r"^\d+\.\d+\.\d+", __version__) is not None


def test_package_exposes_version():
    """Verify that databricks.labs.dqx exposes __version__ matching __version__.py."""
    assert hasattr(dqx, "__version__")
    assert dqx.__version__ == __version__
