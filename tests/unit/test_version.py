import re
import importlib
import importlib.util
from pathlib import Path

import pytest
from databricks.labs import dqx
from databricks.labs.dqx.__version__ import __version__

_OPTIONAL_SUBPACKAGES = {
    "databricks.labs.dqx.pii": "presidio_analyzer",
    "databricks.labs.dqx.llm": "dspy",
    "databricks.labs.dqx.anomaly": "mlflow",
}


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


def _missing_optional_dependency(module_name: str) -> str | None:
    for prefix, dependency in _OPTIONAL_SUBPACKAGES.items():
        if (module_name == prefix or module_name.startswith(f"{prefix}.")) and (
            importlib.util.find_spec(dependency) is None
        ):
            return dependency
    return None


@pytest.mark.parametrize("module_name", _all_module_names())
def test_module_imports_cleanly(module_name: str) -> None:
    missing = _missing_optional_dependency(module_name)
    if missing is not None:
        pytest.skip(f"optional dependency {missing!r} not installed")
    importlib.import_module(module_name)


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
