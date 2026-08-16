import re

from databricks.labs import dqx
from databricks.labs.dqx.__version__ import __version__


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
