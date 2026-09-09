"""Guard the DQX version wiring in the Studio bundle (``app/databricks.yml``).

The task-runner job installs ``databricks-labs-dqx`` via a single ``dqx_version`` bundle variable:
the production pin and the development wheel filename both DERIVE from it. Two invariants must hold or
a deploy silently ships the wrong DQX:

* ``dqx_version`` must equal ``src/databricks/labs/dqx/__version__.py`` — docs/dqx/sync_versions.py keeps
  them in lockstep on ``make fmt``; this test fails if someone bumps ``__version__.py`` without it.
* production (the default ``dqx_task_dependency``) must stay PINNED to the published registry release
  ``databricks-labs-dqx==${var.dqx_version}`` — only a dev target overrides it to a local wheel.

Parsed textually rather than via the Databricks CLI / PyYAML so the check needs no extra tooling.
"""

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BUNDLE = _REPO_ROOT / "app" / "databricks.yml"
_VERSION_MODULE = _REPO_ROOT / "src" / "databricks" / "labs" / "dqx" / "__version__.py"


def _dqx_version_default() -> str:
    """The literal ``dqx_version`` variable default from the bundle."""
    text = _BUNDLE.read_text(encoding="utf-8")
    match = re.search(r"dqx_version:\s*\n\s*default:\s*\"(?P<v>\d+\.\d+\.\d+)\"", text)
    assert match, "dqx_version default not found in app/databricks.yml"
    return match.group("v")


def _library_version() -> str:
    text = _VERSION_MODULE.read_text(encoding="utf-8")
    match = re.search(r'__version__\s*=\s*"(?P<v>\d+\.\d+\.\d+)"', text)
    assert match, "__version__ not found in __version__.py"
    return match.group("v")


def test_dqx_version_tracks_the_library_version() -> None:
    """dqx_version must match __version__.py — run ``make fmt`` (sync_versions.py) if this fails."""
    assert _dqx_version_default() == _library_version()


def test_production_dqx_dependency_is_pinned_to_the_registry() -> None:
    """The default (production) dqx dependency must be the published registry pin derived from the var,
    never a wheel path — only a dev target overrides it to a local build."""
    text = _BUNDLE.read_text(encoding="utf-8")
    match = re.search(r"dqx_task_dependency:\s*\n\s*default:\s*\"(?P<dep>[^\"]+)\"", text)
    assert match, "dqx_task_dependency default not found in app/databricks.yml"
    assert match.group("dep") == "databricks-labs-dqx==${var.dqx_version}"


def test_dev_wheel_filename_derives_from_the_version() -> None:
    """The dev override installs this filename from the wheels volume; it must be exactly what
    ``uv build`` produces for ``dqx_version`` (and what the app's _find_wheels glob matches)."""
    text = _BUNDLE.read_text(encoding="utf-8")
    assert 'default: "databricks_labs_dqx-${var.dqx_version}-py3-none-any.whl"' in text


def test_bundle_artifact_build_uses_the_frozen_app_lock() -> None:
    """Bundle builds must not re-resolve dependencies against a workspace package proxy."""
    text = _BUNDLE.read_text(encoding="utf-8")
    assert "uv run --frozen python scripts/build_app.py" in text
