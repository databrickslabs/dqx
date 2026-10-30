"""Guard the DQX version wiring in the Studio bundle (``app/databricks.yml``).

Studio deploys one published DQX, the ``dqx_version`` bundle variable: the app installs it from the
committed lock and the task-runner job installs the ``databricks-labs-dqx==${var.dqx_version}`` pin, in
DAB and Marketplace deploys alike. Two invariants must hold or a deploy silently ships the wrong DQX:

* ``dqx_version`` is a published DQX release that Studio moves to deliberately, in a Studio release and
  together with its Marketplace lock (``make fmt`` does not bump it). It can never be ahead of
  ``src/databricks/labs/dqx/__version__.py``; the Marketplace lock is checked against it in
  ``test_build_marketplace.py``.
* the task runner's ``dqx_task_dependency`` must stay PINNED to the published registry release
  ``databricks-labs-dqx==${var.dqx_version}``.

Parsed textually rather than via the Databricks CLI / PyYAML so the check needs no extra tooling.
"""

import re
from pathlib import Path

import yaml
from packaging.version import Version

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


def test_dqx_version_is_not_ahead_of_the_library_version() -> None:
    """Studio can only pin a DQX release that exists, never one newer than this checkout's DQX."""
    assert Version(_dqx_version_default()) <= Version(_library_version())


def test_task_runner_dqx_dependency_is_pinned_to_the_registry() -> None:
    """The job's initial dqx dependency must be the published registry pin derived from the var.

    It only applies until the app's first start: from then on the app republishes the task-runner
    wheel as the job's only dependency, and that wheel carries the same pin (see the next test).
    """
    text = _BUNDLE.read_text(encoding="utf-8")
    match = re.search(r"dqx_task_dependency:\s*\n\s*default:\s*\"(?P<dep>[^\"]+)\"", text)
    assert match, "dqx_task_dependency default not found in app/databricks.yml"
    assert match.group("dep") == "databricks-labs-dqx==${var.dqx_version}"


def test_bundle_artifact_build_uses_the_frozen_app_lock() -> None:
    """Bundle builds must not re-resolve dependencies against a workspace package proxy."""
    text = _BUNDLE.read_text(encoding="utf-8")
    assert "uv run --frozen python scripts/build_app.py" in text


def test_bundle_managed_warehouse_stops_after_ten_idle_minutes() -> None:
    """The Studio warehouse must release idle compute promptly."""
    bundle = yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))

    warehouse = bundle["resources"]["sql_warehouses"]["dqx_sql_warehouse"]
    assert warehouse["auto_stop_mins"] == 10


def test_bundle_builds_the_task_runner_wheel_only_through_the_pinned_builder() -> None:
    """build_app.py builds the runner wheel with the DQX pin baked in; a separate plain
    ``uv build tasks/`` would publish a runner wheel with DQX unpinned."""
    bundle = yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))
    assert bundle["artifacts"]["default"]["build"].strip() == "uv run --frozen python scripts/build_app.py"


def test_bundle_app_installs_exactly_the_committed_lock() -> None:
    """Like Marketplace, a DAB deploy must install the committed lock and never re-resolve at start."""
    bundle = yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))
    command = bundle["variables"]["app_config"]["default"]["command"]
    assert command[:3] == ["uv", "run", "--frozen"]
