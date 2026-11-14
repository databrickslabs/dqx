"""Contract tests for deploying a prebuilt Studio release through DABs."""

from pathlib import Path

import yaml


_BUNDLE = Path(__file__).resolve().parents[1] / "databricks.yml"


def test_release_target_uses_prebuilt_marketplace_artifact() -> None:
    """A release deploy must not rebuild the frontend, backend, or runner wheel."""
    bundle = yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))
    release = bundle["targets"]["release"]

    assert release["variables"]["app_source_path"] == "marketplace"
    assert release["variables"]["task_runner_wheel_path"] == "./marketplace/tasks/databricks_labs_dqx_task_runner-*.whl"
    assert release["artifacts"]["default"]["build"] == "echo Using prebuilt DQX Studio release"
    assert bundle["resources"]["apps"]["dqx-studio"]["source_code_path"] == "${var.app_source_path}"
    dependencies = bundle["resources"]["jobs"]["dqx_task_runner"]["environments"][0]["spec"]["dependencies"]
    assert dependencies[0] == "${var.task_runner_wheel_path}"


def test_release_artifact_is_in_bundle_sync() -> None:
    """The release app source must be uploaded when the bundle deploys."""
    bundle = yaml.safe_load(_BUNDLE.read_text(encoding="utf-8"))

    assert bundle["sync"]["include"] == ["${var.app_source_path}"]
    assert bundle["targets"]["release"]["sync"]["exclude"] == [".build/**"]
