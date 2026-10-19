"""Check that the Make deploy path uses prebuilt Studio release artifacts."""

import subprocess
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _dry_run(target: str) -> str:
    result = subprocess.run(
        [
            "make",
            "-n",
            "app-deploy",
            "PROFILE=test-profile",
            f"TARGET={target}",
            "BUNDLE_VARS=--var catalog_name=test_catalog --var dqx_service_principal_application_id=test-sp",
        ],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def test_release_deploy_skips_local_build() -> None:
    """Tagged releases must deploy and run without the local app build."""
    commands = _dry_run("release")

    assert "python scripts/build_app.py" not in commands
    assert "databricks bundle deploy -p test-profile -t release" in commands
    assert "databricks bundle run dqx-studio -p test-profile -t release" in commands


def test_source_deploy_still_builds_locally() -> None:
    """Source targets must retain their existing build prerequisite."""
    commands = _dry_run("dev")

    assert "python scripts/build_app.py" in commands
