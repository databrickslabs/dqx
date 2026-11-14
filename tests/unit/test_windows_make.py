"""Exercise the Windows Make replacement when PowerShell is available."""

import shutil
import subprocess
from pathlib import Path

import pytest


def test_powershell_deployment_workflow() -> None:
    """The deployment entry point validates, builds, and stops on errors."""
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is not installed")

    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [powershell, "-NoProfile", "-File", str(root / "tests" / "powershell" / "test_make.ps1")],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
