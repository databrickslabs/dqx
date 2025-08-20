from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _run(cmd: list[str], env: dict[str, str] | None = None, cwd: str | None = None) -> tuple[int, str]:
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=cwd)
    out, _ = proc.communicate()
    return proc.returncode, out


def test_cli_runs_from_fresh_cli_only_venv(tmp_path: Path):
    # Create a clean venv without test deps
    venv_dir = tmp_path / "cli_venv"
    python = sys.executable
    code, out = _run([python, "-m", "venv", str(venv_dir)])
    assert code == 0, out

    bin_dir = "Scripts" if os.name == "nt" else "bin"
    vpy = venv_dir / bin_dir / ("python.exe" if os.name == "nt" else "python")
    pip = venv_dir / bin_dir / ("pip.exe" if os.name == "nt" else "pip")

    # Upgrade pip tooling to ensure wheels are used when available
    code, out = _run([str(vpy), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
    assert code == 0, out

    # Install dqx with CLI extras only
    repo_root = Path(__file__).resolve().parents[2]
    code, out = _run([str(pip), "install", ".[cli]"], cwd=str(repo_root))
    assert code == 0, out

    # import cli module
    code, out = _run([str(vpy), "-c", "import databricks.labs.dqx.cli; print('cli ok')"])
    assert code == 0 and "cli ok" in out, out

    # Import DQEngine
    code, out = _run([str(vpy), "-c", "from databricks.labs.dqx.engine import DQEngine; print('engine ok')"])
    assert code == 0 and "engine ok" in out, out
