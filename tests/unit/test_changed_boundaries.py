"""Exercise the CI change-detection script against a throwaway git repository."""

import json
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / ".github" / "scripts" / "changed-boundaries.sh"
ALL_BOUNDARIES = ["core", "studio", "anomaly", "mcp", "docs"]


def git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args],
        cwd=root,
        check=True,
        capture_output=True,
    )


def commit(root: Path, files: dict[str, str | None]) -> None:
    """Write (or, for None, delete) each file and commit the result."""
    for name, content in files.items():
        if content is None:
            git(root, "rm", "-q", name)
            continue
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        git(root, "add", name)
    git(root, "commit", "-q", "-m", "change")


def boundaries(root: Path, *args: str) -> list[str]:
    result = subprocess.run(["bash", str(SCRIPT), *args], cwd=root, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository with app, MCP server and docs files on main, checked out on a feature branch."""
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "user.name", "test")
    commit(tmp_path, {"app/moved.py": "x = 1\n", "mcp-server/server.py": "y = 1\n", "docs/index.md": "docs\n"})
    git(tmp_path, "checkout", "-q", "-b", "feature")
    return tmp_path


def test_three_dot_diff_ignores_changes_made_on_the_base_branch(repo: Path) -> None:
    commit(repo, {"app/new.py": "z = 1\n"})
    git(repo, "checkout", "-q", "main")
    commit(repo, {"docs/index.md": "newer docs\n"})
    git(repo, "checkout", "-q", "feature")
    assert boundaries(repo, "main") == ["studio"]


def test_deletion_counts_for_its_boundary(repo: Path) -> None:
    commit(repo, {"mcp-server/server.py": None})
    assert boundaries(repo, "main") == ["mcp"]


def test_rename_counts_for_both_boundaries(repo: Path) -> None:
    commit(repo, {"app/moved.py": None, "mcp-server/moved.py": "x = 1\n"})
    assert boundaries(repo, "main") == ["studio", "mcp"]


def test_unmatched_paths_affect_no_boundary(repo: Path) -> None:
    commit(repo, {"CHANGELOG.md": "notes\n"})
    assert not boundaries(repo, "main")


def test_all_selects_every_boundary(repo: Path) -> None:
    assert boundaries(repo, "--all") == ALL_BOUNDARIES
